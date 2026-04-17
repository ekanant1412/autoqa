#!/usr/bin/env python3
"""
attach_evidence.py
Attach per-test evidence JSON files to the corresponding Xray test runs.
Handles both plain and parametrized tests.
"""

import os
import sys
import re
import json
import glob
import requests

XRAY_BASE     = "https://xray.cloud.getxray.app/api/v2"
CLIENT_ID     = os.environ["XRAY_CLIENT_ID"]
CLIENT_SECRET = os.environ["XRAY_CLIENT_SECRET"]
TEST_EXEC_KEY = os.environ.get("XRAY_TEST_EXEC_KEY", "DMPREC-15999")
EVIDENCE_DIR  = "reports/evidence"


# ─── Auth ────────────────────────────────────────────────────────────────────

def get_token() -> str:
    r = requests.post(
        f"{XRAY_BASE}/authenticate",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


# ─── Xray ────────────────────────────────────────────────────────────────────

def get_test_runs(token: str) -> list:
    hdrs = {"Authorization": f"Bearer {token}"}
    runs, page = [], 1
    while True:
        r = requests.get(
            f"{XRAY_BASE}/testexec/{TEST_EXEC_KEY}/test",
            headers=hdrs,
            params={"limit": 100, "page": page},
            timeout=15,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        runs.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return runs


def attach_file(token: str, run_id: str, filepath: str) -> tuple:
    url  = f"{XRAY_BASE}/testrun/{run_id}/evidence"
    hdrs = {"Authorization": f"Bearer {token}"}
    with open(filepath, "rb") as f:
        r = requests.post(
            url, headers=hdrs,
            files={"file": (os.path.basename(filepath), f, "application/json")},
            timeout=15,
        )
    return r.status_code, r.text


# ─── Matching helpers ─────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Lowercase + strip non-alphanumeric"""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def extract_func_and_param(nodeid_or_summary: str) -> tuple[str, str]:
    """
    Returns (func_name, param)  e.g.
      "test_tc01[top_results]" → ("test_tc01", "top_results")
      "test_tc01"              → ("test_tc01", "")
    """
    # take last segment after "::"
    part = nodeid_or_summary.split("::")[-1].strip()
    m = re.match(r"^([^\[]+)\[(.+)\]$", part)
    if m:
        return m.group(1), m.group(2)
    return part, ""


def build_evidence_map(ev_dir: str) -> dict:
    """
    Build {normalize(full_name): filepath}
    full_name = func_name[param]  or  func_name  (no param)
    Keeps ALL parametrized variants (no overwriting).
    """
    ev_map: dict[str, str] = {}
    for fp in glob.glob(f"{ev_dir}/*.json"):
        try:
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
            nodeid = data.get("test_nodeid", "") or os.path.basename(fp)
        except Exception:
            nodeid = os.path.basename(fp)

        func, param = extract_func_and_param(nodeid)

        # key 1: full name with param  e.g. "test_tc01[top_results]"
        full = f"{func}[{param}]" if param else func
        ev_map[normalize(full)] = fp

        # key 2: func name only (fallback for tests without param)
        ev_map.setdefault(normalize(func), fp)

    return ev_map


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ev_files = glob.glob(f"{EVIDENCE_DIR}/*.json")
    if not ev_files:
        print("No evidence files found in reports/evidence/ — skipping attach step")
        sys.exit(0)

    ev_map = build_evidence_map(EVIDENCE_DIR)
    print(f"Evidence files loaded : {len(ev_files)} files, {len(ev_map)} keys")

    print("Authenticating with Xray...")
    token = get_token()
    print("Authentication successful")

    runs = get_test_runs(token)
    print(f"Test runs in {TEST_EXEC_KEY} : {len(runs)}")

    if runs:
        sample = runs[0]
        print(f"[DEBUG] sample run keys : {list(sample.keys())}")
        print(f"[DEBUG] sample run      : {json.dumps(sample, ensure_ascii=False)[:400]}")

    attached = skipped = failed = 0

    for run in runs:
        run_id   = str(run.get("id", ""))
        test_obj = run.get("test") or {}
        summary  = (
            test_obj.get("summary")
            or test_obj.get("name")
            or run.get("summary")
            or run.get("name")
            or ""
        )

        func, param = extract_func_and_param(summary)
        full = f"{func}[{param}]" if param else func

        # try full name first, then func only
        ev_file = ev_map.get(normalize(full)) or ev_map.get(normalize(func))

        if not ev_file:
            print(f"  – no match : '{summary}'")
            skipped += 1
            continue

        status, body = attach_file(token, run_id, ev_file)
        if status in (200, 201):
            print(f"  ✓ {summary} → {os.path.basename(ev_file)}")
            attached += 1
        else:
            print(f"  ✗ {summary} | HTTP {status} | {body[:200]}")
            failed += 1

    print(f"\nResult — attached: {attached} | skipped: {skipped} | failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
