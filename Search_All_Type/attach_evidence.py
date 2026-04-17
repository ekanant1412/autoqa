#!/usr/bin/env python3
"""
attach_evidence.py
Match evidence JSON files (reports/evidence/) to Xray test runs
in TEST_EXEC_KEY and attach them via the Xray Cloud REST API.
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

def get_token():
    resp = requests.post(
        f"{XRAY_BASE}/authenticate",
        json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ─── Xray helpers ────────────────────────────────────────────────────────────

def get_test_runs(token: str) -> list:
    """Page through all test runs in the execution."""
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


def attach_file(token: str, run_id: str, filepath: str):
    url  = f"{XRAY_BASE}/testrun/{run_id}/evidence"
    hdrs = {"Authorization": f"Bearer {token}"}
    with open(filepath, "rb") as f:
        resp = requests.post(
            url, headers=hdrs,
            files={"file": (os.path.basename(filepath), f, "application/json")},
            timeout=15,
        )
    return resp.status_code, resp.text


# ─── Matching ─────────────────────────────────────────────────────────────────

def normalize(s: str) -> str:
    """Lowercase + strip non-alphanumeric for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def load_evidence_map() -> dict:
    """
    Returns {normalized_func_name: filepath}
    Evidence filename pattern:
      Search_All_Type_Verify_search_711.py__ClassName__test_funcname[param].json
    """
    ev_map = {}
    for fp in glob.glob(f"{EVIDENCE_DIR}/*.json"):
        try:
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
            nodeid = data.get("test_nodeid", "") or os.path.basename(fp)
        except Exception:
            nodeid = os.path.basename(fp)

        # extract function name (last segment after "::", strip parametrize bracket)
        func = nodeid.split("::")[-1].split("[")[0]
        ev_map[normalize(func)] = fp
    return ev_map


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ev_files = glob.glob(f"{EVIDENCE_DIR}/*.json")
    if not ev_files:
        print("No evidence files found — skipping attach step")
        sys.exit(0)

    ev_map = load_evidence_map()
    print(f"Evidence files loaded : {len(ev_map)}")

    print("Authenticating with Xray...")
    token = get_token()
    print("Authentication successful")

    runs = get_test_runs(token)
    print(f"Test runs in {TEST_EXEC_KEY}: {len(runs)}")

    attached = skipped = failed = 0

    for run in runs:
        run_id  = str(run.get("id", ""))
        summary = (
            run.get("summary")
            or run.get("test", {}).get("summary", "")
            or run.get("testKey", "")
        )
        # extract function name from summary (handles "ClassName::func_name" or just "func_name")
        func = summary.split("::")[-1].split("[")[0]
        key  = normalize(func)

        ev_file = ev_map.get(key)
        if not ev_file:
            print(f"  – no evidence match : {summary}")
            skipped += 1
            continue

        status, body = attach_file(token, run_id, ev_file)
        if status in (200, 201):
            print(f"  ✓ attached : {summary} → {os.path.basename(ev_file)}")
            attached += 1
        else:
            print(f"  ✗ failed   : {summary} | HTTP {status} | {body[:200]}")
            failed += 1

    print(f"\nResult — attached: {attached} | skipped: {skipped} | failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
