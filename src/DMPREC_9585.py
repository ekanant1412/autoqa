import requests
import json
from datetime import datetime
import os
import re

# ===================== CONFIG =====================
URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7"
    "?shelfId=BJq5rZqYzjgJ"
    "&total_candidates=200"
    "&pool_limit_category_items=50"
    "&language=th"
    "&limit=100"
    "&userId=null"
    "&pseudoId=null"
    "&cursor=1"
    "&ga_id=100118391.0851155978"
    "&ssoId=22092422"
    "&is_use_live=true"
    "&verbose=debug"
)

TIMEOUT_SEC = 20

TEST_KEY = "DMPREC-9585"

REPORT_DIR = "reports"
LOG_TXT = f"{REPORT_DIR}/{TEST_KEY}_live_check.log"
RESULT_JSON = f"{REPORT_DIR}/{TEST_KEY}_live_check_result.json"

os.makedirs(REPORT_DIR, exist_ok=True)


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


# =================================================
def normalize_activity_id(x):
    s = str(x).strip()
    if not s:
        return s

    if s.isdigit():
        return s

    m = re.search(r"(\d{10,})", s)
    if m:
        return m.group(1)

    return s


# =================================================
def extract_ids_from_items(items):
    ids = []
    if not isinstance(items, list):
        return ids

    for it in items:
        if isinstance(it, str):
            ids.append(normalize_activity_id(it))
            continue

        if isinstance(it, dict):
            for k in ("ActivityId", "activityId", "activity_id", "Id", "id"):
                v = it.get(k)
                if v is not None:
                    ids.append(normalize_activity_id(v))
                    break
    return ids


def get_results_root(j: dict):
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_merge_page_ids(results: dict):
    node = results.get("merge_page", {})
    result = node.get("result", {})
    items = result.get("items", [])

    out = []

    for it in items:
        if isinstance(it, str) and it.strip():
            out.append(it.strip())
            continue

        if not isinstance(it, dict):
            continue

        payload = it.get("payload", {})
        content_type = payload.get("content_type") if isinstance(payload, dict) else None

        # LIVE wrapper
        if content_type == "live":
            inner = it.get("items", [])
            if inner and isinstance(inner[0], dict):
                act = inner[0].get("ActivityId") or inner[0].get("activityId")
                if act:
                    out.append(str(act))
                    continue

        if it.get("id"):
            out.append(str(it["id"]))
        elif it.get("Id"):
            out.append(str(it["Id"]))

    return out


def extract_live_activity_ids(results: dict):
    node = results.get("get_all_live_today", {})
    result = node.get("result", {})
    items = result.get("items", [])
    return extract_ids_from_items(items)


# =================================================
def check_live_positions_until_exhausted(merge_ids, live_ids, n=3):

    live_set = {normalize_activity_id(x) for x in live_ids}

    failures = []
    seen_any_live = False

    for start in range(0, len(merge_ids), n):

        block = merge_ids[start:start + n]
        block_index = start // n + 1

        found_live = any(
            normalize_activity_id(mid) in live_set
            for mid in block
        )

        if found_live:
            seen_any_live = True
            continue

        if not seen_any_live:
            failures.append({
                "block": block_index,
                "items": block
            })
            return failures, "FAIL: live never inserted"

        return [], "live exhausted"

    return [], "PASS"


# =================================================
def run_check():

    open(LOG_TXT, "w").close()

    tlog(f"TEST={TEST_KEY}")
    tlog(f"URL={URL}")

    r = requests.get(URL, timeout=TIMEOUT_SEC)
    tlog(f"HTTP={r.status_code}")
    r.raise_for_status()

    j = r.json()
    results = get_results_root(j)

    merge_ids = extract_merge_page_ids(results)
    live_ids = extract_live_activity_ids(results)

    tlog(f"merge_count={len(merge_ids)}")
    tlog(f"live_count={len(live_ids)}")

    if not live_ids:
        result = {"status": "PASS", "reason": "no live today"}
    else:
        failures, reason = check_live_positions_until_exhausted(
            merge_ids, live_ids, n=3
        )

        result = {
            "status": "FAIL" if failures else "PASS",
            "failures": failures,
            "reason": reason,
        }

    with open(RESULT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    if result["status"] == "FAIL":
        raise AssertionError(f"{TEST_KEY} FAILED: {result}")

    return result


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_DMPREC_9585():
    print("RUN:", TEST_KEY)
    result = run_check()
    print("RESULT:", result)


# =================================================
if __name__ == "__main__":
    run_check()
