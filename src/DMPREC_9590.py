import requests
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9590"

URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7"
    "?shelfId=BJq5rZqYzjgJ"
    "&total_candidates=300"
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
EXPECTED_ITEMS_SIZE = 200

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

LOG_TXT = f"{ART_DIR}/tc_final_result_default_items.log"
OUT_JSON = f"{ART_DIR}/tc_final_result_default_items.json"
OUT_FULL_RESPONSE = f"{ART_DIR}/tc_final_result_default_full_response.json"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def dump_json(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def get_results_root(j: dict) -> dict:
    data = j.get("data", {}) if isinstance(j.get("data", {}), dict) else {}
    results = data.get("results", {}) if isinstance(data.get("results", {}), dict) else {}
    return results


def extract_ids_from_items(items: Any) -> List[str]:
    """
    รองรับ:
    - list[str]
    - list[{"id": "..."}]
    """
    out: List[str] = []
    if not isinstance(items, list):
        return out

    for it in items:
        if isinstance(it, str) and it.strip():
            out.append(it.strip())
            continue
        if isinstance(it, dict):
            _id = it.get("id")
            if isinstance(_id, str) and _id.strip():
                out.append(_id.strip())
    return out


def extract_final_result(results: dict) -> Tuple[Optional[List[str]], Optional[List[str]], Optional[int], Optional[str]]:
    node = results.get("final_result", {})
    if not isinstance(node, dict):
        return None, None, None, "final_result node missing/not dict"

    res = node.get("result", {})
    if not isinstance(res, dict):
        return None, None, None, "final_result.result missing/not dict"

    ids = res.get("ids")
    ids_list = [x for x in ids if isinstance(x, str) and x.strip()] if isinstance(ids, list) else None

    items = res.get("items")
    items_list = extract_ids_from_items(items) if isinstance(items, list) else None

    items_size = res.get("items_size")
    items_size_val = items_size if isinstance(items_size, int) else None

    return ids_list, items_list, items_size_val, None


# =================================================
def run_check() -> Dict[str, Any]:
    # reset log
    open(LOG_TXT, "w", encoding="utf-8").close()

    tlog(f"TEST={TEST_KEY}")
    tlog(f"CWD={os.getcwd()}")
    tlog(f"URL={URL}")
    tlog(f"EXPECTED_ITEMS_SIZE={EXPECTED_ITEMS_SIZE}")

    r = requests.get(URL, timeout=TIMEOUT_SEC)
    tlog(f"HTTP={r.status_code}")
    r.raise_for_status()

    j = r.json()
    dump_json(OUT_FULL_RESPONSE, j)
    tlog(f"Saved full response: {OUT_FULL_RESPONSE}")

    results = get_results_root(j)

    ids_list, items_list, items_size_val, err = extract_final_result(results)
    if err:
        dump_json(OUT_JSON, {"test_key": TEST_KEY, "url": URL, "status": "FAIL", "error": err})
        raise AssertionError(f"{TEST_KEY} FAIL: {err}")

    problems: List[str] = []

    ids_count = len(ids_list) if isinstance(ids_list, list) else None
    items_count = len(items_list) if isinstance(items_list, list) else None

    tlog("=== Extracted ===")
    tlog(f"final_result.result.ids_count={ids_count}")
    tlog(f"final_result.result.items_count={items_count}")
    tlog(f"final_result.result.items_size={items_size_val}")

    # Must exist
    if ids_list is None:
        problems.append("final_result.result.ids missing/not list")
    if items_list is None:
        problems.append("final_result.result.items missing/not list")
    if items_size_val is None:
        problems.append("final_result.result.items_size missing/not int")

    # Must equal EXPECTED_ITEMS_SIZE
    if ids_count is not None and ids_count != EXPECTED_ITEMS_SIZE:
        problems.append(f"ids_count != {EXPECTED_ITEMS_SIZE} (got {ids_count})")
    if items_count is not None and items_count != EXPECTED_ITEMS_SIZE:
        problems.append(f"items_count != {EXPECTED_ITEMS_SIZE} (got {items_count})")
    if items_size_val is not None and items_size_val != EXPECTED_ITEMS_SIZE:
        problems.append(f"items_size != {EXPECTED_ITEMS_SIZE} (got {items_size_val})")

    status = "FAIL" if problems else "PASS"

    out = {
        "test_key": TEST_KEY,
        "url": URL,
        "expected_items_size": EXPECTED_ITEMS_SIZE,
        "final_result": {
            "ids_count": ids_count,
            "items_count": items_count,
            "items_size": items_size_val,
            "ids_sample_10": ids_list[:10] if isinstance(ids_list, list) else None,
            "items_sample_10": items_list[:10] if isinstance(items_list, list) else None,
        },
        "problems": problems,
        "status": status,
    }
    dump_json(OUT_JSON, out)

    tlog(f"Saved JSON: {OUT_JSON}")
    tlog(f"Saved LOG : {LOG_TXT}")

    if problems:
        raise AssertionError(f"{TEST_KEY} FAIL: {problems}")

    return out


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_DMPREC_9590():
    result = run_check()
    print("RESULT:", result["status"])


if __name__ == "__main__":
    run_check()
