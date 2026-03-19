"""
Autocomplete API – Automated Tests
Endpoint: /api/v1/universal/ext_711_mlp_autocomplete
"""

import json
import os
from datetime import datetime

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-autocomplete"

BASE_URL = (
    "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/ext_711_mlp_autocomplete"
)

TIMEOUT_SEC = 15
MAX_RESULT_LIMIT = 10
MIN_TRIGGER_LENGTH = 1        # autocomplete เริ่มทำงานตั้งแต่ 1 ตัวอักษร

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

LOG_TXT = f"{ART_DIR}/autocomplete_check.log"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def call_autocomplete(query: str) -> dict:
    """Call autocomplete API and return parsed JSON."""
    resp = requests.get(BASE_URL, params={"query": query}, timeout=TIMEOUT_SEC)
    tlog(f"  HTTP={resp.status_code}  query={repr(query)}")
    tlog(f"  URL={resp.url}")
    resp.raise_for_status()
    return resp.json()


def save_result(tc_name: str, data: dict):
    """บันทึก response JSON ลง artifact directory"""
    safe = tc_name.replace(" ", "_").replace("/", "_")
    path = f"{ART_DIR}/{safe}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tlog(f"  Saved: {path}")


# =================================================
# TC-01  Verify autocomplete returns suggestions for valid keyword
# =================================================
def run_tc01():
    """TC-01: valid keyword ต้องได้ items ไม่ว่าง"""
    tc = "TC01_valid_keyword"
    tlog(f"\n[{tc}] START")

    data = call_autocomplete("ไก่")
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) > 0, f"Expected non-empty items for valid keyword 'ไก่', got {items}"

    tlog(f"  ✅ PASS  items_count={len(items)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-02  Verify autocomplete returns empty result for non-matching keyword
# =================================================
def run_tc02():
    """TC-02: keyword ที่ไม่มีในระบบต้องได้ items=[]"""
    tc = "TC02_non_matching_keyword"
    tlog(f"\n[{tc}] START")

    keyword = "xyzxyz_no_match_12345"
    data = call_autocomplete(keyword)
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) == 0, (
        f"Expected empty items for non-matching keyword '{keyword}', got {items}"
    )

    tlog(f"  ✅ PASS  items_count={len(items)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-03  Verify autocomplete works with partial keyword
# =================================================
def run_tc03():
    """TC-03: partial keyword (บางส่วน) ต้องได้ผลลัพธ์"""
    tc = "TC03_partial_keyword"
    tlog(f"\n[{tc}] START")

    data = call_autocomplete("ไก")          # บางส่วนของ "ไก่"
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) > 0, (
        "Autocomplete should return results for partial keyword 'ไก'"
    )

    tlog(f"  ✅ PASS  items_count={len(items)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-04  Verify autocomplete works with full keyword
# =================================================
def run_tc04():
    """TC-04: full keyword ต้องได้ผลลัพธ์และมี id ที่มีคำนั้นด้วย"""
    tc = "TC04_full_keyword"
    tlog(f"\n[{tc}] START")

    keyword = "ไก่"
    data = call_autocomplete(keyword)
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) > 0, f"Expected results for full keyword '{keyword}'"

    ids = [item["id"] for item in items]
    assert any(keyword in item_id for item_id in ids), (
        f"At least one suggestion should contain the keyword '{keyword}'. Got: {ids}"
    )

    tlog(f"  ✅ PASS  items_count={len(items)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-05  Verify autocomplete triggers at the correct input length (1 char)
# =================================================
def run_tc05():
    """TC-05: query 1 ตัวอักษรต้องได้ผลลัพธ์ (trigger ที่ MIN_TRIGGER_LENGTH)"""
    tc = "TC05_trigger_at_min_length"
    tlog(f"\n[{tc}] START  MIN_TRIGGER_LENGTH={MIN_TRIGGER_LENGTH}")

    single_char = "ไ"
    assert len(single_char) == MIN_TRIGGER_LENGTH, (
        f"Test setup error: single_char length should be {MIN_TRIGGER_LENGTH}"
    )

    data = call_autocomplete(single_char)
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) > 0, (
        f"Autocomplete must trigger and return results for a {MIN_TRIGGER_LENGTH}-char query '{single_char}'"
    )

    tlog(f"  ✅ PASS  items_count={len(items)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-06  Verify autocomplete result limit (max 10 items)
# =================================================
def run_tc06():
    """TC-06: ผลลัพธ์ต้องไม่เกิน MAX_RESULT_LIMIT รายการ"""
    tc = "TC06_result_limit"
    tlog(f"\n[{tc}] START  MAX_RESULT_LIMIT={MAX_RESULT_LIMIT}")

    data = call_autocomplete("ไก่")
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"
    assert len(items) <= MAX_RESULT_LIMIT, (
        f"Expected at most {MAX_RESULT_LIMIT} items, but got {len(items)}"
    )

    tlog(f"  ✅ PASS  items_count={len(items)} (limit={MAX_RESULT_LIMIT})")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# TC-07  Verify autocomplete does not return duplicate suggestions
# =================================================
def run_tc07():
    """TC-07: ผลลัพธ์ต้องไม่มี id ซ้ำกัน"""
    tc = "TC07_no_duplicate_suggestions"
    tlog(f"\n[{tc}] START")

    data = call_autocomplete("ไก่")
    save_result(tc, data)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    items = data.get("items", [])
    assert isinstance(items, list), "items must be a list"

    ids = [item["id"] for item in items]
    unique_ids = list(dict.fromkeys(ids))

    duplicates = [id_ for id_ in ids if ids.count(id_) > 1]
    assert ids == unique_ids, (
        f"Duplicate suggestions found: {list(set(duplicates))}"
    )

    tlog(f"  ✅ PASS  items_count={len(items)}  unique={len(unique_ids)}")
    return {"tc": tc, "status": "PASS", "items_count": len(items)}


# =================================================
# ✅ PYTEST ENTRIES (Xray mapping)
# =================================================
def test_autocomplete_returns_suggestions_for_valid_keyword():
    """TC-01: Verify autocomplete returns suggestions for valid keyword"""
    open(LOG_TXT, "a", encoding="utf-8").close()
    tlog(f"TEST={TEST_KEY}  BASE_URL={BASE_URL}")
    result = run_tc01()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_returns_empty_for_non_matching_keyword():
    """TC-02: Verify autocomplete returns empty result for non-matching keyword"""
    result = run_tc02()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_works_with_partial_keyword():
    """TC-03: Verify autocomplete works with partial keyword"""
    result = run_tc03()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_works_with_full_keyword():
    """TC-04: Verify autocomplete works with full keyword"""
    result = run_tc04()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_triggers_at_correct_input_length():
    """TC-05: Verify autocomplete triggers at the correct input length"""
    result = run_tc05()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_result_limit():
    """TC-06: Verify autocomplete result limit"""
    result = run_tc06()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


def test_autocomplete_no_duplicate_suggestions():
    """TC-07: Verify autocomplete does not return duplicate suggestions"""
    result = run_tc07()
    print("RESULT:", result["status"], f"| items_count={result['items_count']}")


# =================================================
if __name__ == "__main__":
    open(LOG_TXT, "w", encoding="utf-8").close()
    tlog(f"TEST={TEST_KEY}")
    tlog(f"BASE_URL={BASE_URL}")

    failures = []
    for fn in [run_tc01, run_tc02, run_tc03, run_tc04, run_tc05, run_tc06, run_tc07]:
        try:
            fn()
        except AssertionError as e:
            tlog(f"  ❌ FAIL: {e}")
            failures.append(str(e))

    tlog(f"\n{'='*60}")
    if failures:
        tlog(f"OVERALL: ❌ FAIL  ({len(failures)} failures)")
        for f in failures:
            tlog(f"  ⚠️  {f}")
        raise AssertionError(f"{TEST_KEY} FAIL: {len(failures)} test(s) failed")
    else:
        tlog("OVERALL: ✅ PASS")
