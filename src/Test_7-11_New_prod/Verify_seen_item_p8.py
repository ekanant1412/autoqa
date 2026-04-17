import requests
import json
from datetime import datetime
import os
import time

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9700"

URL = (
    "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p6"
    "?shelfId=zmEXe3EQnXDk"
    "&total_candidates=200"
    "&pool_limit_category_items=100"
    "&language=th&pool_tophit_date=365"
    "&limit=100&userId=null&pseudoId=null"
    "&cursor=1&ga_id=999999999.999999999"
    "&is_use_live=true&verbose=debug&pool_latest_date=365"
)

TIMEOUT_SEC = 20
MAX_SEEN = 100

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

LOG_TXT = f"{ART_DIR}/seen_check.log"
RESULT_JSON = f"{ART_DIR}/seen_check_result.json"
RESP1_JSON = f"{ART_DIR}/response_req1.json"
RESP2_JSON = f"{ART_DIR}/response_req2.json"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def get_results_root(j: dict) -> dict:
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_slice_pagination_ids(results: dict) -> list:
    """ดึง ids จาก slice_pagination.result.items"""
    node = results.get("slice_pagination", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


def extract_seen_item_ids(results: dict) -> list:
    """ดึง ids จาก get_seen_item_redis.result.ids"""
    node = results.get("get_seen_item_redis", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    # รองรับทั้ง ids และ items
    ids = result.get("ids", [])
    if ids:
        return [str(i) for i in ids if i]
    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


# =================================================
def run_check():
    open(LOG_TXT, "w", encoding="utf-8").close()

    tlog(f"TEST={TEST_KEY}")
    tlog(f"URL={URL}")

    # =================================================
    # REQUEST 1
    # =================================================
    tlog("\n======== REQUEST 1 ========")
    r1 = requests.get(URL, timeout=TIMEOUT_SEC)
    tlog(f"HTTP={r1.status_code}")
    r1.raise_for_status()

    j1 = r1.json()
    results1 = get_results_root(j1)

    with open(RESP1_JSON, "w", encoding="utf-8") as f:
        json.dump(j1, f, ensure_ascii=False, indent=2)

    # ดึง slice_pagination ids (ของใหม่ที่จะถูก seen)
    pagination_ids_r1 = extract_slice_pagination_ids(results1)
    seen_ids_r1 = extract_seen_item_ids(results1)

    tlog(f"slice_pagination ids  : {len(pagination_ids_r1)}")
    tlog(f"get_seen_item_redis ids (before) : {len(seen_ids_r1)}")

    tlog("\n--- slice_pagination ids (req1) ---")
    for i, pid in enumerate(pagination_ids_r1):
        tlog(f"  [{i:3d}] {pid}")

    tlog(f"\n--- get_seen_item_redis ids (req1) --- total={len(seen_ids_r1)}")
    for i, sid in enumerate(seen_ids_r1):
        tlog(f"  [{i:3d}] {sid}")

    # =================================================
    # REQUEST 2
    # =================================================
    tlog("\n======== REQUEST 2 ========")
    r2 = requests.get(URL, timeout=TIMEOUT_SEC)
    tlog(f"HTTP={r2.status_code}")
    r2.raise_for_status()

    j2 = r2.json()
    results2 = get_results_root(j2)

    with open(RESP2_JSON, "w", encoding="utf-8") as f:
        json.dump(j2, f, ensure_ascii=False, indent=2)

    seen_ids_r2 = extract_seen_item_ids(results2)
    pagination_ids_r2 = extract_slice_pagination_ids(results2)

    tlog(f"get_seen_item_redis ids (after)  : {len(seen_ids_r2)}")

    tlog(f"\n--- get_seen_item_redis ids (req2) --- total={len(seen_ids_r2)}")
    for i, sid in enumerate(seen_ids_r2):
        tlog(f"  [{i:3d}] {sid}")

    # =================================================
    # VALIDATE
    # =================================================
    tlog("\n=== VALIDATION ===")
    issues = []

    # CHECK 1: seen ต้องไม่เกิน MAX_SEEN
    tlog(f"\n[CHECK 1] seen_ids count <= {MAX_SEEN}")
    if len(seen_ids_r2) > MAX_SEEN:
        msg = f"seen_ids count={len(seen_ids_r2)} exceeds MAX_SEEN={MAX_SEEN}"
        tlog(f"  ❌ FAIL: {msg}")
        issues.append(msg)
    else:
        tlog(f"  ✅ PASS: seen_ids count={len(seen_ids_r2)} <= {MAX_SEEN}")

    # CHECK 2: ids จาก slice_pagination req1 ต้องอยู่ใน seen req2
    tlog(f"\n[CHECK 2] slice_pagination(req1) ids must appear in get_seen_item_redis(req2)")
    seen_set_r2 = set(seen_ids_r2)
    missing_in_seen = [pid for pid in pagination_ids_r1 if pid not in seen_set_r2]
    found_in_seen = [pid for pid in pagination_ids_r1 if pid in seen_set_r2]

    tlog(f"  pagination_ids_r1 count : {len(pagination_ids_r1)}")
    tlog(f"  found in seen_r2        : {len(found_in_seen)}")
    tlog(f"  missing in seen_r2      : {len(missing_in_seen)}")

    if missing_in_seen:
        msg = f"{len(missing_in_seen)} ids from slice_pagination(req1) not found in seen(req2)"
        tlog(f"  ❌ FAIL: {msg}")
        for mid in missing_in_seen:
            tlog(f"    - {mid}")
        issues.append(msg)
    else:
        tlog(f"  ✅ PASS: all {len(pagination_ids_r1)} pagination ids found in seen(req2)")

    # CHECK 3: eviction — ถ้า seen เต็ม (req1 >= MAX_SEEN) ของเก่าสุดต้องถูกดันออก
    tlog(f"\n[CHECK 3] eviction check (if seen was full before req2)")
    if len(seen_ids_r1) >= MAX_SEEN:
        # คาดว่าของเก่าสุด (ต้นลิสต์ req1) จะถูกดันออก
        evict_candidates = seen_ids_r1[:len(pagination_ids_r1)]  # ของที่ควรถูกดันออก
        still_in_seen = [eid for eid in evict_candidates if eid in seen_set_r2]
        evicted = [eid for eid in evict_candidates if eid not in seen_set_r2]

        tlog(f"  seen was full before (count={len(seen_ids_r1)})")
        tlog(f"  expected evict candidates : {len(evict_candidates)}")
        tlog(f"  actually evicted          : {len(evicted)}")
        tlog(f"  still in seen (not evicted): {len(still_in_seen)}")

        if still_in_seen:
            msg = f"{len(still_in_seen)} old items not evicted when seen was full"
            tlog(f"  ❌ FAIL: {msg}")
            for sid in still_in_seen:
                tlog(f"    - {sid}")
            issues.append(msg)
        else:
            tlog(f"  ✅ PASS: old items correctly evicted")
    else:
        tlog(f"  SKIP: seen was not full before req2 (count={len(seen_ids_r1)} < {MAX_SEEN})")

    # =================================================
    # SUMMARY
    # =================================================
    status = "FAIL" if issues else "PASS"

    tlog("\n=== SEEN ITEM SUMMARY ===")
    tlog(f"seen_before (req1)  : {len(seen_ids_r1)}")
    tlog(f"seen_after  (req2)  : {len(seen_ids_r2)}")
    tlog(f"pagination_ids_r1   : {len(pagination_ids_r1)}")
    tlog(f"found_in_seen_r2    : {len(found_in_seen)}")
    tlog(f"missing_in_seen_r2  : {len(missing_in_seen)}")
    tlog(f"STATUS              : {'✅ PASS' if status == 'PASS' else '❌ FAIL'}")
    if issues:
        for iss in issues:
            tlog(f"  ⚠️  {iss}")

    result = {
        "test_key": TEST_KEY,
        "url": URL,
        "seen_before_count": len(seen_ids_r1),
        "seen_after_count": len(seen_ids_r2),
        "pagination_ids_r1": pagination_ids_r1,
        "seen_ids_r1": seen_ids_r1,
        "seen_ids_r2": seen_ids_r2,
        "found_in_seen_r2": found_in_seen,
        "missing_in_seen_r2": missing_in_seen,
        "issues": issues,
        "status": status,
    }

    with open(RESULT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    tlog(f"\nSaved: {RESULT_JSON}")
    tlog(f"Saved: {RESP1_JSON}")
    tlog(f"Saved: {RESP2_JSON}")
    tlog(f"Saved: {LOG_TXT}")

    if status == "FAIL":
        raise AssertionError(
            f"{TEST_KEY} FAIL: {'; '.join(issues)}"
        )

    return result


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_verify_seen_item_p8():
    result = run_check()
    print("RESULT:", result["status"],
          f"| seen_before={result['seen_before_count']}",
          f"| seen_after={result['seen_after_count']}",
          f"| missing={len(result['missing_in_seen_r2'])}")


if __name__ == "__main__":
    run_check()