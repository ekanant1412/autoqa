import json
import time
import requests
import os
from datetime import datetime
from collections import Counter

# ===================== CONFIG =====================
PLACEMENTS = [
    {
        "name": "sfv-p8",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
            "&limit_seen_item=20"
        ),
    },
]

START_CURSOR = 1
CURSOR_STEP = 1
MAX_CURSORS = 500

TIMEOUT_SEC = 20
SLEEP_SEC = 0.05

FAIL_FAST_ON_INTRA_DUP = True
ENABLE_CROSS_CURSOR_CHECK = True

# ✅ อนุโลมให้ pinned/global ids ซ้ำได้
IGNORE_PINNED_FOR_DEDUP = True

LOG_JSON = "cursor_compare_logs.json"
DUP_JSON = "cursor_duplicates.json"
LOG_TXT = "cursor_run.log"

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# =================================================


# ===================== LOGGER =====================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


# ===================== HELPERS =====================
def build_url(base_url: str, cursor: int) -> str:
    joiner = "&" if "?" in base_url else "?"
    return f"{base_url}{joiner}cursor={cursor}"


def dump_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def find_node(obj, key: str):
    """Deep search node by key (robust for wrapped responses)"""
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if key in cur:
                return cur[key]
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return None


def extract_merge_page_ids(j: dict):
    """Extract IDs ONLY from merge_page.result.items[].id"""
    merge_page = find_node(j, "merge_page")
    if not isinstance(merge_page, dict):
        return []

    result = merge_page.get("result", {})
    if not isinstance(result, dict):
        return []

    items = result.get("items", [])
    if not isinstance(items, list):
        return []

    ids = []
    for it in items:
        if isinstance(it, dict):
            _id = it.get("id") or it.get("item_id")
            if isinstance(_id, str) and _id:
                ids.append(_id)
    return ids


def extract_candidate_pin_global_ids(j: dict):
    """
    Extract pinned IDs from candidate_pin_global.result.ids
    ถ้าไม่มี node นี้ จะคืน []
    """
    node = find_node(j, "candidate_pin_global")
    if not isinstance(node, dict):
        return []

    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    ids = result.get("ids", [])
    if not isinstance(ids, list):
        return []

    out = []
    for _id in ids:
        if isinstance(_id, str) and _id:
            out.append(_id)
    return out


def fetch_json(base_url: str, cursor: int):
    url = build_url(base_url, cursor)
    r = requests.get(url, timeout=TIMEOUT_SEC)
    try:
        j = r.json()
    except Exception:
        j = {"_raw": r.text}
    return r.status_code, j, url


def tc01_intra_cursor_no_duplicate(ids):
    c = Counter(ids)
    dups = [k for k, v in c.items() if v > 1]
    return len(dups) == 0, dups


# ===================== PYTEST ENTRY =====================
def run_check(placement: dict, max_cursors: int = 10) -> dict:
    """รัน dedup check สำหรับ placement เดียว และ return summary dict"""
    name = placement["name"]
    base_url = placement["url"]

    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    seen: dict = {}
    cross_duplicates = []
    logs = []
    cursor = START_CURSOR
    first_error = None

    for i in range(max_cursors):
        status, j, url = fetch_json(base_url, cursor)

        if status != 200:
            first_error = f"HTTP {status} at cursor={cursor}, url={url}"
            # บันทึก response ไว้ดู
            dump_json(f"{art_dir}/debug_error_cursor_{cursor}.json", j)
            break

        merge_ids_all = extract_merge_page_ids(j)
        if not merge_ids_all:
            if i == 0:
                first_error = f"merge_page items empty at cursor={cursor}"
                dump_json(f"{art_dir}/debug_empty_cursor_{cursor}.json", j)
            break

        pinned_ids = extract_candidate_pin_global_ids(j) if IGNORE_PINNED_FOR_DEDUP else []
        pinned_set = set(pinned_ids)
        check_ids = [x for x in merge_ids_all if x not in pinned_set] if IGNORE_PINNED_FOR_DEDUP else merge_ids_all

        if not check_ids:
            cursor += CURSOR_STEP
            time.sleep(SLEEP_SEC)
            continue

        tc01_pass, intra_dups = tc01_intra_cursor_no_duplicate(check_ids)
        unique_in_page = len(set(check_ids))

        dup_hits = []
        new_count = 0
        if ENABLE_CROSS_CURSOR_CHECK:
            for _id in set(check_ids):
                if _id in seen:
                    dup_hits.append(_id)
                    cross_duplicates.append({"id": _id, "first_cursor": seen[_id], "now_cursor": cursor})
                else:
                    seen[_id] = cursor
                    new_count += 1

        logs.append({
            "cursor": cursor,
            "merge_ids_count": len(merge_ids_all),
            "check_ids_count": len(check_ids),
            "unique_in_page": unique_in_page,
            "tc01_intra_pass": tc01_pass,
            "tc01_intra_dup_count": len(intra_dups),
            "tc01_intra_dup_sample": intra_dups[:20],
            "tc02_dup_with_previous_count": len(dup_hits) if ENABLE_CROSS_CURSOR_CHECK else None,
            "tc02_dup_with_previous_sample": dup_hits[:20] if ENABLE_CROSS_CURSOR_CHECK else None,
        })

        if FAIL_FAST_ON_INTRA_DUP and not tc01_pass:
            break

        cursor += CURSOR_STEP
        time.sleep(SLEEP_SEC)

    dump_json(f"{art_dir}/dedup_logs.json", logs)
    dump_json(f"{art_dir}/dedup_cross_dups.json", cross_duplicates)

    tc01_failed = [x["cursor"] for x in logs if x.get("tc01_intra_pass") is False]
    status = "PASS"
    if tc01_failed:
        status = "FAIL"
    elif ENABLE_CROSS_CURSOR_CHECK and cross_duplicates:
        status = "FAIL"
    elif not logs:
        status = "ERROR"

    print(f"[{name}] {status}: cursors={len(logs)} intra_fail={len(tc01_failed)} cross_dups={len(cross_duplicates)}")
    if first_error:
        print(f"  ↳ first_error: {first_error}")
    return {
        "placement": name,
        "status": status,
        "cursors_scanned": len(logs),
        "tc01_failed_cursors": tc01_failed,
        "cross_duplicates_count": len(cross_duplicates),
        "cross_duplicates_sample": cross_duplicates[:5],
        "first_error": first_error,
    }


def _assert_result(summary: dict):
    assert summary.get("status") != "ERROR", (
        f"[{summary['placement']}] no data returned — {summary.get('first_error', 'unknown reason')}. "
        f"ดู debug_*.json ใน reports/{summary['placement']}/ เพื่อ inspect response"
    )
    assert summary["tc01_failed_cursors"] == [], (
        f"[{summary['placement']}] intra-cursor duplicates at cursors: {summary['tc01_failed_cursors']}"
    )
    assert summary["cross_duplicates_count"] == 0, (
        f"[{summary['placement']}] {summary['cross_duplicates_count']} cross-cursor duplicates found. "
        f"Sample: {summary['cross_duplicates_sample']}"
    )


def test_verify_no_duplicate_item_ids_sfv_p8():
    """Verify sfv-p8: ไม่มี duplicate IDs ใน merge_page (intra + cross, 10 cursors)"""
    _assert_result(run_check(PLACEMENTS[0], max_cursors=10))


if __name__ == "__main__":
    overall = 0
    for p in PLACEMENTS:
        result = run_check(p, max_cursors=MAX_CURSORS)
        if result["status"] != "PASS":
            overall = 1
    raise SystemExit(overall)
