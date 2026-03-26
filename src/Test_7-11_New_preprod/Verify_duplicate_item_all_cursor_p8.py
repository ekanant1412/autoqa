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
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?id=lrR51P10yayK"
            "&isOnlyId=true"
            "&language=th"
            "&limit=10"
            "&pseudoId=null"
            "&returnItemMetadata=false"
            "&ssoId=nologin"
            "&userId=null"
            "&verbose=debug"
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
IGNORE_PINNED_FOR_DEDUP = True

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
    if "cursor=" in base_url:
        import re
        return re.sub(r"([?&])cursor=\d+", rf"\1cursor={cursor}", base_url)
    joiner = "&" if "?" in base_url else "?"
    return f"{base_url}{joiner}cursor={cursor}"


def dump_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def find_node(obj, key: str):
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
    node = find_node(j, "candidate_pin_global")
    if not isinstance(node, dict):
        return []

    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    ids = result.get("ids", [])
    if not isinstance(ids, list):
        return []

    return [x for x in ids if isinstance(x, str) and x]


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


# ===================== MAIN =====================
def run_check(placement: dict, max_cursors: int = 10) -> dict:
    name = placement["name"]
    base_url = placement["url"]

    art_dir = os.path.join(REPORT_DIR, name)
    os.makedirs(art_dir, exist_ok=True)

    seen = {}
    cross_duplicates = []
    logs = []

    cursor = START_CURSOR
    first_error = None

    tlog(f"START run_check placement={name} max_cursors={max_cursors}")

    for i in range(max_cursors):
        status, j, url = fetch_json(base_url, cursor)
        tlog(f"FETCH cursor={cursor} status={status} url={url}")

        cursor_entry = {
            "cursor": cursor,
            "url": url,
            "http_status": status,
            "merge_ids_all": [],
            "pinned_ids": [],
            "ids": [],
            "unique_in_page": 0,
            "dup_with_previous": [],
            "tc01_intra_pass": True,
            "tc01_intra_dup_count": 0,
            "tc01_intra_dup_sample": [],
            "error": None,
        }

        if status != 200:
            first_error = f"HTTP {status} at cursor={cursor}"
            cursor_entry["error"] = first_error
            logs.append(cursor_entry)
            dump_json(os.path.join(art_dir, f"debug_error_cursor_{cursor}.json"), j)
            tlog(first_error)
            break

        merge_ids_all = extract_merge_page_ids(j)
        cursor_entry["merge_ids_all"] = merge_ids_all

        if not merge_ids_all:
            if i == 0:
                first_error = f"merge_page empty at cursor={cursor}"
                cursor_entry["error"] = first_error
                logs.append(cursor_entry)
                dump_json(os.path.join(art_dir, f"debug_empty_cursor_{cursor}.json"), j)
                tlog(first_error)
            else:
                tlog(f"STOP no merge_page items at cursor={cursor}")
            break

        pinned_ids = extract_candidate_pin_global_ids(j) if IGNORE_PINNED_FOR_DEDUP else []
        pinned_set = set(pinned_ids)
        check_ids = [x for x in merge_ids_all if x not in pinned_set] if IGNORE_PINNED_FOR_DEDUP else merge_ids_all

        cursor_entry["pinned_ids"] = pinned_ids
        cursor_entry["ids"] = check_ids
        cursor_entry["unique_in_page"] = len(set(check_ids))

        print(f"\n=== Cursor {cursor} ===")
        print(f"URL: {url}")
        print(f"merge_ids_all ({len(merge_ids_all)}): {merge_ids_all}")
        if IGNORE_PINNED_FOR_DEDUP:
            print(f"pinned_ids ({len(pinned_ids)}): {pinned_ids}")
        print(f"check_ids ({len(check_ids)}):")
        for idx, _id in enumerate(check_ids, 1):
            print(f"{idx}. {_id}")

        if not check_ids:
            tlog(f"cursor={cursor} all items were pinned or empty after filter")
            logs.append(cursor_entry)
            cursor += CURSOR_STEP
            time.sleep(SLEEP_SEC)
            continue

        tc01_pass, intra_dups = tc01_intra_cursor_no_duplicate(check_ids)
        cursor_entry["tc01_intra_pass"] = tc01_pass
        cursor_entry["tc01_intra_dup_count"] = len(intra_dups)
        cursor_entry["tc01_intra_dup_sample"] = intra_dups[:20]

        dup_hits = []
        for _id in check_ids:
            if _id in seen:
                hit = {
                    "id": _id,
                    "first_cursor": seen[_id],
                    "now_cursor": cursor,
                }
                dup_hits.append(hit)
                cross_duplicates.append(hit)
            else:
                seen[_id] = cursor

        cursor_entry["dup_with_previous"] = dup_hits

        if intra_dups:
            print(f"⚠️ INTRA DUP at cursor {cursor}: {intra_dups}")

        if dup_hits:
            print(f"⚠️ CROSS DUP at cursor {cursor}: {dup_hits}")

        logs.append(cursor_entry)

        if FAIL_FAST_ON_INTRA_DUP and not tc01_pass:
            tlog(f"FAIL_FAST_ON_INTRA_DUP break at cursor={cursor}")
            break

        cursor += CURSOR_STEP
        time.sleep(SLEEP_SEC)

    # ✅ ไฟล์เดียวรวมทุก cursor
    combined_output = {
        "placement": name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "config": {
            "start_cursor": START_CURSOR,
            "cursor_step": CURSOR_STEP,
            "max_cursors": max_cursors,
            "ignore_pinned_for_dedup": IGNORE_PINNED_FOR_DEDUP,
            "enable_cross_cursor_check": ENABLE_CROSS_CURSOR_CHECK,
            "fail_fast_on_intra_dup": FAIL_FAST_ON_INTRA_DUP,
        },
        "summary": {
            "cursors_scanned": len(logs),
            "cross_duplicates_count": len(cross_duplicates),
            "cross_duplicates_sample": cross_duplicates[:20],
            "first_error": first_error,
        },
        "cursor_results": logs,
    }

    dump_json(os.path.join(art_dir, "cursor_results_all.json"), combined_output)

    tc01_failed = [x["cursor"] for x in logs if x.get("tc01_intra_pass") is False]

    status = "PASS"
    if tc01_failed:
        status = "FAIL"
    elif ENABLE_CROSS_CURSOR_CHECK and cross_duplicates:
        status = "FAIL"
    elif not logs:
        status = "ERROR"

    print(f"\n[{name}] {status}: cursors={len(logs)} intra_fail={len(tc01_failed)} cross_dups={len(cross_duplicates)}")
    if first_error:
        print(f"first_error: {first_error}")
    print(f"combined results saved to: {os.path.join(art_dir, 'cursor_results_all.json')}")

    tlog(f"END placement={name} status={status} cursors={len(logs)} cross_dups={len(cross_duplicates)}")

    return {
        "placement": name,
        "status": status,
        "cursors_scanned": len(logs),
        "tc01_failed_cursors": tc01_failed,
        "cross_duplicates_count": len(cross_duplicates),
        "cross_duplicates_sample": cross_duplicates[:5],
        "cross_duplicates": cross_duplicates,
        "first_error": first_error,
    }


def _assert_result(summary: dict):
    assert summary.get("status") != "ERROR", (
        f"[{summary['placement']}] no data returned — {summary.get('first_error', 'unknown reason')}. "
        f"ดู cursor_results_all.json ใน reports/{summary['placement']}/"
    )
    assert summary["tc01_failed_cursors"] == [], (
        f"[{summary['placement']}] intra-cursor duplicates at cursors: {summary['tc01_failed_cursors']}"
    )
    assert summary["cross_duplicates_count"] == 0, (
        f"[{summary['placement']}] {summary['cross_duplicates_count']} cross-cursor duplicates found. "
        f"Sample: {summary['cross_duplicates_sample']}"
    )


# ===================== PYTEST TEST =====================
def test_verify_no_duplicate_item_ids_sfv_p8():
    result = run_check(PLACEMENTS[0], max_cursors=10)
    _assert_result(result)


# ===================== DIRECT RUN =====================
if __name__ == "__main__":
    overall = 0
    for p in PLACEMENTS:
        result = run_check(p, max_cursors=MAX_CURSORS)
        if result["status"] != "PASS":
            overall = 1
    raise SystemExit(overall)