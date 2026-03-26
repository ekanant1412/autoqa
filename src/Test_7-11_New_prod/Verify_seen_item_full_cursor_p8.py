import json
import time
import requests
import os
from datetime import datetime
from collections import deque

# ===================== CONFIG =====================
BASE_URL = (
    "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p8"
    "?shelfId=bxAwRPp85gmL"
    "&total_candidates=200"
    "&pool_limit_items=50"
    "&pool_limit_category_items=10"
    "&language=th"
    "&pool_tophit_date=30"
    "&limit=20"
    "&userId=null"
    "&pseudoId=null"
    "&verbose=debug"
    "&GA_ID=12345678.1234338"
)

START_CURSOR = 1
CURSOR_STEP = 1
MAX_CURSORS = 50

TIMEOUT_SEC = 20
SLEEP_SEC = 0.05

SEEN_LIMIT = 40

OUT_JSON = "tc_seen_checks_log.json"
OUT_LOG = "tc_seen_checks.log"
# =================================================


# ===================== FILE LOG =====================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(OUT_LOG, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


# ===================== HELPERS =====================
def build_url(cursor: int) -> str:
    return f"{BASE_URL}&cursor={cursor}"


def dump_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def extract_slice_pagination_ids(j: dict):
    data = j.get("data", {}) if isinstance(j.get("data", {}), dict) else {}
    results = data.get("results", {}) if isinstance(data.get("results", {}), dict) else {}

    node = results.get("slice_pagination", {})
    if not isinstance(node, dict):
        return []

    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    items = result.get("items", [])
    if not isinstance(items, list):
        return []

    return [it["id"] for it in items if isinstance(it, dict) and it.get("id")]


def extract_seen_ids(j: dict):
    data = j.get("data", {}) if isinstance(j.get("data", {}), dict) else {}
    results = data.get("results", {}) if isinstance(data.get("results", {}), dict) else {}

    node = results.get("get_seen_item_redis", {})
    if not isinstance(node, dict):
        return []

    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    ids = result.get("ids", [])
    out = []
    for x in ids:
        if isinstance(x, str):
            out.append(x)
        elif isinstance(x, dict) and x.get("id"):
            out.append(x["id"])
    return out


def fetch_json(cursor: int):
    url = build_url(cursor)
    r = requests.get(url, timeout=TIMEOUT_SEC)
    try:
        j = r.json()
    except Exception:
        j = {"_raw": r.text}
    return r.status_code, j, url


def fifo_push_strict(queue: deque, incoming_ids: list, limit: int):
    evicted = []
    for _id in incoming_ids:
        if _id in queue:
            continue
        queue.append(_id)
        while len(queue) > limit:
            evicted.append(queue.popleft())
    return evicted


# ===================== MAIN =====================
def run():
    # reset log
    open(OUT_LOG, "w", encoding="utf-8").close()
    tlog("START TC-B seen FIFO strict check")
    tlog(f"CWD = {os.getcwd()}")
    tlog(f"SEEN_LIMIT = {SEEN_LIMIT}")

    logs = []
    expected_seen = deque()
    prev_slice_ids = None

    cursor = START_CURSOR

    for step in range(MAX_CURSORS):
        status, j, url = fetch_json(cursor)
        tlog(f"\n[FETCH] cursor={cursor}")

        if status != 200:
            tlog(f"[ERROR] HTTP {status}")
            dump_json(f"error_cursor_{cursor}.json", j)
            break

        if step == 0:
            dump_json(f"first_response_cursor_{cursor}.json", j)

        slice_ids = extract_slice_pagination_ids(j)
        seen_ids = extract_seen_ids(j)

        if not slice_ids:
            tlog("[STOP] no slice ids")
            break

        tlog(f"[SLICE] {len(slice_ids)} ids")
        tlog(f"[SEEN(redis)] {len(seen_ids)} ids")

        if prev_slice_ids is None:
            tlog("[TC-B] SKIP (no previous slice yet)")
            evicted = []
            tc_b_pass = None
            missing = []
            extra = []
        else:
            evicted = fifo_push_strict(expected_seen, prev_slice_ids, SEEN_LIMIT)

            expected_set = set(expected_seen)
            actual_set = set(seen_ids)

            missing = sorted(expected_set - actual_set)
            extra = sorted(actual_set - expected_set)

            size_ok = len(seen_ids) <= SEEN_LIMIT
            tc_b_pass = size_ok and not missing

            tlog(
                f"[TC-B] {'PASS' if tc_b_pass else 'FAIL'} | "
                f"model={len(expected_seen)} redis={len(seen_ids)} "
                f"missing={len(missing)} extra={len(extra)}"
            )

            if missing:
                tlog(f"❌ missing_expected: {missing}")
            if extra:
                tlog(f"⚠️ unexpected_extra: {extra}")

        logs.append({
            "cursor": cursor,
            "slice_ids": slice_ids,
            "seen_ids": seen_ids,
            "expected_seen_model": list(expected_seen),
            "evicted_by_model": evicted,
            "tc_b_pass": tc_b_pass,
            "missing_expected": missing,
            "unexpected_extra": extra,
        })

        prev_slice_ids = slice_ids
        cursor += CURSOR_STEP
        time.sleep(SLEEP_SEC)

    dump_json(OUT_JSON, logs)
    tlog(f"\nSaved JSON log: {OUT_JSON}")
    tlog("END")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
