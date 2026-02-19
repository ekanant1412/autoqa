import requests
import json
from datetime import datetime
import os
import re

# ================= DEBUG =================
print("\n=== SCRIPT STARTED ===")
print("RUNNING FILE:", __file__)
print("WORKDIR:", os.getcwd())
print("================================\n")

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
LOG_TXT = "live_check.log"


# =================================================
def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


# =================================================
# NORMALIZE ActivityId (กัน format service เพี้ยน)
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
# EXTRACTORS
# =================================================
def extract_ids_from_items(items, prefer_keys=("ActivityId", "activityId", "activity_id", "Id", "id")):
    ids = []

    if not isinstance(items, list):
        return ids

    for it in items:
        if isinstance(it, str):
            ids.append(normalize_activity_id(it))
            continue

        if isinstance(it, dict):
            for k in prefer_keys:
                v = it.get(k)
                if v is not None:
                    ids.append(normalize_activity_id(v))
                    break

    return ids


def get_results_root(j: dict):
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_merge_page_ids(results: dict):
    """
    merge_page.result.items มีหลายรูปแบบ:
    1) {"id": "..."}                       -> return id
    2) {"items":[{"ActivityId":123}], "payload":{"content_type":"live"}} -> return ActivityId (เป็น live id)
    """
    node = results.get("merge_page", {})
    if not isinstance(node, dict):
        return []

    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    items = result.get("items", [])
    if not isinstance(items, list):
        return []

    out = []
    for it in items:
        # case: string
        if isinstance(it, str) and it.strip():
            out.append(it.strip())
            continue

        if not isinstance(it, dict):
            continue

        payload = it.get("payload", {})
        content_type = None
        if isinstance(payload, dict):
            content_type = payload.get("content_type")

        # ✅ case LIVE wrapper
        if content_type == "live":
            inner = it.get("items", [])
            if isinstance(inner, list) and inner:
                live_obj = inner[0]
                if isinstance(live_obj, dict):
                    # live id = ActivityId
                    act = live_obj.get("ActivityId")
                    if act is None:
                        act = live_obj.get("activityId")
                    if act is None:
                        act = live_obj.get("Id")
                    if act is not None:
                        out.append(str(act).strip())
                        continue

        # normal case
        _id = it.get("id")
        if isinstance(_id, str) and _id.strip():
            out.append(_id.strip())
            continue

        # fallback เผื่อบางอันเป็น Id
        _id2 = it.get("Id")
        if _id2 is not None:
            out.append(str(_id2).strip())

    return out



def extract_live_activity_ids(results: dict):
    node = results.get("get_all_live_today", {})
    result = node.get("result", {})
    items = result.get("items", [])
    return extract_ids_from_items(items)


# =================================================
# LIVE INSERT CHECK (CORRECT LOGIC)
# =================================================
def check_live_positions_until_exhausted(merge_ids, live_ids, n=3):

    live_set = {normalize_activity_id(x) for x in live_ids}

    blocks_ok = []
    failures = []

    stopped_at_block = None
    stopped_reason = None

    seen_any_live = False

    for start in range(0, len(merge_ids), n):

        block_index = start // n + 1
        block = merge_ids[start:start + n]

        lives_in_block = []

        for i, mid in enumerate(block):
            if normalize_activity_id(mid) in live_set:
                lives_in_block.append({
                    "live_id": mid,
                    "pos0": i,
                    "pos1": i + 1,
                    "rank": start + i + 1
                })

        # ✅ block มี live
        if lives_in_block:
            seen_any_live = True
            blocks_ok.append({
                "block": block_index,
                "rank_range": (start + 1, start + len(block)),
                "lives": lives_in_block
            })
            continue

        # ❌ block ไม่มี live
        if not seen_any_live:
            failures.append({
                "block": block_index,
                "rank_range": (start + 1, start + len(block)),
                "items": block
            })
            stopped_at_block = block_index
            stopped_reason = "FAIL: live never inserted (block1 has no live)"
            break
        else:
            stopped_at_block = block_index
            stopped_reason = "live exhausted -> stop checking"
            break

    return blocks_ok, failures, stopped_at_block, stopped_reason


# =================================================
# MAIN
# =================================================
def main():

    open(LOG_TXT, "w").close()

    tlog(f"URL={URL}")

    r = requests.get(URL, timeout=TIMEOUT_SEC)
    tlog(f"HTTP={r.status_code}")

    j = r.json()

    results = get_results_root(j)

    merge_ids = extract_merge_page_ids(results)
    live_ids = extract_live_activity_ids(results)

    tlog("=== Extracted ===")
    tlog(f"merge_page_ids_count={len(merge_ids)}")
    tlog(f"live_activity_ids_count={len(live_ids)}")

    if not live_ids:
        tlog("PASS no live today -> skip")
        return

    tlog("=== TC-LIVE-BLOCK CHECK ===")

    ok_blocks, failures, stop_block, reason = \
        check_live_positions_until_exhausted(
            merge_ids,
            live_ids,
            n=3
        )

    if failures:
        tlog("❌ FAIL LIVE INSERTION")
        f = failures[0]
        tlog(f"first failure block={f['block']} ranks={f['rank_range']}")
        tlog(f"items={f['items']}")

    else:
        for b in ok_blocks:
            ids = [x["live_id"] for x in b["lives"]]
            ranks = [x["rank"] for x in b["lives"]]

            tlog(
                f"BLOCK {b['block']} "
                f"ranks {b['rank_range'][0]}-{b['rank_range'][1]} "
                f"live_ids={ids} ranks={ranks}"
            )

    if stop_block:
        tlog(f"STOP at block {stop_block}: {reason}")

    with open("live_check_result.json", "w", encoding="utf-8") as f:
        json.dump({
            "merge_page_ids": merge_ids,
            "live_ids": live_ids,
            "ok_blocks": ok_blocks,
            "failures": failures,
            "stop_block": stop_block,
            "reason": reason
        }, f, indent=2, ensure_ascii=False)

    tlog("Saved live_check_result.json")
    tlog("DONE")


# =================================================
if __name__ == "__main__":
    main()
