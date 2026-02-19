import requests
import json
from datetime import datetime
import os

# ================= DEBUG =================
print("\n=== SCRIPT STARTED ===")
print("RUNNING FILE:", __file__)
print("WORKDIR:", os.getcwd())
print("================================\n")

# ===================== CONFIG =====================
URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7"
    "?shelfId=Kaw6MLVzPWmo"
    "&total_candidates=200"
    "&pool_limit_category_items=50"
    "&language=th"
    "&limit=100"
    "&userId=null"
    "&pseudoId=null"
    "&cursor=1"
    "&ga_id=100118391.0851155978"
    "&ssoId=22092422"
    "&is_use_live=false"
    "&verbose=debug"
)

TIMEOUT_SEC = 20
LOG_TXT = "pin_check.log"
# =================================================


def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def extract_ids_from_items(items):
    ids = []
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("id"), str):
                ids.append(it["id"])
            elif isinstance(it, str):
                ids.append(it)
    return ids


def get_results_root(j: dict):
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_candidate_pin_global_ids(results: dict):
    node = results.get("candidate_pin_global", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    ids = result.get("ids")
    if isinstance(ids, list):
        return [x for x in ids if isinstance(x, str) and x.strip()]

    return extract_ids_from_items(result.get("items", []))


def extract_merge_page_ids(results: dict):
    node = results.get("merge_page", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    return extract_ids_from_items(result.get("items", []))


# =================================================
# CHECK: PIN POSITION PER BLOCK (STOP WHEN EXHAUSTED)
# =================================================
def check_pin_positions_until_exhausted(merge_ids, pin_ids, n=5):
    """
    - ตรวจตำแหน่ง pin ในแต่ละ block (0..n-1)
    - ถ้า block ไหนไม่เจอ pin => ถือว่า pin หมด -> STOP (ไม่เช็ค block ต่อๆไป)
    """
    pin_set = set(pin_ids)

    found_blocks = []
    stopped_at_block = None
    stopped_reason = None

    for start in range(0, len(merge_ids), n):
        block_index = start // n + 1
        block = merge_ids[start:start + n]

        pins_in_block = []
        for i, mid in enumerate(block):
            if mid in pin_set:
                pins_in_block.append(
                    {
                        "pin_id": mid,
                        "pos0": i,                 # 0-4
                        "pos1": i + 1,             # 1-5
                        "rank": start + i + 1      # global rank
                    }
                )

        # ถ้า block นี้ไม่มี pin → ถือว่าหมดแล้ว
        if not pins_in_block:
            stopped_at_block = block_index
            stopped_reason = "pin exhausted -> stop checking next blocks"
            break

        found_blocks.append(
            {
                "block": block_index,
                "rank_range": (start + 1, start + len(block)),
                "pins": pins_in_block,
            }
        )

    return found_blocks, stopped_at_block, stopped_reason


def main():
    # reset log
    open(LOG_TXT, "w", encoding="utf-8").close()

    tlog(f"URL={URL}")

    try:
        r = requests.get(URL, timeout=TIMEOUT_SEC)
    except Exception as e:
        tlog(f"REQUEST ERROR: {repr(e)}")
        return

    tlog(f"HTTP={r.status_code}")

    try:
        j = r.json()
    except Exception:
        tlog("Response not JSON")
        tlog(r.text[:1000])
        return

    if r.status_code != 200:
        tlog(json.dumps(j, indent=2, ensure_ascii=False)[:2000])
        return

    results = get_results_root(j)

    pin_ids = extract_candidate_pin_global_ids(results)
    merge_ids = extract_merge_page_ids(results)

    tlog("=== Extracted ===")
    tlog(f"pin_ids_count={len(pin_ids)}")
    tlog(f"merge_page_ids_count={len(merge_ids)}")

    if not merge_ids:
        tlog("[WARN] merge_page_ids is empty or missing (merge_page.result.items not found)")

    # ---------------------------------------------
    # CHECK 0: if no pin in candidate_pin_global => OK (no need to insert)
    # ---------------------------------------------
    tlog("=== TC-PIN: candidate_pin_global availability ===")
    if not pin_ids:
        tlog("PASS no pin_ids in candidate_pin_global (pin exhausted) -> SKIP insertion checks")

        missing_in_merge = []
        pin_block_positions = []
        stopped_at_block = None
        stopped_reason = "no pin_ids in candidate_pin_global"

    else:
        # ---------------------------------------------
        # CHECK 1: pin must exist in merge_page
        # ---------------------------------------------
        merge_set = set(merge_ids)
        missing_in_merge = [pid for pid in pin_ids if pid not in merge_set]

        tlog("=== TC-PIN: pin must be in merge_page ===")
        if missing_in_merge:
            tlog(f"FAIL missing_in_merge={missing_in_merge}")
        else:
            tlog("PASS all pin_ids exist in merge_page")

        # ---------------------------------------------
        # CHECK 2: pin position per block (0-4) until exhausted
        # ---------------------------------------------
        tlog("=== TC-PIN-BLOCK: pin position (0-4) per block (STOP when exhausted) ===")

        pin_block_positions, stopped_at_block, stopped_reason = (
            check_pin_positions_until_exhausted(merge_ids, pin_ids, n=5)
        )

        if not pin_block_positions:
            tlog("WARN no pin detected in merge_page blocks")
        else:
            for b in pin_block_positions:
                ids = [p["pin_id"] for p in b["pins"]]
                pos0 = [p["pos0"] for p in b["pins"]]
                pos1 = [p["pos1"] for p in b["pins"]]
                ranks = [p["rank"] for p in b["pins"]]

                tlog(
                    f"BLOCK {b['block']} ranks {b['rank_range'][0]}-{b['rank_range'][1]} | "
                    f"pin_ids={ids} | pos0={pos0} pos1={pos1} | ranks={ranks}"
                )

        if stopped_at_block is not None:
            tlog(f"STOP at BLOCK {stopped_at_block}: {stopped_reason}")
        else:
            tlog("DONE checked all blocks (no exhaustion detected)")

    # ---------------------------------------------
    # SAVE RESULT
    # ---------------------------------------------
    with open("pin_check_result.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "url": URL,
                "pin_ids": pin_ids,
                "merge_page_ids": merge_ids,
                "missing_in_merge": missing_in_merge,
                "pin_block_positions": pin_block_positions,
                "pin_check_stopped_at_block": stopped_at_block,
                "pin_check_stopped_reason": stopped_reason,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    with open("pin_check_full_response.json", "w", encoding="utf-8") as f:
        json.dump(j, f, indent=2, ensure_ascii=False)

    tlog("Saved: pin_check_result.json")
    tlog("Saved: pin_check_full_response.json")
    tlog(f"Saved: {LOG_TXT}")


if __name__ == "__main__":
    try:
        print(">>> ENTER MAIN()")
        main()
        print(">>> MAIN() FINISHED")
    except Exception:
        print("\n!!! FATAL ERROR !!!")
        import traceback
        traceback.print_exc()
