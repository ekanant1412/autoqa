import requests
import json
from datetime import datetime
import os

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9587"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&limit=100&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
        ),
    },
    {
        "name": "sfv-p6",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p6"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&limit=100&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            # "&partner_id=AN9PjZR1wEol"
        ),
    },
]

TIMEOUT_SEC = 20
BLOCK_SIZE = 4

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)


# =================================================
def tlog(msg: str, log_path: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def get_results_root(j: dict) -> dict:
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_insert_pin_node(results: dict) -> dict:
    return results.get("insert_pin_candidates", {})


def extract_reserved_positions(node: dict) -> dict:
    result = node.get("result", {})
    if not isinstance(result, dict):
        return {}

    reserved = result.get("reservedPositions")
    if not isinstance(reserved, dict):  # ครอบคลุม None, list, str
        return {}

    out = {}
    for k, v in reserved.items():
        if str(k).isdigit() and isinstance(v, str):
            pin_id = v[4:] if v.startswith("pin_") else v
            out[int(k)] = pin_id
    return out


def extract_pin_items(node: dict) -> list:
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []

    reserved = result.get("reservedPositions")
    if not isinstance(reserved, dict):
        return []

    return [
        v[4:] if isinstance(v, str) and v.startswith("pin_") else v
        for v in reserved.values()
        if isinstance(v, str)
    ]


def extract_merge_page_ids(results: dict) -> list:
    node = results.get("merge_page", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    items = result.get("items", [])
    ids = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if "id" in it:
            ids.append(str(it["id"]))
        elif "items" in it and isinstance(it["items"], list):
            for sub in it["items"]:
                if isinstance(sub, dict):
                    activity_id = sub.get("ActivityId") or sub.get("activity_id")
                    if activity_id is not None:
                        ids.append(str(activity_id))
    return ids


def check_pin_in_blocks(merge_ids: list, reserved: dict, pin_items: list, n: int = 5):
    pin_set = set(pin_items)
    last_pin_block = max((rank // n + 1 for rank in reserved), default=0)

    results_by_block = []
    fail_blocks = []
    pass_blocks = []

    total_blocks = (len(merge_ids) + n - 1) // n

    for block_index in range(1, total_blocks + 1):
        start = (block_index - 1) * n
        block = merge_ids[start:start + n]

        expected_pins = {
            rank: pid
            for rank, pid in reserved.items()
            if rank // n + 1 == block_index
        }
        actual_pins = {
            start + i: mid
            for i, mid in enumerate(block)
            if mid in pin_set
        }

        block_info = {
            "block": block_index,
            "rank_range": (start, start + len(block) - 1),
            "expected_pins": expected_pins,
            "actual_pins": actual_pins,
            "status": None,
            "issues": [],
        }

        if block_index > last_pin_block:
            block_info["status"] = "SKIP (pin exhausted)"
        else:
            issues = []
            for rank, pin_id in expected_pins.items():
                actual = merge_ids[rank] if rank < len(merge_ids) else None
                if actual != pin_id:
                    issues.append(f"rank={rank} expected={pin_id} actual={actual}")
            if not expected_pins:
                issues.append("no pin reserved for this block (but pin not exhausted yet)")

            if issues:
                block_info["status"] = "FAIL"
                block_info["issues"] = issues
                fail_blocks.append(block_index)
            else:
                block_info["status"] = "PASS"
                pass_blocks.append(block_index)

        results_by_block.append(block_info)

    return results_by_block, pass_blocks, fail_blocks, last_pin_block


# =================================================
def run_check(placement: dict) -> dict:
    name = placement["name"]
    url = placement["url"]

    log_path = f"{ART_DIR}/pin_check_{name}.log"
    result_json = f"{ART_DIR}/pin_check_result_{name}.json"
    full_response_json = f"{ART_DIR}/pin_check_full_response_{name}.json"

    open(log_path, "w", encoding="utf-8").close()

    def log(msg):
        tlog(msg, log_path)

    log(f"TEST={TEST_KEY}  PLACEMENT={name}")
    log(f"URL={url}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    log(f"HTTP={r.status_code}")
    r.raise_for_status()

    j = r.json()
    results = get_results_root(j)

    with open(full_response_json, "w", encoding="utf-8") as f:
        json.dump(j, f, ensure_ascii=False, indent=2)

    pin_node = extract_insert_pin_node(results)
    reserved = extract_reserved_positions(pin_node)
    pin_items = extract_pin_items(pin_node)
    merge_ids = extract_merge_page_ids(results)

    log("=== Extracted ===")
    log(f"insert_pin_candidates items : {len(pin_items)}")
    log(f"reservedPositions count     : {len(reserved)}")
    log(f"merge_page_ids count        : {len(merge_ids)}")

    if not merge_ids:
        log("[WARN] merge_page is empty")

    if not pin_items:
        log("PASS no pin items in insert_pin_candidates -> SKIP all checks")
        status = "PASS"
        results_by_block = []
        pass_blocks = []
        fail_blocks = []
        last_pin_block = 0
    else:
        log(f"\n=== TC-PIN-BLOCK: checking {BLOCK_SIZE}-item blocks ===")

        results_by_block, pass_blocks, fail_blocks, last_pin_block = check_pin_in_blocks(
            merge_ids, reserved, pin_items, n=BLOCK_SIZE
        )

        log(f"last block with pin : block {last_pin_block}")

        for b in results_by_block:
            if b["status"] == "SKIP (pin exhausted)":
                continue
            icon = "✅" if b["status"] == "PASS" else "❌"
            log(f"{icon} BLOCK {b['block']} ranks {b['rank_range'][0]}-{b['rank_range'][1]} | {b['status']}")
            for rank, pin_id in sorted(b["expected_pins"].items()):
                actual = merge_ids[rank] if rank < len(merge_ids) else None
                match_icon = "✅" if actual == pin_id else "❌"
                log(f"     {match_icon} rank={rank} pos={rank % BLOCK_SIZE} expected={pin_id} actual={actual}")
            for issue in b["issues"]:
                log(f"     ⚠️  {issue}")

        status = "FAIL" if fail_blocks else "PASS"

        log("\n--- reservedPositions (global_rank -> pin_id) ---")
    if not reserved:
        log("  (none)")
    else:
        for rank in sorted(reserved.keys()):
            log(f"  rank={rank:3d}  block={rank // BLOCK_SIZE + 1}  pos={rank % BLOCK_SIZE}  | {reserved[rank]}")    

    log("\n=== PIN USAGE SUMMARY ===")
    log(f"placement        : {name}")
    log(f"pin_items_total  : {len(pin_items)}")
    log(f"reserved_count   : {len(reserved)}")
    log(f"last_pin_block   : {last_pin_block}")
    log(f"pass_blocks      : {pass_blocks}")
    log(f"fail_blocks      : {fail_blocks}")
    log(f"STATUS           : {'✅ PASS' if status == 'PASS' else '❌ FAIL'}")



    result = {
        "test_key": TEST_KEY,
        "placement": name,
        "url": url,
        "pin_items_total": len(pin_items),
        "reserved_positions": {str(k): v for k, v in reserved.items()},
        "reserved_count": len(reserved),
        "merge_page_count": len(merge_ids),
        "last_pin_block": last_pin_block,
        "pass_blocks": pass_blocks,
        "fail_blocks": fail_blocks,
        "block_details": results_by_block,
        "status": status,
    }

    with open(result_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    log(f"\nSaved: {result_json}")
    log(f"Saved: {full_response_json}")
    log(f"Saved: {log_path}")

    if status == "FAIL":
        raise AssertionError(
            f"{TEST_KEY} [{name}] FAIL: fail_blocks={fail_blocks} "
            f"(pin_total={len(pin_items)}, reserved={len(reserved)})"
        )

    return result


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_verify_pin_logic_sfv_p7():
    result = run_check(PLACEMENTS[0])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| pin_total={result['pin_items_total']}",
          f"| fail_blocks={result['fail_blocks']}")


def test_verify_pin_logic_sfv_p8():
    result = run_check(PLACEMENTS[1])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| pin_total={result['pin_items_total']}",
          f"| fail_blocks={result['fail_blocks']}")
    
# ✅ Single entrypoint for test_all.py (วางท้ายสุด)
def test_verify_pin_logic():
    test_verify_pin_logic_sfv_p7()
    test_verify_pin_logic_sfv_p8()


if __name__ == "__main__":
    for p in PLACEMENTS:
        run_check(p)