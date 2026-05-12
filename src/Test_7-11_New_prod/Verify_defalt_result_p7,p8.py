import requests
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9590"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "test_key": "DMPREC-9590-P7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=zmEXe3EQnXDk"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
        ),
    },
    {
        "name": "sfv-p6",
        "test_key": "DMPREC-9590-P6",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p6"
            "?shelfId=zmEXe3EQnXDk"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&limit=3"
            "&limit_seen_item=1"
        ),
    },
]

TIMEOUT_SEC = 20
EXPECTED_ITEMS_SIZE = 200

REPORT_DIR = "reports"
ART_DIR = os.path.join(REPORT_DIR, TEST_KEY)
os.makedirs(ART_DIR, exist_ok=True)


# =================================================
def make_paths(test_key: str, placement_name: str):
    safe_name = placement_name.replace("/", "_")
    base = f"{test_key}_{safe_name}"

    log_txt = os.path.join(ART_DIR, f"{base}_final_result_default_items.log")
    out_json = os.path.join(ART_DIR, f"{base}_final_result_default_items.json")
    out_full_response = os.path.join(ART_DIR, f"{base}_final_result_default_full_response.json")

    return log_txt, out_json, out_full_response


def tlog(log_file: str, msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_file, "a", encoding="utf-8") as f:
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


def extract_final_result(
    results: dict,
) -> Tuple[Optional[List[str]], Optional[List[str]], Optional[int], Optional[str]]:
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
def run_single_check(cfg: Dict[str, Any]) -> Dict[str, Any]:
    name = cfg["name"]
    test_key = cfg.get("test_key", TEST_KEY)
    url = cfg["url"]

    log_txt, out_json, out_full_response = make_paths(test_key, name)

    # reset log
    open(log_txt, "w", encoding="utf-8").close()

    tlog(log_txt, f"TEST={test_key}")
    tlog(log_txt, f"PLACEMENT={name}")
    tlog(log_txt, f"CWD={os.getcwd()}")
    tlog(log_txt, f"URL={url}")
    tlog(log_txt, f"EXPECTED_ITEMS_SIZE={EXPECTED_ITEMS_SIZE}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    tlog(log_txt, f"HTTP={r.status_code}")
    r.raise_for_status()

    j = r.json()
    dump_json(out_full_response, j)
    tlog(log_txt, f"Saved full response: {out_full_response}")

    results = get_results_root(j)

    ids_list, items_list, items_size_val, err = extract_final_result(results)
    if err:
        fail_out = {
            "test_key": test_key,
            "placement": name,
            "url": url,
            "status": "FAIL",
            "error": err,
        }
        dump_json(out_json, fail_out)
        raise AssertionError(f"{test_key} FAIL [{name}]: {err}")

    problems: List[str] = []

    ids_count = len(ids_list) if isinstance(ids_list, list) else None
    items_count = len(items_list) if isinstance(items_list, list) else None

    tlog(log_txt, "=== Extracted ===")
    tlog(log_txt, f"final_result.result.ids_count={ids_count}")
    tlog(log_txt, f"final_result.result.items_count={items_count}")
    tlog(log_txt, f"final_result.result.items_size={items_size_val}")

    # Must exist
    if ids_list is None:
        problems.append("final_result.result.ids missing/not list")
    if items_list is None:
        problems.append("final_result.result.items missing/not list")
    if items_size_val is None:
        problems.append("final_result.result.items_size missing/not int")

    # Must be <= EXPECTED_ITEMS_SIZE
    if ids_count is not None and ids_count > EXPECTED_ITEMS_SIZE:
        problems.append(f"ids_count > {EXPECTED_ITEMS_SIZE} (got {ids_count})")
    if items_count is not None and items_count > EXPECTED_ITEMS_SIZE:
        problems.append(f"items_count > {EXPECTED_ITEMS_SIZE} (got {items_count})")
    if items_size_val is not None and items_size_val > EXPECTED_ITEMS_SIZE:
        problems.append(f"items_size > {EXPECTED_ITEMS_SIZE} (got {items_size_val})")

    status = "FAIL" if problems else "PASS"

    out = {
        "test_key": test_key,
        "placement": name,
        "url": url,
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

    dump_json(out_json, out)

    tlog(log_txt, f"Saved JSON: {out_json}")
    tlog(log_txt, f"Saved LOG : {log_txt}")

    if problems:
        raise AssertionError(f"{test_key} FAIL [{name}]: {problems}")

    return out


def run_all_checks() -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []

    for cfg in PLACEMENTS:
        try:
            result = run_single_check(cfg)
            all_results.append(result)
        except Exception as e:
            failed_result = {
                "test_key": cfg.get("test_key", TEST_KEY),
                "placement": cfg.get("name"),
                "url": cfg.get("url"),
                "status": "FAIL",
                "problems": [str(e)],
            }
            all_results.append(failed_result)

            log_txt, out_json, _ = make_paths(
                cfg.get("test_key", TEST_KEY),
                cfg.get("name", "unknown")
            )
            dump_json(out_json, failed_result)
            tlog(log_txt, f"ERROR={str(e)}")

    failed = [x for x in all_results if x["status"] == "FAIL"]
    if failed:
        raise AssertionError(
            "Some placements failed:\n" +
            json.dumps(failed, ensure_ascii=False, indent=2)
        )

    return all_results


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_verify_default_result():
    results = run_all_checks()
    print("RESULTS:", json.dumps(results, ensure_ascii=False, indent=2))


# =================================================
if __name__ == "__main__":
    results = run_all_checks()
    print(json.dumps(results, ensure_ascii=False, indent=2))