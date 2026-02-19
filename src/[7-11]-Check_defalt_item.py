import requests
import json
import os
from datetime import datetime

# ===================== CONFIG =====================
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

EXPECTED_ITEMS_SIZE = 200  # ต้องได้ 200 เสมอ
LOG_TXT = "tc_final_result_default_items.log"
OUT_JSON = "tc_final_result_default_items.json"
OUT_FULL_RESPONSE = "tc_final_result_default_full_response.json"
# =================================================


def tlog(msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(LOG_TXT, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def dump_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def get_results_root(j: dict):
    data = j.get("data", {}) if isinstance(j.get("data", {}), dict) else {}
    results = data.get("results", {}) if isinstance(data.get("results", {}), dict) else {}
    return results


def extract_ids_from_items(items):
    """
    รองรับ:
    - list[str]
    - list[{"id": "..."}]
    """
    out = []
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


def extract_final_result(results: dict):
    """
    final_result.result.ids (list[str])
    final_result.result.items (list[{"id": "..."}] or list[str])
    final_result.result.items_size (int)
    """
    node = results.get("final_result", {})
    if not isinstance(node, dict):
        return None, None, None, "final_result node missing/not dict"

    res = node.get("result", {})
    if not isinstance(res, dict):
        return None, None, None, "final_result.result missing/not dict"

    ids = res.get("ids")
    if isinstance(ids, list):
        ids_list = [x for x in ids if isinstance(x, str) and x.strip()]
    else:
        ids_list = None

    items = res.get("items")
    items_list = extract_ids_from_items(items) if isinstance(items, list) else None

    items_size = res.get("items_size")
    items_size_val = items_size if isinstance(items_size, int) else None

    return ids_list, items_list, items_size_val, None


def main():
    # reset log
    open(LOG_TXT, "w", encoding="utf-8").close()

    tlog("START TC: final_result default items check")
    tlog(f"CWD={os.getcwd()}")
    tlog(f"URL={URL}")
    tlog(f"EXPECTED_ITEMS_SIZE={EXPECTED_ITEMS_SIZE}")

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
        tlog(r.text[:1200])
        return

    dump_json(OUT_FULL_RESPONSE, j)
    tlog(f"Saved full response: {OUT_FULL_RESPONSE}")

    if r.status_code != 200:
        tlog("Non-200 response (see full response file)")
        return

    results = get_results_root(j)

    ids_list, items_list, items_size_val, err = extract_final_result(results)
    if err:
        tlog(f"FAIL: {err}")
        dump_json(OUT_JSON, {"url": URL, "error": err})
        tlog(f"Saved JSON: {OUT_JSON}")
        return

    # --------- Validate counts ----------
    problems = []

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

    # --------- Report ----------
    if problems:
        tlog("❌ FAIL")
        for p in problems:
            tlog(f"- {p}")
    else:
        tlog("✅ PASS: final_result has 200 ids, 200 items, items_size=200")

    out = {
        "url": URL,
        "expected_items_size": EXPECTED_ITEMS_SIZE,
        "final_result": {
            "ids_count": ids_count,
            "items_count": items_count,
            "items_size": items_size_val,
            "ids_sample_10": ids_list[:10] if isinstance(ids_list, list) else None,
            "items_sample_10": items_list[:10] if isinstance(items_list, list) else None
        },
        "problems": problems,
    }
    dump_json(OUT_JSON, out)

    tlog(f"Saved JSON: {OUT_JSON}")
    tlog(f"Saved LOG: {LOG_TXT}")
    tlog("END")


if __name__ == "__main__":
    main()
