import requests
import json
from datetime import datetime
import os
import re

# ===================== CONFIG =====================
PLACEMENTS = [
    {
        "name": "sfv-p7",
        "test_key": "DMPREC-9585-P7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=BJq5rZqYzjgJ"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
        ),
        "live_every_n": 3,
    },
    {
        "name": "sfv-p8",
        "test_key": "DMPREC-9585-P8",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
            "&limit_seen_item=1"
        ),
        "live_every_n": 3,
    },
]

TIMEOUT_SEC = 20

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)


# =================================================
def make_paths(test_key: str, placement_name: str):
    safe_name = placement_name.replace("/", "_")
    log_txt = os.path.join(REPORT_DIR, f"{test_key}_{safe_name}_live_check.log")
    result_json = os.path.join(REPORT_DIR, f"{test_key}_{safe_name}_live_check_result.json")
    return log_txt, result_json


def tlog(log_file: str, msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


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
def extract_ids_from_items(items):
    ids = []
    if not isinstance(items, list):
        return ids

    for it in items:
        if isinstance(it, str):
            ids.append(normalize_activity_id(it))
            continue

        if isinstance(it, dict):
            for k in ("ActivityId", "activityId", "activity_id", "Id", "id"):
                v = it.get(k)
                if v is not None:
                    ids.append(normalize_activity_id(v))
                    break
    return ids


def get_results_root(j: dict):
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_merge_page_ids(results: dict):
    node = results.get("merge_page", {})
    result = node.get("result", {})
    items = result.get("items", [])

    out = []

    for it in items:
        if isinstance(it, str) and it.strip():
            out.append(normalize_activity_id(it))
            continue

        if not isinstance(it, dict):
            continue

        payload = it.get("payload", {})
        content_type = payload.get("content_type") if isinstance(payload, dict) else None

        # LIVE wrapper
        if content_type == "live":
            inner = it.get("items", [])
            if inner and isinstance(inner[0], dict):
                act = inner[0].get("ActivityId") or inner[0].get("activityId")
                if act:
                    out.append(normalize_activity_id(act))
                    continue

        if it.get("id") is not None:
            out.append(normalize_activity_id(it["id"]))
        elif it.get("Id") is not None:
            out.append(normalize_activity_id(it["Id"]))

    return out


def extract_live_activity_ids(results: dict):
    node = results.get("get_all_live_today", {})
    result = node.get("result", {})
    items = result.get("items", [])
    return extract_ids_from_items(items)


# =================================================
def check_live_positions_until_exhausted(merge_ids, live_ids, n=3):
    """
    Rule:
    - ตรวจทีละ block ขนาด n
    - ถ้ายังมี live เหลืออยู่ ต้องเจออย่างน้อย 1 live ในทุก block
    - ถ้าหลังจากเคยเจอ live แล้ว block ถัดไปไม่เจอ live -> ถือว่า live exhausted
    - ถ้า block แรก ๆ ไม่เจอ live เลย -> FAIL
    """
    live_set = {normalize_activity_id(x) for x in live_ids}

    failures = []
    seen_any_live = False

    for start in range(0, len(merge_ids), n):
        block = merge_ids[start:start + n]
        block_index = start // n + 1

        found_live = any(normalize_activity_id(mid) in live_set for mid in block)

        if found_live:
            seen_any_live = True
            continue

        if not seen_any_live:
            failures.append({
                "block": block_index,
                "items": block,
                "reason": "expected live in first block(s) but none found"
            })
            return failures, "FAIL: live never inserted"

        return [], "live exhausted"

    return [], "PASS"


# =================================================
def run_single_check(cfg: dict):
    name = cfg["name"]
    test_key = cfg.get("test_key", "LIVE-CHECK")
    url = cfg["url"]
    live_every_n = cfg.get("live_every_n", 3)

    log_txt, result_json = make_paths(test_key, name)

    open(log_txt, "w", encoding="utf-8").close()

    tlog(log_txt, f"TEST={test_key}")
    tlog(log_txt, f"PLACEMENT={name}")
    tlog(log_txt, f"URL={url}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    tlog(log_txt, f"HTTP={r.status_code}")
    r.raise_for_status()

    j = r.json()
    results = get_results_root(j)

    merge_ids = extract_merge_page_ids(results)
    live_ids = extract_live_activity_ids(results)

    tlog(log_txt, f"merge_count={len(merge_ids)}")
    tlog(log_txt, f"live_count={len(live_ids)}")
    tlog(log_txt, f"merge_ids={merge_ids}")
    tlog(log_txt, f"live_ids={live_ids}")

    if not merge_ids:
        result = {
            "placement": name,
            "test_key": test_key,
            "status": "FAIL",
            "reason": "merge_page returned no items",
            "failures": [{"block": 1, "items": []}],
        }
    elif not live_ids:
        result = {
            "placement": name,
            "test_key": test_key,
            "status": "PASS",
            "reason": "no live today",
            "failures": [],
        }
    else:
        failures, reason = check_live_positions_until_exhausted(
            merge_ids, live_ids, n=live_every_n
        )
        result = {
            "placement": name,
            "test_key": test_key,
            "status": "FAIL" if failures else "PASS",
            "reason": reason,
            "failures": failures,
        }

    with open(result_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    tlog(log_txt, f"FINAL_RESULT={json.dumps(result, ensure_ascii=False)}")

    return result


def run_all_checks():
    all_results = []

    for cfg in PLACEMENTS:
        try:
            result = run_single_check(cfg)
            all_results.append(result)
        except Exception as e:
            failed_result = {
                "placement": cfg.get("name"),
                "test_key": cfg.get("test_key"),
                "status": "FAIL",
                "reason": str(e),
                "failures": [],
            }
            all_results.append(failed_result)

            log_txt, result_json = make_paths(
                cfg.get("test_key", "LIVE-CHECK"),
                cfg.get("name", "unknown")
            )

            with open(result_json, "w", encoding="utf-8") as f:
                json.dump(failed_result, f, indent=2, ensure_ascii=False)

            tlog(log_txt, f"ERROR={str(e)}")

    failed = [x for x in all_results if x["status"] == "FAIL"]
    if failed:
        raise AssertionError(
            "Some placements failed: " +
            json.dumps(failed, ensure_ascii=False, indent=2)
        )

    return all_results


# =================================================
# ✅ PYTEST ENTRY
# =================================================
def test_verify_live_logic():
    print("RUN: DMPREC-9585 live check")
    results = run_all_checks()
    print("RESULTS:", json.dumps(results, ensure_ascii=False, indent=2))


# =================================================
if __name__ == "__main__":
    results = run_all_checks()
    print(json.dumps(results, ensure_ascii=False, indent=2))