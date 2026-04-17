import requests
import json
import os
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional

# ===================== CONFIG =====================
PLACEMENTS = [
    {
        "name": "sfv-p7",
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
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p6"
            "?shelfId=zmEXe3EQnXDk"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=30"
            "&limit=3"
            "&limit_seen_item=1"
        ),
    },
]
TIMEOUT = 30
NODE_NAME = "bucketize_tophit_sfv"
WINDOW_DAYS = 30

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# ==================================================

# ---------------------- helpers ----------------------
def parse_iso_z(dt_str: str) -> datetime:
    """
    Parse ISO datetime like '2026-01-27T17:00:00.000Z' into aware datetime (UTC).
    """
    if not isinstance(dt_str, str) or not dt_str:
        raise ValueError(f"publish_date is not a valid string: {dt_str!r}")
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)

def deep_find_node(obj: Any, target_node_name: str) -> Optional[Dict[str, Any]]:
    """
    Recursively find node named target_node_name in debug response.
    Supports:
      - {"name": "...", "result": {...}}
      - {"bucketize_latest_sfv": {...}}
    """
    if isinstance(obj, dict):
        if target_node_name in obj and isinstance(obj[target_node_name], dict):
            return obj[target_node_name]
        if obj.get("name") == target_node_name:
            return obj
        for v in obj.values():
            found = deep_find_node(v, target_node_name)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = deep_find_node(v, target_node_name)
            if found:
                return found
    return None

def validate_result_groups(result_obj: Dict[str, Any], window_days: int, now_utc: datetime) -> Dict[str, Any]:
    min_dt = now_utc - timedelta(days=window_days)

    report = {
        "now_utc": now_utc.isoformat(),
        "window_days": window_days,
        "min_utc": min_dt.isoformat(),
        "max_utc": now_utc.isoformat(),
        "groups": {},
        "summary": {
            "total_items_checked": 0,
            "total_groups_checked": 0,
            "groups_with_any_issue": 0,
        },
    }

    for group_name, items in result_obj.items():
        if not isinstance(items, list):
            continue

        group_report = {
            "items_count": len(items),
            "order_violations": [],
            "window_violations": [],
        }

        prev_dt = None
        for idx, it in enumerate(items):
            report["summary"]["total_items_checked"] += 1
            item_id = it.get("id")
            pd_raw = it.get("publish_date")

            try:
                pd_dt = parse_iso_z(pd_raw)
            except Exception as e:
                group_report["window_violations"].append({
                    "index": idx,
                    "id": item_id,
                    "publish_date": pd_raw,
                    "reason": f"parse_error: {e}",
                })
                prev_dt = None
                continue

            # window check (last 28 days)
            if pd_dt < min_dt or pd_dt > now_utc:
                group_report["window_violations"].append({
                    "index": idx,
                    "id": item_id,
                    "publish_date": pd_raw,
                    "publish_utc": pd_dt.isoformat(),
                    "reason": "out_of_28_day_window",
                })

            # order check: desc (allow equal)
            if prev_dt is not None and pd_dt > prev_dt:
                group_report["order_violations"].append({
                    "index": idx,
                    "id": item_id,
                    "publish_utc": pd_dt.isoformat(),
                    "prev_publish_utc": prev_dt.isoformat(),
                    "reason": "publish_date_increased (not desc)",
                })

            prev_dt = pd_dt

        has_issue = bool(group_report["order_violations"] or group_report["window_violations"])
        report["groups"][group_name] = group_report
        report["summary"]["total_groups_checked"] += 1
        if has_issue:
            report["summary"]["groups_with_any_issue"] += 1

    return report

def print_report(report: Dict[str, Any], show_top_n: int = 10) -> None:
    print("=== publish_date validation (bucketize_latest_sfv) ===")
    print(f"NOW (UTC):  {report['now_utc']}")
    print(f"WINDOW:     last {report['window_days']} days")
    print(f"RANGE UTC:  [{report['min_utc']} .. {report['max_utc']}]")
    print()

    s = report["summary"]
    print(f"Groups checked: {s['total_groups_checked']}")
    print(f"Items checked:  {s['total_items_checked']}")
    print(f"Groups with issues: {s['groups_with_any_issue']}")
    print()

    for gname, g in report["groups"].items():
        if not g["order_violations"] and not g["window_violations"]:
            continue

        print(f"--- Issues in group: {gname} (items={g['items_count']}) ---")
        if g["order_violations"]:
            print(f"Order violations: {len(g['order_violations'])} (top {show_top_n})")
            for x in g["order_violations"][:show_top_n]:
                print(f"  idx={x['index']} id={x['id']} publish={x['publish_utc']} prev={x['prev_publish_utc']}")
        if g["window_violations"]:
            print(f"Window violations: {len(g['window_violations'])} (top {show_top_n})")
            for x in g["window_violations"][:show_top_n]:
                print(f"  idx={x['index']} id={x.get('id')} publish_raw={x.get('publish_date')} reason={x.get('reason')}")
        print()

# =================================================
# ✅ PYTEST ENTRY
# =================================================
def run_check(placement: dict) -> dict:
    """รัน check สำหรับ placement เดียว และ return summary dict"""
    name = placement["name"]
    url = placement["url"]

    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    now_utc = datetime.now(ZoneInfo("Asia/Bangkok")).astimezone(timezone.utc)

    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"placement": name, "status": "ERROR", "error": str(e), "issues": -1}

    node = deep_find_node(data, NODE_NAME)
    if not node:
        return {"placement": name, "status": "ERROR",
                "error": f"node '{NODE_NAME}' not found", "issues": -1}

    result_obj = node.get("result")
    if not isinstance(result_obj, dict):
        return {"placement": name, "status": "ERROR",
                "error": "result is not a dict", "issues": -1}

    report = validate_result_groups(result_obj, window_days=WINDOW_DAYS, now_utc=now_utc)
    issues = report["summary"]["groups_with_any_issue"]

    out_path = f"{art_dir}/tc_latest_publish_date.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    status = "PASS" if issues == 0 else "FAIL"
    print(f"[{name}] {status}: issues={issues}")
    return {"placement": name, "status": status, "issues": issues}


def _assert_result(summary: dict):
    assert summary.get("status") != "ERROR", (
        f"[{summary['placement']}] error: {summary.get('error')}"
    )
    assert summary["issues"] == 0, (
        f"[{summary['placement']}] {summary['issues']} group(s) มีปัญหา publish_date "
        f"(window={WINDOW_DAYS} days, node={NODE_NAME})"
    )


def test_latest_sfv_publish_date_sfv_p7():
    """latest sfv-p7: bucketize_tophit_sfv publish_date check"""
    _assert_result(run_check(PLACEMENTS[0]))


def test_latest_sfv_publish_date_sfv_p8():
    """latest sfv-p8: bucketize_tophit_sfv publish_date check"""
    _assert_result(run_check(PLACEMENTS[1]))


def test_latest_sfv_publish_date():
    """รันทั้ง p7 + p8 ในครั้งเดียว"""
    test_latest_sfv_publish_date_sfv_p7()
    test_latest_sfv_publish_date_sfv_p8()


# ---------------------- main ----------------------
if __name__ == "__main__":
    for p in PLACEMENTS:
        result = run_check(p)
        if result["status"] == "ERROR":
            print(f"  error: {result.get('error')}")
