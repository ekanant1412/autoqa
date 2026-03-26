import json
import os
from typing import Any, Dict, List, Tuple

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9589"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=60"
            "&language=th"
            "&pool_tophit_date=365"
            "&limit=100"
            "&userId=null"
            "&pseudoId=null"
            "&cursor=1"
            "&ga_id=999999999.999999999"
            "&is_use_live=true"
            "&verbose=debug"
            "&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
        ),
    },
    {
        "name": "sfv-p8",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=60"
            "&language=th"
            "&pool_tophit_date=365"
            "&limit=100"
            "&userId=null"
            "&pseudoId=null"
            "&cursor=1"
            "&ga_id=999999999.999999999"
            "&is_use_live=true"
            "&verbose=debug"
            "&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
        ),
    },
]

TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "25"))

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

TARGET_NODES = [
    "bucketize_tophit_sfv",
    "bucketize_latest_sfv",
    "bucketize_latest_ugc_sfv",
    "bucketize_tophit_ugc_sfv",
]

ALLOWED_PARTNER_ID = "AN9PjZR1wEol"


# =====================================================
# Helpers
# =====================================================
def deep_find_nodes(obj: Any, target_names: List[str]) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if obj.get("name") in target_names:
            found.append(obj)
        for v in obj.values():
            found.extend(deep_find_nodes(v, target_names))
    elif isinstance(obj, list):
        for it in obj:
            found.extend(deep_find_nodes(it, target_names))
    return found


def iter_items_from_result(result: Any) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(result, list):
        for it in result:
            if isinstance(it, dict):
                out.append(("_list_", it))
        return out
    if isinstance(result, dict):
        for bucket_name, v in result.items():
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        out.append((str(bucket_name), it))
        return out
    return out


def is_partner_allowed(partner_value: Any) -> bool:
    if isinstance(partner_value, str):
        return partner_value == ALLOWED_PARTNER_ID
    if isinstance(partner_value, list):
        return ALLOWED_PARTNER_ID in partner_value
    return False


def validate_partner_related_allowed(node: Dict[str, Any]) -> Dict[str, Any]:
    node_name = node.get("name", "(no_name)")
    result = node.get("result")

    report = {
        "node": node_name,
        "total_items": 0,
        "failed_items": 0,
        "pass_items": [],
        "fail_samples": [],
        "fail_all": [],
        "note": "",
    }

    if result is None:
        report["note"] = "result is null/None (0 items) - treated as OK"
        return report

    pairs = iter_items_from_result(result)
    if not pairs:
        report["note"] = "result has no iterable item lists (0 items) - treated as OK"
        return report

    for bucket_name, it in pairs:
        report["total_items"] += 1
        item_id = it.get("id") or it.get("content_id") or it.get("_id") or "(no_id)"
        partner_value = it.get("partner_related", None)

        if is_partner_allowed(partner_value):
            report["pass_items"].append({
                "id": item_id,
                "partner_related": partner_value,
            })
        else:
            report["failed_items"] += 1
            row = {
                "node": node_name,
                "bucket": bucket_name,
                "id": item_id,
                "partner_related": partner_value,
                "reason": f"partner_related must be '{ALLOWED_PARTNER_ID}'",
            }
            report["fail_all"].append(row)
            if len(report["fail_samples"]) < 20:
                report["fail_samples"].append(row)

    return report


# =====================================================
# Core logic
# =====================================================
def run_check(placement: dict) -> Dict[str, Any]:
    name = placement["name"]
    url = placement["url"]

    placement_dir = f"{ART_DIR}/{name}"
    os.makedirs(placement_dir, exist_ok=True)

    raw_path = f"{placement_dir}/universal_debug_response.json"
    report_path = f"{placement_dir}/bucketize_partner_related_report.json"

    print(f"\n{'='*60}")
    print(f"  PLACEMENT: {name}")
    print(f"{'='*60}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    nodes = deep_find_nodes(data, TARGET_NODES)
    found_names = {n.get("name") for n in nodes if isinstance(n, dict)}
    missing_nodes = [n for n in TARGET_NODES if n not in found_names]

    node_reports = []
    total_all = 0
    failed_all = 0
    all_fail_rows = []

    for node in sorted(nodes, key=lambda x: x.get("name", "")):
        rep = validate_partner_related_allowed(node)
        node_reports.append(rep)
        total_all += rep["total_items"]
        failed_all += rep["failed_items"]
        all_fail_rows.extend(rep.get("fail_all", []))

        status_icon = "✅" if rep["failed_items"] == 0 else "❌"
        print(f"\n  {status_icon} {rep['node']}  total={rep['total_items']}  failed={rep['failed_items']}")

        if rep["pass_items"]:
            print(f"    ✅ PASS items ({len(rep['pass_items'])}):")
            for p in rep["pass_items"]:
                print(f"      - {p['id']}  partner_related={p['partner_related']!r}")

        if rep["fail_all"]:
            print(f"    ❌ FAIL items ({len(rep['fail_all'])}):")
            for row in rep["fail_all"][:10]:
                print(f"      - {row['id']}  partner_related={row['partner_related']!r}")

    result = {
        "test_key": TEST_KEY,
        "placement": name,
        "url": url,
        "target_nodes": TARGET_NODES,
        "missing_nodes": missing_nodes,
        "allowed_partner_related": [ALLOWED_PARTNER_ID],
        "total_items_all": total_all,
        "failed_all": failed_all,
        "node_reports": node_reports,
        "status": "FAIL" if (failed_all > 0 or missing_nodes) else "PASS",
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({**result, "fail_rows_all": all_fail_rows}, f, ensure_ascii=False, indent=2)

    print(f"\n  Saved -> {report_path}")

    return result


# =====================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =====================================================
def test_verify_owner_711_sfv_p7():
    result = run_check(PLACEMENTS[0])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| failed_all={result['failed_all']}")

    fail_msgs = []
    if result["missing_nodes"]:
        fail_msgs.append(f"Missing nodes: {result['missing_nodes']}")
    if result["failed_all"] > 0:
        all_fail_rows = [
            row for rep in result["node_reports"]
            for row in rep.get("fail_all", [])
        ]
        sample = [
            f"{row['node']}[{row['bucket']}] id={row['id']} partner_related={row['partner_related']!r}"
            for row in all_fail_rows[:10]
        ]
        fail_msgs.append(f"partner_related invalid count={result['failed_all']}. sample={sample}")

    assert not fail_msgs, f"{TEST_KEY} [sfv-p7] FAIL:\n" + "\n".join(fail_msgs)


def test_verify_owner_711_sfv_p8():
    result = run_check(PLACEMENTS[1])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| failed_all={result['failed_all']}")

    fail_msgs = []
    if result["missing_nodes"]:
        fail_msgs.append(f"Missing nodes: {result['missing_nodes']}")
    if result["failed_all"] > 0:
        all_fail_rows = [
            row for rep in result["node_reports"]
            for row in rep.get("fail_all", [])
        ]
        sample = [
            f"{row['node']}[{row['bucket']}] id={row['id']} partner_related={row['partner_related']!r}"
            for row in all_fail_rows[:10]
        ]
        fail_msgs.append(f"partner_related invalid count={result['failed_all']}. sample={sample}")

    assert not fail_msgs, f"{TEST_KEY} [sfv-p8] FAIL:\n" + "\n".join(fail_msgs)

def test_verify_owner_711():
    test_verify_owner_711_sfv_p7()
    test_verify_owner_711_sfv_p8()


if __name__ == "__main__":
    for p in PLACEMENTS:
        run_check(p)