import json
import os
from typing import Any, Dict, List, Tuple

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9589"

DEFAULT_URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p7"
    "?shelfId=Kaw6MLVzPWmo"
    "&total_candidates=200"
    "&pool_limit_category_items=60"
    "&language=th"
    "&pool_tophit_date=365"
    "&limit=100"
    "&userId=null"
    "&pseudoId=null"
    "&cursor=1"
    "&ga_id=100118391.0851155978"
    "&is_use_live=true"
    "&verbose=debug"
    "&pool_latest_date=365"
)

URL = os.getenv("URL", DEFAULT_URL)
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "25"))

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

RAW_PATH = f"{ART_DIR}/universal_debug_response.json"
REPORT_PATH = f"{ART_DIR}/bucketize_partner_related_report.json"

# =====================================================
# Nodes ที่ต้องตรวจ
# =====================================================
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
def run_check() -> Dict[str, Any]:
    r = requests.get(URL, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()

    with open(RAW_PATH, "w", encoding="utf-8") as f:
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
        "url": URL,
        "target_nodes": TARGET_NODES,
        "missing_nodes": missing_nodes,
        "allowed_partner_related": [ALLOWED_PARTNER_ID],
        "total_items_all": total_all,
        "failed_all": failed_all,
        "node_reports": node_reports,
        "status": "FAIL" if (failed_all > 0 or missing_nodes) else "PASS",
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump({**result, "fail_rows_all": all_fail_rows}, f, ensure_ascii=False, indent=2)

    print(f"\n  Saved -> {REPORT_PATH}")

    return result


# =====================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =====================================================
def test_DMPREC_9589():
    result = run_check()
    print("RESULT:", result["status"], "failed_all=", result["failed_all"])

    fail_msgs = []

    if result["missing_nodes"]:
        fail_msgs.append(
            f"Missing nodes in response: {result['missing_nodes']}"
        )

    if result["failed_all"] > 0:
        sample = [
            f"{row['node']}[{row['bucket']}] id={row['id']} partner_related={row['partner_related']!r}"
            for row in result.get("fail_rows_all", [])[:10]  # ดึงจาก result ได้เลย
        ]
        # --- rebuild fail_rows_all จาก node_reports เพราะ run_check ไม่ return ไว้ ---
        all_fail_rows = [
            row
            for rep in result["node_reports"]
            for row in rep.get("fail_all", [])
        ]
        sample = [
            f"{row['node']}[{row['bucket']}] id={row['id']} partner_related={row['partner_related']!r}"
            for row in all_fail_rows[:10]
        ]
        fail_msgs.append(
            f"partner_related invalid count={result['failed_all']}. sample={sample}"
        )

    assert not fail_msgs, f"{TEST_KEY} FAIL:\n" + "\n".join(fail_msgs)


if __name__ == "__main__":
    run_check()
