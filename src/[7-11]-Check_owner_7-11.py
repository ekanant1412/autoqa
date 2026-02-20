import json
import os
import sys
from typing import Any, Dict, List, Tuple

import requests

# =====================================================
# 1) URL (ใช้ของคุณเป็น default แต่ override ได้ด้วย env URL)
# =====================================================
DEFAULT_URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p7"
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

URL = os.getenv("URL", DEFAULT_URL)
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "25"))

# =====================================================
# 2) Nodes ที่ต้องตรวจ (แก้เพิ่ม/ลดได้)
# =====================================================
TARGET_NODES = [
    "bucketize_tophit_sfv",
    "bucketize_latest_sfv",
    "bucketize_latest_ugc_sfv",
    "bucketize_tophit_ugc_sfv",
]

# =====================================================
# 3) partner_related allowed values
# =====================================================
ALLOWED_PARTNER_ID = "AN9PjZR1wEol"


# =====================================================
# Find nodes by "name"
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


# =====================================================
# Iterate items from node.result
# - result can be dict: {bucket_name: [items...], ...}
# - or list: [items...]
# =====================================================
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


# =====================================================
# Allowed check (FIX: handle list safely)
# partner_related must be: None OR [] OR "AN9PjZR1wEol"
# =====================================================
def is_partner_allowed(partner_value: Any) -> bool:
    if partner_value is None:
        return True
    if partner_value == ALLOWED_PARTNER_ID:
        return True
    if isinstance(partner_value, list) and len(partner_value) == 0:
        return True
    return False


# =====================================================
# Validate: partner_related must be allowed
# =====================================================
def validate_partner_related_allowed(node: Dict[str, Any]) -> Dict[str, Any]:
    node_name = node.get("name", "(no_name)")
    result = node.get("result")

    report = {
        "node": node_name,
        "total_items": 0,
        "failed_items": 0,
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

        if not is_partner_allowed(partner_value):
            report["failed_items"] += 1
            row = {
                "node": node_name,
                "bucket": bucket_name,
                "id": item_id,
                "partner_related": partner_value,
                "reason": f"partner_related must be null, [] or '{ALLOWED_PARTNER_ID}'",
            }
            report["fail_all"].append(row)
            if len(report["fail_samples"]) < 20:
                report["fail_samples"].append(row)

    return report


# =====================================================
# Main
# =====================================================
def main():
    print("Fetching URL:")
    print(URL)
    print("Timeout:", TIMEOUT_SEC, "sec")

    r = requests.get(URL, timeout=TIMEOUT_SEC)
    print("HTTP:", r.status_code)
    r.raise_for_status()
    data = r.json()

    # Save raw response for evidence
    os.makedirs("artifacts", exist_ok=True)
    raw_path = "artifacts/universal_debug_response.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved raw response:", raw_path)

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

    print("\n========== Partner Related Check (Bucketize Nodes) ==========")
    print("Allowed partner_related:", "null", "[]", f"'{ALLOWED_PARTNER_ID}'")
    print("Target nodes:", ", ".join(TARGET_NODES))
    print("")

    for rep in node_reports:
        print(f"NODE: {rep['node']}")
        print(f"  total_items  : {rep['total_items']}")
        print(f"  failed_items : {rep['failed_items']}")
        if rep["note"]:
            print(f"  note         : {rep['note']}")
        if rep["failed_items"] > 0:
            print("  FAIL samples:")
            for row in rep["fail_samples"]:
                print(
                    f"    - node={row['node']} bucket={row['bucket']} id={row['id']} "
                    f"partner_related={row['partner_related']!r}"
                )
                print(f"      reason: {row['reason']}")
        print("")

    if missing_nodes:
        print("⚠️ Missing nodes (not found in response JSON):", ", ".join(missing_nodes))
        print("   (ไม่ถือว่า fail ถ้า response ไม่ได้ include node นั้นใน verbose/debug)\n")

    # Save report
    report_path = "artifacts/bucketize_partner_related_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "url": URL,
                "target_nodes": TARGET_NODES,
                "missing_nodes": missing_nodes,
                "allowed_partner_related": [None, [], ALLOWED_PARTNER_ID],
                "total_items_all": total_all,
                "failed_all": failed_all,
                "fail_rows_all": all_fail_rows,
                "node_reports": node_reports,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print("Saved report:", report_path)

    if failed_all > 0:
        print(f"\n❌ FAIL: พบ item ที่ partner_related ไม่ตรงตาม allowed = {failed_all} รายการ")
        sys.exit(1)

    print("\n✅ PASS: ทุก item ใน bucketize nodes partner_related อยู่ใน allowed (null/[]/'AN9PjZR1wEol')")
    sys.exit(0)


if __name__ == "__main__":
    main()