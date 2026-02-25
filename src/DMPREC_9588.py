import json
import os
import pytest
import requests

REPORT_DIR = "reports/DMPREC-9588"
os.makedirs(REPORT_DIR, exist_ok=True)

UNIVERSAL_URL = (
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

METADATA_URL = (
    "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

TIMEOUT_SEC = 30
METADATA_BATCH_SIZE = 50
METADATA_NODES = [
    "append_bucketizes",
    "generate_candidates",
    "modifier_generate_candidates",
]


# =============================================================
# Helpers
# =============================================================
def fetch_metadata_batch(ids: list[str], fields: list[str]) -> dict:
    if not ids:
        return {}
    payload = {"parameters": {"id": ids, "fields": fields}}
    resp = requests.post(METADATA_URL, json=payload, timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return {item["id"]: item for item in items if "id" in item}


def fetch_metadata_all(ids: list[str], fields: list[str]) -> dict:
    result = {}
    for i in range(0, len(ids), METADATA_BATCH_SIZE):
        batch = ids[i: i + METADATA_BATCH_SIZE]
        print(f"  fetching metadata batch {i // METADATA_BATCH_SIZE + 1}: {len(batch)} ids")
        result.update(fetch_metadata_batch(batch, fields))
    return result


def run_check() -> dict:
    # ----------------------------------------------------------
    # Step 1: Call Universal API
    # ----------------------------------------------------------
    print("[Step 1] Calling Universal API...")
    resp = requests.get(UNIVERSAL_URL, timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    data = resp.json()

    with open(f"{REPORT_DIR}/universal_response.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    results = data.get("data", {}).get("results", {})

    # ----------------------------------------------------------
    # Step 2: Get final_ids
    # ----------------------------------------------------------
    final_ids = results.get("final_result", {}).get("result", {}).get("ids", [])
    final_ids = [x for x in final_ids if isinstance(x, str) and x.strip()]
    print(f"[Step 2] final_ids: {len(final_ids)}")
    assert final_ids, "final_result ids not found or empty"

    # ----------------------------------------------------------
    # Step 3: Build item_map from metadata nodes
    # ----------------------------------------------------------
    print("\n[Step 3] Building item lookup map from final_ids only...")
    item_map = {}
    for node_name in METADATA_NODES:
        node_data = results.get(node_name, {})
        node_result = node_data.get("result", {})
        candidates = (
            node_result.get("items", [])
            if isinstance(node_result, dict)
            else node_result
            if isinstance(node_result, list)
            else []
        )
        for item in candidates:
            if isinstance(item, dict) and item.get("id") in final_ids:
                if item["id"] not in item_map:
                    item_map[item["id"]] = {**item, "_found_in_node": node_name}

    print(f"  final_ids       : {len(final_ids)}")
    print(f"  Mapped from nodes: {len(item_map)}")

    missing_in_nodes = [fid for fid in final_ids if fid not in item_map]
    if missing_in_nodes:
        print(f"  ⚠️  IDs not found in any metadata node: {len(missing_in_nodes)}")

    # ----------------------------------------------------------
    # Step 4: Collect relate_content IDs
    # ----------------------------------------------------------
    print("\n[Step 4] Collecting relate_content IDs from final_ids...")
    final_id_to_relate = {}
    for fid in final_ids:
        relate = item_map.get(fid, {}).get("relate_content", [])
        final_id_to_relate[fid] = relate if isinstance(relate, list) else []

    all_relate_ids = [rid for ids in final_id_to_relate.values() for rid in ids]
    print(f"  Total relate_content IDs to check: {len(all_relate_ids)}")

    # ----------------------------------------------------------
    # Step 5: Fetch metadata for relate_content IDs
    # ----------------------------------------------------------
    print("\n[Step 5] Fetching metadata for relate_content IDs...")
    relate_meta_map = fetch_metadata_all(
        all_relate_ids, fields=["id", "content_type", "publish_date"]
    )
    print(f"  Metadata returned: {len(relate_meta_map)} items")

    with open(f"{REPORT_DIR}/relate_metadata.json", "w", encoding="utf-8") as f:
        json.dump(relate_meta_map, f, ensure_ascii=False, indent=2)

    # ----------------------------------------------------------
    # Step 6: Validate
    # ----------------------------------------------------------
    print("\n[Step 6] Validating logic...")
    pass_items = []
    fail_no_relate = []
    fail_no_ecommerce = []

    for fid in final_ids:
        relate_ids = final_id_to_relate[fid]

        if not relate_ids:
            fail_no_relate.append({
                "id": fid,
                "reason": "relate_content is empty or missing",
                "title": item_map.get(fid, {}).get("title", ""),
            })
            continue

        ecommerce_found = [
            rid for rid in relate_ids
            if relate_meta_map.get(rid, {}).get("content_type") == "ecommerce"
        ]

        if ecommerce_found:
            pass_items.append({
                "id": fid,
                "relate_content": relate_ids,
                "ecommerce_ids": ecommerce_found,
            })
        else:
            fail_no_ecommerce.append({
                "id": fid,
                "reason": "no relate_content with content_type=ecommerce",
                "relate_content": relate_ids,
                "relate_content_types": {
                    rid: relate_meta_map.get(rid, {}).get("content_type", "NOT_FOUND")
                    for rid in relate_ids
                },
                "title": item_map.get(fid, {}).get("title", ""),
            })

    # ----------------------------------------------------------
    # Step 7: Save & return summary
    # ----------------------------------------------------------
    total_fail = len(fail_no_relate) + len(fail_no_ecommerce)
    summary = {
        "total_final_ids": len(final_ids),
        "pass_count": len(pass_items),
        "fail_empty_relate_count": len(fail_no_relate),
        "fail_no_ecommerce_count": len(fail_no_ecommerce),
        "pass": total_fail == 0,
        "fail_empty_relate": fail_no_relate,
        "fail_no_ecommerce": fail_no_ecommerce,
    }

    with open(f"{REPORT_DIR}/validation_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(f"{REPORT_DIR}/pass_items.json", "w", encoding="utf-8") as f:
        json.dump(pass_items, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(f"  final_ids total          : {len(final_ids)}")
    print(f"  ✅ PASS (has ecommerce)  : {len(pass_items)}")
    print(f"  ❌ FAIL (empty relate)   : {len(fail_no_relate)}")
    print(f"  ❌ FAIL (no ecommerce)   : {len(fail_no_ecommerce)}")
    print("=" * 60)

    return summary


# =============================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =============================================================
def test_DMPREC_9588():
    summary = run_check()

    fail_msgs = []
    if summary["fail_empty_relate_count"] > 0:
        ids = [x["id"] for x in summary["fail_empty_relate"]]
        fail_msgs.append(
            f"FAIL empty relate_content ({summary['fail_empty_relate_count']} items): {ids[:10]}"
        )
    if summary["fail_no_ecommerce_count"] > 0:
        ids = [x["id"] for x in summary["fail_no_ecommerce"]]
        fail_msgs.append(
            f"FAIL no ecommerce in relate_content ({summary['fail_no_ecommerce_count']} items): {ids[:10]}"
        )

    assert not fail_msgs, "\n".join(fail_msgs)


if __name__ == "__main__":
    run_check()
