import json
import os
import requests

REPORT_DIR = "reports/DMPREC-9588"
os.makedirs(REPORT_DIR, exist_ok=True)

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
            "&limit_seen_item=1"
        ),
    },
]

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


def run_check(placement: dict) -> dict:
    name = placement["name"]
    url = placement["url"]
    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  PLACEMENT: {name}")
    print(f"{'='*60}")

    # ----------------------------------------------------------
    # Step 1: Call Universal API
    # ----------------------------------------------------------
    print("[Step 1] Calling Universal API...")
    resp = requests.get(url, timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    data = resp.json()

    with open(f"{art_dir}/universal_response.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    results = data.get("data", {}).get("results", {})

    # ----------------------------------------------------------
    # Step 2: Get final_ids
    # ----------------------------------------------------------
    final_ids = results.get("final_result", {}).get("result", {}).get("ids", [])
    final_ids = [x for x in final_ids if isinstance(x, str) and x.strip()]
    print(f"[Step 2] final_ids: {len(final_ids)}")
    assert final_ids, f"[{name}] final_result ids not found or empty"

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
            if isinstance(item, dict):
                item_id = item.get("id")
                if item_id in final_ids and item_id not in item_map:
                    item_map[item_id] = {**item, "_found_in_node": node_name}

    print(f"  final_ids        : {len(final_ids)}")
    print(f"  Mapped from nodes: {len(item_map)}")

    missing_in_nodes = [fid for fid in final_ids if fid not in item_map]
    if missing_in_nodes:
        print(f"  ⚠️ IDs not found in any metadata node: {len(missing_in_nodes)}")

    # ----------------------------------------------------------
    # Step 4: Collect relate_content IDs
    # ----------------------------------------------------------
    print("\n[Step 4] Collecting relate_content IDs from final_ids...")
    final_id_to_relate = {}

    for fid in final_ids:
        relate = item_map.get(fid, {}).get("relate_content", [])
        final_id_to_relate[fid] = relate if isinstance(relate, list) else []

    all_relate_ids = []
    for ids in final_id_to_relate.values():
        all_relate_ids.extend(ids)

    # dedupe while preserving order
    all_relate_ids = list(
        dict.fromkeys(
            [rid for rid in all_relate_ids if isinstance(rid, str) and rid.strip()]
        )
    )

    print(f"  Total unique relate_content IDs to check: {len(all_relate_ids)}")

    # ----------------------------------------------------------
    # Step 5: Fetch metadata for relate_content IDs
    # ----------------------------------------------------------
    print("\n[Step 5] Fetching metadata for relate_content IDs...")
    relate_meta_map = fetch_metadata_all(
        all_relate_ids,
        fields=["id", "content_type", "publish_date", "partner_related"]
    )
    print(f"  Metadata returned: {len(relate_meta_map)} items")

    with open(f"{art_dir}/relate_metadata.json", "w", encoding="utf-8") as f:
        json.dump(relate_meta_map, f, ensure_ascii=False, indent=2)

    # ----------------------------------------------------------
    # Step 6: Validate
    # ----------------------------------------------------------
    print("\n[Step 6] Validating logic...")

    EXPECTED_PARTNER = "AN9PjZR1wEol"

    pass_items = []
    fail_no_relate = []
    fail_no_ecommerce = []
    fail_no_partner_related = []

    for fid in final_ids:
        relate_ids = final_id_to_relate.get(fid, [])
        item_title = item_map.get(fid, {}).get("title", "")
        found_node = item_map.get(fid, {}).get("_found_in_node", "")

        if not relate_ids:
            fail_no_relate.append({
                "id": fid,
                "reason": "relate_content is empty or missing",
                "title": item_title,
                "found_in_node": found_node,
            })
            continue

        ecommerce_ids = []
        ecommerce_partner_match_ids = []

        for rid in relate_ids:
            meta = relate_meta_map.get(rid, {})
            content_type = meta.get("content_type")
            partner_related = meta.get("partner_related")

            if content_type == "ecommerce":
                ecommerce_ids.append(rid)

                if partner_related == EXPECTED_PARTNER:
                    ecommerce_partner_match_ids.append(rid)

        if not ecommerce_ids:
            fail_no_ecommerce.append({
                "id": fid,
                "reason": "no relate_content with content_type=ecommerce",
                "title": item_title,
                "found_in_node": found_node,
                "relate_content": relate_ids,
                "relate_content_types": {
                    rid: relate_meta_map.get(rid, {}).get("content_type", "NOT_FOUND")
                    for rid in relate_ids
                },
                "partner_related_values": {
                    rid: relate_meta_map.get(rid, {}).get("partner_related", "NOT_FOUND")
                    for rid in relate_ids
                },
            })
            continue

        if not ecommerce_partner_match_ids:
            fail_no_partner_related.append({
                "id": fid,
                "reason": f"found ecommerce relate_content but none with partner_related={EXPECTED_PARTNER}",
                "title": item_title,
                "found_in_node": found_node,
                "relate_content": relate_ids,
                "ecommerce_ids": ecommerce_ids,
                "partner_related_values": {
                    rid: relate_meta_map.get(rid, {}).get("partner_related", "NOT_FOUND")
                    for rid in ecommerce_ids
                },
            })
            continue

        pass_items.append({
            "id": fid,
            "title": item_title,
            "found_in_node": found_node,
            "relate_content": relate_ids,
            "ecommerce_ids": ecommerce_ids,
            "matched_partner_related_ids": ecommerce_partner_match_ids,
        })

    # ----------------------------------------------------------
    # Step 7: Save summary/report
    # ----------------------------------------------------------
    total_fail = (
        len(fail_no_relate)
        + len(fail_no_ecommerce)
        + len(fail_no_partner_related)
    )

    summary = {
        "placement": name,
        "url": url,
        "total_final_ids": len(final_ids),
        "mapped_items": len(item_map),
        "missing_in_nodes_count": len(missing_in_nodes),
        "pass_count": len(pass_items),
        "fail_empty_relate_count": len(fail_no_relate),
        "fail_no_ecommerce_count": len(fail_no_ecommerce),
        "fail_no_partner_related_count": len(fail_no_partner_related),
        "pass": total_fail == 0,
    }

    report = {
        "summary": summary,
        "missing_in_nodes": missing_in_nodes,
        "pass_items": pass_items,
        "fail_no_relate": fail_no_relate,
        "fail_no_ecommerce": fail_no_ecommerce,
        "fail_no_partner_related": fail_no_partner_related,
    }

    with open(f"{art_dir}/validation_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(f"{art_dir}/validation_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(f"{art_dir}/pass_items.json", "w", encoding="utf-8") as f:
        json.dump(pass_items, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print(f"  placement                         : {name}")
    print(f"  final_ids total                   : {len(final_ids)}")
    print(f"  mapped from metadata nodes        : {len(item_map)}")
    print(f"  missing in metadata nodes         : {len(missing_in_nodes)}")
    print(f"  ✅ PASS                           : {len(pass_items)}")
    print(f"  ❌ FAIL (empty relate)            : {len(fail_no_relate)}")
    print(f"  ❌ FAIL (no ecommerce)            : {len(fail_no_ecommerce)}")
    print(f"  ❌ FAIL (partner_related mismatch): {len(fail_no_partner_related)}")
    print(f"  OVERALL PASS                      : {total_fail == 0}")
    print("=" * 60)

    return report


# =============================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =============================================================
def test_verify_relate_ecom_sfv_p7():
    summary = run_check(PLACEMENTS[0])
    fail_msgs = []
    if summary["fail_empty_relate_count"] > 0:
        ids = [x["id"] for x in summary["fail_empty_relate"]]
        fail_msgs.append(
            f"FAIL empty relate_content ({summary['fail_empty_relate_count']} items): {ids[:10]}"
        )
    if summary["fail_no_ecommerce_count"] > 0:
        ids = [x["id"] for x in summary["fail_no_ecommerce"]]
        fail_msgs.append(
            f"FAIL no ecommerce ({summary['fail_no_ecommerce_count']} items): {ids[:10]}"
        )
    assert not fail_msgs, "\n".join(fail_msgs)


def test_verify_relate_ecom_sfv_p8():
    summary = run_check(PLACEMENTS[1])
    fail_msgs = []
    if summary["fail_empty_relate_count"] > 0:
        ids = [x["id"] for x in summary["fail_empty_relate"]]
        fail_msgs.append(
            f"FAIL empty relate_content ({summary['fail_empty_relate_count']} items): {ids[:10]}"
        )
    if summary["fail_no_ecommerce_count"] > 0:
        ids = [x["id"] for x in summary["fail_no_ecommerce"]]
        fail_msgs.append(
            f"FAIL no ecommerce ({summary['fail_no_ecommerce_count']} items): {ids[:10]}"
        )
    assert not fail_msgs, "\n".join(fail_msgs)

def test_verify_relate_ecom():
    test_verify_relate_ecom_sfv_p7()
    test_verify_relate_ecom_sfv_p8()    


if __name__ == "__main__":
    for p in PLACEMENTS:
        run_check(p)