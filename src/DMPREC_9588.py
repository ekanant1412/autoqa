import json
import os
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
METADATA_BATCH_SIZE = 50  # เรียก metadata ครั้งละกี่ ID


# =============================================================
# Helper: เรียก metadata API แบบ batch
# =============================================================
def fetch_metadata_batch(ids: list[str], fields: list[str]) -> dict:
    """Return dict: id -> metadata item"""
    if not ids:
        return {}
    payload = {"parameters": {"id": ids, "fields": fields}}
    resp = requests.post(METADATA_URL, json=payload, timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return {item["id"]: item for item in items if "id" in item}


def fetch_metadata_all(ids: list[str], fields: list[str]) -> dict:
    """Batch เป็นชุดๆ เพื่อไม่ให้ request ใหญ่เกิน"""
    result = {}
    for i in range(0, len(ids), METADATA_BATCH_SIZE):
        batch = ids[i: i + METADATA_BATCH_SIZE]
        print(f"    fetching metadata batch {i//METADATA_BATCH_SIZE + 1}: {len(batch)} ids")
        result.update(fetch_metadata_batch(batch, fields))
    return result


# =============================================================
# Step 1: เรียก Universal API
# =============================================================
print("[Step 1] Calling Universal API...")
resp = requests.get(UNIVERSAL_URL, timeout=TIMEOUT_SEC)
data = resp.json()

with open(f"{REPORT_DIR}/universal_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

results = data.get("data", {}).get("results", {})

# =============================================================
# Step 2: ดึง final_ids
# =============================================================
final_ids = results.get("final_result", {}).get("result", {}).get("ids", [])
final_ids = [x for x in final_ids if isinstance(x, str) and x.strip()]

if not final_ids:
    raise SystemExit("ERROR: final_result ids not found or empty")

print(f"[Step 2] final_ids: {len(final_ids)}")

# =============================================================
# Step 3: Build item_map เฉพาะจาก final_ids x node ที่มี metadata
# =============================================================
print("\n[Step 3] Building item lookup map from final_ids only...")

# Node ที่รู้ว่ามี full metadata (มี relate_content, title, etc.)
METADATA_NODES = [
    "append_bucketizes",
    "generate_candidates", 
    "modifier_generate_candidates",
]

item_map = {}

for node_name in METADATA_NODES:
    node_data = results.get(node_name, {})
    node_result = node_data.get("result", {})
    
    candidates = []
    if isinstance(node_result, dict):
        candidates = node_result.get("items", [])
    elif isinstance(node_result, list):
        candidates = node_result

    for item in candidates:
        if isinstance(item, dict) and item.get("id") in final_ids:
            if item["id"] not in item_map:
                item_map[item["id"]] = {**item, "_found_in_node": node_name}

print(f"  final_ids: {len(final_ids)}")
print(f"  Mapped from metadata nodes: {len(item_map)}")

# IDs ที่หาไม่เจอใน metadata nodes เลย
missing_in_nodes = [fid for fid in final_ids if fid not in item_map]
if missing_in_nodes:
    print(f"  ⚠️  IDs not found in any metadata node: {len(missing_in_nodes)}")
    for mid in missing_in_nodes:
        print(f"    - {mid}")

# =============================================================
# Step 4: รวบรวม relate_content IDs ของทุก final_id
# =============================================================
print("\n[Step 4] Collecting relate_content IDs from final_ids...")

final_id_to_relate = {}

for fid in final_ids:
    item = item_map.get(fid, {})
    relate = item.get("relate_content", [])
    if not isinstance(relate, list):
        relate = []
    final_id_to_relate[fid] = relate

# เอาทุกตัว ไม่ deduplicate (ใช้นับ / validate)
all_relate_ids = [rid for ids in final_id_to_relate.values() for rid in ids]
print(f"  Total relate_content IDs to check: {len(all_relate_ids)}")

# =============================================================
# Step 5: เรียก Metadata API เพื่อเช็ค content_type
# =============================================================
print("\n[Step 5] Fetching metadata for relate_content IDs...")
relate_meta_map = fetch_metadata_all(
    all_relate_ids,
    fields=["id", "content_type", "publish_date"]
)
print(f"  Metadata returned: {len(relate_meta_map)} items")

with open(f"{REPORT_DIR}/relate_metadata.json", "w", encoding="utf-8") as f:
    json.dump(relate_meta_map, f, ensure_ascii=False, indent=2)
# หลัง fetch metadata แล้ว
not_found_in_metadata = [rid for rid in all_relate_ids if rid not in relate_meta_map]

if not_found_in_metadata:
    print(f"  ⚠️  relate_content IDs not found in metadata: {len(not_found_in_metadata)}")
    for rid in not_found_in_metadata[:10]:
        # หา final_id ที่ relate ไป ID นี้
        owner = [fid for fid, rels in final_id_to_relate.items() if rid in rels]
        print(f"    - {rid}  (owned by final_id: {owner})")


# =============================================================
# Step 6: Validate แต่ละ final_id
# =============================================================
print("\n[Step 6] Validating logic...")

pass_items   = []   # ✅ มี relate_content และมี ecommerce อย่างน้อย 1
fail_no_relate = []  # ❌ relate_content ว่างเปล่า → ไม่ควรออกมา
fail_no_ecommerce = []  # ❌ relate_content มี ID แต่ไม่มี ecommerce เลย

for fid in final_ids:
    relate_ids = final_id_to_relate[fid]

    # กรณี relate_content ว่าง → ไม่ควรออกมา
    if not relate_ids:
        fail_no_relate.append({
            "id": fid,
            "reason": "relate_content is empty or missing",
            "title": item_map.get(fid, {}).get("title", ""),
        })
        continue

    # เช็คว่ามี relate ID ที่เป็น ecommerce ไหม
    ecommerce_found = []
    for rid in relate_ids:
        meta = relate_meta_map.get(rid, {})
        if meta.get("content_type") == "ecommerce":
            ecommerce_found.append(rid)

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

# =============================================================
# Step 7: Summary Report
# =============================================================
total_fail = len(fail_no_relate) + len(fail_no_ecommerce)

print("\n" + "="*60)
print(f"  final_ids total           : {len(final_ids)}")
print(f"  ✅ PASS (has ecommerce)   : {len(pass_items)}")
print(f"  ❌ FAIL (empty relate)    : {len(fail_no_relate)}")
print(f"  ❌ FAIL (no ecommerce)    : {len(fail_no_ecommerce)}")
print("="*60)

if fail_no_relate:
    print("\n  ❌ IDs ที่ relate_content ว่าง (ไม่ควรออกมาใน final_result):")
    for item in fail_no_relate:
        print(f"    - {item['id']}  | {item['title'][:50]}")

if fail_no_ecommerce:
    print("\n  ❌ IDs ที่ relate_content ไม่มี content_type=ecommerce:")
    for item in fail_no_ecommerce:
        types_str = ", ".join(f"{k}:{v}" for k, v in item["relate_content_types"].items())
        print(f"    - {item['id']}  | relate types: {types_str}")
        print(f"      title: {item['title'][:60]}")

# Save reports
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

print(f"\n  Saved -> {REPORT_DIR}/validation_summary.json")
print(f"  Saved -> {REPORT_DIR}/pass_items.json")
print(f"  Saved -> {REPORT_DIR}/relate_metadata.json")

if total_fail > 0:
    raise SystemExit(f"\n❌ TEST FAILED: {total_fail} items ไม่ผ่าน logic")
else:
    print("\n✅ TEST PASSED: All final_ids มี relate_content ที่เป็น ecommerce")