"""
extract_ids_3nodes.py
ยิง metadata API แล้วดึง IDs แยก 3 node:
  - hits.hits[]
  - aggregations.agg_tophit.buckets[].sort_by_hit_count.hits.hits[]
  - aggregations.agg_latest.buckets[].<inner_key>.hits.hits[]
    (auto-detect inner key: sort_by_publish_date / latest / hits)

Output:
  - hits_ids.txt
  - tophit_ids.txt
  - latest_ids.txt

Usage:
  python extract_ids_3nodes.py
"""

import json
import subprocess
import sys

# ======================================================
# CONFIG
# ======================================================
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/sfv_series"

REQUEST_BODY = {
    "parameters": {
        "limit": 100,
        # agg_tophit: จัดกลุ่มตาม article_category เรียงตาม PLAY_COUNT_DAY_30
        "agg_tophit_group_field": "article_category",
        "agg_tophit_limit": 100,
        "agg_tophit_output_fields": ["*"],
        "agg_tophit_sort_by": "PLAY_COUNT_DAY_30",
        # agg_latest: จัดกลุ่มตาม article_category เรียงตาม publish date
        "agg_latest_group_field": "article_category",
        "agg_latest_limit": 100,
        "agg_latest_output_fields": ["*"]
    },
    "options": {
        "rename_mapping": False,
        "dry_run": False,
        "debug": True,
        "cache": False
    }
}

# ======================================================
# STEP 1: ยิง API
# ======================================================
def fetch_response():
    print("🚀 กำลังยิง API...")
    curl_cmd = [
        "curl", "--location", "--silent",
        API_URL,
        "--header", "Content-Type: application/json",
        "--data", json.dumps(REQUEST_BODY)
    ]

    result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=60)

    if result.returncode != 0:
        print(f"❌ curl error: {result.stderr}")
        sys.exit(1)

    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        print(f"Raw (500 chars): {result.stdout[:500]}")
        sys.exit(1)

    print("✅ API response OK")
    return response


# ======================================================
# STEP 2: Extract IDs จาก node ต่างๆ
# ======================================================
def extract_id(hit):
    """ดึง id จาก _source.id เท่านั้น (ไม่ใช้ _id, ไม่เติม prefix)"""
    return hit.get("_source", {}).get("id") or None


def extract_hits_ids(data):
    """ดึง IDs จาก hits.hits[]"""
    hits = data.get("hits", {}).get("hits", [])
    ids = []
    seen = set()
    for h in hits:
        id_val = extract_id(h)
        if id_val and id_val not in seen:
            ids.append(id_val)
            seen.add(id_val)
    print(f"  📌 hits: {len(ids)} IDs")
    return ids


def extract_agg_ids(data, agg_name, inner_key):
    """ดึง IDs จาก aggregations buckets ด้วย inner_key ที่ระบุ"""
    buckets = data.get("aggregations", {}).get(agg_name, {}).get("buckets", [])
    ids = []
    seen = set()
    for bucket in buckets:
        inner_hits = bucket.get(inner_key, {}).get("hits", {}).get("hits", [])
        for h in inner_hits:
            id_val = extract_id(h)
            if id_val and id_val not in seen:
                ids.append(id_val)
                seen.add(id_val)
    print(f"  📌 {agg_name} (key='{inner_key}'): {len(ids)} IDs จาก {len(buckets)} buckets")
    return ids


def auto_extract_agg_ids(data, agg_name, candidate_keys):
    """
    ลอง inner_key ตามลำดับใน candidate_keys
    ใช้อันแรกที่ได้ IDs > 0
    ถ้าไม่มีเลย ให้ auto-detect จาก key จริงใน bucket แรก
    """
    buckets = data.get("aggregations", {}).get(agg_name, {}).get("buckets", [])
    if not buckets:
        print(f"  ⚠️  {agg_name}: ไม่มี buckets")
        return []

    # ลอง candidate keys ก่อน
    for key in candidate_keys:
        ids = extract_agg_ids(data, agg_name, key)
        if ids:
            return ids

    # Auto-detect: ดู keys ใน bucket แรกที่ไม่ใช่ meta fields
    meta_keys = {"doc_count", "key", "key_as_string"}
    detected_keys = [k for k in buckets[0].keys() if k not in meta_keys]
    print(f"  🔍 Auto-detect keys ใน {agg_name}: {detected_keys}")
    for key in detected_keys:
        ids = extract_agg_ids(data, agg_name, key)
        if ids:
            return ids

    print(f"  ⚠️  {agg_name}: ดึง IDs ไม่ได้จาก key ใดเลย")
    return []


# ======================================================
# STEP 3: Save ไฟล์
# ======================================================
def save_ids(ids, filename):
    with open(filename, "w") as f:
        f.write("\n".join(ids))
    print(f"  💾 บันทึก {len(ids)} IDs → {filename}")


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    response = fetch_response()
    data = response.get("data", {})

    print("\n📦 กำลัง extract IDs จาก nodes...")

    # Node 1: hits ปกติ
    hits_ids = extract_hits_ids(data)

    # Node 2: agg_tophit — ลอง sort_by_hit_count → hits → auto-detect
    tophit_ids = auto_extract_agg_ids(data, "agg_tophit", ["sort_by_hit_count", "hits"])

    # Node 3: agg_latest — ลอง sort_by_publish_date → latest → hits → auto-detect
    latest_ids = auto_extract_agg_ids(data, "agg_latest", ["sort_by_publish_date", "latest", "hits"])

    # Save
    print("\n💾 บันทึกไฟล์...")
    save_ids(hits_ids,   "hits_ids.txt")
    save_ids(tophit_ids, "tophit_ids.txt")
    save_ids(latest_ids, "latest_ids.txt")

    # Summary
    all_ids = list(dict.fromkeys(hits_ids + tophit_ids + latest_ids))
    print(f"\n✅ สรุป:")
    print(f"  hits_ids.txt    : {len(hits_ids)} IDs")
    print(f"  tophit_ids.txt  : {len(tophit_ids)} IDs")
    print(f"  latest_ids.txt  : {len(latest_ids)} IDs")
    print(f"  รวม unique IDs  : {len(all_ids)} IDs")

    # Preview Spanner format ของแต่ละชุด
    print("\n📋 Spanner ARRAY format:")
    for label, ids in [("hits", hits_ids), ("tophit", tophit_ids), ("latest", latest_ids)]:
        arr = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
        print(f"\n  -- {label} --")
        print(f"  CAST({arr} AS ARRAY<STRING>)")
