"""
Verify_channel_vs_spanner.py

เปรียบเทียบ IDs ระหว่าง Metadata API (channel) กับ Spanner:
  - standard (hits)

Table: mst_channel_nonprod (flat columns)
DB:    ai_raas_nonprod @ g1d-ai-spannerb01

Usage:
  pip install google-cloud-spanner --break-system-packages
  python Verify_channel_vs_spanner.py
"""

import json
import subprocess
import sys
import os
import warnings
import logging
import re

# ปิด Spanner telemetry warnings
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)

# ======================================================
# CONFIG
# ======================================================
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/channel"

SP_PROJECT  = "tdg-ai-platform-nonprod02-bxev"
SP_INSTANCE = "g1d-ai-spannerb01"
SP_DATABASE = "ai_raas_nonprod"

# ======================================================
# SQL file paths
# ======================================================
SQL_DIR      = os.path.dirname(os.path.abspath(__file__))
SQL_STANDARD = os.path.join(SQL_DIR, "channel_main.sql")


def inject_ids(sql_path, ids):
    """
    อ่าน SQL file แล้ว inject IDs จาก API เข้าไปใน p_id_list
    แทนที่:  CAST([] AS ARRAY<STRING>) AS p_id_list
    ด้วย:    CAST(['id1','id2',...] AS ARRAY<STRING>) AS p_id_list
    และแทน LIMIT 10 ด้วยจำนวน IDs ที่ inject เพื่อให้ได้ผลครบ
    """
    with open(sql_path, "r") as f:
        sql = f.read()
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    sql = sql.replace(
        "CAST([] AS ARRAY<STRING>) AS p_id_list",
        f"CAST({id_array} AS ARRAY<STRING>) AS p_id_list"
    )
    # แทน LIMIT hardcode ให้รองรับ IDs ทั้งหมดที่ inject
    new_limit = max(len(ids), 1000)
    sql = re.sub(r'\bLIMIT\s+10\b', f'LIMIT {new_limit}', sql)
    return sql


# ======================================================
# REQUEST BODY — ตาม curl ที่ระบุ
# ======================================================
REQUEST_BODY_STANDARD = {
    "parameters": {
        "fields": ["id", "create_date", "title", "article_category"],
        "language": "th",
        "id": ["03K4qwG1egE6", "163n5ZYqPmrN"],
        "limit": 100
    },
    "options": {
        "rename_mapping": False,
        "dry_run": False,
        "debug": True,
        "cache": False
    }
}


# ======================================================
# STEP 1: ดึง IDs จาก Metadata API
# ======================================================
def fetch_api_response(request_body, label="API"):
    print(f"🚀 กำลังยิง Metadata API (channel) [{label}]...")
    print(f"📤 Request body [{label}]:")
    print(json.dumps(request_body, ensure_ascii=False, indent=2))
    curl_cmd = [
        "curl", "--location", "--silent",
        API_URL,
        "--header", "Content-Type: application/json",
        "--data", json.dumps(request_body)
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
    print(f"✅ API response OK [{label}]")
    return response


def extract_id(hit):
    return hit.get("_source", {}).get("id") or None


def get_hits_ids(response):
    """Extract standard hits IDs จาก response"""
    data = response.get("data", {})
    hits = data.get("hits", {}).get("hits", [])
    seen = set()
    hits_ids = []
    for h in hits:
        id_val = extract_id(h)
        if id_val and id_val not in seen:
            hits_ids.append(id_val)
            seen.add(id_val)
    print(f"  📌 API hits (standard): {len(hits_ids)} IDs")
    return hits_ids


# ======================================================
# STEP 2: Query Spanner
# ======================================================
def build_existence_check_sql(ids):
    """
    SQL อย่างง่ายสำหรับเช็คว่า IDs จาก API มีอยู่ใน Spanner ไหม
    """
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_channel_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""


def query_spanner(sql):
    try:
        from google.cloud import spanner
    except ImportError:
        print("❌ ไม่พบ google-cloud-spanner กรุณารัน:")
        print("   pip install google-cloud-spanner --break-system-packages")
        sys.exit(1)

    client   = spanner.Client(project=SP_PROJECT)
    instance = client.instance(SP_INSTANCE)
    database = instance.database(SP_DATABASE)

    ids = []
    seen = set()
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(sql)
        for row in results:
            row_dict = dict(zip([col.name for col in results.fields], row))
            id_val = row_dict.get("id")
            if id_val and id_val not in seen:
                ids.append(id_val)
                seen.add(id_val)
    return ids


# ======================================================
# STEP 3: Compare
# ======================================================
def compare(label, api_ids, sp_ids):
    api_set = set(api_ids)
    sp_set  = set(sp_ids)

    only_api = sorted(api_set - sp_set)
    only_sp  = sorted(sp_set - api_set)
    both     = sorted(api_set & sp_set)

    print(f"\n{'='*60}")
    print(f"📊 [{label}]")
    print(f"{'='*60}")
    print(f"  Extract จาก API    : {len(api_ids)} IDs")
    print(f"  Inject เข้า Spanner: {len(api_ids)} IDs")
    print(f"  Spanner คืนมา      : {len(sp_ids)} IDs")
    print(f"\n  ✅ MATCH ({len(both)} IDs):")
    for id_ in both:
        print(f"      {id_}")
    print(f"\n  ❌ อยู่ใน API แต่ไม่มีใน Spanner ({len(only_api)} IDs):")
    if only_api:
        for id_ in only_api:
            print(f"      {id_}")
    else:
        print("      (ไม่มี)")
    print(f"\n  ⚠️  อยู่ใน Spanner แต่ไม่มีใน API ({len(only_sp)} IDs):")
    if only_sp:
        for id_ in only_sp:
            print(f"      {id_}")
    else:
        print("      (ไม่มี)")

    return {
        "label": label,
        "api": len(api_ids),
        "spanner": len(sp_ids),
        "match": len(both),
        "only_api": only_api,
        "only_spanner": only_sp
    }


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    # Step 1: API — ยิง standard
    response_standard = fetch_api_response(REQUEST_BODY_STANDARD, label="standard")

    print("\n📦 Extract IDs จาก API...")
    hits_ids = get_hits_ids(response_standard)

    # Step 2: Spanner — ลอง inject ผ่าน SQL file ก่อน ถ้าไม่มีไฟล์ใช้ existence check แทน
    print("\n🔍 Query Spanner...")
    if os.path.exists(SQL_STANDARD):
        print("  กำลัง query standard (via SQL file)...")
        sp_standard = query_spanner(inject_ids(SQL_STANDARD, hits_ids))
    else:
        print(f"  ⚠️  ไม่พบ {SQL_STANDARD} — ใช้ existence check แทน")
        sp_standard = query_spanner(build_existence_check_sql(hits_ids))
    print(f"  ✅ standard : {len(sp_standard)} IDs")

    # Step 3: Compare
    print("\n📋 ผลการเปรียบเทียบ:")
    results = [
        compare("standard (hits)", hits_ids, sp_standard),
    ]

    # Summary
    print(f"\n{'='*60}")
    print("📈 สรุปรวม (channel):")
    print(f"{'='*60}")
    print(f"  {'Type':<20} {'API':>6} {'Spanner':>8} {'Match':>7} {'Only API':>9} {'Only SP':>8}")
    print(f"  {'-'*20} {'-'*6} {'-'*8} {'-'*7} {'-'*9} {'-'*8}")
    for r in results:
        print(f"  {r['label']:<20} {r['api']:>6} {r['spanner']:>8} {r['match']:>7} {len(r['only_api']):>9} {len(r['only_spanner']):>8}")

    # Save IDs ที่ไม่ match ออกเป็นไฟล์
    print("\n💾 บันทึกไฟล์ IDs ที่ไม่ match...")
    out_dir = os.path.dirname(os.path.abspath(__file__))
    for r in results:
        slug = r['label'].replace(" ", "_").replace("(", "").replace(")", "")
        if r['only_api']:
            fname = os.path.join(out_dir, f"channel_not_in_spanner_{slug}.txt")
            with open(fname, "w") as f:
                f.write(json.dumps(r['only_api'], ensure_ascii=False, indent=2))
            print(f"  📄 channel_not_in_spanner_{slug}.txt — {len(r['only_api'])} IDs")
        if r['only_spanner']:
            fname = os.path.join(out_dir, f"channel_not_in_api_{slug}.txt")
            with open(fname, "w") as f:
                f.write(json.dumps(r['only_spanner'], ensure_ascii=False, indent=2))
            print(f"  📄 channel_not_in_api_{slug}.txt — {len(r['only_spanner'])} IDs")

    # Step 4: เช็ค IDs ที่ไม่มีใน Spanner ว่ามีใน source table mst_channel_nonprod ไหม
    print(f"\n{'='*60}")
    print("🔎 ตรวจสอบ IDs ที่หายจาก Spanner ใน source table (mst_channel_nonprod)...")
    print(f"{'='*60}")

    # รวม IDs ที่ไม่มีใน Spanner ทุก type (unique)
    all_missing = sorted({id_ for r in results for id_ in r['only_api']})

    if not all_missing:
        print("  ✅ ไม่มี IDs ที่หายจาก Spanner")
    else:
        print(f"  🔍 ตรวจสอบ {len(all_missing)} IDs ใน source table...")
        id_array = "[" + ", ".join(f"'{i}'" for i in all_missing) + "]"

        # เช็ค 1: มีในตารางไหม (ไม่มี filter ใดๆ)
        sql_exists = f"""
SELECT DISTINCT id
FROM mst_channel_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""
        # เช็ค 2: ผ่าน business logic filters ไหม
        sql_filtered = f"""
SELECT DISTINCT id
FROM mst_channel_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
  AND status = 'publish'
  AND searchable = 'Y'
  AND publish_date <= CURRENT_TIMESTAMP()
  AND (expire_date IS NULL OR expire_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE))
  AND publish_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
"""
        found_in_source = query_spanner(sql_exists)
        passed_filter   = query_spanner(sql_filtered)

        found_set    = set(found_in_source)
        passed_set   = set(passed_filter)
        not_in_source = sorted(set(all_missing) - found_set)
        filtered_out  = sorted(found_set - passed_set)

        print(f"\n  📊 ผล source table check:")
        print(f"  ❌ ไม่มีในตารางเลย              : {len(not_in_source)} IDs")
        print(f"  ⚠️  มีในตาราง แต่ตกหล่น filter  : {len(filtered_out)} IDs")
        print(f"       (status/searchable/publish_date/expire_date ไม่ผ่าน)")
        print(f"  ✅ ผ่าน filter ครบ               : {len(passed_filter)} IDs")

        if filtered_out:
            print(f"\n  IDs ที่ตกหล่น filter ({len(filtered_out)}):")
            for id_ in filtered_out:
                print(f"      {id_}")

        if not_in_source:
            fname = os.path.join(out_dir, "channel_missing_not_in_source.txt")
            with open(fname, "w") as f:
                f.write(json.dumps(not_in_source, ensure_ascii=False, indent=2))
            print(f"\n  📄 channel_missing_not_in_source.txt — {len(not_in_source)} IDs")
            print("     (ไม่มีในตารางเลย)")

        if filtered_out:
            fname = os.path.join(out_dir, "channel_missing_filtered_out.txt")
            with open(fname, "w") as f:
                f.write(json.dumps(filtered_out, ensure_ascii=False, indent=2))
            print(f"  📄 channel_missing_filtered_out.txt — {len(filtered_out)} IDs")
            print("     (มีในตาราง แต่ไม่ผ่าน status/searchable/publish_date/expire_date)")

        if passed_filter:
            fname = os.path.join(out_dir, "channel_missing_but_passed_filter.txt")
            with open(fname, "w") as f:
                f.write(json.dumps(sorted(passed_filter), ensure_ascii=False, indent=2))
            print(f"  📄 channel_missing_but_passed_filter.txt — {len(passed_filter)} IDs")
            print("     (ผ่าน filter ทุกอย่างแต่ไม่มีใน Spanner — น่าจะเป็น sync issue)")
