"""
test_shelf_parameters.py

Test script สำหรับตรวจสอบ parameters ของ Metadata API (shelf / standard)
แต่ละ test จะ:
  1. ยิง API ด้วย parameter ที่กำหนด
  2. เช็คเงื่อนไข parameter นั้นๆ
  3. เอา IDs จาก API → inject เข้า Spanner → เปรียบเทียบ (เหมือนต้นฉบับ)

Test Cases — STANDARD:
  1. id filter  — IDs ที่ออกมาอยู่ใน id list ที่กำหนดเท่านั้น
"""

import json
import subprocess
import sys
import os
import re
import warnings
import logging

# ปิด Spanner telemetry warnings
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)

# ======================================================
# CONFIG
# ======================================================
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/shelf"

SP_PROJECT  = "tdg-ai-platform-nonprod02-bxev"
SP_INSTANCE = "g1d-ai-spannerb01"
SP_DATABASE = "ai_trueidcms_nonprod"

DEFAULT_OPTIONS = {
    "rename_mapping": False,
    "dry_run": False,
    "debug": True,
    "cache": False
}

# ค่า id list จริงจาก curl request
ID_LIST = ["2KJmZ7zeMpGX","2zNZV92YL7Xz"]

# ======================================================
# SQL file paths
# ======================================================
SQL_DIR      = os.path.dirname(os.path.abspath(__file__))
SQL_STANDARD = os.path.join(SQL_DIR, "shelf_main.sql")


# ======================================================
# API HELPERS
# ======================================================
def call_api(params: dict) -> dict:
    """ยิง API แล้ว return response dict"""
    body = {"parameters": params, "options": DEFAULT_OPTIONS}
    curl_cmd = [
        "curl", "--location", "--silent",
        API_URL,
        "--header", "Content-Type: application/json",
        "--data", json.dumps(body, ensure_ascii=False)
    ]
    result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"curl error: {result.stderr}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON parse error: {e}\nRaw: {result.stdout[:300]}")


def get_sources(response: dict) -> list:
    """ดึง list ของ _source dict จาก standard hits"""
    hits = response.get("data", {}).get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


def get_ids(sources: list) -> list:
    """ดึง list ของ id จาก sources"""
    seen, ids = set(), []
    for s in sources:
        id_val = s.get("id")
        if id_val and id_val not in seen:
            ids.append(id_val)
            seen.add(id_val)
    return ids


# ======================================================
# SPANNER HELPERS
# ======================================================
def query_spanner(sql: str) -> list:
    """รัน SQL ใน Spanner แล้วคืน list ของ id"""
    try:
        from google.cloud import spanner
    except ImportError:
        raise RuntimeError(
            "ไม่พบ google-cloud-spanner กรุณารัน:\n"
            "  pip install google-cloud-spanner --break-system-packages"
        )
    client   = spanner.Client(project=SP_PROJECT)
    instance = client.instance(SP_INSTANCE)
    database = instance.database(SP_DATABASE)

    ids, seen = [], set()
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(sql)
        for row in results:
            row_dict = dict(zip([col.name for col in results.fields], row))
            id_val = row_dict.get("id")
            if id_val and id_val not in seen:
                ids.append(id_val)
                seen.add(id_val)
    return ids


def inject_ids(sql_path: str, ids: list) -> str:
    """
    อ่าน SQL file แล้ว inject IDs เข้าไปใน p_id_list
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
    new_limit = max(len(ids), 1000)
    sql = re.sub(r'\bLIMIT\s+10\b', f'LIMIT {new_limit}', sql)
    return sql


def build_existence_sql(ids: list) -> str:
    """SQL fallback เช็คว่า IDs จาก API มีใน mst_shelf_nonprod ไหม"""
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_shelf_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""


def compare_with_spanner(label: str, api_ids: list) -> tuple:
    """
    เอา IDs จาก API ไปเช็คใน Spanner ผ่าน shelf_main.sql
    (fallback ใช้ existence check ถ้าไม่มี SQL file)
    คืน (passed, summary_msg, detail_dict)
    - passed = True ถ้า api_ids ทุกตัวมีใน Spanner (only_api ว่าง)
    """
    if not api_ids:
        return True, "ไม่มี IDs ให้ตรวจสอบใน Spanner", {}

    if os.path.exists(SQL_STANDARD):
        sql = inject_ids(SQL_STANDARD, api_ids)
    else:
        print(f"    ⚠️  ไม่พบ {SQL_STANDARD} — ใช้ existence check แทน")
        sql = build_existence_sql(api_ids)

    sp_ids  = query_spanner(sql)
    api_set = set(api_ids)
    sp_set  = set(sp_ids)

    only_api = sorted(api_set - sp_set)
    only_sp  = sorted(sp_set  - api_set)
    both     = sorted(api_set & sp_set)

    detail = {
        "api_count":     len(api_ids),
        "spanner_count": len(sp_ids),
        "match":         len(both),
        "only_api":      only_api,
        "only_spanner":  only_sp,
    }

    print(f"\n    📊 Spanner compare [{label}]")
    print(f"       API IDs       : {len(api_ids)}")
    print(f"       Spanner found : {len(sp_ids)}")
    print(f"       ✅ Match      : {len(both)}")
    if only_api:
        print(f"       ❌ Only API (ไม่มีใน Spanner): {only_api}")
    if only_sp:
        print(f"       ⚠️  Only Spanner (API ไม่ส่งออกมา): {only_sp}")

    if only_api:
        return False, (
            f"Spanner: {len(only_api)} IDs อยู่ใน API แต่ไม่มีใน Spanner → {only_api[:5]}"
        ), detail
    return True, f"Spanner: ทุก {len(both)} IDs match ✓", detail


# ======================================================
# TEST RUNNER
# ======================================================
test_results = []

def run_test(name: str, fn):
    print(f"\n{'─'*60}")
    print(f"🧪  {name}")
    print(f"{'─'*60}")
    try:
        passed, msg = fn()
        icon = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {icon}: {msg}")
        test_results.append({"name": name, "passed": passed, "msg": msg})
    except Exception as e:
        print(f"  💥 ERROR: {e}")
        test_results.append({"name": name, "passed": False, "msg": f"ERROR: {e}"})


# ======================================================
# TEST CASES
# ======================================================

# ── 1. id filter ────────────────────────────────────────
def test_id_filter():
    """IDs ที่ออกมาต้องอยู่ใน ID_LIST เท่านั้น และ match Spanner"""
    resp    = call_api({"id": ID_LIST})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    returned_ids = [s.get("id") for s in sources]
    invalid = [i for i in returned_ids if i not in set(ID_LIST)]
    print(f"    กำหนด {len(ID_LIST)} IDs, ได้กลับมา {len(returned_ids)} IDs")

    if invalid:
        return False, f"มี {len(invalid)} IDs ที่ไม่อยู่ใน id filter: {invalid}"

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("id filter", api_ids)
    if not sp_ok:
        return False, f"id filter OK แต่ {sp_msg}"
    return True, f"IDs ทั้ง {len(returned_ids)} ตัวอยู่ใน id list และ {sp_msg}"



# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  Privilege ID — Parameter Test Suite (Standard)")
    print(f"    API     : {API_URL}")
    print(f"    Spanner : {SP_PROJECT} / {SP_INSTANCE} / {SP_DATABASE}")
    print("=" * 60)

    print(f"\n{'━'*60}")
    print("📌  STANDARD")
    print(f"{'━'*60}")
    run_test("1. id filter — IDs อยู่ใน id list เท่านั้น",   test_id_filter)

    print(f"\n{'='*60}")
    print("📋  สรุปผลการทดสอบ")
    print(f"{'='*60}")
    passed_count = sum(1 for r in test_results if r["passed"])
    total_count  = len(test_results)

    for r in test_results:
        icon = "✅" if r["passed"] else "❌"
        print(f"  {icon}  {r['name']}")

    print(f"\n  ผลรวม: {passed_count}/{total_count} passed", end="")
    if passed_count == total_count:
        print("  🎉")
    else:
        print(f"  ({total_count - passed_count} failed)")

    out_dir     = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(out_dir, "test_shelf_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 บันทึกผลที่: {report_path}")

    sys.exit(0 if passed_count == total_count else 1)
