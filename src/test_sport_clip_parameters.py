"""
test_sport_clip_parameters.py

Test script สำหรับตรวจสอบ parameters ของ Metadata API (sport_clip / standard)
แต่ละ test จะ:
  1. ยิง API ด้วย parameter ที่กำหนด
  2. เช็คเงื่อนไข parameter นั้นๆ
  3. เอา IDs จาก API → inject เข้า Spanner → เปรียบเทียบ (เหมือนต้นฉบับ)

Test Cases — STANDARD:
  1. limit              — จำนวน items ที่ได้ไม่เกิน limit ที่กำหนด
  2. language=th        — API คืน items เมื่อ language=th
  3. language=en        — API คืน items เมื่อ language=en
  4. fields             — _source มีครบตาม fields ที่กำหนดพอดี
  5. sort_order=desc    — items เรียงตาม publish_date จากมากไปน้อย
  6. sort_order=asc     — items เรียงตาม publish_date จากน้อยไปมาก
  7. sort desc vs asc   — ลำดับของ desc และ asc ตรงกันข้ามกัน
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
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/sport_clip"

SP_PROJECT  = "tdg-ai-platform-nonprod02-bxev"
SP_INSTANCE = "g1d-ai-spannerb01"
SP_DATABASE = "ai_trueidcms_nonprod"

DEFAULT_OPTIONS = {
    "rename_mapping": False,
    "dry_run": False,
    "debug": True,
    "cache": False
}

# 4 fields จาก curl request (publish_date แทน create_date)
FIELDS_4 = ["id", "publish_date", "title", "article_category"]

# sort field ที่ใช้เรียง
SORT_FIELD = "publish_date"

# ======================================================
# SQL file paths
# ======================================================
SQL_DIR      = os.path.dirname(os.path.abspath(__file__))
SQL_STANDARD = os.path.join(SQL_DIR, "sport_clip_main.sql")


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
    """SQL fallback เช็คว่า IDs จาก API มีใน mst_sport_clip_nonprod ไหม"""
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_sport_clip_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""


def compare_with_spanner(label: str, api_ids: list) -> tuple:
    """
    เอา IDs จาก API ไปเช็คใน Spanner ผ่าน sport_clip_main.sql
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
# SORT ORDER HELPER
# ======================================================
def check_sort_order(sources: list, field: str, order: str) -> tuple:
    """
    ตรวจสอบว่า sources เรียงตาม field ตาม order (asc/desc) ถูกต้องไหม
    คืน (passed, msg, violations)
    - ข้าม items ที่ field เป็น None
    - เปรียบเทียบแบบ string (ISO 8601 เรียงแบบ lexicographic ได้ถูกต้อง)
    """
    values = [(s.get("id"), s.get(field)) for s in sources if s.get(field) is not None]

    if len(values) < 2:
        return True, f"มีข้อมูลน้อยเกินไปจะตรวจสอบการเรียง ({len(values)} items)", []

    violations = []
    for i in range(len(values) - 1):
        id_cur,  val_cur  = values[i]
        id_next, val_next = values[i + 1]
        v_cur  = str(val_cur)
        v_next = str(val_next)
        if order == "desc" and v_cur < v_next:
            violations.append({
                "pos": i, "id": id_cur,
                "val": v_cur, "next_id": id_next, "next_val": v_next
            })
        elif order == "asc" and v_cur > v_next:
            violations.append({
                "pos": i, "id": id_cur,
                "val": v_cur, "next_id": id_next, "next_val": v_next
            })

    if violations:
        return False, (
            f"sort_order={order} ผิดลำดับ {len(violations)} จุด "
            f"(จาก {len(values)} items) → ตัวอย่าง: {violations[0]}"
        ), violations

    return True, f"sort_order={order} เรียงถูกต้องทุก {len(values)} items ✓", []


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

# ── 1. limit ────────────────────────────────────────────
def test_limit():
    """ทดสอบ limit 5, 20, 50 — items ที่ได้ต้องไม่เกิน limit"""
    for lim in [5, 20, 50]:
        resp    = call_api({"limit": lim, "language": "th"})
        sources = get_sources(resp)
        count   = len(sources)
        print(f"    limit={lim} → ได้ {count} items")
        if count > lim:
            return False, f"limit={lim} แต่ได้ {count} items (เกิน!)"

        api_ids = get_ids(sources)
        sp_ok, sp_msg, _ = compare_with_spanner(f"limit={lim}", api_ids)
        if not sp_ok:
            return False, f"limit={lim} — {sp_msg}"

    return True, "limit 5/20/50 — จำนวน items ไม่เกิน limit และทุก IDs match Spanner ✓"


# ── 2. language = th ────────────────────────────────────
def test_language_th():
    """language=th ต้องได้ items กลับมา และ IDs match Spanner"""
    resp    = call_api({"limit": 10, "language": "th"})
    sources = get_sources(resp)
    if not sources:
        return False, "language=th → ไม่มี items ใน response"

    print(f"    language=th → ได้ {len(sources)} items")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("language=th", api_ids)
    if not sp_ok:
        return False, f"language=th parameter OK แต่ {sp_msg}"
    return True, f"language=th → {len(sources)} items, {sp_msg}"


# ── 3. language = en ────────────────────────────────────
def test_language_en():
    """language=en ต้องได้ items กลับมา และ IDs match Spanner"""
    resp    = call_api({"limit": 10, "language": "en"})
    sources = get_sources(resp)
    if not sources:
        return False, "language=en → ไม่มี items ใน response"

    print(f"    language=en → ได้ {len(sources)} items")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("language=en", api_ids)
    if not sp_ok:
        return False, f"language=en parameter OK แต่ {sp_msg}"
    return True, f"language=en → {len(sources)} items, {sp_msg}"


# ── 4. fields ───────────────────────────────────────────
def test_fields():
    """กำหนด fields → _source ทุกตัวต้องมีครบตาม fields ที่กำหนดพอดี และ IDs match Spanner"""
    resp    = call_api({"limit": 20, "language": "th", "fields": FIELDS_4})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    expected = set(FIELDS_4)
    for s in sources:
        actual  = set(s.keys())
        missing = expected - actual
        extra   = actual - expected
        if missing or extra:
            return False, (
                f"ID={s.get('id')} — fields ไม่ตรง: "
                f"missing={missing or '∅'}, extra={extra or '∅'}"
            )

    print(f"    fields → ทุก {len(sources)} items มีครบ {FIELDS_4}")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("fields", api_ids)
    if not sp_ok:
        return False, f"fields OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มีครบ {len(FIELDS_4)} fields และ {sp_msg}"


# ── 5. sort_order = desc ────────────────────────────────
def test_sort_desc():
    """
    sort_field=publish_date, sort_order=desc
    → items ต้องเรียงจาก publish_date มากไปน้อย และ IDs match Spanner
    (รัน Spanner check ต่อเสมอแม้ sort จะผิด)
    """
    resp = call_api({
        "limit": 50,
        "language": "th",
        "sort_field": SORT_FIELD,
        "sort_order": "desc",
        "fields": FIELDS_4
    })
    sources = get_sources(resp)
    if not sources:
        return False, "sort_order=desc → ไม่มี items ใน response"

    print(f"    sort_order=desc → ได้ {len(sources)} items")

    sort_ok, sort_msg, _ = check_sort_order(sources, SORT_FIELD, "desc")
    print(f"    {'✅' if sort_ok else '❌'} {sort_msg}")

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("sort_order=desc", api_ids)

    passed = sort_ok and sp_ok
    msgs = []
    if not sort_ok:
        msgs.append(sort_msg)
    if not sp_ok:
        msgs.append(sp_msg)
    if passed:
        return True, f"{sort_msg} และ {sp_msg}"
    return False, " | ".join(msgs)


# ── 6. sort_order = asc ─────────────────────────────────
def test_sort_asc():
    """
    sort_field=publish_date, sort_order=asc
    → items ต้องเรียงจาก publish_date น้อยไปมาก และ IDs match Spanner
    (รัน Spanner check ต่อเสมอแม้ sort จะผิด)
    """
    resp = call_api({
        "limit": 50,
        "language": "th",
        "sort_field": SORT_FIELD,
        "sort_order": "asc",
        "fields": FIELDS_4
    })
    sources = get_sources(resp)
    if not sources:
        return False, "sort_order=asc → ไม่มี items ใน response"

    print(f"    sort_order=asc → ได้ {len(sources)} items")

    sort_ok, sort_msg, _ = check_sort_order(sources, SORT_FIELD, "asc")
    print(f"    {'✅' if sort_ok else '❌'} {sort_msg}")

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("sort_order=asc", api_ids)

    passed = sort_ok and sp_ok
    msgs = []
    if not sort_ok:
        msgs.append(sort_msg)
    if not sp_ok:
        msgs.append(sp_msg)
    if passed:
        return True, f"{sort_msg} และ {sp_msg}"
    return False, " | ".join(msgs)


# ── 7. sort desc vs asc — first item ต่างกัน ────────────
def test_sort_desc_vs_asc():
    """
    desc กับ asc ต้องได้ลำดับตรงกันข้าม:
    publish_date แรกสุดของ desc >= publish_date แรกสุดของ asc
    """
    resp_desc = call_api({
        "limit": 50, "language": "th",
        "sort_field": SORT_FIELD, "sort_order": "desc",
        "fields": FIELDS_4
    })
    resp_asc = call_api({
        "limit": 50, "language": "th",
        "sort_field": SORT_FIELD, "sort_order": "asc",
        "fields": FIELDS_4
    })

    src_desc = get_sources(resp_desc)
    src_asc  = get_sources(resp_asc)

    if not src_desc or not src_asc:
        return False, "ไม่มี items ใน response (desc หรือ asc)"

    first_desc = str(src_desc[0].get(SORT_FIELD, ""))
    first_asc  = str(src_asc[0].get(SORT_FIELD, ""))
    last_desc  = str(src_desc[-1].get(SORT_FIELD, ""))
    last_asc   = str(src_asc[-1].get(SORT_FIELD, ""))

    print(f"    desc → first={first_desc}, last={last_desc}")
    print(f"    asc  → first={first_asc},  last={last_asc}")

    if first_desc < first_asc:
        return False, (
            f"desc first ({first_desc}) น้อยกว่า asc first ({first_asc}) — "
            f"ลำดับน่าจะสลับกัน"
        )

    return True, (
        f"desc first={first_desc} >= asc first={first_asc} ✓ "
        f"(ลำดับตรงกันข้ามถูกต้อง)"
    )


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  Sport Clip — Parameter Test Suite (Standard)")
    print(f"    API     : {API_URL}")
    print(f"    Spanner : {SP_PROJECT} / {SP_INSTANCE} / {SP_DATABASE}")
    print("=" * 60)

    print(f"\n{'━'*60}")
    print("📌  STANDARD")
    print(f"{'━'*60}")
    run_test("1. limit (5 / 20 / 50)",                                    test_limit)
    run_test("2. language = th",                                           test_language_th)
    run_test("3. language = en",                                           test_language_en)
    run_test("4. fields — _source ครบตาม fields ที่กำหนด",               test_fields)
    run_test("5. sort_order=desc — เรียงจาก publish_date มากไปน้อย",     test_sort_desc)
    run_test("6. sort_order=asc  — เรียงจาก publish_date น้อยไปมาก",    test_sort_asc)
    run_test("7. sort desc vs asc — ลำดับตรงกันข้าม",                    test_sort_desc_vs_asc)

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
    report_path = os.path.join(out_dir, "test_sport_clip_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 บันทึกผลที่: {report_path}")

    sys.exit(0 if passed_count == total_count else 1)
