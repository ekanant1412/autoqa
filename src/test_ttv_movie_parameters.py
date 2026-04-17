"""
test_ttv_movie_parameters.py

Test script สำหรับตรวจสอบ parameters ของ Metadata API (ttv-movie / standard)
แต่ละ test จะ:
  1. ยิง API ด้วย parameter ที่กำหนด
  2. เช็คเงื่อนไข parameter นั้นๆ
  3. เอา IDs จาก API → inject เข้า Spanner → เปรียบเทียบ (เหมือนต้นฉบับ)

Test Cases — STANDARD:
  1.  limit                — จำนวน items ที่ได้ไม่เกิน limit ที่กำหนด
  2.  language=th          — API คืน items เมื่อ language=th
  3.  language=en          — API คืน items เมื่อ language=en
  4.  id filter            — IDs ที่ออกมาอยู่ใน id list ที่กำหนดเท่านั้น
  5.  fields               — _source มีครบตาม fields ที่กำหนดพอดี
  6.  article_category     — ทุก items มีอย่างน้อย 1 category ที่อยู่ใน list ที่กำหนด
                             (article_category เป็น list → ต้อง overlap กับ ARTICLE_CATEGORIES)
  7.  exclude_ids          — ID ที่กำหนดไม่ปรากฏใน response
  8.  is_trailer=yes       — ทุก items มี is_trailer = 'yes'
  9.  is_promo=yes         — ทุก items มี is_promo = 'yes'
  10. movie_type=movie     — ทุก items มี movie_type = 'movie'
  11. predictions          — items เรียงตาม predictions score จากมากไปน้อย
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
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/ttv-movie"

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
ID_LIST = ["dLMEGlbO28PL", "o6aGrlZW0agv"]

# fields จาก curl request
FIELDS_4 = ["id", "create_date", "title", "article_category"]

# article_category ที่ยอมรับ
ARTICLE_CATEGORIES = ["ca-thai-movie", "ca-thriller-and-horror", "dhammahub"]

# ID ที่ต้องการ exclude
EXCLUDE_ID = "dLMEGlbO28PL"

# predictions จาก curl request
PREDICTIONS = {
    "dLMEGlbO28PL": 0.9067820906639099,
    "o6aGrlZW0agv": 0.9061188697814941
}

# ======================================================
# SQL file paths
# ======================================================
SQL_DIR      = os.path.dirname(os.path.abspath(__file__))
SQL_STANDARD = os.path.join(SQL_DIR, "ttv-movie_main.sql")


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
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_ttv_movie_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""


def compare_with_spanner(label: str, api_ids: list) -> tuple:
    """
    เอา IDs จาก API ไปเช็คใน Spanner ผ่าน ttv-movie_main.sql
    (fallback ใช้ existence check ถ้าไม่มี SQL file)
    รัน Spanner check ต่อเสมอ คืน (passed, msg, detail)
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

# ── 1. limit ────────────────────────────────────────────
def test_limit():
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


# ── 4. id filter ────────────────────────────────────────
def test_id_filter():
    resp    = call_api({"id": ID_LIST, "fields": FIELDS_4, "limit": 100, "language": "th"})
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


# ── 5. fields ───────────────────────────────────────────
def test_fields():
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


# ── 6. article_category ─────────────────────────────────
def test_article_category():
    """
    article_category เป็น list ต่อ item
    เงื่อนไข: ทุก item ต้องมีอย่างน้อย 1 category ที่อยู่ใน ARTICLE_CATEGORIES
    (overlap check — ไม่บังคับว่าทุก category ต้องอยู่ใน list)
    """
    resp = call_api({
        "limit": 50, "language": "th",
        "article_category": ARTICLE_CATEGORIES,
        "fields": FIELDS_4
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    allowed_set = set(ARTICLE_CATEGORIES)
    violations  = []

    for s in sources:
        cats = s.get("article_category")

        # รองรับทั้ง string เดี่ยวและ list
        if isinstance(cats, str):
            cats = [cats]
        elif not isinstance(cats, list):
            cats = []

        # ต้องมีอย่างน้อย 1 category ที่อยู่ใน ARTICLE_CATEGORIES
        matched = [c for c in cats if c in allowed_set]
        if not matched:
            violations.append({
                "id":               s.get("id"),
                "article_category": s.get("article_category"),
                "matched":          matched
            })
        else:
            print(f"    ✓ id={s.get('id')} → matched={matched}")

    if violations:
        return False, (
            f"มี {len(violations)} items ที่ไม่มี article_category อยู่ใน "
            f"{ARTICLE_CATEGORIES} เลย: {violations[:3]}"
        )

    print(f"    article_category → ทุก {len(sources)} items มีอย่างน้อย 1 category ใน list ✓")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("article_category", api_ids)
    if not sp_ok:
        return False, f"article_category OK แต่ {sp_msg}"
    return True, (
        f"ทุก {len(sources)} items มี article_category อย่างน้อย 1 ใน "
        f"{ARTICLE_CATEGORIES} และ {sp_msg}"
    )


# ── 7. exclude_ids ──────────────────────────────────────
def test_exclude_ids():
    resp     = call_api({"limit": 100, "language": "th", "exclude_ids": EXCLUDE_ID})
    sources  = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    exclude_ids='{EXCLUDE_ID}' → ได้ {len(returned_ids)} items")

    if EXCLUDE_ID in returned_ids:
        return False, f"'{EXCLUDE_ID}' ยังปรากฏใน response ทั้งที่กำหนด exclude_ids"

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("exclude_ids", api_ids)
    if not sp_ok:
        return False, f"exclude_ids OK แต่ {sp_msg}"
    return True, f"'{EXCLUDE_ID}' ไม่มีใน response และ {sp_msg}"


# ── 8. is_trailer = yes ─────────────────────────────────
def test_is_trailer_yes():
    fields_with_trailer = FIELDS_4 + ["is_trailer"]
    resp    = call_api({
        "limit": 50, "language": "th",
        "is_trailer": "yes",
        "fields": fields_with_trailer
    })
    sources = get_sources(resp)
    if not sources:
        return False, "is_trailer=yes → ไม่มี items ใน response"

    violations = [s.get("id") for s in sources if s.get("is_trailer") != "yes"]
    if violations:
        print(f"    ⚠️  is_trailer=yes แต่ {len(violations)} items ไม่มี is_trailer='yes': {violations[:5]}")
    else:
        print(f"    is_trailer=yes → ทุก {len(sources)} items ผ่าน")

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("is_trailer=yes", api_ids)

    if violations and not sp_ok:
        return False, (
            f"is_trailer=yes แต่ {len(violations)} items ไม่มี is_trailer='yes': {violations[:5]} | {sp_msg}"
        )
    if violations:
        return False, f"is_trailer=yes แต่ {len(violations)} items ไม่มี is_trailer='yes': {violations[:5]}"
    if not sp_ok:
        return False, f"is_trailer OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี is_trailer='yes' และ {sp_msg}"


# ── 9. is_promo = yes ───────────────────────────────────
def test_is_promo_yes():
    fields_with_promo = FIELDS_4 + ["is_promo"]
    resp    = call_api({
        "limit": 50, "language": "th",
        "is_promo": "yes",
        "fields": fields_with_promo
    })
    sources = get_sources(resp)
    if not sources:
        return False, "is_promo=yes → ไม่มี items ใน response"

    violations = [s.get("id") for s in sources if s.get("is_promo") != "yes"]
    if violations:
        print(f"    ⚠️  is_promo=yes แต่ {len(violations)} items ไม่มี is_promo='yes': {violations[:5]}")
    else:
        print(f"    is_promo=yes → ทุก {len(sources)} items ผ่าน")

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("is_promo=yes", api_ids)

    if violations and not sp_ok:
        return False, (
            f"is_promo=yes แต่ {len(violations)} items ไม่มี is_promo='yes': {violations[:5]} | {sp_msg}"
        )
    if violations:
        return False, f"is_promo=yes แต่ {len(violations)} items ไม่มี is_promo='yes': {violations[:5]}"
    if not sp_ok:
        return False, f"is_promo OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี is_promo='yes' และ {sp_msg}"


# ── 10. movie_type = movie ──────────────────────────────
def test_movie_type():
    """movie_type=movie → ทุก items ต้องมี movie_type = 'movie' และ IDs match Spanner"""
    fields_with_movie_type = FIELDS_4 + ["movie_type"]
    resp    = call_api({
        "limit": 50, "language": "th",
        "movie_type": "movie",
        "fields": fields_with_movie_type
    })
    sources = get_sources(resp)
    if not sources:
        return False, "movie_type=movie → ไม่มี items ใน response"

    violations = [s.get("id") for s in sources if s.get("movie_type") != "movie"]
    if violations:
        return False, (
            f"movie_type=movie แต่ {len(violations)} items "
            f"ไม่มี movie_type='movie': {violations[:5]}"
        )

    print(f"    movie_type=movie → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("movie_type=movie", api_ids)
    if not sp_ok:
        return False, f"movie_type OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี movie_type='movie' และ {sp_msg}"


# ── 11. predictions ─────────────────────────────────────
def test_predictions():
    """
    ส่ง predictions → items ที่คืนมาต้องเรียงตาม score จากมากไปน้อย
    - IDs ที่ไม่มีใน predictions จะถูกข้ามการตรวจลำดับ
    - Spanner check รันต่อเสมอแม้ sort จะผิด
    """
    resp    = call_api({
        "id":          ID_LIST,
        "fields":      FIELDS_4,
        "limit":       100,
        "language":    "th",
        "predictions": PREDICTIONS
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    print(f"    predictions → ได้ {len(sources)} items")

    ordered_items = [
        (s.get("id"), PREDICTIONS[s.get("id")])
        for s in sources
        if s.get("id") in PREDICTIONS
    ]
    print(f"    items ที่มีใน predictions: {len(ordered_items)}")
    for id_, score in ordered_items:
        print(f"      {id_}: {score}")

    violations = []
    for i in range(len(ordered_items) - 1):
        id_cur,  score_cur  = ordered_items[i]
        id_next, score_next = ordered_items[i + 1]
        if score_cur < score_next:
            violations.append({
                "pos": i,
                "id": id_cur, "score": score_cur,
                "next_id": id_next, "next_score": score_next
            })

    sort_ok  = len(violations) == 0
    sort_msg = (
        f"predictions เรียงถูกต้อง (score desc) ทุก {len(ordered_items)} items ✓"
        if sort_ok else
        f"predictions ผิดลำดับ {len(violations)} จุด → ตัวอย่าง: {violations[0]}"
    )
    print(f"    {'✅' if sort_ok else '❌'} {sort_msg}")

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("predictions", api_ids)

    passed = sort_ok and sp_ok
    if passed:
        return True, f"{sort_msg} และ {sp_msg}"
    msgs = []
    if not sort_ok:
        msgs.append(sort_msg)
    if not sp_ok:
        msgs.append(sp_msg)
    return False, " | ".join(msgs)


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  TTV Movie — Parameter Test Suite (Standard)")
    print(f"    API     : {API_URL}")
    print(f"    Spanner : {SP_PROJECT} / {SP_INSTANCE} / {SP_DATABASE}")
    print("=" * 60)

    print(f"\n{'━'*60}")
    print("📌  STANDARD")
    print(f"{'━'*60}")
    run_test("1.  limit (5 / 20 / 50)",                                      test_limit)
    run_test("2.  language = th",                                             test_language_th)
    run_test("3.  language = en",                                             test_language_en)
    run_test("4.  id filter — IDs อยู่ใน id list เท่านั้น",                 test_id_filter)
    run_test("5.  fields — _source ครบตาม fields ที่กำหนด",                 test_fields)
    run_test("6.  article_category — ทุก items มีอย่างน้อย 1 category ใน list", test_article_category)
    run_test("7.  exclude_ids — ID ไม่ปรากฏใน response",                    test_exclude_ids)
    run_test("8.  is_trailer=yes — ทุก items มี is_trailer='yes'",           test_is_trailer_yes)
    run_test("9.  is_promo=yes — ทุก items มี is_promo='yes'",               test_is_promo_yes)
    run_test("10. movie_type=movie — ทุก items มี movie_type='movie'",       test_movie_type)
    run_test("11. predictions — เรียงตาม score มากไปน้อย",                  test_predictions)

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
    report_path = os.path.join(out_dir, "test_ttv_movie_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 บันทึกผลที่: {report_path}")

    sys.exit(0 if passed_count == total_count else 1)
