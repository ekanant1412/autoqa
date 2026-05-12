"""
test_privilege_parameters.py

Test script สำหรับตรวจสอบ parameters ของ Metadata API (privilege)
แต่ละ test จะ:
  1. ยิง API ด้วย parameter ที่กำหนด
  2. เช็คเงื่อนไข parameter นั้นๆ
  3. เอา IDs จาก API → inject เข้า Spanner → เปรียบเทียบ (เหมือนต้นฉบับ)

Test Cases — STANDARD:
  1.  limit          — จำนวน items ที่ได้ไม่เกิน limit ที่กำหนด
  2.  language=th    — API คืน items เมื่อ language=th
  3.  language=en    — API คืน items เมื่อ language=en
  4.  sort_field     — ใส่ HIT_COUNT_DAY_14 → items ออกมาและ Spanner match
  5.  article_category — ทุก items มี article_category อยู่ใน list ที่กำหนด
  6.  fields         — _source มีครบ 4 fields ตามที่กำหนด
  7.  exclude_ids (excluded) — ID ที่กำหนดไม่ปรากฏใน response
  8.  exclude_ids (included) — ID นั้นปรากฏใน response เมื่อไม่กำหนด exclude_ids

Test Cases — PREDICTIONS + ID:
  P1. predictions+id paired → items ปรากฏ และเรียงตาม prediction score desc
  P2. id อยู่ใน id list แต่ไม่มีใน predictions → item ไม่ปรากฏ
  P3. predictions มี id แต่ไม่อยู่ใน id list → item ไม่ปรากฏ

Test Cases — CARD TYPE:
  C1. card_type=['black'] + full params → ทุก items มี card_type='black'
  C2. card_type เปลี่ยนค่า (red/blue/green/white/no_card) → items ตรงกับ card_type

Test Cases — MAX POINT:
  M1. max_point=200 (>10) → ทุก items มี redeem_point <= 200
  M2. max_point=5 (0-10 bracket) → ทุก items มี redeem_point อยู่ในช่วง 0-10
"""

import json
import subprocess
import sys
import os
import warnings
import logging

# ปิด Spanner telemetry warnings (เหมือนต้นฉบับ)
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
warnings.filterwarnings("ignore")
logging.disable(logging.CRITICAL)

# ======================================================
# CONFIG
# ======================================================
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/privilege"

SP_PROJECT  = "tdg-ai-platform-nonprod02-bxev"
SP_INSTANCE = "g1d-ai-spannerb01"
SP_DATABASE = "ai_raas_nonprod"

DEFAULT_OPTIONS = {
    "rename_mapping": False,
    "dry_run": False,
    "debug": True,
    "cache": False
}

# ค่า id list จริงจาก REQUEST_BODY_STANDARD
ID_LIST = [
    "7dkGJpJG0Nad", "01O07prDlgDW", "ZdaNqxMq0nZj", "8ZxrgW9BaLlQ", "01oregln3ZOx",
    "EB09a3mJQ91y", "GPvZxpewLNGP", "LMnyQ5pP1LJV", "Azxlka2pyE5z", "xmGE9R0YAWrJ"
]

# predictions scores จาก REQUEST_BODY_STANDARD
PREDICTIONS = {
    "7dkGJpJG0Nad": 0.000381824153,
    "01O07prDlgDW": 0.000381823484,
    "01oregln3ZOx": 0.000381830236,
    "8ZxrgW9BaLlQ": 0.000410977111,
    "ZdaNqxMq0nZj": 1,
    "EB09a3mJQ91y": 2,
    "GPvZxpewLNGP": 3,
    "LMnyQ5pP1LJV": 0.000410892884,
    "Azxlka2pyE5z": 4,
    "xmGE9R0YAWrJ": 5
}

# 4 fields สำหรับ standard tests
FIELDS = ["id", "create_date", "card_type", "article_category"]

# 5 fields สำหรับ max_point tests (ต้องการ redeem_point ในการตรวจสอบ)
FIELDS_WITH_POINT = ["id", "create_date", "card_type", "article_category", "redeem_point"]

# ID ที่ใช้ทดสอบ exclude_ids
EXCLUDE_ID = "EB09a3mJQ91y"

# sort field
SORT_FIELD = "HIT_COUNT_DAY_14"

# article_category filter
ARTICLE_CATEGORY = ["dining"]

# card types ที่ต้องทดสอบ (นอกจาก black)
CARD_TYPES_OTHER = ["red", "blue", "green", "white", "no_card"]


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
# SPANNER HELPERS (เหมือนต้นฉบับ)
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


def build_existence_sql(ids: list) -> str:
    """SQL ง่ายๆ เช็คว่า IDs จาก API มีใน mst_privilege_nonprod ไหม"""
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_privilege_nonprod
WHERE id IN UNNEST(CAST({id_array} AS ARRAY<STRING>))
"""


def compare_with_spanner(label: str, api_ids: list) -> tuple:
    """
    เอา IDs จาก API ไปเช็คใน Spanner
    คืน (passed, summary_msg, detail_dict)
    - passed = True ถ้า api_ids ทุกตัวมีใน Spanner (only_api ว่าง)
    """
    if not api_ids:
        return True, "ไม่มี IDs ให้ตรวจสอบใน Spanner", {}

    sp_ids  = query_spanner(build_existence_sql(api_ids))
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
# STANDARD TEST CASES
# ======================================================

# ── 1. limit ────────────────────────────────────────────
def test_limit():
    """จำนวน items ที่ได้ต้องไม่เกิน limit ที่กำหนด และ IDs match Spanner"""
    for lim in [5, 20, 50]:
        resp    = call_api({"limit": lim, "language": "th"})
        sources = get_sources(resp)
        count   = len(sources)
        print(f"    limit={lim:>3} → {count} items")
        if count > lim:
            return False, f"limit={lim} แต่ได้ {count} items (เกิน!)"

        api_ids = get_ids(sources)
        sp_ok, sp_msg, _ = compare_with_spanner(f"limit={lim}", api_ids)
        if not sp_ok:
            return False, f"limit={lim} parameter OK แต่ {sp_msg}"

    return True, "limit 5/20/50 — จำนวน items ไม่เกิน limit และ Spanner match ✓"


# ── 2. language = th ────────────────────────────────────
def test_language_th():
    """language=th → API ต้องคืน items กลับมา และ IDs match Spanner"""
    resp    = call_api({"limit": 20, "language": "th"})
    sources = get_sources(resp)
    if not sources:
        return False, "language=th แต่ไม่มี items ใน response"

    print(f"    language=th → {len(sources)} items")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("language=th", api_ids)
    if not sp_ok:
        return False, f"language=th OK แต่ {sp_msg}"
    return True, f"ได้ {len(sources)} items และ {sp_msg}"


# ── 3. language = en ────────────────────────────────────
def test_language_en():
    """language=en → API ต้องคืน items กลับมา และ IDs match Spanner"""
    resp    = call_api({"limit": 20, "language": "en"})
    sources = get_sources(resp)
    if not sources:
        return False, "language=en แต่ไม่มี items ใน response"

    print(f"    language=en → {len(sources)} items")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("language=en", api_ids)
    if not sp_ok:
        return False, f"language=en OK แต่ {sp_msg}"
    return True, f"ได้ {len(sources)} items และ {sp_msg}"


# ── 4. sort_field ────────────────────────────────────────
def test_sort_field():
    """sort_field=HIT_COUNT_DAY_14 → ต้องได้ items กลับมาและ IDs match Spanner"""
    resp    = call_api({
        "limit": 50,
        "language": "th",
        "sort_field": SORT_FIELD,
        "fields": FIELDS,
    })
    sources = get_sources(resp)
    if not sources:
        return False, f"sort_field={SORT_FIELD} แต่ไม่มี items ใน response"

    print(f"    sort_field={SORT_FIELD} → {len(sources)} items")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("sort_field", api_ids)
    if not sp_ok:
        return False, f"sort_field OK แต่ {sp_msg}"
    return True, f"ได้ {len(sources)} items และ {sp_msg}"


# ── 5. article_category ─────────────────────────────────
def test_article_category():
    """article_category=ARTICLE_CATEGORY → ทุก items ต้องมี article_category อยู่ใน list"""
    resp    = call_api({
        "limit": 50,
        "language": "th",
        "article_category": ARTICLE_CATEGORY,
        "fields": FIELDS,
    })
    sources = get_sources(resp)
    if not sources:
        return False, f"ไม่มี items ใน response เมื่อ article_category={ARTICLE_CATEGORY}"

    # article_category เป็น array — เช็คว่า item มี category ที่กำหนดอย่างน้อย 1 ตัว
    violations = [
        {"id": s.get("id"), "article_category": s.get("article_category")}
        for s in sources
        if not any(cat in (s.get("article_category") or []) for cat in ARTICLE_CATEGORY)
    ]
    if violations:
        return False, (
            f"มี {len(violations)} items ที่ article_category ไม่มี category ใดใน "
            f"{ARTICLE_CATEGORY}: {violations[:5]}"
        )

    print(f"    article_category={ARTICLE_CATEGORY} → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("article_category", api_ids)
    if not sp_ok:
        return False, f"article_category OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี article_category ใน {ARTICLE_CATEGORY} และ {sp_msg}"


# ── 6. fields — ครบ 4 fields ตามที่กำหนด ───────────────
def test_fields():
    """กำหนด fields=FIELDS → _source ทุก item ต้องมีครบ 4 fields พอดี และ IDs match Spanner"""
    resp    = call_api({
        "limit": 20,
        "language": "th",
        "fields": FIELDS,
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    expected = set(FIELDS)
    for s in sources:
        actual  = set(s.keys())
        extra   = actual - expected
        missing = expected - actual
        if extra or missing:
            return False, (
                f"ID={s.get('id')} — fields ไม่ตรง: "
                f"extra={extra or '∅'}, missing={missing or '∅'}"
            )

    print(f"    fields → ทุก {len(sources)} items มีครบ {len(FIELDS)} fields")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("fields", api_ids)
    if not sp_ok:
        return False, f"fields OK แต่ {sp_msg}"
    return True, f"ครบ {len(FIELDS)} fields: {FIELDS} และ {sp_msg}"


# ── 7. exclude_ids — ID ไม่ปรากฏเมื่อกำหนด ────────────
def test_exclude_ids_when_set():
    """exclude_ids=[EXCLUDE_ID] → EXCLUDE_ID ต้องไม่มีใน response"""
    resp    = call_api({
        "limit": 200,
        "language": "th",
        "id": ID_LIST,
        "predictions": PREDICTIONS,
        "exclude_ids": [EXCLUDE_ID],
        "fields": FIELDS,
    })
    sources      = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    exclude_ids=['{EXCLUDE_ID}'] → ได้ {len(returned_ids)} items")

    if EXCLUDE_ID in returned_ids:
        return False, f"'{EXCLUDE_ID}' ยังปรากฏใน response ทั้งที่กำหนด exclude_ids"

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("exclude_ids (set)", api_ids)
    if not sp_ok:
        return False, f"exclude_ids OK แต่ {sp_msg}"
    return True, f"'{EXCLUDE_ID}' ไม่มีใน response และ {sp_msg}"


# ── 8. exclude_ids — ID ปรากฏเมื่อไม่กำหนด ────────────
def test_exclude_ids_when_not_set():
    """ไม่กำหนด exclude_ids → EXCLUDE_ID ต้องปรากฏใน response"""
    resp    = call_api({
        "limit": 200,
        "language": "th",
        "id": ID_LIST,
        "predictions": PREDICTIONS,
        "fields": FIELDS,
    })
    sources      = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    ไม่กำหนด exclude_ids → ได้ {len(returned_ids)} items")

    if EXCLUDE_ID not in returned_ids:
        return False, (
            f"'{EXCLUDE_ID}' ไม่ปรากฏใน response แม้ไม่กำหนด exclude_ids "
            f"(อาจถูก filter จาก params อื่น)"
        )

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("exclude_ids (not set)", api_ids)
    if not sp_ok:
        return False, f"exclude_ids OK แต่ {sp_msg}"
    return True, f"'{EXCLUDE_ID}' ปรากฏใน response และ {sp_msg}"


# ======================================================
# PREDICTIONS + ID TEST CASES
# ======================================================

# ── P1. predictions+id paired → ปรากฏ + เรียง score desc
def test_predictions_paired_and_ordered():
    """
    id และ predictions มีเซต IDs เดียวกัน (paired)
    → items ปรากฏทั้งหมด และเรียงตาม prediction score descending
    """
    # ใช้ IDs ที่มี scores ต่างกันชัดเจน (ไม่รวม EXCLUDE_ID)
    scored_ids = ["xmGE9R0YAWrJ", "Azxlka2pyE5z", "GPvZxpewLNGP", "ZdaNqxMq0nZj"]
    scored_preds = {
        "xmGE9R0YAWrJ": 5,
        "Azxlka2pyE5z": 4,
        "GPvZxpewLNGP": 3,
        "ZdaNqxMq0nZj": 1,
    }

    resp    = call_api({
        "limit": 50,
        "language": "th",
        "id": scored_ids,
        "predictions": scored_preds,
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
        "sort_field": SORT_FIELD,
        "fields": FIELDS,
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response เมื่อใส่ predictions+id"

    returned_ids = [s.get("id") for s in sources]
    print(f"    paired predictions → {len(returned_ids)} items: {returned_ids}")

    # เช็คลำดับ: items ที่มี score ใน scored_preds ต้องเรียง desc
    pred_items = [
        (s.get("id"), scored_preds[s.get("id")])
        for s in sources if s.get("id") in scored_preds
    ]
    for i in range(len(pred_items) - 1):
        id_a, score_a = pred_items[i]
        id_b, score_b = pred_items[i + 1]
        if score_a < score_b:
            return False, (
                f"การเรียงลำดับผิด: {id_a} (score={score_a}) อยู่ก่อน "
                f"{id_b} (score={score_b}) — ควรเรียง score desc"
            )

    print(f"    เรียงลำดับ score desc ✓: {[(i, s) for i, s in pred_items]}")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("predictions paired", api_ids)
    if not sp_ok:
        return False, f"predictions OK แต่ {sp_msg}"
    return True, f"ปรากฏ {len(sources)} items, เรียง score desc ✓ และ {sp_msg}"


# ── P2. id ที่ไม่มีใน predictions → item ไม่ปรากฏ ──────
def test_id_not_in_predictions():
    """
    id=[A, B] + predictions={A: score} เท่านั้น
    → B ต้องไม่ปรากฏใน response (ต้องมีคู่กันทั้ง id และ predictions)
    """
    in_both_id  = "ZdaNqxMq0nZj"  # อยู่ทั้งใน id และ predictions
    only_id     = "GPvZxpewLNGP"   # อยู่ใน id เท่านั้น, ไม่มีใน predictions

    resp    = call_api({
        "limit": 50,
        "language": "th",
        "id": [in_both_id, only_id],
        "predictions": {in_both_id: 1},   # only_id ไม่มีใน predictions
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
        "sort_field": SORT_FIELD,
        "fields": FIELDS,
    })
    sources      = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    id=[{in_both_id}, {only_id}], predictions มีแค่ {in_both_id}")
    print(f"    Response IDs: {returned_ids}")

    if only_id in returned_ids:
        return False, (
            f"'{only_id}' ปรากฏใน response ทั้งที่ไม่มีใน predictions "
            f"(id+predictions ต้องมีคู่กัน)"
        )

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("id not in predictions", api_ids)
    if not sp_ok:
        return False, f"predictions pairing OK แต่ {sp_msg}"
    return True, f"'{only_id}' ไม่ปรากฏ (ไม่มีใน predictions) และ {sp_msg}"


# ── P3. predictions ที่ไม่มีใน id list → item ไม่ปรากฏ ─
def test_predictions_not_in_id():
    """
    id=[A] + predictions={A: score, B: score2}
    → B ต้องไม่ปรากฏใน response (B ไม่อยู่ใน id list)
    """
    in_id_list      = "ZdaNqxMq0nZj"   # อยู่ใน id list
    not_in_id_list  = "GPvZxpewLNGP"   # อยู่ใน predictions แต่ไม่อยู่ใน id list

    resp    = call_api({
        "limit": 50,
        "language": "th",
        "id": [in_id_list],   # not_in_id_list ไม่อยู่ใน id
        "predictions": {in_id_list: 1, not_in_id_list: 3},
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
        "sort_field": SORT_FIELD,
        "fields": FIELDS,
    })
    sources      = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    id=[{in_id_list}], predictions มี {in_id_list} + {not_in_id_list}")
    print(f"    Response IDs: {returned_ids}")

    if not_in_id_list in returned_ids:
        return False, (
            f"'{not_in_id_list}' ปรากฏใน response ทั้งที่ไม่มีใน id list "
            f"(id+predictions ต้องมีคู่กัน)"
        )

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("predictions not in id", api_ids)
    if not sp_ok:
        return False, f"predictions pairing OK แต่ {sp_msg}"
    return True, f"'{not_in_id_list}' ไม่ปรากฏ (ไม่มีใน id list) และ {sp_msg}"


# ======================================================
# CARD TYPE TEST CASES
# ======================================================

# ── C1. card_type=black (full params) ───────────────────
def test_card_type_black():
    """
    card_type=['black'] + is_filter_by_cardtype=True + full params
    (id, predictions, exclude_ids, fields, sort_field, article_category)
    → ทุก items ต้องมี card_type='black' และ IDs match Spanner
    """
    resp    = call_api({
        "limit": 100,
        "language": "th",
        "id": ID_LIST,
        "predictions": PREDICTIONS,
        "exclude_ids": [EXCLUDE_ID],
        "fields": FIELDS,
        "sort_field": SORT_FIELD,
        "article_category": ARTICLE_CATEGORY,
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    violations = [
        s.get("id") for s in sources
        if "black" not in (s.get("card_type") or [])
    ]
    if violations:
        return False, (
            f"card_type=black แต่มี {len(violations)} items "
            f"ที่ card_type ไม่มี 'black': {violations[:5]}"
        )

    print(f"    card_type=['black'] → ทุก {len(sources)} items มี card_type='black'")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("card_type=black", api_ids)
    if not sp_ok:
        return False, f"card_type OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี card_type='black' และ {sp_msg}"


# ── C2. card_type อื่นๆ (red/blue/green/white/no_card) ──
def test_card_type_others():
    """
    เปลี่ยน card_type ทีละค่า (red/blue/green/white/no_card)
    → items ที่ออกมาต้องมี card_type ตรงกับค่าที่กำหนด
    """
    for ct in CARD_TYPES_OTHER:
        resp    = call_api({
            "limit": 50,
            "language": "th",
            "card_type": [ct],
            "is_filter_by_cardtype": True,
            "sort_field": SORT_FIELD,
            "fields": FIELDS,
        })
        sources = get_sources(resp)
        print(f"    card_type={ct} → {len(sources)} items")

        if not sources:
            print(f"    ⚠️  ไม่มี items สำหรับ card_type={ct} (ข้ามการตรวจสอบ)")
            continue

        if ct == "no_card":
            # no_card = ไม่มี card_type (null / empty list / มี "no_card")
            violations = [
                s.get("id") for s in sources
                if (s.get("card_type") or []) and "no_card" not in (s.get("card_type") or [])
            ]
        else:
            # card_type เป็น array — เช็คว่า ct อยู่ใน array
            violations = [
                s.get("id") for s in sources
                if ct not in (s.get("card_type") or [])
            ]

        if violations:
            return False, (
                f"card_type={ct} → มี {len(violations)} items "
                f"ที่ card_type ไม่ตรง: {violations[:5]}"
            )

        api_ids = get_ids(sources)
        sp_ok, sp_msg, _ = compare_with_spanner(f"card_type={ct}", api_ids)
        if not sp_ok:
            return False, f"card_type={ct} OK แต่ {sp_msg}"

    return True, "card_type (red/blue/green/white/no_card) — items มี card_type ตรงกันทุก case ✓"


# ======================================================
# MAX POINT TEST CASES
# ======================================================

# ── M1. max_point > 10 → redeem_point <= max_point ──────
def test_max_point_normal():
    """
    max_point=200 (> 10) → ทุก items ต้องมี redeem_point <= 200
    (ไม่ใส่ article_category ตามที่กำหนด)
    """
    resp    = call_api({
        "limit": 50,
        "language": "th",
        "fields": FIELDS_WITH_POINT,
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
        "sort_field": SORT_FIELD,
        "max_point": "200",
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    violations = []
    for s in sources:
        rp = s.get("redeem_point")
        if rp is not None:
            try:
                if float(rp) > 200:
                    violations.append({"id": s.get("id"), "redeem_point": rp})
            except (TypeError, ValueError):
                pass

    if violations:
        return False, (
            f"max_point=200 แต่มี {len(violations)} items "
            f"ที่ redeem_point > 200: {violations[:5]}"
        )

    print(f"    max_point=200 → ทุก {len(sources)} items มี redeem_point <= 200")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("max_point=200", api_ids)
    if not sp_ok:
        return False, f"max_point OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี redeem_point <= 200 และ {sp_msg}"


# ── M2. max_point 0-10 bracket → redeem_point in [0, 10] ─
def test_max_point_bracket():
    """
    max_point=5 (อยู่ในช่วง 0-10)
    → items ที่ออกสามารถมี redeem_point ได้ทั้งช่วง 0-10 (ไม่ใช่แค่ <= 5)
    → ตรวจสอบว่าทุก items มี redeem_point อยู่ในช่วง [0, 10]
    (ไม่ใส่ article_category ตามที่กำหนด)
    """
    resp    = call_api({
        "limit": 50,
        "language": "th",
        "fields": FIELDS_WITH_POINT,
        "card_type": ["black"],
        "is_filter_by_cardtype": True,
        "sort_field": SORT_FIELD,
        "max_point": "5",
    })
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    violations = []
    for s in sources:
        rp = s.get("redeem_point")
        if rp is not None:
            try:
                if float(rp) > 10:
                    violations.append({"id": s.get("id"), "redeem_point": rp})
            except (TypeError, ValueError):
                pass

    if violations:
        return False, (
            f"max_point=5 (bracket 0-10) แต่มี {len(violations)} items "
            f"ที่ redeem_point > 10: {violations[:5]}"
        )

    print(f"    max_point=5 (bracket) → ทุก {len(sources)} items มี redeem_point in [0, 10]")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("max_point=5 (bracket)", api_ids)
    if not sp_ok:
        return False, f"max_point bracket OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี redeem_point <= 10 (bracket 0-10) และ {sp_msg}"


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  Privilege — Parameter Test Suite (Standard + Predictions + Card Type + Max Point)")
    print(f"    API     : {API_URL}")
    print(f"    Spanner : {SP_PROJECT} / {SP_INSTANCE} / {SP_DATABASE}")
    print("=" * 60)

    # ── Standard ─────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  STANDARD")
    print(f"{'━'*60}")
    run_test("1.  limit (5 / 20 / 50)",                                      test_limit)
    run_test("2.  language = th",                                             test_language_th)
    run_test("3.  language = en",                                             test_language_en)
    run_test("4.  sort_field = HIT_COUNT_DAY_14",                            test_sort_field)
    run_test("5.  article_category — items อยู่ใน category ที่กำหนด",       test_article_category)
    run_test("6.  fields — ครบ 4 fields ตามที่กำหนด",                       test_fields)
    run_test("7.  exclude_ids — ID ไม่ปรากฏเมื่อกำหนด",                     test_exclude_ids_when_set)
    run_test("8.  exclude_ids — ID ปรากฏเมื่อไม่กำหนด",                     test_exclude_ids_when_not_set)

    # ── Predictions + ID ─────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  PREDICTIONS + ID")
    print(f"{'━'*60}")
    run_test("P1. predictions+id paired — items ปรากฏ + เรียง score desc",   test_predictions_paired_and_ordered)
    run_test("P2. id ไม่มีใน predictions — item ไม่ปรากฏ",                  test_id_not_in_predictions)
    run_test("P3. predictions ไม่มีใน id list — item ไม่ปรากฏ",             test_predictions_not_in_id)

    # ── Card Type ─────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  CARD TYPE")
    print(f"{'━'*60}")
    run_test("C1. card_type=['black'] + full params — items มี card_type=black",         test_card_type_black)
    run_test("C2. card_type (red/blue/green/white/no_card) — items ตรงกับ card_type",    test_card_type_others)

    # ── Max Point ─────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  MAX POINT")
    print(f"{'━'*60}")
    run_test("M1. max_point=200 (>10) — redeem_point <= 200",                test_max_point_normal)
    run_test("M2. max_point=5 (0-10 bracket) — redeem_point in [0, 10]",    test_max_point_bracket)

    # ── Summary ──────────────────────────────────────────
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

    # บันทึกผลออกเป็น JSON
    out_dir     = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(out_dir, "test_privilege_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 บันทึกผลที่: {report_path}")

    sys.exit(0 if passed_count == total_count else 1)
