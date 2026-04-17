"""
test_ugcsfv_parameters.py

Test script สำหรับตรวจสอบ parameters ของ Metadata API (ugcsfv)
แต่ละ test จะ:
  1. ยิง API ด้วย parameter ที่กำหนด
  2. เช็คเงื่อนไข parameter นั้นๆ
  3. เอา IDs จาก API → inject เข้า Spanner → เปรียบเทียบ (เหมือนต้นฉบับ)

Test Cases — STANDARD:
  1.  limit          — จำนวน items ที่ได้ไม่เกิน limit ที่กำหนด
  2.  language=th    — API คืน items เมื่อ language=th
  3.  language=en    — API คืน items เมื่อ language=en
  4.  tophit_date_filter 30→50 — filter=50 ควรได้ items >= filter=30
  5.  title "รัก"   — ทุก items มีคำว่า "รัก" ใน title
  6.  is_related_ecommerce=False — ทุก items ไม่มี related_ecommerce_id
  7.  is_related_ecommerce=True  — ทุก items มี related_ecommerce_id
  8.  id filter      — IDs ที่ออกมาอยู่ใน id list ที่กำหนดเท่านั้น
  9a. fields (True)  — is_related_ecommerce=True  → ครบ 5 fields (รวม related_ecommerce_id)
  9b. fields (False) — is_related_ecommerce=False → ได้ 4 fields (ไม่มี related_ecommerce_id)
  10. filter_out_category — ทุก items ไม่มี article_category ที่ถูก filter ออก
  11. exclude_ids (excluded) — ID ที่กำหนดไม่ปรากฏใน response
  12. exclude_ids (included) — ID นั้นปรากฏใน response เมื่อไม่กำหนด exclude_ids

Test Cases — TOPHIT:
  T1. agg_tophit_group_field (required) — ใส่ → มี buckets / ไม่ใส่ → ไม่มี buckets
  T2. agg_tophit_limit 50 / 100 — hits ต่อ bucket ไม่เกิน limit และ limit=100 >= limit=50
  T3. agg_tophit_output_fields — _source ทุกตัวมีครบตาม output_fields ที่กำหนด
  T4. agg_tophit_sort_by (required, PLAY_COUNT_DAY_14) — ใส่ → มี buckets / ไม่ใส่ → ไม่มี buckets

Test Cases — LATEST:
  L1. agg_latest_group_field (required) — ใส่ → มี buckets / ไม่ใส่ → ไม่มี buckets
  L2. agg_latest_limit 50 / 100 — hits ต่อ bucket ไม่เกิน limit และ limit=100 >= limit=50
  L3. agg_latest_output_fields — _source ทุกตัวมีครบตาม output_fields ที่กำหนด
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
API_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/ugcsfv"

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
    "01aXJ4536J6w","1Bl3PPDW8kon","2J49PwZ6plP0","4DRwJzJPkGol","6KBzMY5Bk8XK","8AjN0gQELBeA","x0VWMaeQpaeK","GdWOYbZZ526d","Meo7Qg21Zo7e","KAaqaaj30zXA","LDqgMMK9JBkj"
]

# 5 fields จาก REQUEST_BODY_STANDARD
FIELDS_5 = ["id", "create_date", "title", "article_category", "related_ecommerce_id"]

# category ที่ถูก filter ออก
FILTER_OUT_CATEGORY = ["travel"]

# ID ที่ใช้ทดสอบ exclude_ids
EXCLUDE_ID = "8AjN0gQELBeA"

# ── Tophit config ────────────────────────────────────────
TOPHIT_BASE_PARAMS = {
    "tophit_date_filter": 30,
    "agg_tophit_group_field": "article_category",
    "agg_tophit_limit": 50,
    "agg_tophit_output_fields": [
        "id", "publish_date", "article_category", "tags", "create_by"
    ],
    "agg_tophit_sort_by": "PLAY_COUNT_DAY_14"
}
TOPHIT_OUTPUT_FIELDS = ["id", "publish_date", "article_category", "tags", "create_by"]

# ── Latest config ─────────────────────────────────────────
LATEST_BASE_PARAMS = {
    "tophit_date_filter": 30,
    "agg_latest_group_field": "article_category",
    "agg_latest_limit": 50,
    "agg_latest_output_fields": [
        "id", "publish_date", "article_category", "tags", "create_by"
    ],
}
LATEST_OUTPUT_FIELDS = ["id", "publish_date", "article_category", "tags", "create_by"]


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


# ── Tophit-specific helpers ──────────────────────────────
def get_tophit_buckets(response: dict) -> list:
    """ดึง list ของ buckets จาก aggregations.agg_tophit"""
    return (response.get("data", {})
                    .get("aggregations", {})
                    .get("agg_tophit", {})
                    .get("buckets", []))


def get_bucket_hits(bucket: dict) -> list:
    """ดึง hits จาก bucket (รองรับทั้ง sort_by_hit_count และ hits fallback)"""
    for key in ["sort_by_hit_count", "hits"]:
        hits = bucket.get(key, {}).get("hits", {}).get("hits", [])
        if hits:
            return hits
    return []


def get_all_tophit_sources(response: dict) -> list:
    """ดึง _source ทุกตัวจากทุก bucket รวมกัน"""
    sources = []
    for bucket in get_tophit_buckets(response):
        for h in get_bucket_hits(bucket):
            sources.append(h.get("_source", {}))
    return sources


def get_tophit_ids_from_response(response: dict) -> list:
    """ดึง unique IDs ทั้งหมดจาก tophit buckets"""
    return get_ids(get_all_tophit_sources(response))


# ── Latest-specific helpers ───────────────────────────────
def get_latest_buckets(response: dict) -> list:
    """ดึง list ของ buckets จาก aggregations.agg_latest"""
    return (response.get("data", {})
                    .get("aggregations", {})
                    .get("agg_latest", {})
                    .get("buckets", []))


def get_latest_bucket_hits(bucket: dict) -> list:
    """ดึง hits จาก latest bucket (รองรับ sort_by_publish_date / latest / hits)"""
    for key in ["sort_by_publish_date", "latest", "hits"]:
        hits = bucket.get(key, {}).get("hits", {}).get("hits", [])
        if hits:
            return hits
    return []


def get_all_latest_sources(response: dict) -> list:
    """ดึง _source ทุกตัวจากทุก latest bucket"""
    sources = []
    for bucket in get_latest_buckets(response):
        for h in get_latest_bucket_hits(bucket):
            sources.append(h.get("_source", {}))
    return sources


def get_latest_ids_from_response(response: dict) -> list:
    """ดึง unique IDs ทั้งหมดจาก latest buckets"""
    return get_ids(get_all_latest_sources(response))


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
    """SQL ง่ายๆ เช็คว่า IDs จาก API มีใน mst_ugcsfv_nonprod ไหม"""
    id_array = "[" + ", ".join(f"'{i}'" for i in ids) + "]"
    return f"""
SELECT id
FROM mst_ugcsfv_nonprod
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

    only_api = sorted(api_set - sp_set)   # อยู่ใน API แต่ไม่มีใน Spanner
    only_sp  = sorted(sp_set  - api_set)  # อยู่ใน Spanner แต่ไม่มีใน API (ปกติ)
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
    """ทดสอบ limit 5, 20, 50 — items ที่ได้ต้องไม่เกิน limit"""
    all_sp_detail = []
    for lim in [5, 20, 50]:
        resp    = call_api({"limit": lim, "language": "th"})
        sources = get_sources(resp)
        count   = len(sources)
        print(f"    limit={lim} → ได้ {count} items")
        if count > lim:
            return False, f"limit={lim} แต่ได้ {count} items (เกิน!)"

        # Spanner verify
        api_ids = get_ids(sources)
        sp_ok, sp_msg, sp_detail = compare_with_spanner(f"limit={lim}", api_ids)
        all_sp_detail.append({"limit": lim, **sp_detail})
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


# ── 4. tophit_date_filter 30 → 50 ───────────────────────
def test_tophit_date_filter_50():
    """
    tophit_date_filter=50 ควรได้ items >= tophit_date_filter=30
    และ IDs ทั้ง 2 set match Spanner
    """
    base = {"limit": 200, "language": "th"}
    resp_30 = call_api({**base, "tophit_date_filter": 30})
    resp_50 = call_api({**base, "tophit_date_filter": 50})
    c30 = len(get_sources(resp_30))
    c50 = len(get_sources(resp_50))
    print(f"    tophit_date_filter=30 → {c30} items")
    print(f"    tophit_date_filter=50 → {c50} items")

    if c50 < c30:
        return False, f"filter=50 ({c50}) น้อยกว่า filter=30 ({c30}) — ผิดปกติ"

    # Spanner verify ทั้ง 2 set
    for label, resp in [("filter=30", resp_30), ("filter=50", resp_50)]:
        api_ids = get_ids(get_sources(resp))
        sp_ok, sp_msg, _ = compare_with_spanner(label, api_ids)
        if not sp_ok:
            return False, f"{label} parameter OK แต่ {sp_msg}"

    return True, f"filter=50 ({c50}) >= filter=30 ({c30}) และทุก IDs match Spanner ✓"


# ── 5. title ต้องมีคำว่า "รัก" ทุก items ────────────────
def test_title_contains_rak():
    """ทุก _source.title ต้องมีคำว่า 'รัก' และ IDs match Spanner"""
    resp    = call_api({"limit": 50, "language": "th", "title": "รัก", "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    failed = [
        {"id": s.get("id"), "title": s.get("title")}
        for s in sources
        if "รัก" not in (s.get("title") or "")
    ]
    if failed:
        return False, f"มี {len(failed)} items ที่ title ไม่มีคำว่า 'รัก': {failed[:3]}"

    print(f"    title 'รัก' → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("title=รัก", api_ids)
    if not sp_ok:
        return False, f"title OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี 'รัก' ใน title และ {sp_msg}"


# ── 6. is_related_ecommerce = False ─────────────────────
def test_is_related_ecommerce_false():
    """ทุก items ต้องไม่มี related_ecommerce_id และ IDs match Spanner"""
    resp    = call_api({"limit": 50, "language": "th",
                        "is_related_ecommerce": False, "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    has_related = [s.get("id") for s in sources if s.get("related_ecommerce_id")]
    if has_related:
        return False, (
            f"is_related_ecommerce=False แต่ {len(has_related)} items "
            f"ยังมี related_ecommerce_id: {has_related[:5]}"
        )

    print(f"    is_related_ecommerce=False → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("is_related_ecommerce=False", api_ids)
    if not sp_ok:
        return False, f"parameter OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items ไม่มี related_ecommerce_id และ {sp_msg}"


# ── 7. is_related_ecommerce = True ──────────────────────
def test_is_related_ecommerce_true():
    """ทุก items ต้องมี related_ecommerce_id และ IDs match Spanner"""
    resp    = call_api({"limit": 50, "language": "th",
                        "is_related_ecommerce": True, "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    no_related = [s.get("id") for s in sources if not s.get("related_ecommerce_id")]
    if no_related:
        return False, (
            f"is_related_ecommerce=True แต่ {len(no_related)} items "
            f"ไม่มี related_ecommerce_id: {no_related[:5]}"
        )

    print(f"    is_related_ecommerce=True → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("is_related_ecommerce=True", api_ids)
    if not sp_ok:
        return False, f"parameter OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มี related_ecommerce_id และ {sp_msg}"


# ── 8. id filter ────────────────────────────────────────
def test_id_filter():
    """IDs ที่ออกมาต้องอยู่ใน ID_LIST เท่านั้น และ match Spanner"""
    resp    = call_api({"limit": 100, "language": "th",
                        "id": ID_LIST, "fields": FIELDS_5})
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


# ── 9a. fields — is_related_ecommerce=True → ครบ 5 fields
def test_fields_5_with_related():
    """is_related_ecommerce=True → ครบ 5 fields และ IDs match Spanner"""
    resp    = call_api({"limit": 20, "language": "th",
                        "is_related_ecommerce": True, "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    expected = set(FIELDS_5)
    for s in sources:
        actual = set(s.keys())
        if actual != expected:
            extra   = actual - expected
            missing = expected - actual
            return False, (
                f"ID={s.get('id')} — fields ไม่ตรง: "
                f"extra={extra or '∅'}, missing={missing or '∅'}"
            )

    print(f"    fields (True) → ทุก {len(sources)} items มีครบ 5 fields")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("fields-5 (True)", api_ids)
    if not sp_ok:
        return False, f"fields OK แต่ {sp_msg}"
    return True, f"ครบ 5 fields: {FIELDS_5} และ {sp_msg}"


# ── 9b. fields — is_related_ecommerce=False → 4 fields ─
def test_fields_4_without_related():
    """is_related_ecommerce=False → 4 fields (ไม่มี related_ecommerce_id) และ IDs match Spanner"""
    FIELDS_4 = [f for f in FIELDS_5 if f != "related_ecommerce_id"]
    resp    = call_api({"limit": 20, "language": "th",
                        "is_related_ecommerce": False, "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    expected = set(FIELDS_4)
    for s in sources:
        actual = set(s.keys())
        if "related_ecommerce_id" in actual:
            return False, (
                f"ID={s.get('id')} — is_related_ecommerce=False "
                f"แต่ยังมี related_ecommerce_id ใน _source"
            )
        missing = expected - actual
        if missing:
            return False, f"ID={s.get('id')} — ขาด fields: {missing}"

    print(f"    fields (False) → ทุก {len(sources)} items มี 4 fields")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("fields-4 (False)", api_ids)
    if not sp_ok:
        return False, f"fields OK แต่ {sp_msg}"
    return True, f"มี 4 fields {FIELDS_4} (ไม่มี related_ecommerce_id) และ {sp_msg}"


# ── 10. filter_out_category ─────────────────────────────
def test_filter_out_category():
    """ทุก items ต้องไม่มี article_category ใน FILTER_OUT_CATEGORY และ match Spanner"""
    resp    = call_api({"limit": 100, "language": "th",
                        "filter_out_category": FILTER_OUT_CATEGORY, "fields": FIELDS_5})
    sources = get_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน response"

    violations = [
        {"id": s.get("id"), "article_category": s.get("article_category")}
        for s in sources
        if s.get("article_category") in FILTER_OUT_CATEGORY
    ]
    if violations:
        return False, (
            f"มี {len(violations)} items ที่ article_category อยู่ใน "
            f"filter_out_category {FILTER_OUT_CATEGORY}: {violations[:5]}"
        )

    print(f"    filter_out_category → ทุก {len(sources)} items ผ่าน")
    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("filter_out_category", api_ids)
    if not sp_ok:
        return False, f"filter_out_category OK แต่ {sp_msg}"
    return True, (
        f"ทุก {len(sources)} items ไม่มี article_category "
        f"ใน {FILTER_OUT_CATEGORY} และ {sp_msg}"
    )


# ── 11. exclude_ids — ID ไม่ปรากฏเมื่อกำหนด ────────────
def test_exclude_ids_when_set():
    """exclude_ids=[EXCLUDE_ID] → EXCLUDE_ID ต้องไม่มีใน response และ remaining IDs match Spanner"""
    resp    = call_api({"limit": 200, "language": "th",
                        "id": ID_LIST, "exclude_ids": [EXCLUDE_ID], "fields": FIELDS_5})
    sources = get_sources(resp)
    returned_ids = [s.get("id") for s in sources]
    print(f"    exclude_ids=['{EXCLUDE_ID}'] → ได้ {len(returned_ids)} items")

    if EXCLUDE_ID in returned_ids:
        return False, f"'{EXCLUDE_ID}' ยังปรากฏใน response ทั้งที่กำหนด exclude_ids"

    api_ids = get_ids(sources)
    sp_ok, sp_msg, _ = compare_with_spanner("exclude_ids (set)", api_ids)
    if not sp_ok:
        return False, f"exclude_ids OK แต่ {sp_msg}"
    return True, f"'{EXCLUDE_ID}' ไม่มีใน response และ {sp_msg}"


# ── 12. exclude_ids — ID ปรากฏเมื่อไม่กำหนด ────────────
def test_exclude_ids_when_not_set():
    """ไม่กำหนด exclude_ids → EXCLUDE_ID ต้องปรากฏใน response และ IDs match Spanner"""
    resp    = call_api({"limit": 200, "language": "th",
                        "id": ID_LIST, "fields": FIELDS_5})
    sources = get_sources(resp)
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
# TOPHIT TEST CASES
# ======================================================

# ── T1. agg_tophit_group_field (required) ───────────────
def test_tophit_group_field_required():
    """
    ใส่ agg_tophit_group_field → ต้องได้ buckets กลับมา
    ไม่ใส่ → ต้องไม่มี agg_tophit buckets (required field)
    """
    # Case A: ใส่ group_field → ต้องได้ buckets
    resp_with    = call_api(TOPHIT_BASE_PARAMS)
    buckets_with = get_tophit_buckets(resp_with)
    print(f"    ใส่ group_field    → {len(buckets_with)} buckets")
    if not buckets_with:
        return False, "ใส่ agg_tophit_group_field แล้วแต่ไม่มี buckets ใน response"

    # Case B: ไม่ใส่ group_field → ต้องไม่มี agg_tophit buckets
    params_without  = {k: v for k, v in TOPHIT_BASE_PARAMS.items()
                       if k != "agg_tophit_group_field"}
    resp_without    = call_api(params_without)
    buckets_without = get_tophit_buckets(resp_without)
    print(f"    ไม่ใส่ group_field → {len(buckets_without)} buckets")
    if buckets_without:
        return False, (
            f"ไม่ใส่ agg_tophit_group_field แต่ยังได้ {len(buckets_without)} buckets "
            f"(ควรไม่มีเพราะเป็น required field)"
        )

    # Spanner verify กับ IDs จาก Case A
    api_ids = get_tophit_ids_from_response(resp_with)
    sp_ok, sp_msg, _ = compare_with_spanner("tophit group_field", api_ids)
    if not sp_ok:
        return False, f"required field OK แต่ {sp_msg}"
    return True, f"ใส่ → {len(buckets_with)} buckets / ไม่ใส่ → 0 buckets (required ✓) และ {sp_msg}"


# ── T2. agg_tophit_limit 50 / 100 ───────────────────────
def test_tophit_limit():
    """
    limit=50: hits ต่อ bucket <= 50
    limit=100: hits ต่อ bucket <= 100
    limit=100 รวม >= limit=50 รวม (ช่วงเดียวกัน แต่เลือกได้มากกว่า)
    """
    results = {}
    for lim in [50, 100]:
        params = {**TOPHIT_BASE_PARAMS, "agg_tophit_limit": lim}
        resp   = call_api(params)
        buckets = get_tophit_buckets(resp)
        if not buckets:
            return False, f"agg_tophit_limit={lim} → ไม่มี buckets"

        total_hits = 0
        for bucket in buckets:
            hits = get_bucket_hits(bucket)
            bucket_count = len(hits)
            total_hits += bucket_count
            if bucket_count > lim:
                return False, (
                    f"limit={lim} แต่ bucket '{bucket.get('key')}' "
                    f"มี {bucket_count} hits (เกิน!)"
                )
        results[lim] = {"total": total_hits, "buckets": len(buckets)}
        print(f"    limit={lim:>3} → {len(buckets)} buckets, {total_hits} hits รวม")

        # Spanner verify
        api_ids = get_tophit_ids_from_response(resp)
        sp_ok, sp_msg, _ = compare_with_spanner(f"tophit limit={lim}", api_ids)
        if not sp_ok:
            return False, f"limit={lim} parameter OK แต่ {sp_msg}"

    if results[100]["total"] < results[50]["total"]:
        return False, (
            f"limit=100 ({results[100]['total']} hits) น้อยกว่า "
            f"limit=50 ({results[50]['total']} hits) — ผิดปกติ"
        )
    return True, (
        f"limit=50→{results[50]['total']} hits, "
        f"limit=100→{results[100]['total']} hits, ทุก bucket ไม่เกิน limit และ Spanner match ✓"
    )


# ── T3. agg_tophit_output_fields ────────────────────────
def test_tophit_output_fields():
    """
    กำหนด agg_tophit_output_fields → _source ทุกตัวต้องมีครบตาม output_fields พอดี
    """
    resp    = call_api(TOPHIT_BASE_PARAMS)
    sources = get_all_tophit_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน tophit response"

    expected = set(TOPHIT_OUTPUT_FIELDS)
    for s in sources:
        actual  = set(s.keys())
        missing = expected - actual
        extra   = actual - expected
        if missing or extra:
            return False, (
                f"ID={s.get('id')} — output_fields ไม่ตรง: "
                f"missing={missing or '∅'}, extra={extra or '∅'}"
            )

    print(f"    output_fields → ทุก {len(sources)} items มีครบ {TOPHIT_OUTPUT_FIELDS}")
    api_ids = get_tophit_ids_from_response(resp)
    sp_ok, sp_msg, _ = compare_with_spanner("tophit output_fields", api_ids)
    if not sp_ok:
        return False, f"output_fields OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มีครบ {len(TOPHIT_OUTPUT_FIELDS)} fields และ {sp_msg}"


# ── T4. agg_tophit_sort_by (required, valid: PLAY_COUNT_DAY_14)
def test_tophit_sort_by():
    """
    ใส่ agg_tophit_sort_by=PLAY_COUNT_DAY_14 → ต้องได้ buckets กลับมา
    ไม่ใส่ → ต้องไม่มี agg_tophit buckets (required field)
    """
    # Case A: ใส่ sort_by → ต้องได้ buckets
    resp_with    = call_api(TOPHIT_BASE_PARAMS)
    buckets_with = get_tophit_buckets(resp_with)
    total_with   = sum(len(get_bucket_hits(b)) for b in buckets_with)
    print(f"    ใส่ sort_by=PLAY_COUNT_DAY_14 → {len(buckets_with)} buckets, {total_with} hits")
    if not buckets_with:
        return False, "ใส่ agg_tophit_sort_by แล้วแต่ไม่มี buckets ใน response"

    # Case B: ไม่ใส่ sort_by → ต้องไม่มี agg_tophit buckets
    params_without  = {k: v for k, v in TOPHIT_BASE_PARAMS.items()
                       if k != "agg_tophit_sort_by"}
    resp_without    = call_api(params_without)
    buckets_without = get_tophit_buckets(resp_without)
    print(f"    ไม่ใส่ sort_by          → {len(buckets_without)} buckets")
    if buckets_without:
        return False, (
            f"ไม่ใส่ agg_tophit_sort_by แต่ยังได้ {len(buckets_without)} buckets "
            f"(ควรไม่มีเพราะเป็น required field)"
        )

    # Spanner verify กับ IDs จาก Case A
    api_ids = get_tophit_ids_from_response(resp_with)
    sp_ok, sp_msg, _ = compare_with_spanner("tophit sort_by", api_ids)
    if not sp_ok:
        return False, f"required field OK แต่ {sp_msg}"
    return True, (
        f"ใส่ PLAY_COUNT_DAY_14 → {len(buckets_with)} buckets / "
        f"ไม่ใส่ → 0 buckets (required ✓) และ {sp_msg}"
    )


# ======================================================
# LATEST TEST CASES
# ======================================================

# ── L1. agg_latest_group_field (required) ───────────────
def test_latest_group_field_required():
    """
    ใส่ agg_latest_group_field → ต้องได้ buckets กลับมา
    ไม่ใส่ → ต้องไม่มี agg_latest buckets (required field)
    """
    # Case A: ใส่ group_field → ต้องได้ buckets
    resp_with    = call_api(LATEST_BASE_PARAMS)
    buckets_with = get_latest_buckets(resp_with)
    print(f"    ใส่ group_field    → {len(buckets_with)} buckets")
    if not buckets_with:
        return False, "ใส่ agg_latest_group_field แล้วแต่ไม่มี buckets ใน response"

    # Case B: ไม่ใส่ group_field → ต้องไม่มี agg_latest buckets
    params_without  = {k: v for k, v in LATEST_BASE_PARAMS.items()
                       if k != "agg_latest_group_field"}
    resp_without    = call_api(params_without)
    buckets_without = get_latest_buckets(resp_without)
    print(f"    ไม่ใส่ group_field → {len(buckets_without)} buckets")
    if buckets_without:
        return False, (
            f"ไม่ใส่ agg_latest_group_field แต่ยังได้ {len(buckets_without)} buckets "
            f"(ควรไม่มีเพราะเป็น required field)"
        )

    api_ids = get_latest_ids_from_response(resp_with)
    sp_ok, sp_msg, _ = compare_with_spanner("latest group_field", api_ids)
    if not sp_ok:
        return False, f"required field OK แต่ {sp_msg}"
    return True, f"ใส่ → {len(buckets_with)} buckets / ไม่ใส่ → 0 buckets (required ✓) และ {sp_msg}"


# ── L2. agg_latest_limit 50 / 100 ───────────────────────
def test_latest_limit():
    """
    limit=50: hits ต่อ bucket <= 50
    limit=100: hits ต่อ bucket <= 100
    limit=100 รวม >= limit=50 รวม
    """
    results = {}
    for lim in [50, 100]:
        params  = {**LATEST_BASE_PARAMS, "agg_latest_limit": lim}
        resp    = call_api(params)
        buckets = get_latest_buckets(resp)
        if not buckets:
            return False, f"agg_latest_limit={lim} → ไม่มี buckets"

        total_hits = 0
        for bucket in buckets:
            hits = get_latest_bucket_hits(bucket)
            bucket_count = len(hits)
            total_hits += bucket_count
            if bucket_count > lim:
                return False, (
                    f"limit={lim} แต่ bucket '{bucket.get('key')}' "
                    f"มี {bucket_count} hits (เกิน!)"
                )
        results[lim] = {"total": total_hits, "buckets": len(buckets)}
        print(f"    limit={lim:>3} → {len(buckets)} buckets, {total_hits} hits รวม")

        api_ids = get_latest_ids_from_response(resp)
        sp_ok, sp_msg, _ = compare_with_spanner(f"latest limit={lim}", api_ids)
        if not sp_ok:
            return False, f"limit={lim} parameter OK แต่ {sp_msg}"

    if results[100]["total"] < results[50]["total"]:
        return False, (
            f"limit=100 ({results[100]['total']} hits) น้อยกว่า "
            f"limit=50 ({results[50]['total']} hits) — ผิดปกติ"
        )
    return True, (
        f"limit=50→{results[50]['total']} hits, "
        f"limit=100→{results[100]['total']} hits, ทุก bucket ไม่เกิน limit และ Spanner match ✓"
    )


# ── L3. agg_latest_output_fields ────────────────────────
def test_latest_output_fields():
    """
    กำหนด agg_latest_output_fields → _source ทุกตัวต้องมีครบตาม output_fields พอดี
    """
    resp    = call_api(LATEST_BASE_PARAMS)
    sources = get_all_latest_sources(resp)
    if not sources:
        return False, "ไม่มี items ใน latest response"

    expected = set(LATEST_OUTPUT_FIELDS)
    for s in sources:
        actual  = set(s.keys())
        missing = expected - actual
        extra   = actual - expected
        if missing or extra:
            return False, (
                f"ID={s.get('id')} — output_fields ไม่ตรง: "
                f"missing={missing or '∅'}, extra={extra or '∅'}"
            )

    print(f"    output_fields → ทุก {len(sources)} items มีครบ {LATEST_OUTPUT_FIELDS}")
    api_ids = get_latest_ids_from_response(resp)
    sp_ok, sp_msg, _ = compare_with_spanner("latest output_fields", api_ids)
    if not sp_ok:
        return False, f"output_fields OK แต่ {sp_msg}"
    return True, f"ทุก {len(sources)} items มีครบ {len(LATEST_OUTPUT_FIELDS)} fields และ {sp_msg}"


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("=" * 60)
    print("🚀  UGC SFV — Parameter Test Suite (Standard + Tophit + Latest)")
    print(f"    API     : {API_URL}")
    print(f"    Spanner : {SP_PROJECT} / {SP_INSTANCE} / {SP_DATABASE}")
    print("=" * 60)

    # ── Standard ─────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  STANDARD")
    print(f"{'━'*60}")
    run_test("1.  limit (5 / 20 / 50)",                                   test_limit)
    run_test("2.  language = th",                                          test_language_th)
    run_test("3.  language = en",                                          test_language_en)
    run_test("4.  tophit_date_filter 30 → 50",                            test_tophit_date_filter_50)
    run_test("5.  title — ทุก items มีคำว่า 'รัก'",                      test_title_contains_rak)
    run_test("6.  is_related_ecommerce=False → ไม่มี related",           test_is_related_ecommerce_false)
    run_test("7.  is_related_ecommerce=True  → มี related ทุก item",     test_is_related_ecommerce_true)
    run_test("8.  id filter — IDs อยู่ใน id list เท่านั้น",             test_id_filter)
    run_test("9a. fields — is_related_ecommerce=True  → ครบ 5 fields",   test_fields_5_with_related)
    run_test("9b. fields — is_related_ecommerce=False → ได้ 4 fields",   test_fields_4_without_related)
    run_test("10. filter_out_category — ไม่มี category ต้องห้าม",       test_filter_out_category)
    run_test("11. exclude_ids — ID ไม่ปรากฏเมื่อกำหนด",                 test_exclude_ids_when_set)
    run_test("12. exclude_ids — ID ปรากฏเมื่อไม่กำหนด",                 test_exclude_ids_when_not_set)

    # ── Tophit ───────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  TOPHIT")
    print(f"{'━'*60}")
    run_test("T1. agg_tophit_group_field — required, แบ่งกลุ่มตาม article_category", test_tophit_group_field_required)
    run_test("T2. agg_tophit_limit 50 / 100 — hits ต่อ bucket ไม่เกิน limit",        test_tophit_limit)
    run_test("T3. agg_tophit_output_fields — _source ครบตาม fields ที่กำหนด",        test_tophit_output_fields)
    run_test("T4. agg_tophit_sort_by — required (PLAY_COUNT_DAY_14), ไม่ใส่ → ไม่มี buckets", test_tophit_sort_by)

    # ── Latest ───────────────────────────────────────────
    print(f"\n{'━'*60}")
    print("📌  LATEST")
    print(f"{'━'*60}")
    run_test("L1. agg_latest_group_field — required, แบ่งกลุ่มตาม article_category", test_latest_group_field_required)
    run_test("L2. agg_latest_limit 50 / 100 — hits ต่อ bucket ไม่เกิน limit",        test_latest_limit)
    run_test("L3. agg_latest_output_fields — _source ครบตาม fields ที่กำหนด",        test_latest_output_fields)

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
    report_path = os.path.join(out_dir, "test_ugcsfv_result.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_results, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 บันทึกผลที่: {report_path}")

    sys.exit(0 if passed_count == total_count else 1)
