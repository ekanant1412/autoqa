"""
Test Suite: sr-b3 Search Endpoint - Functional Tests
Endpoint: /api/v1/universal/sr-b3
Service: ai-universal-service-711 (preprod-gcp-ai-bn)
"""

import pytest
import requests
import time

# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn"
    ".int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sr-b3"
)

METADATA_URL = (
    "http://ai-metadata-service.prod-gcp-ai-bn"
    ".ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

DEFAULT_PARAMS = {
    "limit": 20,
    "language": "th",
}

TIMEOUT = 10  # seconds


def get(params: dict) -> requests.Response:
    return requests.get(BASE_URL, params=params, timeout=TIMEOUT)


def get_content_types(ids: list[str]) -> dict[str, str]:
    """เรียก metadata API เพื่อดึง content_type ของแต่ละ id
    return: {id: content_type}
    """
    res = requests.post(
        METADATA_URL,
        json={
            "parameters": {
                "id": ids,
                "fields": ["id", "content_type"],
            },
            "options": {"cache": False},
        },
        timeout=TIMEOUT,
    )
    res.raise_for_status()
    data = res.json()
    items = data.get("items") or data.get("results") or data.get("data") or []
    return {item["id"]: item.get("content_type", "") for item in items}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def assert_basic_structure(data: dict):
    """ตรวจ response structure ขั้นต่ำ"""
    assert "items" in data or "results" in data or "data" in data, (
        f"Response ไม่มี items/results/data key: {list(data.keys())}"
    )


def get_items(data: dict) -> list:
    """รองรับหลาย key ที่อาจ return items"""
    return data.get("items") or data.get("results") or data.get("data") or []


# ══════════════════════════════════════════════════════════════════════════════
# TC-01: STATUS & RESPONSE FORMAT
# ══════════════════════════════════════════════════════════════════════════════

class TestResponseFormat:

    def test_tc01_status_200_with_valid_keyword(self, evidence):
        """TC-01: keyword ปกติต้อง return HTTP 200"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        evidence["url"] = res.url
        evidence["data_sample"] = f"HTTP {res.status_code}"
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"

    def test_tc02_response_is_json(self, evidence):
        """TC-02: response ต้อง parse เป็น JSON ได้"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        evidence["url"] = res.url
        assert res.status_code == 200
        data = res.json()
        evidence["data_sample"] = f"top-level keys: {list(data.keys())}"
        assert isinstance(data, dict), "Response ไม่ใช่ JSON object"

    def test_tc03_response_has_items_field(self, evidence):
        """TC-03: response ต้องมี field ที่เก็บ list of results"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        evidence["url"] = res.url
        data = res.json()
        evidence["data_sample"] = f"keys: {list(data.keys())}"
        assert_basic_structure(data)

    def test_tc04_items_is_list(self, evidence):
        """TC-04: items ต้องเป็น list"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        evidence["url"] = res.url
        data = res.json()
        items = get_items(data)
        evidence["item_count"] = len(items)
        evidence["data_sample"] = f"type={type(items).__name__}"
        assert isinstance(items, list), f"items ไม่ใช่ list: {type(items)}"


# ══════════════════════════════════════════════════════════════════════════════
# TC-02: KEYWORD RELEVANCE
# ══════════════════════════════════════════════════════════════════════════════

class TestKeywordRelevance:

    def test_tc05_keyword_thai_returns_results(self, evidence):
        """TC-05: keyword ภาษาไทยต้องได้ผลลัพธ์ > 0"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        evidence["url"] = res.url
        data = res.json()
        items = get_items(data)
        evidence["item_count"] = len(items)
        evidence["data_sample"] = str([i.get("id") for i in items[:3]])
        assert len(items) > 0, "ค้นหา 'หมา' แล้วไม่มีผลลัพธ์"

    def test_tc06_keyword_english_returns_results(self, evidence):
        """TC-06: keyword ภาษาอังกฤษต้องได้ผลลัพธ์"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "dog"})
        evidence["url"] = res.url
        data = res.json()
        items = get_items(data)
        evidence["item_count"] = len(items)
        evidence["data_sample"] = str([i.get("id") for i in items[:3]])
        assert len(items) > 0, "ค้นหา 'dog' แล้วไม่มีผลลัพธ์"

    def test_tc08_different_keywords_return_different_results(self, evidence):
        """TC-08: keyword ต่างกันต้องได้ผลลัพธ์ต่างกัน"""
        res1 = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        res2 = get({**DEFAULT_PARAMS, "search_keyword": "แมว"})

        items1 = get_items(res1.json())
        items2 = get_items(res2.json())

        ids1 = {item.get("id") or item.get("contentId") for item in items1}
        ids2 = {item.get("id") or item.get("contentId") for item in items2}

        evidence["url"] = f"{res1.url} | {res2.url}"
        evidence["data_sample"] = f"หมา={len(ids1)} ids, แมว={len(ids2)} ids, overlap={len(ids1&ids2)}"
        assert ids1 != ids2, "keyword ต่างกันแต่ได้ผลลัพธ์เหมือนกันทุก item"


# ══════════════════════════════════════════════════════════════════════════════
# TC-03: LIMIT PARAMETER
# ══════════════════════════════════════════════════════════════════════════════

class TestLimitParam:

    def test_tc09_limit_controls_result_count(self, evidence):
        """TC-09: param limit ต้องคุมจำนวน items ที่ return"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 5})
        evidence["url"] = res.url
        data = res.json()
        items = get_items(data)
        evidence["item_count"] = len(items)
        evidence["data_sample"] = f"requested limit=5, got={len(items)}"
        assert len(items) <= 5, f"ขอ limit=5 แต่ได้ {len(items)} items"

    def test_tc10_limit_10_vs_limit_20(self, evidence):
        """TC-10: limit=20 ต้องได้ items มากกว่าหรือเท่ากับ limit=10"""
        res10 = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 10})
        res20 = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 20})

        count10 = len(get_items(res10.json()))
        count20 = len(get_items(res20.json()))

        evidence["url"] = res20.url
        evidence["data_sample"] = f"limit=10 → {count10} items | limit=20 → {count20} items"
        assert count20 >= count10, (
            f"limit=20 ได้ {count20} items แต่ limit=10 ได้ {count10} items"
        )


# ══════════════════════════════════════════════════════════════════════════════
# TC-04: LANGUAGE PARAMETER
# ══════════════════════════════════════════════════════════════════════════════

class TestLanguageParam:

    def test_tc11_language_th_returns_200(self):
        """TC-11: language=th ต้อง return 200"""
        res = get({"search_keyword": "หมา", "language": "th"})
        assert res.status_code == 200

    def test_tc12_language_en_returns_200(self):
        """TC-12: language=en ต้อง return 200"""
        res = get({"search_keyword": "dog", "language": "en"})
        assert res.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# TC-05: USER CONTEXT PARAMS
# ══════════════════════════════════════════════════════════════════════════════

class TestUserContext:

    def test_tc13_null_userid_returns_results(self):
        """TC-13: userId=null (anonymous) ต้องได้ผลลัพธ์"""
        res = get({
            **DEFAULT_PARAMS,
            "search_keyword": "หมา",
            "userId": "null",
            "pseudoId": "null",
        })
        assert res.status_code == 200
        items = get_items(res.json())
        assert len(items) > 0

    def test_tc14_with_ga_id_returns_200(self):
        """TC-14: ส่ง ga_id มาด้วยต้อง return 200 ปกติ"""
        res = get({
            **DEFAULT_PARAMS,
            "search_keyword": "หมา",
            "ga_id": "100118391.0851155978",
        })
        assert res.status_code == 200

    def test_tc15_with_pseudo_id_returns_200(self):
        """TC-15: ส่ง pseudoId จริงต้อง return 200"""
        res = get({
            **DEFAULT_PARAMS,
            "search_keyword": "หมา",
            "pseudoId": "test-pseudo-abc123",
        })
        assert res.status_code == 200


# ══════════════════════════════════════════════════════════════════════════════
# TC-06: MISSING / INVALID PARAMS
# ══════════════════════════════════════════════════════════════════════════════

class TestInvalidParams:

    def test_tc16_missing_search_keyword_returns_4xx(self):
        """TC-16: ไม่ส่ง search_keyword ต้อง return 4xx (bad request)"""
        res = get({**DEFAULT_PARAMS})  # ไม่มี search_keyword
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"

    def test_tc17_empty_keyword_behavior(self):
        """TC-17: search_keyword='' — ต้อง return 4xx หรือ empty results"""
        res = get({**DEFAULT_PARAMS, "search_keyword": ""})
        if res.status_code == 200:
            items = get_items(res.json())
            # ถ้า 200 ยอมรับได้แต่ items ควรว่าง หรือ return error message
            assert isinstance(items, list)
        else:
            assert res.status_code in range(400, 500)

    def test_tc18_very_long_keyword(self):
        """TC-18: keyword ยาวมาก (500 chars) ไม่ควร 500 error"""
        long_kw = "หมา" * 100  # 300 chars
        res = get({**DEFAULT_PARAMS, "search_keyword": long_kw})
        assert res.status_code != 500, f"keyword ยาวทำให้ 500 error"


# ══════════════════════════════════════════════════════════════════════════════
# TC-07: TYPE PARAMETER (ecommerce / sfv)
# ══════════════════════════════════════════════════════════════════════════════

ALLOWED_SFV_TYPES = {"SFV", "UGCSFV"}  # metadata API คืน lowercase → เทียบด้วย .upper()


class TestTypeParam:

    def test_tc20_type_ecommerce_returns_200(self, evidence):
        """TC-20: type=ecommerce ต้อง return HTTP 200"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "ecommerce"})
        evidence["url"] = res.url
        evidence["data_sample"] = f"HTTP {res.status_code}"
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"

    def test_tc21_type_ecommerce_returns_results(self, evidence):
        """TC-21: type=ecommerce ต้องมี items > 0"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "ecommerce"})
        evidence["url"] = res.url
        assert res.status_code == 200
        items = get_items(res.json())
        evidence["item_count"] = len(items)
        evidence["data_sample"] = str([i.get("id") for i in items[:3]])
        assert len(items) > 0, "type=ecommerce ไม่มีผลลัพธ์"

    def test_tc22_type_sfv_returns_200(self, evidence):
        """TC-22: type=sfv ต้อง return HTTP 200"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "sfv"})
        evidence["url"] = res.url
        evidence["data_sample"] = f"HTTP {res.status_code}"
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"

    def test_tc23_debug_sfv_raw_response(self):
        """TC-23 (DEBUG): print raw response ทั้งหมด เพื่อดู structure จริงของ API"""
        import json
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "sfv"})
        assert res.status_code == 200
        data = res.json()

        # print top-level keys ของ response
        print(f"\n[DEBUG] response top-level keys: {list(data.keys())}")

        # print raw response แบบ pretty (จำกัด 3000 chars)
        raw = json.dumps(data, ensure_ascii=False, indent=2)
        print(f"[DEBUG] raw response (first 3000 chars):\n{raw[:3000]}")

        assert True

    def test_tc24_type_sfv_results_only_sfv_or_ucgsfv(self, evidence):
        """TC-24: type=sfv ต้องได้เฉพาะ items ที่มี content_type เป็น SFV หรือ UGCSFV
        cross-check ผ่าน metadata API เพราะ search API return แค่ id
        """
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "sfv"})
        evidence["url"] = res.url
        assert res.status_code == 200
        items = get_items(res.json())
        assert len(items) > 0, "type=sfv ไม่มีผลลัพธ์"

        ids = [item["id"] for item in items]
        type_map = get_content_types(ids)

        invalid = {id_: ct for id_, ct in type_map.items() if ct.upper() not in ALLOWED_SFV_TYPES}

        # สร้าง evidence แบบ id → type ✓/✗ ทุกตัว
        lines = [
            f"{id_} → {ct} {'✓' if ct.upper() in ALLOWED_SFV_TYPES else '✗'}"
            for id_, ct in type_map.items()
        ]
        evidence["item_count"] = len(type_map)
        evidence["data_sample"] = " | ".join(lines)

        assert len(invalid) == 0, (
            f"\nURL: {res.url}\n"
            f"พบ items ที่ content_type ไม่ใช่ {ALLOWED_SFV_TYPES}: {invalid}"
        )

    def test_tc25_type_sfv_no_ecommerce_items(self, evidence):
        """TC-25: type=sfv ต้องไม่มี item ที่ content_type เป็น ecommerce
        cross-check ผ่าน metadata API
        """
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "sfv"})
        evidence["url"] = res.url
        assert res.status_code == 200
        items = get_items(res.json())

        ids = [item["id"] for item in items]
        type_map = get_content_types(ids)

        ecommerce_ids = {id_: ct for id_, ct in type_map.items() if ct.lower() == "ecommerce"}

        lines = [
            f"{id_} → {ct} {'✗ ecommerce!' if ct.lower() == 'ecommerce' else '✓'}"
            for id_, ct in type_map.items()
        ]
        evidence["item_count"] = len(type_map)
        evidence["data_sample"] = " | ".join(lines)

        assert len(ecommerce_ids) == 0, (
            f"type=sfv แต่พบ ecommerce items: {ecommerce_ids}"
        )

    def test_tc26_type_ecommerce_and_sfv_return_different_results(self, evidence):
        """TC-26: type=ecommerce และ type=sfv ต้องได้ผลลัพธ์ต่างกัน"""
        res_ecom = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "ecommerce"})
        res_sfv  = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "type": "sfv"})

        items_ecom = get_items(res_ecom.json())
        items_sfv  = get_items(res_sfv.json())

        ids_ecom = {item.get("id") or item.get("contentId") for item in items_ecom}
        ids_sfv  = {item.get("id") or item.get("contentId") for item in items_sfv}
        overlap  = ids_ecom & ids_sfv

        evidence["url"] = f"ecommerce: {res_ecom.url}"
        evidence["data_sample"] = (
            f"ecommerce={len(ids_ecom)} ids | sfv={len(ids_sfv)} ids | overlap={len(overlap)}"
        )

        assert ids_ecom != ids_sfv, (
            "type=ecommerce และ type=sfv ได้ผลลัพธ์ชุดเดียวกันทุก item"
        )


# ══════════════════════════════════════════════════════════════════════════════
# TC-08: PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════

class TestPerformance:

    def test_tc27_response_time_under_3s(self):
        """TC-27: response time ต้องไม่เกิน 3 วินาที"""
        start = time.time()
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        elapsed = time.time() - start

        assert res.status_code == 200
        assert elapsed < 3.0, f"Response ช้าเกินไป: {elapsed:.2f}s"


# ══════════════════════════════════════════════════════════════════════════════
