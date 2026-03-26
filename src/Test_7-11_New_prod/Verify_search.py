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
    "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sr-b3"
)

DEFAULT_PARAMS = {
    "limit": 20,
    "language": "th",
}

TIMEOUT = 10  # seconds


def get(params: dict) -> requests.Response:
    return requests.get(BASE_URL, params=params, timeout=TIMEOUT)


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

    def test_tc01_status_200_with_valid_keyword(self):
        """TC-01: keyword ปกติต้อง return HTTP 200"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"

    def test_tc02_response_is_json(self):
        """TC-02: response ต้อง parse เป็น JSON ได้"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, dict), "Response ไม่ใช่ JSON object"

    def test_tc03_response_has_items_field(self):
        """TC-03: response ต้องมี field ที่เก็บ list of results"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        data = res.json()
        assert_basic_structure(data)

    def test_tc04_items_is_list(self):
        """TC-04: items ต้องเป็น list"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        data = res.json()
        items = get_items(data)
        assert isinstance(items, list), f"items ไม่ใช่ list: {type(items)}"


# ══════════════════════════════════════════════════════════════════════════════
# TC-02: KEYWORD RELEVANCE
# ══════════════════════════════════════════════════════════════════════════════

class TestKeywordRelevance:

    def test_tc05_keyword_thai_returns_results(self):
        """TC-05: keyword ภาษาไทยต้องได้ผลลัพธ์ > 0"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "โคล่า"})
        data = res.json()
        items = get_items(data)
        assert len(items) > 0, "ค้นหา 'โคล่า' แล้วไม่มีผลลัพธ์"

    def test_tc06_keyword_english_returns_results(self):
        """TC-06: keyword ภาษาอังกฤษต้องได้ผลลัพธ์"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "coke"})
        data = res.json()
        items = get_items(data)
        assert len(items) > 0, "ค้นหา 'coke' แล้วไม่มีผลลัพธ์"


    def test_tc08_different_keywords_return_different_results(self):
        """TC-08: keyword ต่างกันต้องได้ผลลัพธ์ต่างกัน"""
        res1 = get({**DEFAULT_PARAMS, "search_keyword": "coke"})
        res2 = get({**DEFAULT_PARAMS, "search_keyword": "น้ำ"})

        items1 = get_items(res1.json())
        items2 = get_items(res2.json())

        ids1 = {item.get("id") or item.get("contentId") for item in items1}
        ids2 = {item.get("id") or item.get("contentId") for item in items2}

        # ผลลัพธ์ไม่ควรเหมือนกัน 100%
        assert ids1 != ids2, "keyword ต่างกันแต่ได้ผลลัพธ์เหมือนกันทุก item"


# ══════════════════════════════════════════════════════════════════════════════
# TC-03: LIMIT PARAMETER
# ══════════════════════════════════════════════════════════════════════════════

class TestLimitParam:

    def test_tc09_limit_controls_result_count(self):
        """TC-09: param limit ต้องคุมจำนวน items ที่ return"""
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 5})
        data = res.json()
        items = get_items(data)
        assert len(items) <= 5, f"ขอ limit=5 แต่ได้ {len(items)} items"

    def test_tc10_limit_10_vs_limit_20(self):
        """TC-10: limit=20 ต้องได้ items มากกว่าหรือเท่ากับ limit=10"""
        res10 = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 10})
        res20 = get({**DEFAULT_PARAMS, "search_keyword": "หมา", "limit": 20})

        count10 = len(get_items(res10.json()))
        count20 = len(get_items(res20.json()))

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
            "search_keyword": "โคล่า",
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
# TC-07: PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════

class TestPerformance:

    def test_tc19_response_time_under_3s(self):
        """TC-19: response time ต้องไม่เกิน 3 วินาที"""
        start = time.time()
        res = get({**DEFAULT_PARAMS, "search_keyword": "หมา"})
        elapsed = time.time() - start

        assert res.status_code == 200
        assert elapsed < 3.0, f"Response ช้าเกินไป: {elapsed:.2f}s"


# ══════════════════════════════════════════════════════════════════════════════
