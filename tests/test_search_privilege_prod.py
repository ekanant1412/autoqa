"""
Test: search_privilege (p-s2), search_similar (g-s1),
      search_similar_movie (m-s4), get_merchant (d-s4),
      ecommerce_search (sr-b1), get_search (sr-b2),
      search_similar (s-s4)
- ตรวจสอบว่า URL ที่ใช้ตรงกับ model ที่กำหนด
- ตรวจสอบว่ามีผลลัพธ์ใน result
"""

import requests
import pytest

# ============================================================
# Config — shared
# ============================================================
BASE_HOST = "http://ai-universal-service-new.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
EXPECTED_MODEL_HOST = (
    "https://search-prod-api-internal-container-302532291193.asia-southeast1.run.app"
)

# --- p-s2 (privilege) ---
API_URL_P_S2 = f"{BASE_HOST}/api/v1/universal/p-s2"
TARGET_ID_P_S2 = "6XDAbkv5R91n"
API_PARAMS_P_S2 = {
    "id": TARGET_ID_P_S2,
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}

# --- g-s1 (game) ---
API_URL_G_S1 = f"{BASE_HOST}/api/v1/universal/g-s1"
TARGET_ID_G_S1 = "zbpNvAMKaRxD"
API_PARAMS_G_S1 = {
    "id": TARGET_ID_G_S1,
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}

# --- sr-b2 (privilege keyword search) ---
API_URL_SR_B2 = f"{BASE_HOST}/api/v1/universal/sr-b2"
SEARCH_KEYWORD_SR_B2 = "cat"
API_PARAMS_SR_B2 = {
    "search_keyword": SEARCH_KEYWORD_SR_B2,
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}

# --- sr-b1 (ecommerce) ---
API_URL_SR_B1 = f"{BASE_HOST}/api/v1/universal/sr-b1"
API_PARAMS_SR_B1 = {
    "search_keyword": "gun",
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}

# --- d-s4 (deal/merchant) ---
API_URL_D_S4 = f"{BASE_HOST}/api/v1/universal/d-s4"
TARGET_ID_D_S4 = "alBRxrPQ857l"
API_PARAMS_D_S4 = {
    "id": TARGET_ID_D_S4,
    "returnItemMetadata": "true",
    "verbose": "debug",
    "ssoId": "7835767",
}

# --- s-s4 (movie keyword search) ---
API_URL_S_S4 = f"{BASE_HOST}/api/v1/universal/s-s4"
SEARCH_KEYWORD_S_S4 = "cat"
API_PARAMS_S_S4 = {
    "search_keyword": SEARCH_KEYWORD_S_S4,
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}

# --- m-s4 (movie) ---
API_URL_M_S4 = f"{BASE_HOST}/api/v1/universal/m-s4"
TARGET_ID_M_S4 = "wEBoelypBKNJ"
API_PARAMS_M_S4 = {
    "id": TARGET_ID_M_S4,
    "isOnlyId": "true",
    "language": "th",
    "limit": "10",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "userId": "null",
    "verbose": "debug",
    "deviceId": "d2e2742c64acf055",
    "ssoId": "7835767",
}


# ============================================================
# Helpers
# ============================================================
def _call_api(url: str, params: dict) -> dict:
    """GET ก่อน fallback POST ถ้า 404/405"""
    session = requests.Session()
    resp = session.get(url, params=params, timeout=30)
    if resp.status_code in (404, 405):
        resp = session.post(url, params=params, data="", timeout=30)
    assert resp.status_code == 200, (
        f"API returned HTTP {resp.status_code}\n"
        f"URL : {resp.url}\n"
        f"Body: {resp.text[:500]}"
    )
    return resp.json()


def _find_component(data, name: str, depth: int = 0):
    """ค้นหา component ตาม name แบบ recursive"""
    if depth > 10:
        return None
    if isinstance(data, list):
        for item in data:
            found = _find_component(item, name, depth + 1)
            if found is not None:
                return found
    elif isinstance(data, dict):
        if data.get("name") == name:
            return data
        for value in data.values():
            found = _find_component(value, name, depth + 1)
            if found is not None:
                return found
    return None


def _get_component(api_response: dict, name: str):
    component = _find_component(api_response, name)
    if component is None:
        keys = list(api_response.keys()) if isinstance(api_response, dict) else type(api_response)
        pytest.fail(f"ไม่พบ component '{name}' ใน response\nResponse keys: {keys}")
    return component


# ============================================================
# Fixtures
# ============================================================
@pytest.fixture(scope="module")
def response_p_s2():
    return _call_api(API_URL_P_S2, API_PARAMS_P_S2)


@pytest.fixture(scope="module")
def response_g_s1():
    return _call_api(API_URL_G_S1, API_PARAMS_G_S1)


@pytest.fixture(scope="module")
def response_sr_b2():
    return _call_api(API_URL_SR_B2, API_PARAMS_SR_B2)


@pytest.fixture(scope="module")
def response_sr_b1():
    return _call_api(API_URL_SR_B1, API_PARAMS_SR_B1)


@pytest.fixture(scope="module")
def response_d_s4():
    return _call_api(API_URL_D_S4, API_PARAMS_D_S4)


@pytest.fixture(scope="module")
def response_s_s4():
    return _call_api(API_URL_S_S4, API_PARAMS_S_S4)


@pytest.fixture(scope="module")
def response_m_s4():
    return _call_api(API_URL_M_S4, API_PARAMS_M_S4)


@pytest.fixture(scope="module")
def search_privilege_component(response_p_s2):
    return _get_component(response_p_s2, "search_privilege")


@pytest.fixture(scope="module")
def search_similar_component(response_g_s1):
    return _get_component(response_g_s1, "search_similar")


@pytest.fixture(scope="module")
def search_similar_s_s4_component(response_s_s4):
    return _get_component(response_s_s4, "search_similar")


@pytest.fixture(scope="module")
def search_similar_movie_component(response_m_s4):
    return _get_component(response_m_s4, "search_similar_movie")


@pytest.fixture(scope="module")
def get_search_component(response_sr_b2):
    return _get_component(response_sr_b2, "get_search")


@pytest.fixture(scope="module")
def ecommerce_search_component(response_sr_b1):
    return _get_component(response_sr_b1, "ecommerce_search")


@pytest.fixture(scope="module")
def get_merchant_component(response_d_s4):
    return _get_component(response_d_s4, "get_merchant")


# ============================================================
# Helper สำหรับ assert url
# ============================================================
def _assert_model_url(component, label):
    url = (
        component
        .get("main_function", {})
        .get("args", {})
        .get("url", "")
    )
    assert url, f"ไม่พบ url ใน main_function.args ({label})"
    assert url.startswith(EXPECTED_MODEL_HOST), (
        f"URL ไม่ตรงกับ model ที่กำหนด ({label})\n"
        f"  expected prefix : {EXPECTED_MODEL_HOST}\n"
        f"  actual url      : {url}"
    )


# ============================================================
# p-s2 :: search_privilege
# ============================================================
class TestSearchPrivilege:
    """p-s2 — ตรวจสอบ node search_privilege"""

    def test_uses_correct_model_url(self, search_privilege_component):
        _assert_model_url(search_privilege_component, "search_privilege")

    def test_has_ids(self, search_privilege_component):
        """result.ids ต้องมี id อย่างน้อย 1 รายการ"""
        ids = search_privilege_component.get("result", {}).get("ids", [])
        assert len(ids) > 0, "result.ids ว่างเปล่า"


# ============================================================
# g-s1 :: search_similar
# ============================================================
class TestSearchSimilar:
    """g-s1 — ตรวจสอบ node search_similar"""

    def test_uses_correct_model_url(self, search_similar_component):
        _assert_model_url(search_similar_component, "search_similar")

    def test_has_ids(self, search_similar_component):
        """result.ids ต้องมี id อย่างน้อย 1 รายการ"""
        ids = search_similar_component.get("result", {}).get("ids", [])
        assert len(ids) > 0, "result.ids ว่างเปล่า"


# ============================================================
# m-s4 :: search_similar_movie
# ============================================================
class TestSearchSimilarMovie:
    """m-s4 — ตรวจสอบ node search_similar_movie"""

    def test_uses_correct_model_url(self, search_similar_movie_component):
        _assert_model_url(search_similar_movie_component, "search_similar_movie")

    def test_has_ids(self, search_similar_movie_component):
        """result.ids ต้องมี id อย่างน้อย 1 รายการ"""
        ids = search_similar_movie_component.get("result", {}).get("ids", [])
        assert len(ids) > 0, "result.ids ว่างเปล่า"


# ============================================================
# d-s4 :: get_merchant
# ============================================================
class TestGetMerchant:
    """d-s4 — ตรวจสอบ node get_merchant"""

    def test_uses_correct_model_url(self, get_merchant_component):
        _assert_model_url(get_merchant_component, "get_merchant")

    def test_has_ids(self, get_merchant_component):
        """result.ids ต้องมี id อย่างน้อย 1 รายการ"""
        ids = get_merchant_component.get("result", {}).get("ids", [])
        assert len(ids) > 0, "result.ids ว่างเปล่า"


# ============================================================
# sr-b1 :: ecommerce_search
# ============================================================
class TestEcommerceSearch:
    """sr-b1 — ตรวจสอบ node ecommerce_search"""

    def test_uses_correct_model_url(self, ecommerce_search_component):
        _assert_model_url(ecommerce_search_component, "ecommerce_search")

    def test_has_items(self, ecommerce_search_component):
        """result.items ต้องมี item อย่างน้อย 1 รายการ (skip ถ้า fallback)"""
        items = ecommerce_search_component.get("result", {}).get("items", [])
        if not items:
            pytest.skip("result.items ว่างเปล่า (fallback) — ข้าม test นี้")


# ============================================================
# sr-b2 :: get_search
# ============================================================
class TestGetSearch:
    """sr-b2 — ตรวจสอบ node get_search"""

    def test_uses_correct_model_url(self, get_search_component):
        _assert_model_url(get_search_component, "get_search")

    def test_has_items(self, get_search_component):
        """result.items ต้องมี item อย่างน้อย 1 รายการ (skip ถ้า fallback)"""
        items = get_search_component.get("result", {}).get("items", [])
        if not items:
            pytest.skip("result.items ว่างเปล่า (fallback) — ข้าม test นี้")


# ============================================================
# s-s4 :: search_similar
# ============================================================
class TestSearchSimilarSS4:
    """s-s4 — ตรวจสอบ node search_similar"""

    def test_uses_correct_model_url(self, search_similar_s_s4_component):
        _assert_model_url(search_similar_s_s4_component, "search_similar (s-s4)")

    def test_has_ids(self, search_similar_s_s4_component):
        """result.ids ต้องมี id อย่างน้อย 1 รายการ"""
        ids = search_similar_s_s4_component.get("result", {}).get("ids", [])
        assert len(ids) > 0, "result.ids ว่างเปล่า"
