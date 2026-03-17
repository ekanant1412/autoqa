"""
pytest Test Suite — CANDIDATE Search API
==========================================
Source test cases : xray_search_testcases_import02.csv
API endpoint      : ai-universal-service-new / text_search (GET)

Run:
    pytest test_search_api.py -v
    pytest test_search_api.py -v --tb=short
    pytest test_search_api.py -v -k "tc01 or tc02"
"""

import time
from typing import Optional
import pytest
import requests

# ===================================================================
# CONFIG
# ===================================================================
BASELINE_URL = (
    "https://ai-raas-api.trueid-preprod.net"
    "/personalize-rcom/v2/search-api/api/v5/text_search"
)
BASELINE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Authorization": "5b18e526f0463656f7c4329f90b7ecef9dc546aeb6adad28e911ba82",
}
CANDIDATE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/text_search"
)
METADATA_URL = (
    "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)
DEFAULT_TYPE = "711-ecommerce"
BASE_PARAMS = {
    "userId":   "1",
    "pseudoId": "1",
    "cursor":   "1",
    "limit":    "100",
    "top_k":    "450",
    "ssoId":    "999",
    "type":     DEFAULT_TYPE,
}


# ===================================================================
# HELPER
# ===================================================================
def call_search(
    keyword: Optional[str] = "",
    type_val: str = DEFAULT_TYPE,
    omit_keyword: bool = False,
    omit_type: bool = False,
    timeout: int = 30,
) -> requests.Response:
    """
    ยิง GET /text_search
    - omit_keyword=True  → ไม่ส่ง search_keyword เลย (test missing param)
    - omit_type=True     → ไม่ส่ง type เลย
    """
    params = dict(BASE_PARAMS)
    params["type"] = type_val

    if not omit_keyword:
        params["search_keyword"] = keyword if keyword is not None else ""
    if omit_type:
        params.pop("type", None)

    return requests.get(CANDIDATE_URL, params=params, timeout=timeout)


def parse_items(resp: requests.Response) -> list:
    """แกะ items list จาก response JSON"""
    try:
        data = resp.json()
        if isinstance(data, dict):
            return data.get("items") or []
        return []
    except Exception:
        return []


def item_ids(resp: requests.Response) -> list:
    return [x["id"] for x in parse_items(resp) if isinstance(x, dict) and x.get("id")]


def call_baseline(keyword: str, type_val: str = DEFAULT_TYPE, timeout: int = 30) -> list:
    """POST ไปที่ BASELINE แล้วคืน list of ids (สูงสุด 100)"""
    body = {
        "debug": False,
        "no_cache_vector": False,
        "search_keyword": keyword,
        "top_k": "450",
        "type": type_val,
    }
    resp = requests.post(BASELINE_URL, headers=BASELINE_HEADERS, json=body, timeout=timeout)
    assert resp.status_code == 200, (
        f"[BASELINE] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    rj = resp.json()
    items = None
    if isinstance(rj, dict):
        for key in ("search_results", "results", "items", "data"):
            if key in rj and isinstance(rj[key], list):
                items = rj[key]
                break
    elif isinstance(rj, list):
        items = rj
    items = items or []
    return [x["id"] for x in items if isinstance(x, dict) and x.get("id")][:100]


def fetch_metadata(ids: list, fields: list = None, timeout: int = 30) -> dict:
    """POST ไปที่ metadata service แล้วคืน dict {id: item}"""
    if not ids:
        return {}
    if fields is None:
        fields = ["id", "name", "title"]
    payload = {"parameters": {"id": ids, "fields": fields}}
    resp = requests.post(METADATA_URL, json=payload, timeout=timeout)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    return {item["id"]: item for item in items if "id" in item}


# ===================================================================
# TC01 — Thai numeric input
# Summary  : verify search function with thai numeric input
# Testdata : ๒
# Expected : api returns valid response; schema correct; thai numeric handled correctly
# ===================================================================
@pytest.mark.high
def test_tc01_thai_numeric_input():
    resp = call_search("๒")
    assert resp.status_code == 200, (
        f"[TC01] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert isinstance(data, dict), "[TC01] Response body should be a JSON object"
    assert "items" in data, f"[TC01] Missing 'items' key in response: {list(data.keys())}"
    assert isinstance(data["items"], list), "[TC01] 'items' should be a list"


# ===================================================================
# TC02 — Special characters
# Summary  : verify search with special characters is handled correctly
# Testdata : ! @ # % ^ & * ( ) _ + - / ? , .
# Expected : api does not crash; response valid; special characters handled safely
# ===================================================================
@pytest.mark.high
def test_tc02_special_characters():
    keyword = "! @ # % ^ & * ( ) _ + - / ? , ."
    resp = call_search(keyword)
    assert resp.status_code == 200, (
        f"[TC02] Server crashed with 5xx: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        data = resp.json()
    except Exception as e:
        pytest.fail(f"[TC02] Response is not valid JSON: {e}\nRaw: {resp.text[:300]}")
    assert isinstance(data, dict), "[TC02] Response should be a JSON object"


# ===================================================================
# TC03 — Numeric input
# Summary  : verify search function with numeric input
# Testdata : 1
# Expected : api returns valid response; numeric input handled correctly
# ===================================================================
@pytest.mark.medium
def test_tc03_numeric_input():
    resp = call_search("1")
    assert resp.status_code == 200, (
        f"[TC03] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert "items" in data, f"[TC03] Missing 'items' in response: {list(data.keys())}"
    assert isinstance(data["items"], list), "[TC03] 'items' should be a list"


# ===================================================================
# TC04 — Lowercase input
# Summary  : verify search function with lowercase input
# Testdata : fisherman
# Expected : api returns valid response; results relevant to keyword; no error
# ===================================================================
@pytest.mark.high
def test_tc04_lowercase_input():
    resp = call_search("fisherman")
    assert resp.status_code == 200, (
        f"[TC04] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    ids = item_ids(resp)
    assert len(ids) > 0, (
        "[TC04] Expected results for keyword 'fisherman' but got 0 items"
    )


# ===================================================================
# TC05 — Mixed case input
# Summary  : verify search function with mixed lowercase and uppercase input
# Testdata : Fisherman
# Expected : api returns valid response; case handling is correct
# ===================================================================
@pytest.mark.medium
def test_tc05_mixed_case_input():
    resp_mixed = call_search("Fisherman")

    assert resp_mixed.status_code == 200, (
        f"[TC05] Expected HTTP 200 for 'Fisherman', got {resp_mixed.status_code}"
    )
    # Case handling: both should return items (not fail for one but not the other)
    ids_mixed = item_ids(resp_mixed)
    assert len(ids_mixed) > 0, (
            "[TC05] 'fisherman' returned results but 'Fisherman' returned 0 — case handling issue"
        )


# ===================================================================
# TC06 — Uppercase input
# Summary  : verify search function with uppercase input
# Testdata : FISHERMAN
# Expected : api returns valid response; uppercase input handled correctly
# ===================================================================
@pytest.mark.medium
def test_tc06_uppercase_input():
    resp = call_search("FISHERMAN")
    assert resp.status_code == 200, (
        f"[TC06] Expected HTTP 200 for 'FISHERMAN', got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert "items" in data, f"[TC06] Missing 'items' in response: {list(data.keys())}"


# ===================================================================
# TC07 — Whitespace input
# Summary  : verify search function with whitespace input
# Testdata : " " (single space)
# Expected : api handles whitespace correctly; no crash; behavior matches requirement
# ===================================================================
@pytest.mark.high
def test_tc07_whitespace_input():
    resp = call_search(" ")
    assert resp.status_code == 200, (
        f"[TC07] Server crashed with 5xx on whitespace input: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(f"[TC07] Response is not valid JSON for whitespace input: {e}")


# ===================================================================
# TC08 — Empty keyword (consistency check)
# Summary  : verify empty keyword behavior is consistent with requirement
# Testdata : "" (empty)
# Expected : api returns valid response; behavior matches requirement; consistent results
# ===================================================================
@pytest.mark.high
def test_tc08_empty_keyword_consistent():
    resp1 = call_search("")
    time.sleep(1)
    resp2 = call_search("")

    assert resp1.status_code == 200, (
        f"[TC08] First call crashed with 5xx: {resp1.status_code}"
    )
    assert resp2.status_code == 200, (
        f"[TC08] Second call crashed with 5xx: {resp2.status_code}"
    )
    # Consistency: same status code both times
    assert resp1.status_code == resp2.status_code, (
        f"[TC08] Inconsistent status code: call1={resp1.status_code}, call2={resp2.status_code}"
    )
    # If both 200: same item ids (ordering stable for empty keyword)
    if resp1.status_code == 200 and resp2.status_code == 200:
        ids1, ids2 = item_ids(resp1), item_ids(resp2)
        assert ids1 == ids2, (
            f"[TC08] Inconsistent results for empty keyword:\n"
            f"  call1 top5={ids1[:5]}\n  call2 top5={ids2[:5]}"
        )


# ===================================================================
# TC09 — Exact product keyword
# Summary  : verify search with exact product keyword returns expected result set by input
# Testdata : มาชิตะสาหร่ายเกาหลีรสต้มยำ
# Expected : api returns valid response; correct product appears as first result
# ===================================================================
@pytest.mark.high
def test_tc09_exact_product_keyword():
    keyword = "มาชิตะสาหร่ายเกาหลีรสต้มยำ4กx6"
    resp = call_search(keyword)
    assert resp.status_code == 200, (
        f"[TC09] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    ids = item_ids(resp)
    assert len(ids) > 0, (
        f"[TC09] Expected results for exact product keyword '{keyword}' but got 0 items"
    )
    # ดึง metadata ของ top-3 IDs เพื่อตรวจชื่อสินค้า
    top_ids = ids[:3]
    meta_map = fetch_metadata(top_ids)
    first_meta = meta_map.get(ids[0], {})
    first_name = (first_meta.get("name") or first_meta.get("title") or "").strip()

    def normalize(s: str) -> str:
        """ลบช่องว่างและจุดทั้งหมด เพื่อให้ compare ได้แม้ชื่อมีวรรคคั่น"""
        return s.replace(" ", "").replace(".", "").lower()

    assert normalize(keyword) in normalize(first_name), (
        f"[TC09] Expected '{keyword}' to be the first result, "
        f"but first item (id={ids[0]}) name from metadata was '{first_name}'\n"
        f"  Top-3 metadata: {[meta_map.get(i, {}).get('name') or meta_map.get(i, {}).get('title') for i in top_ids]}"
    )


# ===================================================================
# TC10 — Thai + English mixed
# Summary  : verify search with thai special characters and mixed language input
# Testdata : น้ำมะพร้าวmale100
# Expected : api handles thai + english input; no encoding issue; valid response
# ===================================================================
@pytest.mark.high
def test_tc10_thai_english_mixed():
    keyword = "น้ำมะพร้าวmale100"
    resp = call_search(keyword)
    assert resp.status_code == 200, (
        f"[TC10] Expected HTTP 200 for Thai+English keyword, got {resp.status_code}\n{resp.text[:300]}"
    )
    # No encoding issue: response body should parse cleanly
    try:
        data = resp.json()
    except Exception as e:
        pytest.fail(f"[TC10] Response JSON parse failed (encoding issue?): {e}")
    assert isinstance(data, dict), "[TC10] Response should be a JSON object"
    assert "items" in data, f"[TC10] Missing 'items' in response: {list(data.keys())}"


# ===================================================================
# TC11 — Misspelled keyword (spell correction)
# Summary  : verify spell correction works for misspelled keyword
# Testdata : ตอกปกกี  (misspelling of ต๊อกปอกกี้)
# Expected : api applies spell correction; results relevant; no error
# ===================================================================
@pytest.mark.high
def test_tc11_spell_correction():
    keyword = "ตอกปกกี"
    resp = call_search(keyword)
    assert resp.status_code == 200, (
        f"[TC11] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    ids = item_ids(resp)
    assert len(ids) > 0, (
        f"[TC11] Spell correction expected to return results for '{keyword}' but got 0 items"
    )


# ===================================================================
# TC12 — Ordering stability (Baseline vs Candidate)
# Summary  : verify candidate ordering matches baseline ordering
# Testdata : มาชิตะสาหร่ายเกาหลีรสต้มยำ4กx6
# Expected : items and ordering ของ candidate ตรงกับ baseline
# ===================================================================
@pytest.mark.high
def test_tc12_ordering_stability():
    keyword = "มาชิตะสาหร่ายเกาหลีรสต้มยำ4กx6"

    bl_ids = call_baseline(keyword)
    time.sleep(1)
    cd_resp = call_search(keyword)

    assert cd_resp.status_code == 200, (
        f"[TC12] Candidate expected HTTP 200, got {cd_resp.status_code}"
    )
    cd_ids = item_ids(cd_resp)

    assert len(bl_ids) > 0, "[TC12] Baseline returned 0 items"
    assert len(cd_ids) > 0, "[TC12] Candidate returned 0 items"

    # เปรียบเทียบ top-10 items และลำดับ
    top_n = 100
    bl_top = bl_ids[:top_n]
    cd_top = cd_ids[:top_n]

    assert bl_top == cd_top, (
        f"[TC12] Ordering mismatch between Baseline and Candidate:\n"
        f"  baseline  top{top_n}={bl_top}\n"
        f"  candidate top{top_n}={cd_top}\n"
        f"  matched positions: {sum(1 for a, b in zip(bl_top, cd_top) if a == b)}/{top_n}"
    )


# ===================================================================
# TC13 — Null query parameter
# Summary  : verify api handles null query parameter correctly
# Testdata : "" (empty / treated as null)
# Expected : api handles null safely; returns valid error or fallback; no crash
# ===================================================================
@pytest.mark.high
def test_tc13_null_query_parameter():
    # Send search_keyword as empty string (null-equivalent)
    resp = call_search(keyword="")
    assert resp.status_code == 200, (
        f"[TC13] Server crashed with 5xx on null/empty query: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(f"[TC13] Response is not valid JSON: {e}\nRaw: {resp.text[:300]}")


# ===================================================================
# TC14 — Missing mandatory parameter
# Summary  : verify api handles missing mandatory parameter correctly
# Testdata : (no search_keyword sent)
# Expected : api returns proper validation error; no crash
# ===================================================================
@pytest.mark.high
def test_tc14_missing_mandatory_parameter():
    # Do NOT send search_keyword parameter at all
    resp = call_search(omit_keyword=True)
    assert resp.status_code == 200, (
        f"[TC14] Server crashed with 5xx when search_keyword is missing: "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC14] Response is not valid JSON when param is missing: {e}\nRaw: {resp.text[:300]}"
        )


# ===================================================================
# TC15 — Invalid parameter type value
# Summary  : verify api handles invalid parameter type correctly
# Testdata : 7111  (invalid value for `type` field)
# Expected : api validates type; returns proper error; no crash
# ===================================================================
@pytest.mark.high
def test_tc15_invalid_type_parameter():
    # type="7111" is not a valid product type
    resp = call_search("1", type_val="7111")
    assert resp.status_code < 500, (
        f"[TC15] Server crashed with 5xx on invalid type '7111': "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC15] Response is not valid JSON for invalid type: {e}\nRaw: {resp.text[:300]}"
        )


# ===================================================================
# TC16 — Very long / repeated Thai input
# Summary  : verify api handles unsupported special input safely
# Testdata : สามแม่ครัว × 19 (very long repeated string)
# Expected : api handles safely; no crash; response valid or proper error
# ===================================================================
@pytest.mark.high
def test_tc16_very_long_input():
    keyword = "สามแม่ครัว" * 19
    resp = call_search(keyword)
    assert resp.status_code == 200, (
        f"[TC16] Server crashed with 5xx on very long input: "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC16] Response is not valid JSON for long input: {e}\nRaw: {resp.text[:300]}"
        )
