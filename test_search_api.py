"""
pytest Test Suite — CANDIDATE Search API
==========================================
Source test cases : xray_search_testcases_import02.csv
API endpoint      : ai-universal-service-new / text_search (GET)

Run:
    pytest test_search_api.py -v
    pytest test_search_api.py -v --tb=short
    pytest test_search_api.py -v -k "tc01 or tc02"
    pytest test_search_api.py -v -k "ecommerce"
"""

import time
from typing import Optional, Union
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
DEFAULT_TYPE = [
    "711-ecommerce",
    "top_results",
    "sfvseries",
    "watch",
    "privilege",
    "sfv",
    "channel",
    "read",
    "ecommerce",
    "game",
    "movie",
    "series",
    "livetv",
]
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
    type_val: Union[str, list] = DEFAULT_TYPE,
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


def call_baseline(keyword: str, type_val: Union[str, list] = DEFAULT_TYPE, timeout: int = 30) -> list:
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
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc01_thai_numeric_input(type_val):
    resp = call_search("๒", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC01][{type_val}] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert isinstance(data, dict), f"[TC01][{type_val}] Response body should be a JSON object"
    assert "items" in data, f"[TC01][{type_val}] Missing 'items' key in response: {list(data.keys())}"
    assert isinstance(data["items"], list), f"[TC01][{type_val}] 'items' should be a list"


# ===================================================================
# TC02 — Special characters
# Summary  : verify search with special characters is handled correctly
# Testdata : ! @ # % ^ & * ( ) _ + - / ? , .
# Expected : api does not crash; response valid; special characters handled safely
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc02_special_characters(type_val):
    keyword = "! @ # % ^ & * ( ) _ + - / ? , ."
    resp = call_search(keyword, type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC02][{type_val}] Server crashed with 5xx: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        data = resp.json()
    except Exception as e:
        pytest.fail(f"[TC02][{type_val}] Response is not valid JSON: {e}\nRaw: {resp.text[:300]}")
    assert isinstance(data, dict), f"[TC02][{type_val}] Response should be a JSON object"


# ===================================================================
# TC03 — Numeric input
# Summary  : verify search function with numeric input
# Testdata : 1
# Expected : api returns valid response; numeric input handled correctly
# ===================================================================
@pytest.mark.medium
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc03_numeric_input(type_val):
    resp = call_search("1", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC03][{type_val}] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert "items" in data, f"[TC03][{type_val}] Missing 'items' in response: {list(data.keys())}"
    assert isinstance(data["items"], list), f"[TC03][{type_val}] 'items' should be a list"


# ===================================================================
# TC04 — Lowercase input
# Summary  : verify search function with lowercase input
# Testdata : fisherman
# Expected : api returns valid response; results relevant to keyword; no error
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc04_lowercase_input(type_val):
    resp = call_search("fisherman", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC04][{type_val}] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    ids = item_ids(resp)
    assert len(ids) > 0, (
        f"[TC04][{type_val}] Expected results for keyword 'fisherman' but got 0 items"
    )


# ===================================================================
# TC05 — Mixed case input
# Summary  : verify search function with mixed lowercase and uppercase input
# Testdata : Fisherman
# Expected : api returns valid response; case handling is correct
# ===================================================================
@pytest.mark.medium
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc05_mixed_case_input(type_val):
    resp_mixed = call_search("Fisherman", type_val=type_val)
    assert resp_mixed.status_code == 200, (
        f"[TC05][{type_val}] Expected HTTP 200 for 'Fisherman', got {resp_mixed.status_code}"
    )
    ids_mixed = item_ids(resp_mixed)
    assert len(ids_mixed) > 0, (
        f"[TC05][{type_val}] 'Fisherman' returned 0 items — case handling issue"
    )


# ===================================================================
# TC06 — Uppercase input
# Summary  : verify search function with uppercase input
# Testdata : FISHERMAN
# Expected : api returns valid response; uppercase input handled correctly
# ===================================================================
@pytest.mark.medium
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc06_uppercase_input(type_val):
    resp = call_search("FISHERMAN", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC06][{type_val}] Expected HTTP 200 for 'FISHERMAN', got {resp.status_code}\n{resp.text[:300]}"
    )
    data = resp.json()
    assert "items" in data, f"[TC06][{type_val}] Missing 'items' in response: {list(data.keys())}"


# ===================================================================
# TC07 — Whitespace input
# Summary  : verify search function with whitespace input
# Testdata : " " (single space)
# Expected : api handles whitespace correctly; no crash; behavior matches requirement
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc07_whitespace_input(type_val):
    resp = call_search(" ", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC07][{type_val}] Server crashed with 5xx on whitespace input: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(f"[TC07][{type_val}] Response is not valid JSON for whitespace input: {e}")


# ===================================================================
# TC08 — Empty keyword (consistency check)
# Summary  : verify empty keyword behavior is consistent with requirement
# Testdata : "" (empty)
# Expected : api returns valid response; behavior matches requirement; consistent results
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc08_empty_keyword_consistent(type_val):
    resp1 = call_search("", type_val=type_val)
    time.sleep(1)
    resp2 = call_search("", type_val=type_val)

    assert resp1.status_code == 200, (
        f"[TC08][{type_val}] First call crashed with 5xx: {resp1.status_code}"
    )
    assert resp2.status_code == 200, (
        f"[TC08][{type_val}] Second call crashed with 5xx: {resp2.status_code}"
    )
    assert resp1.status_code == resp2.status_code, (
        f"[TC08][{type_val}] Inconsistent status code: call1={resp1.status_code}, call2={resp2.status_code}"
    )
    if resp1.status_code == 200 and resp2.status_code == 200:
        ids1, ids2 = item_ids(resp1), item_ids(resp2)
        assert ids1 == ids2, (
            f"[TC08][{type_val}] Inconsistent results for empty keyword:\n"
            f"  call1 top5={ids1[:5]}\n  call2 top5={ids2[:5]}"
        )

# ===================================================================
# TC09 — Thai + English mixed
# Summary  : verify search with thai special characters and mixed language input
# Testdata : น้ำมะพร้าวmale100
# Expected : api handles thai + english input; no encoding issue; valid response
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc09_thai_english_mixed(type_val):
    keyword = "น้ำมะพร้าวmale100"
    resp = call_search(keyword, type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC09][{type_val}] Expected HTTP 200 for Thai+English keyword, got {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        data = resp.json()
    except Exception as e:
        pytest.fail(f"[TC09][{type_val}] Response JSON parse failed (encoding issue?): {e}")
    assert isinstance(data, dict), f"[TC09][{type_val}] Response should be a JSON object"
    assert "items" in data, f"[TC09][{type_val}] Missing 'items' in response: {list(data.keys())}"


# ===================================================================
# TC10 — Misspelled keyword (spell correction)
# Summary  : verify spell correction works for misspelled keyword
# Testdata : ตอกปกกี  (misspelling of ต๊อกปอกกี้)
# Expected : api applies spell correction; results relevant; no error
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc10_spell_correction(type_val):
    keyword = "ตอกปกกี"
    resp = call_search(keyword, type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC10][{type_val}] Expected HTTP 200, got {resp.status_code}\n{resp.text[:300]}"
    )
    ids = item_ids(resp)
    assert len(ids) > 0, (
        f"[TC10][{type_val}] Spell correction expected to return results for '{keyword}' but got 0 items"
    )


# ===================================================================
# TC11 — Ordering stability (Baseline vs Candidate)
# Summary  : verify candidate ordering matches baseline ordering
# Testdata : มาชิตะสาหร่ายเกาหลีรสต้มยำ4กx6
# Expected : items and ordering ของ candidate ตรงกับ baseline
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc11_ordering_stability(type_val):
    keyword = "มาชิตะสาหร่ายเกาหลีรสต้มยำ4กx6"

    bl_ids = call_baseline(keyword, type_val=type_val)
    time.sleep(1)
    cd_resp = call_search(keyword, type_val=type_val)

    assert cd_resp.status_code == 200, (
        f"[TC11][{type_val}] Candidate expected HTTP 200, got {cd_resp.status_code}"
    )
    cd_ids = item_ids(cd_resp)

    assert len(bl_ids) > 0, f"[TC11][{type_val}] Baseline returned 0 items"
    assert len(cd_ids) > 0, f"[TC11][{type_val}] Candidate returned 0 items"

    top_n = 100
    bl_top = bl_ids[:top_n]
    cd_top = cd_ids[:top_n]

    assert bl_top == cd_top, (
        f"[TC12][{type_val}] Ordering mismatch between Baseline and Candidate:\n"
        f"  baseline  top{top_n}={bl_top}\n"
        f"  candidate top{top_n}={cd_top}\n"
        f"  matched positions: {sum(1 for a, b in zip(bl_top, cd_top) if a == b)}/{top_n}"
    )


# ===================================================================
# TC12 — Null query parameter
# Summary  : verify api handles null query parameter correctly
# Testdata : "" (empty / treated as null)
# Expected : api handles null safely; returns valid error or fallback; no crash
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc12_null_query_parameter(type_val):
    resp = call_search(keyword="", type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC12][{type_val}] Server crashed with 5xx on null/empty query: {resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(f"[TC12][{type_val}] Response is not valid JSON: {e}\nRaw: {resp.text[:300]}")


# ===================================================================
# TC13 — Missing mandatory parameter
# Summary  : verify api handles missing mandatory parameter correctly
# Testdata : (no search_keyword sent)
# Expected : api returns proper validation error; no crash
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc13_missing_mandatory_parameter(type_val):
    resp = call_search(omit_keyword=True, type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC13][{type_val}] Server crashed with 5xx when search_keyword is missing: "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC13][{type_val}] Response is not valid JSON when param is missing: {e}\nRaw: {resp.text[:300]}"
        )


# ===================================================================
# TC14 — Invalid parameter type value
# Summary  : verify api handles invalid parameter type correctly
# Testdata : 7111  (invalid value for `type` field)
# Expected : api validates type; returns proper error; no crash
# Note     : ไม่ parametrize เพราะเทส behavior ของ invalid type โดยเฉพาะ
# ===================================================================
@pytest.mark.high
def test_tc14_invalid_type_parameter():
    resp = call_search("1", type_val="7111")
    assert resp.status_code < 500, (
        f"[TC14] Server crashed with 5xx on invalid type '7111': "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC14] Response is not valid JSON for invalid type: {e}\nRaw: {resp.text[:300]}"
        )


# ===================================================================
# TC15 — Very long / repeated Thai input
# Summary  : verify api handles unsupported special input safely
# Testdata : สามแม่ครัว × 19 (very long repeated string)
# Expected : api handles safely; no crash; response valid or proper error
# ===================================================================
@pytest.mark.high
@pytest.mark.parametrize("type_val", DEFAULT_TYPE)
def test_tc15_very_long_input(type_val):
    keyword = "สามแม่ครัว" * 19
    resp = call_search(keyword, type_val=type_val)
    assert resp.status_code == 200, (
        f"[TC15][{type_val}] Server crashed with 5xx on very long input: "
        f"{resp.status_code}\n{resp.text[:300]}"
    )
    try:
        resp.json()
    except Exception as e:
        pytest.fail(
            f"[TC15][{type_val}] Response is not valid JSON for long input: {e}\nRaw: {resp.text[:300]}"
        )
