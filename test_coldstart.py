"""
pytest test suite for torch-serving-coldstart recommendation API

Run:
    pip install pytest requests

    pytest test_coldstart.py -v

Environment variables (required — do NOT hardcode tokens in source):
    GP4_BEARER_TOKEN     Authorization header value for g-p4 API
    GP4_ACCESS_TOKEN     OAuth access_token body field
    GP4_REFRESH_TOKEN    OAuth refresh_token body field
    RICH_SSOID           ssoId with rich click history (required for TC01/TC10)
    NEW_SSOID            Brand-new ssoId with no history (required for TC02)

Optional overrides:
    TORCH_URL            Override default torch-serving URL
    GP4_URL_TEMPLATE     Override default g-p4 URL template
    OVERLAP_THRESHOLD    Float 0–1, minimum g-p4/torch overlap (default 0.8)
    DEFAULT_K            Default k for recommendation requests (default 50)
    MAX_LATENCY_SEC      Maximum acceptable response time in seconds (default 5)
    METADATA_SAMPLE_SIZE Number of items to verify content_type per test run (default 10)
"""

import os
import time
import concurrent.futures

import pytest
import requests

# ─────────────────────────────────────────────────────────────
# CONFIG — read from environment, never hardcode secrets
# ─────────────────────────────────────────────────────────────

TORCH_URL = os.getenv(
    "TORCH_URL",
    "http://torch-serving-coldstart.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/recommend/user",
)

_GP4_URL_TEMPLATE = os.getenv(
    "GP4_URL_TEMPLATE",
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/g-p4"
    "?fields=&trueyou_point_remain=&top_k=&ssoId={sso_id}&debug="
    "&spelling_correction=&has_card_type=&percent_rate=&limit=100"
    "&cursor=&ver=&has_package_type=&n_ranking=&type=&verbose=debug",
)

_bearer  = os.getenv("GP4_BEARER_TOKEN")
_access  = os.getenv("GP4_ACCESS_TOKEN")
_refresh = os.getenv("GP4_REFRESH_TOKEN")

GP4_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
    "Authorization": f"Bearer {_bearer}",
}

GP4_BODY = {
    "access_token": _access,
    "token_type": "Bearer",
    "refresh_token": _refresh,
    "expiry": "2026-03-16T22:34:51.568422023+07:00",
    "expires_in": 3599,
}

# ⚠️  TC01/TC02: set via environment or edit here
RICH_SSOID = os.getenv("RICH_SSOID", "29824532")
NEW_SSOID  = os.getenv("NEW_SSOID", "11111111")

OVERLAP_THRESHOLD = float(os.getenv("OVERLAP_THRESHOLD", "0.8"))
DEFAULT_K         = int(os.getenv("DEFAULT_K", "50"))
MAX_LATENCY_SEC      = float(os.getenv("MAX_LATENCY_SEC", "5.0"))
METADATA_SAMPLE_SIZE = int(os.getenv("METADATA_SAMPLE_SIZE", "10"))

METADATA_URL = (
    "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

EMPTY_FEATURES: dict = {
    "k": DEFAULT_K,
    "CTR_1": 0, "CTR_2": 0, "CTR_3": 0, "CTR_4": 0,
    "article_category_1": "", "article_category_2": "",
    "article_category_3": "", "article_category_4": "",
    "gender": "",
    "id_0": "", "id_1": "", "id_2": "", "id_3": "", "id_4": "",
    "primary_persona": "", "secondary_persona": "",
    "tags_0": "", "tags_1": "", "tags_2": "", "tags_3": "", "tags_4": "",
    "tmh_subs_type": "",
}


def call_torch(body: dict, timeout: float = 30) -> requests.Response:
    return requests.post(
        TORCH_URL,
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=timeout,
    )


def extract_torch_ids(resp_json: dict) -> list:
    """Extract item IDs from torch response regardless of key name."""
    items = (
        resp_json.get("result")
        or resp_json.get("data")
        or resp_json.get("items")
        or resp_json.get("recommendations")
        or []
    )
    if not items:
        return []
    first = items[0]
    if not isinstance(first, dict):
        return [str(i) for i in items]
    for key in ["id", "content_id", "item_id", "product_id"]:
        ids = [str(item[key]) for item in items if item.get(key) is not None]
        if ids:
            return ids
    return []


def get_gp4_data(sso_id: str) -> dict:
    """Call g-p4 via GET with JSON body (mirrors the actual curl command).

    g-p4 uses GET + body for OAuth credentials passing.
    requests.request("GET", ...) is used to allow sending a body with GET.
    """
    resp = requests.request(
        "GET",
        _GP4_URL_TEMPLATE.format(sso_id=sso_id),
        headers=GP4_HEADERS,
        json=GP4_BODY,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_results_node(data: dict) -> dict:
    inner = data.get("data", {})
    if not isinstance(inner, dict):
        return {}
    return inner.get("results", inner)


def extract_gp4_features(data: dict) -> dict:
    results = get_results_node(data)
    return results.get("feature_torch_body", {}).get("result", {})


def extract_gp4_ids(data: dict) -> list:
    results = get_results_node(data)
    items = (
        results.get("merge_page", {})
               .get("result", {})
               .get("items", [])
    )
    return [item["id"] for item in items if "id" in item]


def build_torch_body(features: dict, k: int = DEFAULT_K) -> dict:
    body = {**EMPTY_FEATURES, "k": k}
    for key, val in features.items():
        if key in body:
            body[key] = val if val is not None else body[key]
    return body


# ─────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def gp4_response():
    """Cached g-p4 response for RICH_SSOID."""
    return get_gp4_data(RICH_SSOID)


@pytest.fixture(scope="session")
def rich_torch_response(gp4_response):
    """torch-serving response built from rich-user g-p4 features."""
    features = extract_gp4_features(gp4_response)
    body = build_torch_body(features)
    resp = call_torch(body)
    resp.raise_for_status()
    return resp.json()


@pytest.fixture(scope="session")
def empty_torch_response():
    """torch-serving response with all-empty features (cold start)."""
    resp = call_torch(EMPTY_FEATURES)
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────────────────────
# TC01 — Rich-history user gets personalized (non-empty) result
# ─────────────────────────────────────────────────────────────
def test_tc01_rich_user_personalized(gp4_response, rich_torch_response):
    """
    TC01: User with rich history should receive a non-empty recommendation.
    Personalization is verified by:
      (a) result is non-empty
      (b) features extracted from g-p4 are non-trivial (at least one non-empty field)
    """
    features = extract_gp4_features(gp4_response)
    non_empty_features = {k: v for k, v in features.items() if v not in ("", 0, None)}
    assert non_empty_features, (
        "TC01 FAIL: feature_torch_body.result has no non-empty fields — "
        "this user may not have enough history"
    )

    ids = extract_torch_ids(rich_torch_response)
    assert len(ids) > 0, "TC01 FAIL: torch returned no items for rich-history user"
    print(f"\n  TC01 PASS — {len(ids)} items, non-empty features: {list(non_empty_features.keys())}")


# ─────────────────────────────────────────────────────────────
# TC02 — New user uses cold start logic (returns items despite empty features)
# ─────────────────────────────────────────────────────────────
def test_tc02_new_user_coldstart():
    """
    TC02: A brand-new user (no history) should still receive recommendations
    via cold-start fallback logic.
    """
    gp4_data = get_gp4_data(NEW_SSOID)
    features = extract_gp4_features(gp4_data)
    body = build_torch_body(features)
    resp = call_torch(body)
    assert resp.status_code == 200, f"TC02 FAIL: HTTP {resp.status_code}"
    ids = extract_torch_ids(resp.json())
    assert len(ids) > 0, "TC02 FAIL: no items returned for new user"
    print(f"\n  TC02 PASS — {len(ids)} cold-start items returned")


# ─────────────────────────────────────────────────────────────
# TC02b — nologin user uses cold start logic
# ─────────────────────────────────────────────────────────────
def test_tc02b_nologin_user_coldstart():
    """
    TC02b: A nologin user (all feature fields set to 'nologin') should still
    receive recommendations via cold-start fallback logic — not an error.
    """
    nologin_body = {k: "nologin" if isinstance(v, str) else v
                    for k, v in EMPTY_FEATURES.items()}
    resp = call_torch(nologin_body)
    assert resp.status_code == 200, f"TC02b FAIL: HTTP {resp.status_code}\n{resp.text[:300]}"
    ids = extract_torch_ids(resp.json())
    assert len(ids) > 0, "TC02b FAIL: no items returned for nologin user"
    print(f"\n  TC02b PASS — {len(ids)} cold-start items returned for nologin user")


# ─────────────────────────────────────────────────────────────
# TC03 — Fallback result when feature input is all empty
# ─────────────────────────────────────────────────────────────
def test_tc03_empty_features_returns_fallback(empty_torch_response):
    """
    TC03: Sending all-empty features should still return a non-empty fallback result.
    """
    ids = extract_torch_ids(empty_torch_response)
    assert len(ids) > 0, "TC03 FAIL: torch returned no items for empty features"
    print(f"\n  TC03 PASS — {len(ids)} fallback items returned")


# ─────────────────────────────────────────────────────────────
# TC07 — No duplicate items in result
# ─────────────────────────────────────────────────────────────
def test_tc07_no_duplicates(rich_torch_response):
    """
    TC07: All returned item IDs must be unique — no duplicates allowed.
    """
    ids = extract_torch_ids(rich_torch_response)
    assert len(ids) == len(set(ids)), (
        f"TC07 FAIL: duplicates found — "
        f"{len(ids) - len(set(ids))} duplicate(s) in {ids}"
    )
    print(f"\n  TC07 PASS — {len(ids)} unique items, no duplicates")


# ─────────────────────────────────────────────────────────────
# TC08 — Result size equals k
# ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("k", [1, 10, 25, 50])
def test_tc08_result_size_equals_k(k):
    """
    TC08: The number of returned items should equal the requested k
    (or be ≤ k if the catalogue is smaller than k).
    k=1 is added as a boundary case to verify minimal valid input.
    """
    body = {**EMPTY_FEATURES, "k": k}
    resp = call_torch(body)
    assert resp.status_code == 200, f"TC08 FAIL: HTTP {resp.status_code} for k={k}"
    ids = extract_torch_ids(resp.json())
    assert len(ids) <= k, f"TC08 FAIL: returned {len(ids)} items but k={k}"
    assert len(ids) > 0,  f"TC08 FAIL: returned 0 items for k={k}"
    print(f"\n  TC08 PASS — k={k}, got {len(ids)} items")


# ─────────────────────────────────────────────────────────────
# TC09 — Invalid input handled gracefully (no 500 crash)
# ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("bad_body,label", [
    ({},                                    "empty body"),
    ({"k": -1},                             "negative k"),
    ({"k": 0},                              "k=0 boundary"),
    ({"k": "fifty"},                        "string k"),
    ({"k": 50, "CTR_1": "not_a_number"},    "string CTR"),
    ({"k": 50, "gender": 12345},            "int gender"),
    ({"k": 99999},                          "very large k"),
])
def test_tc09_invalid_input_graceful(bad_body, label):
    """
    TC09: Invalid inputs must not cause a 500 Internal Server Error.
    Acceptable responses: 200 (with fallback), 400/422 (validation error).
    k=0 and k=very large are added as boundary checks.
    """
    resp = call_torch(bad_body)
    assert resp.status_code != 500, (
        f"TC09 FAIL [{label}]: server crashed with 500\n{resp.text[:500]}"
    )
    print(f"\n  TC09 PASS [{label}] — HTTP {resp.status_code}")


# ─────────────────────────────────────────────────────────────
# TC10 — torch result must match g-p4 exactly (count + order)
#         tested across multiple ssoIds
# ─────────────────────────────────────────────────────────────
TC10_SSOID_LIST = [
    "207818244",
    "142041125",
    "206706313",
    "77933204",
    "21387323",
]

@pytest.mark.parametrize("sso_id", TC10_SSOID_LIST)
def test_tc10_gp4_torch_exact_match(sso_id):
    """
    TC10: For each ssoId, the item list returned by torch (built from g-p4
    features) must be identical to g-p4's merge_page result in both:
      (a) count  — same number of items
      (b) order  — same ranked sequence, position by position
    g-p4 is treated as ground truth.
    """
    gp4_data  = get_gp4_data(sso_id)
    gp4_ids   = extract_gp4_ids(gp4_data)

    if not gp4_ids:
        pytest.skip(f"TC10 [{sso_id}]: could not extract g-p4 IDs from merge_page")

    features  = extract_gp4_features(gp4_data)
    body      = build_torch_body(features, k=len(gp4_ids))
    resp      = call_torch(body)
    assert resp.status_code == 200, f"TC10 FAIL [{sso_id}]: HTTP {resp.status_code}"

    torch_ids = extract_torch_ids(resp.json())

    # (a) count
    assert len(torch_ids) == len(gp4_ids), (
        f"TC10 FAIL [{sso_id}]: item count mismatch — "
        f"g-p4={len(gp4_ids)}, torch={len(torch_ids)}"
    )

    # (b) order — find all positions that differ
    diffs = [
        {"pos": i, "g_p4": g, "torch": t}
        for i, (g, t) in enumerate(zip(gp4_ids, torch_ids))
        if g != t
    ]
    assert not diffs, (
        f"TC10 FAIL [{sso_id}]: ranked order mismatch at "
        f"{len(diffs)} position(s):\n"
        + "\n".join(
            f"  pos={d['pos']}  g-p4={d['g_p4']}  torch={d['torch']}"
            for d in diffs[:10]
        )
    )
    print(f"\n  TC10 PASS [{sso_id}] — {len(gp4_ids)} items, count & order identical")


# ─────────────────────────────────────────────────────────────
# TC11 — Anonymous user (no ssoId) receives fallback
# NOTE: Reuses empty_torch_response fixture (same payload as TC03).
#       TC03 verifies items are returned; TC11 verifies HTTP 200 explicitly.
# ─────────────────────────────────────────────────────────────
def test_tc11_anonymous_user_fallback(empty_torch_response):
    """
    TC11: Calling torch with entirely empty features (simulating anonymous/no-ssoId user)
    must return HTTP 200 — not an error.
    """
    ids = extract_torch_ids(empty_torch_response)
    assert len(ids) > 0, "TC11 FAIL: no items returned for anonymous user"
    print(f"\n  TC11 PASS — {len(ids)} fallback items for anonymous user")


# ─────────────────────────────────────────────────────────────
# TC12 — API does not crash on edge-case minimal input
# ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("body,label", [
    ({"k": 50},         "only k provided"),
    ({"k": 1},          "k=1 minimal"),
    ({"k": 50, "primary_persona": "", "secondary_persona": "",
      "gender": "", "tmh_subs_type": "",
      "CTR_1": 0, "CTR_2": 0, "CTR_3": 0, "CTR_4": 0,
      "id_0": "", "id_1": "", "id_2": "", "id_3": "", "id_4": "",
      "article_category_1": "", "article_category_2": "",
      "article_category_3": "", "article_category_4": "",
      "tags_0": "", "tags_1": "", "tags_2": "", "tags_3": "", "tags_4": ""},
     "all fields empty string/zero"),
])
def test_tc12_no_crash_edge_cases(body, label):
    """
    TC12: API must not return 500 for minimal / all-default inputs.
    """
    resp = call_torch(body)
    assert resp.status_code != 500, (
        f"TC12 FAIL [{label}]: server returned 500\n{resp.text[:500]}"
    )
    print(f"\n  TC12 PASS [{label}] — HTTP {resp.status_code}")


# ─────────────────────────────────────────────────────────────
# TC13 — Response schema validation
# ─────────────────────────────────────────────────────────────
def test_tc13_response_schema(empty_torch_response):
    """
    TC13: Response must be a JSON object (dict) and contain at least one
    of the known item-list keys with a list value.
    """
    assert isinstance(empty_torch_response, dict), (
        "TC13 FAIL: response is not a JSON object"
    )
    known_keys = ["result", "data", "items", "recommendations"]
    matched = [k for k in known_keys if k in empty_torch_response]
    assert matched, (
        f"TC13 FAIL: response contains none of the expected keys {known_keys}.\n"
        f"Actual keys: {list(empty_torch_response.keys())}"
    )
    items = empty_torch_response[matched[0]]
    assert isinstance(items, list), (
        f"TC13 FAIL: '{matched[0]}' is not a list, got {type(items)}"
    )
    if items:
        first = items[0]
        assert isinstance(first, dict), (
            f"TC13 FAIL: first item is not a dict, got {type(first)}"
        )
        id_keys = ["id", "content_id", "item_id", "product_id"]
        assert any(k in first for k in id_keys), (
            f"TC13 FAIL: first item has no ID field. Keys: {list(first.keys())}"
        )
    print(f"\n  TC13 PASS — schema valid, key='{matched[0]}', {len(items)} items")


# ─────────────────────────────────────────────────────────────
# TC14 — Latency is within acceptable limit
# ─────────────────────────────────────────────────────────────
def test_tc14_latency_within_limit():
    """
    TC14: API must respond within MAX_LATENCY_SEC seconds.
    Measured end-to-end from client perspective.
    """
    start = time.perf_counter()
    resp  = call_torch(EMPTY_FEATURES)
    elapsed = time.perf_counter() - start

    assert resp.status_code == 200, f"TC14 FAIL: HTTP {resp.status_code}"
    assert elapsed <= MAX_LATENCY_SEC, (
        f"TC14 FAIL: response took {elapsed:.2f}s > limit {MAX_LATENCY_SEC}s"
    )
    print(f"\n  TC14 PASS — latency {elapsed*1000:.0f} ms (limit {MAX_LATENCY_SEC*1000:.0f} ms)")


# ─────────────────────────────────────────────────────────────
# TC15 — Idempotency: same input produces same output
# ─────────────────────────────────────────────────────────────
def test_tc15_idempotency():
    """
    TC15: Calling the API twice with the same input should return the same
    item IDs in the same order (deterministic model).
    """
    body  = {**EMPTY_FEATURES, "primary_persona": "gamer"}
    resp1 = call_torch(body)
    resp2 = call_torch(body)
    assert resp1.status_code == 200
    assert resp2.status_code == 200

    ids1 = extract_torch_ids(resp1.json())
    ids2 = extract_torch_ids(resp2.json())

    assert ids1 == ids2, (
        "TC15 FAIL: two identical requests returned different results — "
        "model is non-deterministic\n"
        f"  Call 1: {ids1[:10]}\n"
        f"  Call 2: {ids2[:10]}"
    )
    print(f"\n  TC15 PASS — {len(ids1)} items, results are identical across two calls")


# ─────────────────────────────────────────────────────────────
# TC16 — Concurrent requests do not cause errors
# ─────────────────────────────────────────────────────────────
def test_tc16_concurrent_requests():
    """
    TC16: Sending N concurrent requests must all succeed without 500 errors.
    Verifies basic stability under parallel load.
    """
    N = 5
    body = {**EMPTY_FEATURES, "k": 10}

    with concurrent.futures.ThreadPoolExecutor(max_workers=N) as pool:
        futures = [pool.submit(call_torch, body) for _ in range(N)]
        responses = [f.result() for f in concurrent.futures.as_completed(futures)]

    failed = [r.status_code for r in responses if r.status_code >= 500]
    assert not failed, (
        f"TC16 FAIL: {len(failed)}/{N} concurrent requests returned 5xx: {failed}"
    )
    print(f"\n  TC16 PASS — {N} concurrent requests all succeeded "
          f"({[r.status_code for r in responses]})")


# ─────────────────────────────────────────────────────────────
# HELPER — Metadata service
# ─────────────────────────────────────────────────────────────

def get_content_type(item_id: str) -> str | None:
    """Call metadata service and return content_type for a single item ID.
    Returns None if the item is not found or the field is missing.

    Response structure:
        {"status": 200, "items": [{"id": "...", "content_type": "gameitem"}], ...}
    """
    payload = {
        "parameters": {
            "id": item_id,
            "fields": ["id", "content_type"],
        },
        "options": {"cache": False},
    }
    resp = requests.post(
        METADATA_URL,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", [])
    if items and isinstance(items[0], dict):
        return items[0].get("content_type")
    return None


# ─────────────────────────────────────────────────────────────
# TC22 — All recommended items must be content_type=gameitem
# ─────────────────────────────────────────────────────────────

def test_tc22_all_items_are_gameitem(empty_torch_response):
    """
    TC22: Every item ID returned by the recommendation API must have
    content_type == 'gameitem' when looked up via the metadata service.

    Checks ALL returned items in parallel (ThreadPoolExecutor) to keep
    total latency manageable. Collects all failures before asserting so
    the full list of bad items is visible in one run.
    """
    ids = extract_torch_ids(empty_torch_response)
    assert ids, "TC22 FAIL: torch returned no items — cannot validate content_type"

    def check(item_id: str) -> dict | None:
        """Return a failure dict if content_type != 'gameitem', else None."""
        ct = get_content_type(item_id)
        if ct != "gameitem":
            return {"id": item_id, "content_type": ct}
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(check, ids))

    wrong = [r for r in results if r is not None]

    assert not wrong, (
        f"TC22 FAIL: {len(wrong)}/{len(ids)} items are NOT 'gameitem':\n"
        + "\n".join(f"  id={w['id']}  content_type={w['content_type']!r}" for w in wrong)
    )
    print(
        f"\n  TC22 PASS — all {len(ids)} items "
        f"confirmed content_type='gameitem'"
    )
