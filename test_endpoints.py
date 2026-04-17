"""
pytest test suite — tags / thumb_horizontal / is_portrait
──────────────────────────────────────────────────────────
Integration : เรียก 7 endpoints จริง พร้อมกัน (parallel)

รัน:
  pytest test_endpoints.py -v
  pytest test_endpoints.py -v --tb=short
"""

import json, subprocess
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

BASE = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn"
    ".int-ai-platform.gcp.dmp.true.th/api/v1/universal"
)

NODES = ["get_all_live_today", "merge_page"]

ENDPOINTS = [
    {"name": "lc-b1",  "nodes": NODES,
     "url": f"{BASE}/lc-b1?verbose=debug"},
    {"name": "f-b1",   "nodes": NODES,
     "url": f"{BASE}/f-b1?shelfId=mD8jnXqkmwZa&ssoId=101316473&limit=100&cursor=1&verbose=debug"},
    {"name": "ec-p2",  "nodes": NODES,
     "url": f"{BASE}/ec-p2?deviceId=c6d6f295a3ee22fe&id=0QnZ0adYGJnQ&isOnlyId=true"
            "&language=th&limit=11&pseudoId=&returnItemMetadata=false"
            "&shelfId=ZpPDZEkBA0Mp&ssoId=76031835&userId=1&verbose=debug"},
    {"name": "ec-b1",  "nodes": NODES,
     "url": f"{BASE}/ec-b1?ssoId=22838335&deviceId=atlas&userId=1&pseudoId=1"
            "&limit=10&returnItemMetadata=false&id=&shelfId=r6JYZj5paXDW"
            "&titleId=&category_name=&verbose=debug"},
    {"name": "ec-p1",  "nodes": NODES,
     "url": f"{BASE}/ec-p1?ssoId=22838335&deviceId=atlas&userId=1&pseudoId=1"
            "&limit=10&returnItemMetadata=false&id=&titleId=&category_name=&verbose=debug"},
    {"name": "sfv-p5", "nodes": NODES,
     "url": f"{BASE}/sfv-p5?shelfId=zmEXe3EQnXDk&total_candidates=400"
            "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
            "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
            "&verbose=debug&cursor=1&limit_seen_items=20"},
    {"name": "sfv-p4", "nodes": NODES,
     "url": f"{BASE}/sfv-p4?shelfId=zmEXe3EQnXDk&total_candidates=400"
            "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
            "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
            "&verbose=debug&cursor=1&limit_seen_items=20"},
]

# ── Business logic ─────────────────────────────────────────────────────────────
def expected_is_portrait(site_tags: list) -> bool:
    if isinstance(site_tags, list):
        for tag in site_tags:
            if isinstance(tag, dict) and tag.get("Name") == "Preview":
                return tag.get("Value") != "Landscape"
    return True

def check_item(item: dict) -> list[dict]:
    errors = []
    site_tags   = item.get("SiteTags")
    cover_image = item.get("CoverImage")
    tags        = item.get("tags")
    thumb_h     = item.get("thumb_horizontal")
    is_portrait = item.get("is_portrait")

    if tags != site_tags:
        errors.append({"field": "tags == SiteTags",
                        "expected": site_tags, "actual": tags})
    if thumb_h != cover_image:
        errors.append({"field": "thumb_horizontal == CoverImage",
                        "expected": cover_image, "actual": thumb_h})
    exp = expected_is_portrait(site_tags)
    if is_portrait != exp:
        errors.append({"field": "is_portrait",
                        "expected": exp, "actual": is_portrait})
    return errors

# ── Fetch ──────────────────────────────────────────────────────────────────────
def fetch(url: str) -> tuple[int, Any]:
    r = subprocess.run(
        ["curl", "--silent", "--location", "--write-out", "\n%{http_code}", url],
        capture_output=True, text=True, timeout=30,
    )
    lines = r.stdout.rsplit("\n", 1)
    body, code = lines[0], lines[-1].strip()
    status = int(code) if code.isdigit() else 0
    try:    data = json.loads(body)
    except: data = None
    return status, data

# ── Node / item extraction ─────────────────────────────────────────────────────
def locate_node_result(data: dict, node_name: str):
    # actual response: {"status":..., "message":..., "data": {<nodes>}, "request_id":...}
    candidates = [
        data.get("data"),       # ← structure จริง: nodes อยู่ใน data["data"]
        data,
        data.get("nodes"),
        (data.get("debug") or {}),
        (data.get("debug") or {}).get("nodes"),
        data.get("pipeline"),
        data.get("graph"),
        data.get("dag"),
    ]
    for wrapper in candidates:
        if not isinstance(wrapper, dict):
            continue
        if node_name in wrapper:
            node = wrapper[node_name]
            if isinstance(node, dict):
                return node.get("result", node)
    return None

def is_live_item(obj: dict) -> bool:
    return "ActivityId" in obj or "activityId" in obj

def _flat_items(obj: Any) -> list[dict]:
    """
    get_all_live_today → items[] flat (มี ActivityId โดยตรง)
    merge_page         → items[] ปนกัน:
      {"id": "xyz"}                          → ข้าม
      {"items": [...live...], "payload": {}} → recurse
      {"ActivityId": ..., ...}               → เก็บ
    """
    if isinstance(obj, list):
        out = []
        for elem in obj:
            if not isinstance(elem, dict):
                continue
            if is_live_item(elem):
                out.append(elem)
            elif "items" in elem and isinstance(elem["items"], list):
                out.extend(_flat_items(elem["items"]))
        return out
    if isinstance(obj, dict):
        for key in ("items", "results", "contents", "recommendations", "candidates"):
            if key in obj:
                return _flat_items(obj[key])
    return []

def extract_items_from_node(result: dict) -> list[dict]:
    return _flat_items(result)

def response_structure(data: dict, depth: int = 0, max_depth: int = 3) -> str:
    if not isinstance(data, dict) or depth > max_depth:
        return ""
    lines = []
    indent = "  " * depth
    for k, v in data.items():
        t = type(v).__name__
        n = f"(len={len(v)})" if isinstance(v, (list, dict)) else f"= {repr(v)[:40]}"
        lines.append(f"{indent}{k}: {t} {n}")
        if isinstance(v, dict):
            lines.append(response_structure(v, depth + 1, max_depth))
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            lines.append(f"{indent}  [0]:")
            lines.append(response_structure(v[0], depth + 2, max_depth))
    return "\n".join(filter(None, lines))

# ── Fixture ────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def all_responses():
    results = {}
    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {pool.submit(fetch, ep["url"]): ep for ep in ENDPOINTS}
        for future in as_completed(futures):
            ep = futures[future]
            status, data = future.result()
            results[ep["name"]] = {"ep": ep, "status": status, "data": data}
    return results

# ── Parametrize ────────────────────────────────────────────────────────────────
def pytest_generate_tests(metafunc):
    if "endpoint_name" in metafunc.fixturenames and "node_name" in metafunc.fixturenames:
        params, ids = [], []
        for ep in ENDPOINTS:
            for node in ep["nodes"]:
                params.append((ep["name"], node))
                ids.append(f"{ep['name']}::{node}")
        metafunc.parametrize("endpoint_name,node_name", params, ids=ids)

# ── Debug helper (รันแยกเพื่อดู response structure จริง) ───────────────────────
class TestDebug:
    """รัน: pytest test_endpoints.py::TestDebug -v -s"""

    def test_dump_lc_b1_structure(self):
        """พิมพ์ top-level keys และ node keys จริงของ lc-b1 response"""
        ep   = next(e for e in ENDPOINTS if e["name"] == "lc-b1")
        status, data = fetch(ep["url"])
        print(f"\nHTTP: {status}")
        if not isinstance(data, dict):
            print("data is not a dict:", type(data)); return

        print(f"Top-level keys: {list(data.keys())}")

        for node_name in NODES:
            result = locate_node_result(data, node_name)
            print(f"\n--- Node: {node_name} ---")
            if result is None:
                print("  NOT FOUND in response")
            else:
                print(f"  result keys: {list(result.keys()) if isinstance(result, dict) else type(result)}")
                items_raw = result.get("items", []) if isinstance(result, dict) else []
                print(f"  result.items count: {len(items_raw)}")
                if items_raw:
                    first = items_raw[0]
                    print(f"  items[0] keys: {list(first.keys()) if isinstance(first, dict) else first}")
                items = extract_items_from_node(result)
                print(f"  extracted ActivityId items: {len(items)}")

        pytest.skip("debug only — ดู output ด้วย -s")


# ── Integration Tests ──────────────────────────────────────────────────────────
class TestIntegration:

    def test_http_200(self, all_responses, endpoint_name, node_name):
        """Endpoint returns HTTP 200."""
        r = all_responses[endpoint_name]
        assert r["status"] == 200, f"{endpoint_name} returned HTTP {r['status']}"

    def test_response_is_json(self, all_responses, endpoint_name, node_name):
        """Response body is valid JSON."""
        r = all_responses[endpoint_name]
        assert r["data"] is not None, f"{endpoint_name} response is not valid JSON"

    def test_items_exist(self, all_responses, endpoint_name, node_name):
        """Node exists in response (FAIL if missing); live items may be empty → SKIP."""
        r = all_responses[endpoint_name]
        data = r["data"]
        if data is None or r["status"] != 200:
            pytest.skip("Endpoint not reachable")

        result = locate_node_result(data, node_name)
        assert result is not None, (
            f"{endpoint_name}/{node_name}: node not found in response\n"
            f"Top-level keys: {list(data.keys()) if isinstance(data, dict) else type(data)}"
        )

        items = extract_items_from_node(result)
        if len(items) == 0:
            pytest.skip(
                f"{endpoint_name}/{node_name}: 0 live items (may be filtered by pipeline)"
            )

    def test_tags_equals_site_tags(self, all_responses, endpoint_name, node_name):
        """tags must deep-equal SiteTags for every item."""
        self._assert_no_field_errors(all_responses, endpoint_name, node_name, "tags == SiteTags")

    def test_thumb_horizontal_equals_cover_image(self, all_responses, endpoint_name, node_name):
        """thumb_horizontal must equal CoverImage for every item."""
        self._assert_no_field_errors(all_responses, endpoint_name, node_name, "thumb_horizontal == CoverImage")

    def test_is_portrait_logic(self, all_responses, endpoint_name, node_name):
        """is_portrait follows SiteTags[Preview]=Landscape → false, else → true."""
        self._assert_no_field_errors(all_responses, endpoint_name, node_name, "is_portrait")

    # ── helpers ───────────────────────────────────────────────────────────────
    def _get_items(self, data, endpoint_name, node_name):
        result = locate_node_result(data, node_name)
        if result is None:
            return []
        return extract_items_from_node(result)

    def _assert_no_field_errors(self, all_responses, endpoint_name, node_name, field: str):
        r = all_responses[endpoint_name]
        if r["data"] is None or r["status"] != 200:
            pytest.skip("Endpoint not reachable")
        items = self._get_items(r["data"], endpoint_name, node_name)
        if not items:
            pytest.skip("No items to validate")

        failures = []
        for item in items:
            iid  = item.get("ActivityId") or item.get("Id") or "?"
            name = item.get("Name") or ""
            for e in check_item(item):
                if e["field"].startswith(field.split(" ")[0]):
                    failures.append(
                        f"ActivityId={iid} {name}: {e['field']}\n"
                        f"  expected={str(e['expected'])[:80]}\n"
                        f"  actual  ={str(e['actual'])[:80]}"
                    )

        assert not failures, (
            f"{endpoint_name}/{node_name} — {len(failures)} item(s) FAILED [{field}]:\n"
            + "\n".join(failures)
        )
