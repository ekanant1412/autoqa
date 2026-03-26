"""
test_live_commerce.py
─────────────────────
pytest — validate live commerce items จาก 7 endpoints

Test cases:
  [Endpoint]       E1: HTTP 200 | E2: node found | E3: response time ≤ 5s
  [Item]           I1–I11 per ActivityId
  [Cross-node]     C1: consistent fields | C2: merge_page IDs ⊆ get_all_live_today
  [Pagination]     P1: no duplicate ActivityId across cursor 1-5

รัน:  pytest test_live_commerce.py -v
      pytest test_live_commerce.py -v -k "e1 or e2"
"""

import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

# ── Config ───────────────────────────────────────────────────────────────────
BASE = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn"
    ".int-ai-platform.gcp.dmp.true.th/api/v1/universal"
)
RESPONSE_TIME_LIMIT = 5.0
CURSOR_RANGE = range(1, 6)
NODES = ["get_all_live_today", "merge_page"]

ENDPOINTS = [
    {"name": "lc-b1",
     "nodes": ["get_all_live_today"],
     "url": f"{BASE}/lc-b1?verbose=debug"},
    {"name": "f-b1",
     "url": f"{BASE}/f-b1?shelfId=mD8jnXqkmwZa&ssoId=101316473&limit=100&cursor=1&verbose=debug"},
    {"name": "ec-p2",
     "url": f"{BASE}/ec-p2?deviceId=c6d6f295a3ee22fe&id=0QnZ0adYGJnQ&isOnlyId=true"
             "&language=th&limit=11&pseudoId=&returnItemMetadata=false"
             "&shelfId=ZpPDZEkBA0Mp&ssoId=76031835&userId=1&verbose=debug"},
    {"name": "ec-b1",
     "url": f"{BASE}/ec-b1?ssoId=22838335&deviceId=atlas&userId=1&pseudoId=1"
             "&limit=10&returnItemMetadata=false&id=&shelfId=r6JYZj5paXDW"
             "&titleId=&category_name=&verbose=debug"},
    {"name": "ec-p1",
     "url": f"{BASE}/ec-p1?ssoId=22838335&deviceId=atlas&userId=1&pseudoId=1"
             "&limit=10&returnItemMetadata=false&id=&titleId=&category_name=&verbose=debug"},
    {"name": "sfv-p5",
     "pagination": True,
     "url": f"{BASE}/sfv-p5?shelfId=zmEXe3EQnXDk&total_candidates=400"
             "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
             "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
             "&verbose=debug&cursor=1&limit_seen_items=20"},
    {"name": "sfv-p4",
     "pagination": True,
     "url": f"{BASE}/sfv-p4?shelfId=zmEXe3EQnXDk&total_candidates=400"
             "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
             "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
             "&verbose=debug&cursor=1&limit_seen_items=20"},
]

# parametrize values
EP_NAMES      = [ep["name"] for ep in ENDPOINTS]
EP_NODE_PAIRS = [(ep["name"], node)
                 for ep in ENDPOINTS
                 for node in ep.get("nodes", NODES)]
EP_BY_NAME    = {ep["name"]: ep for ep in ENDPOINTS}


# ── HTTP + extraction helpers ─────────────────────────────────────────────────
def fetch(url: str) -> tuple[int, float, Any]:
    try:
        t0  = time.monotonic()
        res = subprocess.run(
            ["curl", "-s", "-o", "-", "-w", "\n__STATUS__%{http_code}", url],
            capture_output=True, text=True, timeout=30,
        )
        elapsed = time.monotonic() - t0
        *body_parts, status_line = res.stdout.rsplit("\n__STATUS__", 1)
        body   = "\n__STATUS__".join(body_parts)
        status = int(status_line.strip()) if status_line.strip().isdigit() else 0
        return status, elapsed, json.loads(body)
    except Exception as e:
        return 0, 0.0, {"_error": str(e)}


def _find_node_in(obj: Any, node_name: str):
    if isinstance(obj, dict):
        if obj.get("name") == node_name and "result" in obj:
            return obj["result"]
        node = obj.get(node_name)
        if isinstance(node, dict):
            return node.get("result", node)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _find_node_in(v, node_name)
                if found is not None:
                    return found
    elif isinstance(obj, list):
        for elem in obj:
            if isinstance(elem, (dict, list)):
                found = _find_node_in(elem, node_name)
                if found is not None:
                    return found
    return None


def get_node_result(response_data: dict, node_name: str):
    data = response_data.get("data", response_data)
    return _find_node_in(data, node_name)


def collect_live_items(obj: Any) -> list[dict]:
    items = []
    if isinstance(obj, dict):
        if "ActivityId" in obj or "activityId" in obj:
            return [obj]
        if "items" in obj and isinstance(obj["items"], list):
            for elem in obj["items"]:
                items.extend(collect_live_items(elem))
    elif isinstance(obj, list):
        for elem in obj:
            items.extend(collect_live_items(elem))
    return items


def make_cursor_url(url: str, cursor: int) -> str:
    if re.search(r"[?&]cursor=\d+", url):
        return re.sub(r"(cursor=)\d+", rf"\g<1>{cursor}", url)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}cursor={cursor}"


# ── Business logic ────────────────────────────────────────────────────────────
def expected_is_portrait(site_tags: list) -> bool:
    if isinstance(site_tags, list):
        for tag in site_tags:
            if isinstance(tag, dict) and tag.get("Name") == "Preview":
                return tag.get("Value") != "Landscape"
    return True


def is_valid_url(val: Any) -> bool:
    return isinstance(val, str) and (
        val.startswith("http://") or val.startswith("https://")
    )


# ── Session-scoped fixtures ───────────────────────────────────────────────────
@pytest.fixture(scope="session")
def all_responses():
    """Fetch ทุก endpoint พร้อมกัน ครั้งเดียวต่อ test session"""
    results = {}
    with ThreadPoolExecutor(max_workers=len(ENDPOINTS)) as ex:
        futures = {ex.submit(fetch, ep["url"]): ep for ep in ENDPOINTS}
        for fut in as_completed(futures):
            ep = futures[fut]
            status, elapsed, data = fut.result()
            results[ep["name"]] = {
                "status":  status,
                "elapsed": elapsed,
                "data":    data if status == 200 else None,
            }
    return results


@pytest.fixture(scope="session")
def pagination_responses():
    """Fetch cursor 1-5 ของทุก endpoint ที่มี merge_page พร้อมกัน"""
    paged_eps = [ep for ep in ENDPOINTS if ep.get("pagination", False)]
    results   = {ep["name"]: {} for ep in paged_eps}

    def _fetch_one(ep, cursor):
        url    = make_cursor_url(ep["url"], cursor)
        status, _, data = fetch(url)
        if status != 200 or data is None:
            return ep["name"], cursor, []
        result = get_node_result(data, "merge_page")
        if result is None:
            return ep["name"], cursor, []
        items = collect_live_items(result)
        return ep["name"], cursor, [
            str(i.get("ActivityId") or i.get("activityId")) for i in items
        ]

    tasks = [(ep, c) for ep in paged_eps for c in CURSOR_RANGE]
    with ThreadPoolExecutor(max_workers=len(tasks)) as ex:
        futures = [ex.submit(_fetch_one, ep, c) for ep, c in tasks]
        for fut in as_completed(futures):
            name, cursor, ids = fut.result()
            results[name][cursor] = ids

    return results


# ── Helper: get live items, skip if unavailable ───────────────────────────────
def _require_items(all_responses, ep_name: str, node_name: str) -> list[dict]:
    """คืน live items หรือ pytest.skip ถ้า endpoint/node ไม่พร้อม"""
    r = all_responses[ep_name]
    if r["status"] != 200:
        pytest.skip(f"HTTP {r['status']}")
    result = get_node_result(r["data"], node_name)
    if result is None:
        pytest.skip(f"node '{node_name}' not found in response")
    items = collect_live_items(result)
    if not items:
        pytest.skip("0 live items (filtered by pipeline)")
    return items


# ════════════════════════════════════════════════════════════════════════════
# E — Endpoint tests
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("ep_name", EP_NAMES)
def test_e1_http_200(all_responses, ep_name):
    """E1: endpoint ต้องตอบกลับ HTTP 200"""
    r = all_responses[ep_name]
    assert r["status"] == 200, f"got HTTP {r['status']}"


@pytest.mark.parametrize("ep_name", EP_NAMES)
def test_e3_response_time(all_responses, ep_name):
    """E3: response time ต้องไม่เกิน RESPONSE_TIME_LIMIT วินาที"""
    r = all_responses[ep_name]
    if r["status"] != 200:
        pytest.skip(f"HTTP {r['status']}")
    assert r["elapsed"] <= RESPONSE_TIME_LIMIT, (
        f"response time {r['elapsed']:.2f}s > {RESPONSE_TIME_LIMIT}s"
    )


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_e2_node_found(all_responses, ep_name, node_name):
    """E2: node ที่กำหนดต้องอยู่ใน response"""
    r = all_responses[ep_name]
    if r["status"] != 200:
        pytest.skip(f"HTTP {r['status']}")
    result = get_node_result(r["data"], node_name)
    assert result is not None, f"node '{node_name}' not found in response"


# ════════════════════════════════════════════════════════════════════════════
# I — Item-level tests (parametrize by endpoint × node)
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i8_no_duplicate_activity_id(all_responses, ep_name, node_name):
    """I8: ไม่มี ActivityId ซ้ำภายใน node เดียวกัน"""
    items = _require_items(all_responses, ep_name, node_name)
    ids   = [str(i.get("ActivityId") or i.get("activityId")) for i in items]
    seen, dupes = set(), []
    for aid in ids:
        if aid in seen:
            dupes.append(aid)
        seen.add(aid)
    assert not dupes, f"duplicate ActivityId: {dupes}"


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i1_activity_id_not_null(all_responses, ep_name, node_name):
    """I1: ทุก item ต้องมี ActivityId"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = [i for i in items if not (i.get("ActivityId") or i.get("activityId"))]
    assert not failed, f"{len(failed)} item(s) ไม่มี ActivityId"


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i10_activity_id_positive_integer(all_responses, ep_name, node_name):
    """I10: ActivityId ต้องเป็น positive integer"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if not (isinstance(aid, (int, float)) and int(aid) > 0):
            failed.append(f"{aid!r} (type={type(aid).__name__})")
    assert not failed, f"ActivityId ไม่ใช่ positive integer: {failed}"


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i2_site_tags_is_list(all_responses, ep_name, node_name):
    """I2: SiteTags ต้องเป็น list"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if not isinstance(i.get("SiteTags"), list):
            failed.append(f"{aid}: SiteTags={i.get('SiteTags')!r}")
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i11_tags_is_list(all_responses, ep_name, node_name):
    """I11: tags ต้องเป็น list"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if not isinstance(i.get("tags"), list):
            failed.append(f"{aid}: tags={i.get('tags')!r}")
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i3_cover_image_not_null(all_responses, ep_name, node_name):
    """I3: CoverImage ต้องมีค่า"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = [str(i.get("ActivityId") or i.get("activityId"))
              for i in items if not i.get("CoverImage")]
    assert not failed, f"CoverImage เป็น null/empty: {failed}"


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i9_cover_image_valid_url(all_responses, ep_name, node_name):
    """I9: CoverImage ต้องเป็น URL ที่ valid (http:// หรือ https://)"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if not is_valid_url(i.get("CoverImage")):
            failed.append(f"{aid}: {i.get('CoverImage')!r}")
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i4_is_portrait_is_bool(all_responses, ep_name, node_name):
    """I4: is_portrait ต้องเป็น bool เท่านั้น"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        val = i.get("is_portrait")
        if not isinstance(val, bool):
            failed.append(f"{aid}: is_portrait={val!r} (type={type(val).__name__})")
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i5_tags_equals_site_tags(all_responses, ep_name, node_name):
    """I5: tags ต้องเท่ากับ SiteTags"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if i.get("tags") != i.get("SiteTags"):
            failed.append(
                f"{aid}:\n"
                f"  tags     = {json.dumps(i.get('tags'), ensure_ascii=False)}\n"
                f"  SiteTags = {json.dumps(i.get('SiteTags'), ensure_ascii=False)}"
            )
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i6_thumb_horizontal_equals_cover_image(all_responses, ep_name, node_name):
    """I6: thumb_horizontal ต้องเท่ากับ CoverImage"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid = i.get("ActivityId") or i.get("activityId")
        if i.get("thumb_horizontal") != i.get("CoverImage"):
            failed.append(
                f"{aid}:\n"
                f"  thumb_horizontal = {i.get('thumb_horizontal')}\n"
                f"  CoverImage       = {i.get('CoverImage')}"
            )
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name,node_name", EP_NODE_PAIRS)
def test_i7_is_portrait_rule(all_responses, ep_name, node_name):
    """I7: is_portrait ต้องเป็นไปตาม rule SiteTags[Preview]"""
    items  = _require_items(all_responses, ep_name, node_name)
    failed = []
    for i in items:
        aid      = i.get("ActivityId") or i.get("activityId")
        site_tags = i.get("SiteTags")
        exp      = expected_is_portrait(site_tags)
        actual   = i.get("is_portrait")
        if actual != exp:
            preview = next(
                (t.get("Value") for t in (site_tags or [])
                 if isinstance(t, dict) and t.get("Name") == "Preview"),
                "—",
            )
            failed.append(
                f"{aid}: expected={exp}, actual={actual}, "
                f"SiteTags[Preview].Value={preview}"
            )
    assert not failed, "\n".join(failed)


# ════════════════════════════════════════════════════════════════════════════
# C — Cross-node tests
# ════════════════════════════════════════════════════════════════════════════

def _get_cross_items(all_responses, ep_name):
    """คืน (items_live, items_merge) หรือ skip ถ้าไม่มีข้อมูล"""
    ep = EP_BY_NAME[ep_name]
    if "merge_page" not in ep.get("nodes", NODES):
        pytest.skip(f"{ep_name} ไม่มี merge_page")
    r = all_responses[ep_name]
    if r["status"] != 200:
        pytest.skip(f"HTTP {r['status']}")
    res_live  = get_node_result(r["data"], "get_all_live_today")
    res_merge = get_node_result(r["data"], "merge_page")
    if res_live is None or res_merge is None:
        pytest.skip("node not found")
    items_live  = collect_live_items(res_live)
    items_merge = collect_live_items(res_merge)
    return items_live, items_merge


CROSS_EP_NAMES = [
    ep["name"] for ep in ENDPOINTS
    if "merge_page" in ep.get("nodes", NODES)
]

PAGINATION_EP_NAMES = [
    ep["name"] for ep in ENDPOINTS
    if ep.get("pagination", False)
]


@pytest.mark.parametrize("ep_name", CROSS_EP_NAMES)
def test_c1_consistent_fields_across_nodes(all_responses, ep_name):
    """C1: item ActivityId เดียวกันใน 2 node ต้องมี tags/thumb_horizontal/is_portrait ตรงกัน"""
    items_live, items_merge = _get_cross_items(all_responses, ep_name)
    map_live  = {str(i.get("ActivityId") or i.get("activityId")): i for i in items_live}
    map_merge = {str(i.get("ActivityId") or i.get("activityId")): i for i in items_merge}
    shared = set(map_live) & set(map_merge)
    if not shared:
        pytest.skip("ไม่มี ActivityId ที่ซ้ำกันระหว่าง 2 node")
    failed = []
    for aid in sorted(shared):
        a, b = map_live[aid], map_merge[aid]
        for field in ("tags", "thumb_horizontal", "is_portrait"):
            if a.get(field) != b.get(field):
                failed.append(
                    f"{aid}.{field}: "
                    f"get_all_live_today={a.get(field)!r} vs merge_page={b.get(field)!r}"
                )
    assert not failed, "\n".join(failed)


@pytest.mark.parametrize("ep_name", CROSS_EP_NAMES)
def test_c2_merge_page_ids_subset_of_live_today(all_responses, ep_name):
    """C2: ทุก ActivityId ใน merge_page ต้องอยู่ใน get_all_live_today ด้วย"""
    items_live, items_merge = _get_cross_items(all_responses, ep_name)
    ids_live  = {str(i.get("ActivityId") or i.get("activityId")) for i in items_live}
    ids_merge = {str(i.get("ActivityId") or i.get("activityId")) for i in items_merge}
    if not ids_merge:
        pytest.skip("merge_page ไม่มี live item")
    extra = sorted(ids_merge - ids_live)
    assert not extra, (
        f"IDs อยู่ใน merge_page แต่ไม่อยู่ใน get_all_live_today: {extra}"
    )


# ════════════════════════════════════════════════════════════════════════════
# P — Pagination tests
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("ep_name", PAGINATION_EP_NAMES)
def test_p1_no_duplicate_activity_id_across_cursors(pagination_responses, ep_name):
    """P1: ActivityId ใน merge_page ต้องไม่ซ้ำกันข้าม cursor 1-5"""
    cursor_ids = pagination_responses.get(ep_name, {})
    all_ids    = [(aid, c) for c in sorted(cursor_ids) for aid in cursor_ids[c]]

    if not all_ids:
        pytest.skip("ทุก cursor ไม่มี live item")

    seen: dict[str, int] = {}
    dupes = []
    for aid, cursor in all_ids:
        if aid in seen:
            dupes.append(f"{aid} (cursor {seen[aid]} & cursor {cursor})")
        else:
            seen[aid] = cursor

    # แสดง summary per cursor ใน failure message
    summary = {c: len(cursor_ids.get(c, [])) for c in CURSOR_RANGE}
    assert not dupes, (
        f"duplicate ActivityId ข้าม cursor:\n  " +
        "\n  ".join(dupes) +
        f"\n\ncursor summary: {summary}"
    )
