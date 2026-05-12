"""
pytest script สำหรับตรวจสอบการเรียงลำดับ card_type ใน merge_page

Logic การตรวจสอบ:
- ดู dgi_truecard_type ของ user จาก node user_feature
- ดู card_type ของแต่ละ item จาก node order_object_card_type
- ตรวจสอบ merge_page ว่า:
  1. Items ที่มี card_type ตรงกับ user ควรขึ้นก่อน
  2. ตามด้วย items ที่เป็น no_card / open_deal / empty
  3. Items ที่มี card_type นอกเหนือจากของ user (และไม่ใช่ no_card/open_deal/empty) ไม่ควรปรากฏ
"""

import csv
import pathlib
import pytest
import requests
from typing import Optional
from urllib.parse import urlencode

REPORT_DIR = pathlib.Path(__file__).parent / "card_type_reports"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal"
)

# card_type เหล่านี้ถือว่าเป็น "universal" ใช้ได้กับทุก user
SECONDARY_CARD_TYPES = {"no_card", "open_deal"}

# ssoId ที่ต้องการทดสอบ (ครอบคลุม card_type หลายแบบ)
SSO_IDS = [
    "21387323",
    "29824532",
    "207818244",
    "142041125",
    "207535257",
    "27035080"
]

# Endpoint configs (ไม่มี ssoId — จะ inject ทีหลัง)
_ENDPOINT_CONFIGS = [
    {
        "endpoint": "p-s2",
        "result_node": "merge_page",
        "base_params": {
            "id": "6XDAbkv5R91n",
            "isOnlyId": "true",
            "language": "th",
            "limit": "100",
            "pseudoId": "null",
            "returnItemMetadata": "false",
            "userId": "null",
            "verbose": "debug",
            "deviceId": "d2e2742c64acf055"
        },
        "method": "GET",
    },
    {
        "endpoint": "p-p5",
        "result_node": "merge_page",
        "base_params": {
            "verbose": "debug",
            "limit": "100",
        },
        "method": "GET",
    },
    {
        "endpoint": "p-p4",
        "result_node": "merge_final_result",
        "user_card_type_node": "feature_segments",
        "base_params": {
            "returnItemMetadata": "false",
            "limit": "100",
            "isOnlyId": "true",
            "language": "th",
            "pseudoId": "null",
            "shelfId": "DGr6bVP2J5MG",
            "userId": "null",
            "deviceId": "895d8b0ed3a9eafc",
            "allow_app": "TrueIDApp",
            "verbose": "debug",
        },
        "method": "GET",
    },
    {
        "endpoint": "p-p2",
        "result_node": "merge_page",
        "base_params": {
            "deviceId": "c032d17c6a7e3864",
            "isOnlyId": "true",
            "language": "th",
            "limit": "100",
            "pseudoId": "null",
            "returnItemMetadata": "false",
            "seen_items": "",
            "userId": "null",
            "verbose": "debug",
        },
        "method": "GET",
    },
]

# point variants ที่ต้องการทดสอบต่อ endpoint
# None  = ไม่ส่ง trueyou_point_remain → ถือว่า 0, effective_max=10
# 10    = boundary (อยู่ใน range 0-10, effective_max=10)
# 152   = สูง, effective_max=152
POINT_VARIANTS = [0, 10, 152]

# Generate TEST_CASES: 4 endpoints × 5 ssoIds × 3 point variants = 60 cases
TEST_CASES = []
for _cfg in _ENDPOINT_CONFIGS:
    for _sso in SSO_IDS:
        for _point in POINT_VARIANTS:
            _params = {**_cfg["base_params"], "ssoId": _sso}
            if _point is not None:
                _params["trueyou_point_remain"] = str(_point)
            _point_label = "no_point" if _point is None else f"point={_point}"
            _full_url = f"{BASE_URL}/{_cfg['endpoint']}?{urlencode(_params)}"
            TEST_CASES.append({
                "name": f"{_cfg['endpoint']} | ssoId={_sso} | {_point_label}",
                "endpoint": _cfg["endpoint"],
                "result_node": _cfg["result_node"],
                "user_card_type_node": _cfg.get("user_card_type_node", "feature_segments"),
                "method": _cfg.get("method", "GET"),
                "params": _params,
                "url": _full_url,
            })


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def call_api(endpoint: str, params: dict, method: str = "GET") -> dict:
    """เรียก API แล้วคืน JSON response พร้อม log URL ที่ถูกเรียก"""
    url = f"{BASE_URL}/{endpoint}"
    full_url = f"{url}?{urlencode(params)}"
    print(f"\n{'='*70}")
    print(f"  METHOD : {method}")
    print(f"  URL    : {full_url}")
    print(f"{'='*70}")
    if method == "POST":
        resp = requests.post(url, params=params, data="", timeout=30)
    else:
        resp = requests.get(url, params=params, timeout=30)
    print(f"  STATUS : {resp.status_code}")
    resp.raise_for_status()
    data = resp.json()
    # แสดง top-level keys เพื่อช่วย debug structure ของ response
    if isinstance(data, dict):
        print(f"  RESPONSE TOP-LEVEL KEYS: {list(data.keys())}")
    return data


def _recursive_find_node(data, node_name: str, _depth: int = 0):
    """
    ค้นหา node แบบ recursive ใน dict/list ทุก level
    คืน node["result"] ถ้าเจอ dict ที่มี key "name" == node_name
    หรือคืน dict ที่ key == node_name แล้วมี "result" อยู่ข้างใน
    """
    if _depth > 5 or data is None:
        return None

    if isinstance(data, dict):
        # กรณี: {"merge_page": {"result": {...}, ...}}
        if node_name in data:
            candidate = data[node_name]
            if isinstance(candidate, dict) and "result" in candidate:
                return candidate["result"]
            return candidate

        # กรณี: {"name": "merge_page", "result": {...}}
        if data.get("name") == node_name and "result" in data:
            return data["result"]

        # ค้นใน values ทุกตัว
        for val in data.values():
            found = _recursive_find_node(val, node_name, _depth + 1)
            if found is not None:
                return found

    elif isinstance(data, list):
        for item in data:
            found = _recursive_find_node(item, node_name, _depth + 1)
            if found is not None:
                return found

    return None


def find_node(data: dict, node_name: str):
    """
    ค้นหา node result จาก verbose=debug response
    ใช้ recursive search เพื่อรองรับทุก structure ที่เป็นไปได้
    """
    result = _recursive_find_node(data, node_name)
    if result is None:
        print(f"  [WARN] ไม่พบ node '{node_name}' ใน response")
    return result


def extract_user_card_type(response: dict, user_card_type_node: str = "feature_segments") -> Optional[str]:
    """
    ดึง card_type ของ user โดยลองตามลำดับ:

    1. feature_segments node → segments.dgi_truecard_type   (default, ใช้ใน p-p4)
       หรือ node ที่ระบุใน user_card_type_node              (ใช้ใน p-s2, p-p5, p-p2)

    2. Fallback: feature_segments → segments.dgi_truecard_type
       (ถ้า node หลักไม่ใช่ feature_segments)

    3. Fallback: static_default_card_type → card_type

    คืนค่า string เช่น "black", "red", "gold" หรือ None
    """
    # --- ลองจาก node หลักที่ระบุ ---
    primary_node = find_node(response, user_card_type_node)
    if primary_node is not None and isinstance(primary_node, dict):
        ct = primary_node.get("segments", {}).get("dgi_truecard_type")
        if ct:
            print(f"  [INFO] user_card_type='{ct}' (จาก {user_card_type_node}.segments.dgi_truecard_type)")
            return ct
        # บางครั้ง node คืน card_type โดยตรง (เช่น static_default_card_type)
        ct = primary_node.get("card_type")
        if ct:
            if isinstance(ct, list):
                ct = ct[0] if ct else None
            if ct:
                print(f"  [INFO] user_card_type='{ct}' (จาก {user_card_type_node}.card_type)")
                return ct

    # --- Fallback: feature_segments ---
    if user_card_type_node != "feature_segments":
        fs_node = find_node(response, "feature_segments")
        if fs_node is not None and isinstance(fs_node, dict):
            ct = fs_node.get("segments", {}).get("dgi_truecard_type")
            if ct:
                print(f"  [INFO] user_card_type='{ct}' (fallback จาก feature_segments)")
                return ct

    # --- Fallback: static_default_card_type ---
    static_node = find_node(response, "static_default_card_type")
    if static_node is not None and isinstance(static_node, dict):
        ct = static_node.get("card_type")
        if ct:
            if isinstance(ct, list):
                ct = ct[0] if ct else None
            if ct:
                print(f"  [INFO] user_card_type='{ct}' (fallback จาก static_default_card_type.card_type)")
                return ct

    print("  [WARN] หา user_card_type ไม่เจอจากทุก node ที่ลอง")
    return None


def _get_order_object_items(response: dict) -> list[dict]:
    """ดึง items list จาก order_object_card_type node (ใช้ร่วมกันระหว่าง lookups)"""
    node = find_node(response, "order_object_card_type")
    if node is None:
        return []
    if isinstance(node, dict):
        return node.get("items", node.get("source_data", []))
    if isinstance(node, list):
        return node
    return []


def build_card_type_lookup(response: dict) -> dict[str, list[str]]:
    """
    สร้าง dict: {item_id → [card_type, ...]}
    จาก order_object_card_type node
    """
    lookup = {}
    for item in _get_order_object_items(response):
        if isinstance(item, dict):
            item_id = item.get("id")
            card_types = item.get("card_type", [])
            if item_id:
                lookup[item_id] = card_types or []
    return lookup


def build_package_type_lookup(response: dict) -> dict:
    """
    สร้าง dict: {item_id → package_type}
    จาก order_object_card_type node
    package_type อาจเป็น "postpaid", "prepaid", "all", "" หรือ None
    """
    lookup = {}
    for item in _get_order_object_items(response):
        if isinstance(item, dict):
            item_id = item.get("id")
            if item_id:
                lookup[item_id] = item.get("package_type")
    return lookup


def extract_user_subs_type(response: dict) -> Optional[str]:
    """
    ดึง dgi_subs_type ของ user จาก feature_segments node
    คืนค่า "post", "pre", "all" หรือ None
    """
    fs_node = find_node(response, "feature_segments")
    if fs_node is not None and isinstance(fs_node, dict):
        subs = fs_node.get("segments", {}).get("dgi_subs_type")
        if subs:
            print(f"  [INFO] user_subs_type='{subs}' (จาก feature_segments.segments.dgi_subs_type)")
            return subs
    print("  [WARN] หา dgi_subs_type ไม่เจอจาก feature_segments")
    return None


def is_package_type_allowed(package_type: Optional[str], subs_type: str) -> bool:
    """
    ตรวจว่า package_type ของ item อนุญาตสำหรับ subs_type นี้ไหม

    กฎ:
      subs_type "post" → อนุญาต: "postpaid", "all", "" หรือ None
      subs_type "pre"  → อนุญาต: "prepaid",  "all", "" หรือ None
      subs_type อื่น  → อนุญาตทั้งหมด (ไม่รู้ rule)
    """
    # empty / null = universal ใช้ได้ทุก subs_type
    if not package_type or package_type == "all":
        return True

    if subs_type == "post":
        return package_type == "postpaid"
    if subs_type == "pre":
        return package_type == "prepaid"

    # subs_type อื่น (เช่น "all") ไม่มี rule → ถือว่า pass
    return True


def build_redeem_point_lookup(response: dict) -> dict:
    """
    สร้าง dict: {item_id → redeem_point}
    จาก order_object_card_type node
    redeem_point อาจเป็น int หรือ None (ถ้าไม่มีข้อมูล)
    """
    lookup = {}
    for item in _get_order_object_items(response):
        if isinstance(item, dict):
            item_id = item.get("id")
            if item_id:
                rp = item.get("redeem_point")
                lookup[item_id] = int(rp) if rp is not None else None
    return lookup


def is_redeem_point_allowed(redeem_point: Optional[int], trueyou_point_remain: int) -> bool:
    """
    ตรวจว่า redeem_point ของ item อนุญาตสำหรับ point ที่ user มีไหม

    กฎ:
      trueyou_point_remain  0-10 → item redeem_point ต้องอยู่ในช่วง 0-10
      trueyou_point_remain >= 11 → item redeem_point ต้องไม่เกิน trueyou_point_remain
      redeem_point = None        → ไม่มีข้อมูล ถือว่าผ่าน
    """
    if redeem_point is None:
        return True
    effective_max = 10 if trueyou_point_remain <= 10 else trueyou_point_remain
    return redeem_point <= effective_max


def extract_result_ids(response: dict, result_node: str) -> list[str]:
    """
    ดึง item IDs จาก node ที่ระบุ (merge_page หรือ merge_final_result) ตามลำดับ

    โครงสร้าง result ที่รองรับ:
      merge_page         → {"items": [{"id": "..."}], "pages": {"cursor": 2}}
      merge_final_result → {"items": [{"id": "..."}], "items_size": 30}

    หมายเหตุ: card_type และ package_type ถูก strip โดย result_modifier_functions
    → ต้องใช้ order_object_card_type เป็น lookup แทน
    """
    node = find_node(response, result_node)
    if node is None:
        return []

    items = []
    if isinstance(node, dict):
        items = node.get("items", [])
    elif isinstance(node, list):
        items = node

    ids = []
    for item in items:
        if isinstance(item, str):
            ids.append(item)
        elif isinstance(item, dict):
            item_id = item.get("id")
            if item_id:
                ids.append(str(item_id))
    return ids


def categorize_item(card_types: list, user_card_type: Optional[str]) -> str:
    """
    จัดหมวดหมู่ item:
      "primary"   - มี card_type ตรงกับ user
      "secondary" - เป็น no_card / open_deal / empty (ใช้ได้ทุก user)
      "invalid"   - card_type ที่ไม่ควรปรากฏสำหรับ user นี้
    """
    # filter ค่า None / ""
    filtered = [ct for ct in card_types if ct]

    if user_card_type and user_card_type in filtered:
        return "primary"

    if not filtered or all(ct in SECONDARY_CARD_TYPES for ct in filtered):
        return "secondary"

    return "invalid"


# ---------------------------------------------------------------------------
# Pytest parametrize
# ---------------------------------------------------------------------------


@pytest.fixture(params=TEST_CASES, ids=lambda tc: tc["name"])
def api_response(request):
    """Fixture: เรียก API แต่ละ endpoint แล้วคืน response dict"""
    tc = request.param
    response = call_api(tc["endpoint"], tc["params"], tc.get("method", "GET"))
    return {
        "name": tc["name"],
        "result_node": tc.get("result_node", "merge_page"),
        "user_card_type_node": tc.get("user_card_type_node", "feature_segments"),
        "params": tc["params"],
        "response": response,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCardTypeOrdering:
    """ตรวจสอบการเรียงลำดับ card_type ใน result node (merge_page / merge_final_result)"""

    def test_merge_page_not_empty(self, api_response):
        """result node ต้องมี items อยู่"""
        result_node = api_response["result_node"]
        ids = extract_result_ids(api_response["response"], result_node)
        assert ids, (
            f"[{api_response['name']}] '{result_node}' ว่างเปล่า "
            f"หรือไม่พบ node '{result_node}' ใน response"
        )

    def test_no_invalid_card_type_items(self, api_response):
        """
        ไม่ควรมี item ที่มี card_type นอกเหนือจาก user's card_type
        (และไม่ใช่ no_card / open_deal / empty)
        """
        resp = api_response["response"]
        result_node = api_response["result_node"]
        uct_node = api_response["user_card_type_node"]
        user_card_type = extract_user_card_type(resp, uct_node)
        card_type_lookup = build_card_type_lookup(resp)
        merge_ids = extract_result_ids(resp, result_node)

        invalid_items = []
        for item_id in merge_ids:
            card_types = card_type_lookup.get(item_id, [])
            category = categorize_item(card_types, user_card_type)
            if category == "invalid":
                invalid_items.append(
                    f"  id={item_id!r}, card_type={card_types}"
                )

        assert not invalid_items, (
            f"[{api_response['name']}] user card_type='{user_card_type}'\n"
            f"พบ {len(invalid_items)} items ที่ไม่ควรอยู่ใน {result_node}:\n"
            + "\n".join(invalid_items)
        )

    def test_primary_items_before_secondary(self, api_response):
        """
        Items ที่มี card_type ตรงกับ user ต้องขึ้นก่อน
        no_card / open_deal / empty items ทั้งหมด
        """
        resp = api_response["response"]
        result_node = api_response["result_node"]
        uct_node = api_response["user_card_type_node"]
        user_card_type = extract_user_card_type(resp, uct_node)
        card_type_lookup = build_card_type_lookup(resp)
        merge_ids = extract_result_ids(resp, result_node)

        # ถ้าไม่มี user_card_type → ข้าม test นี้พร้อมบอกสาเหตุชัดเจน
        if not user_card_type:
            pytest.skip(
                f"[{api_response['name']}] หา user_card_type ไม่เจอจาก "
                f"'{uct_node}', user_feature, และ static_default_card_type "
                f"— ไม่สามารถตรวจลำดับได้"
            )

        # สร้าง sequence ของ category ตามลำดับที่ปรากฏใน merge_page
        sequence = []
        for item_id in merge_ids:
            card_types = card_type_lookup.get(item_id, [])
            cat = categorize_item(card_types, user_card_type)
            sequence.append((item_id, cat, card_types))

        # ถ้า user_card_type เป็น no_card → ไม่สนใจลำดับ
        # (no_card และ open_deal ถือว่า valid ทั้งคู่ ไม่มีนิยาม primary/secondary)
        if user_card_type == "no_card":
            return

        # ตรวจว่ายังไม่เจอ primary หลังจากที่เห็น secondary แล้ว
        seen_secondary = False
        violations = []
        for item_id, cat, card_types in sequence:
            if cat == "secondary":
                seen_secondary = True
            if cat == "primary" and seen_secondary:
                violations.append(
                    f"  id={item_id!r}, card_type={card_types} "
                    f"(primary item ปรากฏหลัง secondary)"
                )

        assert not violations, (
            f"[{api_response['name']}] user card_type='{user_card_type}'\n"
            f"การเรียงลำดับผิดพลาด — primary items ควรอยู่ก่อน secondary:\n"
            + "\n".join(violations)
        )

    def test_package_type_filter(self, api_response):
        """
        ตรวจว่า items ใน result node มี package_type ที่ถูกต้องสำหรับ user's subs_type

        กฎ:
          dgi_subs_type = "post" → package_type ต้องเป็น "postpaid", "all", "" หรือ None
          dgi_subs_type = "pre"  → package_type ต้องเป็น "prepaid",  "all", "" หรือ None
        """
        resp = api_response["response"]
        result_node = api_response["result_node"]
        subs_type = extract_user_subs_type(resp)

        if not subs_type:
            pytest.skip(
                f"[{api_response['name']}] dgi_subs_type=null "
                f"— user ไม่มี subs_type ข้อมูล ถือว่า item ออกได้ทั้ง pre และ post ไม่ตรวจ package_type"
            )
        if subs_type == "all":
            pytest.skip(
                f"[{api_response['name']}] dgi_subs_type='all' "
                f"— user ใช้ได้ทุก package_type ไม่ตรวจ"
            )

        pkg_lookup = build_package_type_lookup(resp)
        merge_ids = extract_result_ids(resp, result_node)

        invalid_items = []
        for item_id in merge_ids:
            pkg = pkg_lookup.get(item_id)  # None = ไม่พบใน order_object
            if pkg is None:
                continue  # ไม่มีข้อมูล package_type → ข้าม
            if not is_package_type_allowed(pkg, subs_type):
                invalid_items.append(
                    f"  id={item_id!r}, package_type={pkg!r}"
                )

        assert not invalid_items, (
            f"[{api_response['name']}] user subs_type='{subs_type}'\n"
            f"พบ {len(invalid_items)} items ที่มี package_type ไม่ตรง:\n"
            + "\n".join(invalid_items)
        )

    def test_redeem_point_filter(self, api_response):
        """
        ตรวจว่า items ใน result node มี redeem_point ไม่เกินสิทธิ์ของ user

        กฎ:
          trueyou_point_remain  0-10 → item redeem_point ต้องอยู่ในช่วง 0-10
          trueyou_point_remain >= 11 → item redeem_point ต้องไม่เกิน trueyou_point_remain
          redeem_point = None        → ไม่มีข้อมูล ถือว่าผ่าน

        ถ้าไม่ส่ง trueyou_point_remain ใน params ถือว่า point = 0 → effective_max = 10
        """
        resp = api_response["response"]
        result_node = api_response["result_node"]
        params = api_response["params"]

        raw_point = params.get("trueyou_point_remain")
        trueyou_point_remain = int(raw_point) if raw_point is not None else 0
        if raw_point is None:
            print(
                f"  [INFO] [{api_response['name']}] ไม่มี trueyou_point_remain ใน params "
                f"— ถือว่า point=0, effective_max=10"
            )


        effective_max = 10 if trueyou_point_remain <= 10 else trueyou_point_remain
        print(
            f"  [INFO] trueyou_point_remain={trueyou_point_remain} "
            f"→ effective_max={effective_max}"
        )

        rp_lookup = build_redeem_point_lookup(resp)
        merge_ids = extract_result_ids(resp, result_node)

        invalid_items = []
        for item_id in merge_ids:
            rp = rp_lookup.get(item_id)
            if rp is None:
                continue  # ไม่มีข้อมูล redeem_point → ข้าม
            if not is_redeem_point_allowed(rp, trueyou_point_remain):
                invalid_items.append(
                    f"  id={item_id!r}, redeem_point={rp} > effective_max={effective_max}"
                )

        assert not invalid_items, (
            f"[{api_response['name']}] trueyou_point_remain={trueyou_point_remain}\n"
            f"พบ {len(invalid_items)} items ที่มี redeem_point เกินสิทธิ์:\n"
            + "\n".join(invalid_items)
        )

    def test_ordering_summary(self, api_response):
        """
        เขียน CSV รายละเอียด item ทุกตัวใน result node พร้อม category
        และ print summary — ไม่ fail ใช้สำหรับ debug
        """
        resp = api_response["response"]
        result_node = api_response["result_node"]
        uct_node = api_response["user_card_type_node"]
        user_card_type = extract_user_card_type(resp, uct_node)
        subs_type = extract_user_subs_type(resp)
        card_type_lookup = build_card_type_lookup(resp)
        pkg_lookup = build_package_type_lookup(resp)
        rp_lookup = build_redeem_point_lookup(resp)
        merge_ids = extract_result_ids(resp, result_node)

        raw_point = api_response["params"].get("trueyou_point_remain")
        trueyou_point_remain = int(raw_point) if raw_point is not None else None

        # สร้าง rows สำหรับ CSV
        rows = []
        counts = {"primary": 0, "secondary": 0, "invalid": 0, "unknown": 0}
        for position, item_id in enumerate(merge_ids, start=1):
            card_types = card_type_lookup.get(item_id)
            pkg = pkg_lookup.get(item_id, "")
            rp = rp_lookup.get(item_id)
            if card_types is None:
                category = "unknown"
                counts["unknown"] += 1
            else:
                category = categorize_item(card_types, user_card_type)
                counts[category] += 1
            rows.append({
                "position": position,
                "item_id": item_id,
                "card_type": "|".join(card_types) if card_types else "",
                "package_type": pkg if pkg is not None else "",
                "redeem_point": rp if rp is not None else "",
                "category": category,
            })

        # เขียน CSV
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = api_response["name"].replace(" ", "_").replace("|", "-").replace("/", "-")
        csv_path = REPORT_DIR / f"{safe_name}.csv"
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "position", "item_id", "card_type",
                    "package_type", "redeem_point", "category",
                ],
            )
            writer.writeheader()
            writer.writerows(rows)

        print(
            f"\n[{api_response['name']}] "
            f"result_node={result_node!r} | "
            f"user_card_type={user_card_type!r} | "
            f"total={len(merge_ids)} items | "
            f"primary={counts['primary']}, "
            f"secondary={counts['secondary']}, "
            f"invalid={counts['invalid']}, "
            f"unknown(not in order_object)={counts['unknown']}"
        )
        print(f"  CSV saved → {csv_path}")
        # test นี้ pass เสมอ ใช้สำหรับ debug เท่านั้น
        assert True
