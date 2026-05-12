import requests
import pytest

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            # "&partner_id=AN9PjZR1wEol"
            "&limit=3"
        ),
    },
    {
        "name": "sfv-p6",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p6"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&limit=3"
            "&limit_seen_item=10"
        ),
    },
]

SEEN_PLACEMENTS = [p for p in PLACEMENTS if p["name"] in ["sfv-p6"]]
FIRST_ORDER_PLACEMENTS = [p for p in PLACEMENTS if p["name"] in ["sfv-p7"]]
RANDOM_PLACEMENTS = [p for p in PLACEMENTS if p["name"] in ["sfv-p6", "sfv-p7"]]

TIMEOUT = 20


def deep_find_node(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = deep_find_node(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for x in obj:
            found = deep_find_node(x, key)
            if found is not None:
                return found
    return None


# =====================================================
# 1) NOT IN SEEN ITEM -> CHECK ONLY P8
# =====================================================
@pytest.mark.parametrize("placement", SEEN_PLACEMENTS, ids=[p["name"] for p in SEEN_PLACEMENTS])
def test_seen_item_not_contains_target(placement):
    target_id = "DZreDb76MBmX"

    r = requests.get(placement["url"], params={"id": target_id}, timeout=TIMEOUT)
    assert r.status_code == 200

    res = r.json()

    seen_node = deep_find_node(res, "get_seen_item_redis")
    assert seen_node is not None, f"{placement['name']} should contain get_seen_item_redis"

    seen_ids = (
        seen_node.get("result", {}).get("ids")
        or [x.get("id") for x in seen_node.get("result", {}).get("items", []) if isinstance(x, dict)]
    )

    assert target_id not in seen_ids, f"{placement['name']} found target_id={target_id} in seen list"


# =====================================================
# 2) FIRST ORDER CHECK -> CHECK P7, P8
# =====================================================
@pytest.mark.parametrize("placement", FIRST_ORDER_PLACEMENTS, ids=[p["name"] for p in FIRST_ORDER_PLACEMENTS])
def test_first_item_equals_input(placement):
    target_id = "DZreDb76MBmX"

    r = requests.get(placement["url"], params={"id": target_id}, timeout=TIMEOUT)
    assert r.status_code == 200

    res = r.json()

    merge_page = deep_find_node(res, "merge_page")
    assert merge_page is not None, f"{placement['name']} missing merge_page node"

    items = merge_page.get("result", {}).get("items", [])
    assert items, f"{placement['name']} merge_page.result.items is empty"

    first_id = items[0].get("id")
    assert first_id == target_id, f"{placement['name']} expected first_id={target_id}, got {first_id}"


# =====================================================
# 3) RANDOM ORDER CHECK -> CHECK P7, P8
# =====================================================
@pytest.mark.parametrize("placement", RANDOM_PLACEMENTS, ids=[p["name"] for p in RANDOM_PLACEMENTS])
def test_merge_page_random(placement):
    signatures = []

    for _ in range(5):
        r = requests.get(placement["url"], timeout=TIMEOUT)
        assert r.status_code == 200

        res = r.json()

        merge_page = deep_find_node(res, "merge_page")
        assert merge_page is not None, f"{placement['name']} missing merge_page node"

        items = merge_page.get("result", {}).get("items", [])
        assert items, f"{placement['name']} merge_page.result.items is empty"

        sig = "|".join((x.get("id") or "") for x in items[:10] if isinstance(x, dict))
        signatures.append(sig)

    assert len(set(signatures)) > 1, f"{placement['name']} merge_page order looks deterministic"