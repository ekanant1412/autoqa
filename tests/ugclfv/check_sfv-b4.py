import pytest
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from conftest import record_evidence

# ============================================================
# Config
# ============================================================
METADATA_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/metadata/all-view-data"

SFV_BASE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-b4"
)

SHELF_IDS = [
    "8rMb9ZqnV5oA",
    "WxOoXRDRgYmx",
    "Rv8MXOYodRpL",
]

CANDIDATE_SELECTIONS = ["mix", "tophit", "feedRandom"]

SSO_ID = "681112259"

# ============================================================
# Fetch node responses from SFV API (verbose=debug)
# ============================================================
def _build_sfv_url(shelf_id: str, candidate_selection: str = "mix") -> str:
    return (
        f"{SFV_BASE_URL}"
        f"?candidate_selection={candidate_selection}"
        f"&shelfId={shelf_id}"
        f"&ugc_sfv_ratio=0"
        f"&verbose=debug"
        f"&ssoId={SSO_ID}"
    )


def _fetch_sfv_debug(shelf_id: str, candidate_selection: str = "mix") -> dict:
    resp = requests.get(_build_sfv_url(shelf_id, candidate_selection), timeout=30)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# Helpers
# ============================================================
def extract_article_category(response: dict) -> list:
    return response.get("result", {}).get("article_category", [])


def extract_agg_ids(response: dict, agg_name: str, sort_by: str) -> list:
    buckets = (
        response.get("result", {})
        .get("data", {})
        .get("aggregations", {})
        .get(agg_name, {})
        .get("buckets", [])
    )
    ids = []
    for bucket in buckets:
        inner_hits = (
            bucket.get(sort_by, {})
            .get("hits", {})
            .get("hits", [])
        )
        for hit in inner_hits:
            id_ = hit.get("_source", {}).get("id")
            if id_:
                ids.append(id_)
    return ids


def extract_candidate_order(response: dict, agg_name: str, sort_by: str) -> dict:
    """return { category: [id, ...] } ตาม sort order จาก candidate pool"""
    buckets = (
        response.get("result", {})
        .get("data", {})
        .get("aggregations", {})
        .get(agg_name, {})
        .get("buckets", [])
    )
    result = {}
    for bucket in buckets:
        category = bucket.get("key")
        inner_hits = (
            bucket.get(sort_by, {})
            .get("hits", {})
            .get("hits", [])
        )
        result[category] = [
            hit["_source"]["id"]
            for hit in inner_hits
            if hit.get("_source", {}).get("id")
        ]
    return result


def extract_bucketize_order(response: dict) -> dict:
    """return { category: [id, ...] } จาก bucketize result"""
    result = response.get("result", {})
    return {
        category: [item["id"] for item in items if item.get("id")]
        for category, items in result.items()
        if isinstance(items, list)
    }


def extract_bucketize_ids(node: dict) -> set:
    """return flat set of all IDs from bucketize node result"""
    result = node.get("result", {})
    return {
        item["id"]
        for items in result.values()
        if isinstance(items, list)
        for item in items
        if item.get("id")
    }


def extract_metadata_items_ids(node: dict) -> list:
    """return [id, ...] จาก metadata_items.result.items"""
    items = node.get("result", {}).get("items", [])
    return [item["id"] for item in items if item.get("id")]


def check_article_category(item_id: str, expected_categories: list) -> dict:
    payload = {
        "parameters": {"id": item_id, "fields": ["id", "article_category"]},
        "options": {"cache": False},
    }
    try:
        resp = requests.post(METADATA_URL, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        item_categories = (
            data.get("items", [{}])[0]
            .get("article_category", [])
        )
        matched = list(set(item_categories) & set(expected_categories))
        return {
            "id": item_id,
            "is_valid": len(matched) > 0,
            "item_categories": item_categories,
            "matched": matched,
        }
    except Exception as e:
        return {"id": item_id, "is_valid": False, "error": str(e)}


# ============================================================
# Fixtures
# ============================================================
@pytest.fixture(
    scope="module",
    params=[
        (shelf_id, mode)
        for shelf_id in SHELF_IDS
        for mode in CANDIDATE_SELECTIONS
    ],
    ids=lambda p: f"{p[0]}-{p[1]}",
)
def sfv_nodes(request):
    shelf_id, candidate_selection = request.param
    debug = _fetch_sfv_debug(shelf_id, candidate_selection)
    nodes = debug.get("data", {}).get("results", {})
    return {
        "shelf_id":            shelf_id,
        "candidate_selection": candidate_selection,
        "metadata_shelf":      nodes.get("metadata_shelf",   {}),
        "candidate_latest":    nodes.get("candidate_latest", {}),
        "candidate_tophit":    nodes.get("candidate_tophit", {}),
        "bucketize_latest":    nodes.get("bucketize_latest", {}),
        "bucketize_tophit":    nodes.get("bucketize_tophit", {}),
        "metadata_items":      nodes.get("metadata_items",   {}),
    }


@pytest.fixture(scope="module")
def article_categories(sfv_nodes):
    return extract_article_category(sfv_nodes["metadata_shelf"])


@pytest.fixture(scope="module")
def latest_ids(sfv_nodes):
    return extract_agg_ids(sfv_nodes["candidate_latest"], "agg_latest", "sort_by_publish_date")


@pytest.fixture(scope="module")
def tophit_ids(sfv_nodes):
    return extract_agg_ids(sfv_nodes["candidate_tophit"], "agg_tophit", "sort_by_hit_count")


@pytest.fixture(scope="module")
def all_ids(latest_ids, tophit_ids):
    return list(dict.fromkeys(latest_ids + tophit_ids))


@pytest.fixture(scope="module")
def category_check_results(all_ids, article_categories):
    """ยิง API concurrent แล้ว cache ผล"""
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(check_article_category, id_, article_categories): id_
            for id_ in all_ids
        }
        for future in as_completed(futures):
            r = future.result()
            results[r["id"]] = r
    return results


# ============================================================
# Tests
# ============================================================
class TestMetadataShelf:
    def test_article_category_not_empty(self, sfv_nodes, article_categories):
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "article_categories":  article_categories,
        })
        assert len(article_categories) > 0, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] metadata_shelf.article_category should not be empty"
        )


class TestCandidateIds:
    def test_latest_ids_not_empty(self, sfv_nodes, latest_ids):
        duplicates = [id_ for id_ in set(latest_ids) if latest_ids.count(id_) > 1]
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "candidate_ids",
            "latest_ids":          latest_ids,
            "latest_count":        len(latest_ids),
            "latest_duplicates":   duplicates,
        })
        assert len(latest_ids) > 0, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] candidate_latest should return at least 1 ID"
        )

    def test_tophit_ids_not_empty(self, sfv_nodes, tophit_ids):
        duplicates = [id_ for id_ in set(tophit_ids) if tophit_ids.count(id_) > 1]
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "candidate_ids",
            "tophit_ids":          tophit_ids,
            "tophit_count":        len(tophit_ids),
            "tophit_duplicates":   duplicates,
        })
        assert len(tophit_ids) > 0, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] candidate_tophit should return at least 1 ID"
        )

    def test_no_duplicate_within_latest(self, sfv_nodes, latest_ids):
        assert len(latest_ids) == len(set(latest_ids)), (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] candidate_latest has duplicate IDs"
        )

    def test_no_duplicate_within_tophit(self, sfv_nodes, tophit_ids):
        assert len(tophit_ids) == len(set(tophit_ids)), (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] candidate_tophit has duplicate IDs"
        )


class TestArticleCategoryValidation:
    def test_all_items_have_valid_category(self, sfv_nodes, all_ids, category_check_results):
        passed = [id_ for id_ in all_ids if category_check_results[id_]["is_valid"]]
        failed_ids = [id_ for id_ in all_ids if not category_check_results[id_]["is_valid"]]
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "article_category_validation",
            "total_ids":           len(all_ids),
            "passed_count":        len(passed),
            "failed_count":        len(failed_ids),
            "item_results": {
                id_: {
                    "is_valid":        category_check_results[id_]["is_valid"],
                    "item_categories": category_check_results[id_].get("item_categories", []),
                    "matched":         category_check_results[id_].get("matched", []),
                }
                for id_ in all_ids
            },
        })
        failed_msgs = [
            f"{id_} → {category_check_results[id_].get('item_categories', [])} | error: {category_check_results[id_].get('error', '')}"
            for id_ in failed_ids
        ]
        assert not failed_msgs, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] {len(failed_msgs)} item(s) have no matching article_category:\n"
            + "\n".join(failed_msgs)
        )

    def test_each_item_category_match(self, sfv_nodes, article_categories, category_check_results):
        candidate_latest = sfv_nodes["candidate_latest"]
        all_item_ids = [
            item.get("_source", {}).get("id")
            for bucket in candidate_latest.get("result", {}).get("data", {})
                .get("aggregations", {}).get("agg_latest", {}).get("buckets", [])
            for item in bucket.get("sort_by_publish_date", {}).get("hits", {}).get("hits", [])
            if item.get("_source", {}).get("id")
        ]
        failed = []
        for item_id in all_item_ids:
            result = category_check_results.get(item_id)
            if result is None:
                failed.append(f"{item_id}: no result")
            elif not result["is_valid"]:
                failed.append(
                    f"{item_id}: article_category {result.get('item_categories')} "
                    f"has no match with {article_categories}"
                )
        assert not failed, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] {len(failed)} item(s) failed category match:\n"
            + "\n".join(failed)
        )


class TestBucketizeOrder:
    def test_bucketize_latest_preserves_candidate_order(self, sfv_nodes):
        candidate_order = extract_candidate_order(
            sfv_nodes["candidate_latest"], "agg_latest", "sort_by_publish_date"
        )
        bucketize_order = extract_bucketize_order(sfv_nodes["bucketize_latest"])
        mismatches = {
            category: {"expected": [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids], "got": b_ids}
            for category, b_ids in bucketize_order.items()
            if [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids] != b_ids
        }
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "bucketize_latest_order",
            "bucketize_order":     bucketize_order,
            "mismatches":          mismatches,
        })
        for category, b_ids in bucketize_order.items():
            c_ids_filtered = [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids]
            assert c_ids_filtered == b_ids, (
                f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] [bucketize_latest] category '{category}' order mismatch:\n"
                f"  expected: {c_ids_filtered}\n"
                f"  got:      {b_ids}"
            )

    def test_bucketize_tophit_preserves_candidate_order(self, sfv_nodes):
        candidate_order = extract_candidate_order(
            sfv_nodes["candidate_tophit"], "agg_tophit", "sort_by_hit_count"
        )
        bucketize_order = extract_bucketize_order(sfv_nodes["bucketize_tophit"])
        mismatches = {
            category: {"expected": [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids], "got": b_ids}
            for category, b_ids in bucketize_order.items()
            if [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids] != b_ids
        }
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "bucketize_tophit_order",
            "bucketize_order":     bucketize_order,
            "mismatches":          mismatches,
        })
        for category, b_ids in bucketize_order.items():
            c_ids_filtered = [id_ for id_ in candidate_order.get(category, []) if id_ in b_ids]
            assert c_ids_filtered == b_ids, (
                f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] [bucketize_tophit] category '{category}' order mismatch:\n"
                f"  expected: {c_ids_filtered}\n"
                f"  got:      {b_ids}"
            )

    def test_bucketize_latest_ids_subset_of_candidate(self, sfv_nodes, latest_ids):
        b_ids = set(
            item["id"]
            for items in sfv_nodes["bucketize_latest"].get("result", {}).values()
            if isinstance(items, list)
            for item in items
        )
        extra = b_ids - set(latest_ids)
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "bucketize_latest_subset",
            "bucketize_ids":       sorted(b_ids),
            "extra_ids":           sorted(extra),
        })
        assert b_ids.issubset(set(latest_ids)), (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] bucketize_latest contains IDs not in candidate_latest: {extra}"
        )

    def test_bucketize_tophit_ids_subset_of_candidate(self, sfv_nodes, tophit_ids):
        b_ids = set(
            item["id"]
            for items in sfv_nodes["bucketize_tophit"].get("result", {}).values()
            if isinstance(items, list)
            for item in items
        )
        extra = b_ids - set(tophit_ids)
        record_evidence({
            "shelf_id":            sfv_nodes["shelf_id"],
            "candidate_selection": sfv_nodes["candidate_selection"],
            "test":                "bucketize_tophit_subset",
            "bucketize_ids":       sorted(b_ids),
            "extra_ids":           sorted(extra),
        })
        assert b_ids.issubset(set(tophit_ids)), (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] bucketize_tophit contains IDs not in candidate_tophit: {extra}"
        )


# ============================================================
# Fixtures: per candidate_selection mode (parametrize by shelf only)
# ============================================================
def _mode_fixture(mode: str):
    @pytest.fixture(scope="module", params=SHELF_IDS, ids=lambda s: f"{s}-{mode}")
    def _fix(request):
        shelf_id = request.param
        debug = _fetch_sfv_debug(shelf_id, mode)
        nodes = debug.get("data", {}).get("results", {})
        return {
            "shelf_id":            shelf_id,
            "candidate_selection": mode,
            "bucketize_latest":    nodes.get("bucketize_latest", {}),
            "bucketize_tophit":    nodes.get("bucketize_tophit", {}),
            "metadata_items":      nodes.get("metadata_items",   {}),
        }
    return _fix

sfv_nodes_mix        = _mode_fixture("mix")
sfv_nodes_tophit     = _mode_fixture("tophit")
sfv_nodes_feedrandom = _mode_fixture("feedRandom")


# ============================================================
# Tests: metadata_items validation per candidate_selection mode
# ============================================================
class TestGenerateCandidates:
    def test_metadata_items_not_empty(self, sfv_nodes):
        item_ids = extract_metadata_items_ids(sfv_nodes["metadata_items"])
        assert len(item_ids) > 0, (
            f"[shelf={sfv_nodes['shelf_id']}][mode={sfv_nodes['candidate_selection']}] "
            f"metadata_items.result.items should not be empty"
        )

    def test_mix_items_half_from_each(self, sfv_nodes_mix):
        """mix mode: metadata_items ต้องมี item จาก latest และ tophit อย่างละครึ่ง (±1)"""
        item_ids   = extract_metadata_items_ids(sfv_nodes_mix["metadata_items"])
        latest_ids = extract_bucketize_ids(sfv_nodes_mix["bucketize_latest"])
        tophit_ids = extract_bucketize_ids(sfv_nodes_mix["bucketize_tophit"])
        tag = f"[shelf={sfv_nodes_mix['shelf_id']}][mode=mix]"

        from_latest = [id_ for id_ in item_ids if id_ in latest_ids]
        from_tophit = [id_ for id_ in item_ids if id_ in tophit_ids]

        record_evidence({
            "shelf_id":            sfv_nodes_mix["shelf_id"],
            "candidate_selection": "mix",
            "metadata_items":      item_ids,
            "from_latest":         from_latest,
            "from_tophit":         from_tophit,
            "from_latest_count":   len(from_latest),
            "from_tophit_count":   len(from_tophit),
            "total":               len(item_ids),
        })

        assert len(from_latest) > 0, f"{tag} metadata_items has no items from bucketize_latest"
        assert len(from_tophit) > 0, f"{tag} metadata_items has no items from bucketize_tophit"
        assert abs(len(from_latest) - len(from_tophit)) <= 1, (
            f"{tag} metadata_items is not ~50/50: "
            f"latest={len(from_latest)}, tophit={len(from_tophit)}"
        )

    def test_tophit_items_only_from_tophit(self, sfv_nodes_tophit):
        """tophit mode: metadata_items ต้องมีเฉพาะ item จาก bucketize_tophit"""
        item_ids   = extract_metadata_items_ids(sfv_nodes_tophit["metadata_items"])
        latest_ids = extract_bucketize_ids(sfv_nodes_tophit["bucketize_latest"])
        tophit_ids = extract_bucketize_ids(sfv_nodes_tophit["bucketize_tophit"])
        tag = f"[shelf={sfv_nodes_tophit['shelf_id']}][mode=tophit]"

        from_latest_only = [id_ for id_ in item_ids if id_ in latest_ids and id_ not in tophit_ids]
        unexpected        = [id_ for id_ in item_ids if id_ not in tophit_ids]

        record_evidence({
            "shelf_id":            sfv_nodes_tophit["shelf_id"],
            "candidate_selection": "tophit",
            "metadata_items":      item_ids,
            "all_from_tophit":     len(unexpected) == 0,
            "unexpected_ids":      unexpected,
            "total":               len(item_ids),
        })

        assert len(from_latest_only) == 0, (
            f"{tag} metadata_items contains IDs exclusive to bucketize_latest: {from_latest_only}"
        )
        assert all(id_ in tophit_ids for id_ in item_ids), (
            f"{tag} metadata_items contains IDs not in bucketize_tophit: {unexpected}"
        )

    def test_feedrandom_items_only_from_latest(self, sfv_nodes_feedrandom):
        """feedRandom mode: metadata_items ต้องมีเฉพาะ item จาก bucketize_latest"""
        item_ids   = extract_metadata_items_ids(sfv_nodes_feedrandom["metadata_items"])
        latest_ids = extract_bucketize_ids(sfv_nodes_feedrandom["bucketize_latest"])
        tophit_ids = extract_bucketize_ids(sfv_nodes_feedrandom["bucketize_tophit"])
        tag = f"[shelf={sfv_nodes_feedrandom['shelf_id']}][mode=feedRandom]"

        from_tophit_only = [id_ for id_ in item_ids if id_ in tophit_ids and id_ not in latest_ids]
        unexpected        = [id_ for id_ in item_ids if id_ not in latest_ids]

        record_evidence({
            "shelf_id":            sfv_nodes_feedrandom["shelf_id"],
            "candidate_selection": "feedRandom",
            "metadata_items":      item_ids,
            "all_from_latest":     len(unexpected) == 0,
            "unexpected_ids":      unexpected,
            "total":               len(item_ids),
        })

        assert len(from_tophit_only) == 0, (
            f"{tag} metadata_items contains IDs exclusive to bucketize_tophit: {from_tophit_only}"
        )
        assert all(id_ in latest_ids for id_ in item_ids), (
            f"{tag} metadata_items contains IDs not in bucketize_latest: {unexpected}"
        )
