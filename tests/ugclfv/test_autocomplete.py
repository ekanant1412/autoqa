"""
Automated Tests for Autocomplete API
Endpoint: /api/v1/universal/ext_711_mlp_autocomplete
"""

import pytest
import requests

BASE_URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/ext_711_mlp_autocomplete"
)


def call_autocomplete(query: str) -> dict:
    """Helper: call autocomplete API and return parsed JSON."""
    response = requests.get(BASE_URL, params={"query": query}, timeout=10)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# TC-01: Verify autocomplete returns suggestions for valid keyword
# ---------------------------------------------------------------------------
def test_autocomplete_returns_suggestions_for_valid_keyword():
    """Autocomplete should return non-empty items for a known keyword."""
    data = call_autocomplete("ไก่")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) > 0, "items should not be empty for valid keyword 'ไก่'"


# ---------------------------------------------------------------------------
# TC-02: Verify autocomplete returns empty result for non-matching keyword
# ---------------------------------------------------------------------------
def test_autocomplete_returns_empty_for_non_matching_keyword():
    """Autocomplete should return empty items list for a keyword that has no match."""
    non_matching_keyword = "xyzxyzxyz_no_match_12345"
    data = call_autocomplete(non_matching_keyword)

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) == 0, (
        f"Expected empty items for non-matching keyword, got {data['items']}"
    )


# ---------------------------------------------------------------------------
# TC-03: Verify autocomplete works with partial keyword
# ---------------------------------------------------------------------------
def test_autocomplete_works_with_partial_keyword():
    """Autocomplete should return suggestions when queried with a partial keyword."""
    # "ไก" is a partial form of "ไก่"
    data = call_autocomplete("ไก")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) > 0, (
        "Autocomplete should return results for partial keyword 'ไก'"
    )


# ---------------------------------------------------------------------------
# TC-04: Verify autocomplete works with full keyword
# ---------------------------------------------------------------------------
def test_autocomplete_works_with_full_keyword():
    """Autocomplete should return suggestions when queried with the full keyword."""
    data = call_autocomplete("ไก่")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) > 0, (
        "Autocomplete should return results for full keyword 'ไก่'"
    )
    # At least one item should match the exact keyword
    ids = [item["id"] for item in data["items"]]
    assert any("ไก่" in item_id for item_id in ids), (
        "At least one suggestion should contain the full keyword 'ไก่'"
    )


# ---------------------------------------------------------------------------
# TC-05: Verify autocomplete triggers at the correct input length (1 character)
# ---------------------------------------------------------------------------
def test_autocomplete_triggers_at_single_character():
    """Autocomplete should return results starting from a single character input."""
    data = call_autocomplete("ไ")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) > 0, (
        "Autocomplete should trigger and return results for a single character 'ไ'"
    )


# ---------------------------------------------------------------------------
# TC-06: Verify autocomplete result limit (max 10 items)
# ---------------------------------------------------------------------------
def test_autocomplete_result_limit():
    """Autocomplete should return at most 10 suggestions per query."""
    data = call_autocomplete("ไก่")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    assert isinstance(data["items"], list), "items should be a list"
    assert len(data["items"]) <= 10, (
        f"Expected at most 10 items, but got {len(data['items'])}"
    )


# ---------------------------------------------------------------------------
# TC-07: Verify autocomplete does not return duplicate suggestions
# ---------------------------------------------------------------------------
def test_autocomplete_no_duplicate_suggestions():
    """Autocomplete results should not contain duplicate suggestion IDs."""
    data = call_autocomplete("ไก่")

    assert data["status"] == 200, f"Expected status 200, got {data['status']}"
    ids = [item["id"] for item in data["items"]]
    unique_ids = list(dict.fromkeys(ids))  # preserve order, remove duplicates

    assert ids == unique_ids, (
        f"Duplicate suggestions found: "
        f"{[id_ for id_ in ids if ids.count(id_) > 1]}"
    )
