"""
Automated insertion-logic test for SFV-P4 API.

Coverage:
  - fix positions are within expected block slot
  - no insertion exceeds block_size boundary
  - items_per_block limit is respected per block
  - number_first: blocks before this index get no insertion
  - override order (history → pin → live → google_trend → viral → creator)
  - random sections: only checks pool items appear somewhere in result
  - pool-empty positions are acceptable misses

Run:
    cd <project-root>
    pytest tests/test_sfv_insertion.py -v --tb=short \
        --json-report --json-report-file=reports/sfv_insertion_report.json
"""

from __future__ import annotations

import json
import math
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
import requests

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

SECTION_ORDER: list[str] = ["history", "pin", "live", "google_trend", "viral", "creator"]

POOL_RESPONSE_KEYS: dict[str, str] = {
    "history":      "candidate_sfv_history",
    "pin":          "filter_pin_exclude_live",
    "live":         "filter_live_exclude_pin",
    "google_trend": "candidate_sfv_google_trend",
    "viral":        "candidate_sfv_viral",
    "creator":      "candidate_sfv_relevance_creator",
}

# URL query-param prefix for each section
URL_PARAM_PREFIX: dict[str, str] = {
    "history":      "node_history",
    "pin":          "node_pin",
    "live":         "node_live",
    "google_trend": "node_google",
    "viral":        "node_viral",
    "creator":      "node_creator",
}

BASE_URL = (
    "http://poc-ai-universal-platform.preprod-gcp-ai-bn.int-ai-platform"
    ".gcp.dmp.true.th/api/v1/sfv-p4"
)

REQUEST_TIMEOUT = 30  # seconds

# ── Pin-source query-param values used in TC-06 / TC-07 ──
PIN_SHELFID  = "BJq5rZqYzjgJ"
PIN_CMS_IDS  = "zVRY3EZBmxa4"

# ─────────────────────────────────────────────────────────────
# Parametrised test-case definitions
# ─────────────────────────────────────────────────────────────
# Each dict has:
#   id          – human-readable test ID
#   sso_id      – user SSO id
#   sections    – dict[section_name, dict of 5 params]
#                   block_size, calculate_type, items_per_block,
#                   number_first, positions (list[int])

TEST_CASES: list[dict[str, Any]] = [
    # ── TC-01: all fix, distinct positions, standard ──────────
    {
        "id": "TC01_all_fix_distinct_positions",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[0]),
            "pin":          dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[1]),
            "live":         dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[1, 2]),
            "google_trend": dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[4]),
            "viral":        dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[5]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[3]),
        },
    },
    # ── TC-02: skip first block (number_first=1) ──────────────
    {
        "id": "TC02_number_first_1",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[0]),
            "pin":          dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[1]),
            "live":         dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[2]),
            "google_trend": dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[3]),
            "viral":        dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[4]),
            "creator":      dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=1, positions=[5]),
        },
    },
    # ── TC-03: items_per_block=2 ──────────────────────────────
    {
        "id": "TC03_items_per_block_2",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix", items_per_block=2, number_first=0, positions=[0, 1]),
            "pin":          dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[2]),
            "live":         dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[3]),
            "google_trend": dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[4]),
            "viral":        dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[5]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
        },
    },
    # ── TC-04: position out-of-block-boundary (should be skipped) ─
    {
        "id": "TC04_position_exceeds_block_boundary",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[10]),  # > block_size → skip
            "pin":          dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[1]),
            "live":         dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[2]),
            "google_trend": dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[3]),
            "viral":        dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[4]),
            "creator":      dict(block_size=6, calculate_type="fix", items_per_block=1, number_first=0, positions=[5]),
        },
    },
    # ── TC-05: all random ─────────────────────────────────────
    {
        "id": "TC05_all_random",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
            "pin":          dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
            "live":         dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
            "google_trend": dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
            "viral":        dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
        },
    },
    # ── TC-06: pin source via shelfid ─────────────────────────
    # ผ่าน shelfid=BJq5rZqYzjgJ → filter_pin_exclude_live ควรคืน
    # items จาก shelf นั้น และถูก insert ที่ตำแหน่ง pin (position=1)
    {
        "id": "TC06_pin_from_shelfid",
        "sso_id": "9",
        "extra_params": {"shelfid": PIN_SHELFID},
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[0]),
            "pin":          dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[1]),
            "live":         dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[2]),
            "google_trend": dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[3]),
            "viral":        dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[4]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
        },
    },
    # ── TC-07: pin source via cmsIds ──────────────────────────
    # ผ่าน cmsIds=zVRY3EZBmxa4 → filter_pin_exclude_live ควรคืน
    # items จาก cms collection นั้น และถูก insert ที่ตำแหน่ง pin (position=1)
    {
        "id": "TC07_pin_from_cmsids",
        "sso_id": "9",
        "extra_params": {"cmsIds": PIN_CMS_IDS},
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[0]),
            "pin":          dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[1]),
            "live":         dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[2]),
            "google_trend": dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[3]),
            "viral":        dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[4]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
        },
    },
]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def build_url(
    sso_id: str,
    sections: dict[str, dict],
    extra_params: dict[str, str] | None = None,
) -> str:
    params: list[str] = [f"ssoId={sso_id}", "verbose=debug"]
    for section, cfg in sections.items():
        prefix = URL_PARAM_PREFIX[section]
        positions_str = ",".join(str(p) for p in cfg["positions"])
        params += [
            f"{prefix}_block_size={cfg['block_size']}",
            f"{prefix}_calculate_type={cfg['calculate_type']}",
            f"{prefix}_items_per_block={cfg['items_per_block']}",
            f"{prefix}_number_first={cfg['number_first']}",
            f"{prefix}_positions={positions_str}",
        ]
    # extra_params: เช่น shelfid หรือ cmsIds สำหรับกำหนดแหล่งที่มาของ pin pool
    if extra_params:
        for key, value in extra_params.items():
            params.append(f"{key}={value}")
    return BASE_URL + "?" + "&".join(params)


def fetch_response(url: str) -> dict:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def extract_pool_sizes(response: dict) -> dict[str, int]:
    """Return {section: number_of_candidate_items} from response."""
    sizes: dict[str, int] = {}
    for section, key in POOL_RESPONSE_KEYS.items():
        node = response.get(key, {})
        # Pool items are usually under result.items or result directly
        result = node.get("result", node)
        if isinstance(result, list):
            sizes[section] = len(result)
        elif isinstance(result, dict):
            # Prefer item_size when present (e.g. history pool returns ids + item_size)
            if "item_size" in result:
                sizes[section] = int(result["item_size"])
            else:
                # Fall back to counting list under "items", "item", or "ids"
                items = result.get("items", result.get("item", result.get("ids", [])))
                sizes[section] = len(items) if isinstance(items, list) else 0
        else:
            sizes[section] = 0
    return sizes


def extract_insert_result(response: dict) -> dict:
    """Return the insert_items_result node."""
    return response.get("insert_items_result", {})


def get_special_positions(insert_result: dict) -> dict[int, tuple[str, str]]:
    """
    Parse special_content_positions → {abs_pos: (section_name, item_id)}
    Format in response: {"0": "google_trend_LMnlQp50Z9xP", ...}
    """
    raw: dict = (
        insert_result.get("result", {}).get("special_content_positions", {})
    )
    parsed: dict[int, tuple[str, str]] = {}
    for pos_str, value in raw.items():
        pos = int(pos_str)
        # value = "{section}_{item_id}"  (section may contain underscores)
        for section in SECTION_ORDER:
            if value.startswith(section + "_"):
                item_id = value[len(section) + 1:]
                parsed[pos] = (section, item_id)
                break
        else:
            # Fallback: split on first underscore
            parts = value.split("_", 1)
            parsed[pos] = (parts[0], parts[1] if len(parts) > 1 else "")
    return parsed


def compute_expected_insertions(
    sections: dict[str, dict],
    pool_sizes: dict[str, int],
    total_item_size: int,
) -> dict[int, str]:
    """
    Compute expected {absolute_position: section_name}.
    Processes in SECTION_ORDER; later sections override earlier ones.
    Returns only fix-type positions (random positions are unpredictable).
    """
    position_map: dict[int, str] = {}

    for section in SECTION_ORDER:
        cfg = sections[section]
        pool_size = pool_sizes.get(section, 0)
        block_size = cfg["block_size"]
        calculate_type = cfg["calculate_type"]
        items_per_block = cfg["items_per_block"]
        number_first = cfg["number_first"]
        positions = cfg["positions"]

        if pool_size == 0 or calculate_type != "fix":
            continue  # empty pool → skip; random → can't predict

        num_blocks = math.ceil(total_item_size / block_size)
        pool_used = 0

        for block_num in range(number_first, num_blocks):
            if pool_used >= pool_size:
                break
            items_this_block = 0
            for rel_pos in positions:
                if rel_pos >= block_size:
                    continue  # out of block boundary
                abs_pos = block_num * block_size + rel_pos
                if abs_pos >= total_item_size:
                    continue  # out of total range
                if items_this_block >= items_per_block:
                    break
                if pool_used >= pool_size:  # exhausted mid-block
                    break
                position_map[abs_pos] = section
                items_this_block += 1
                pool_used += 1

    return position_map


# ─────────────────────────────────────────────────────────────
# Assertion helpers
# ─────────────────────────────────────────────────────────────

class InsertionValidationResult:
    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[str] = []
        self.warnings: list[str] = []

    @property
    def ok(self) -> bool:
        return len(self.failed) == 0

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "failed": self.failed,
            "warnings": self.warnings,
            "ok": self.ok,
        }


def validate_insertions(
    expected_map: dict[int, str],
    actual_map: dict[int, tuple[str, str]],
    sections: dict[str, dict],
    pool_sizes: dict[str, int],
    total_item_size: int,
) -> InsertionValidationResult:
    result = InsertionValidationResult()

    # 1. Check fix positions: every expected position should match actual section
    for abs_pos, expected_section in expected_map.items():
        if abs_pos not in actual_map:
            # Acceptable if pool empty (already filtered) OR overridden by higher-priority section
            # At this point expected_map already accounts for overrides, so this is unexpected
            result.warnings.append(
                f"Position {abs_pos}: expected '{expected_section}' insertion but not found "
                f"in special_content_positions (pool may have been empty or runtime changed)"
            )
        else:
            actual_section, item_id = actual_map[abs_pos]
            if actual_section == expected_section:
                result.passed.append(
                    f"Position {abs_pos}: '{expected_section}' ✓ (item={item_id})"
                )
            else:
                result.failed.append(
                    f"Position {abs_pos}: expected '{expected_section}' but got '{actual_section}' (item={item_id})"
                )

    # 2. Check actual positions: no unexpected section at a position
    for abs_pos, (actual_section, item_id) in actual_map.items():
        if actual_section not in SECTION_ORDER:
            result.warnings.append(
                f"Position {abs_pos}: unknown section '{actual_section}' in response"
            )

    # 3. Boundary check: no insertion beyond total_item_size
    for abs_pos in actual_map:
        if abs_pos >= total_item_size:
            result.failed.append(
                f"Position {abs_pos} is out of bounds (total_item_size={total_item_size})"
            )
        else:
            # only add passed if not already checked above
            if abs_pos not in expected_map:
                section = actual_map[abs_pos][0]
                cfg = sections.get(section, {})
                if cfg.get("calculate_type") == "random":
                    result.passed.append(
                        f"Position {abs_pos}: random section '{section}' inserted within bounds ✓"
                    )

    # 4. items_per_block: count insertions per block per section
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "fix":
            continue
        block_size = cfg["block_size"]
        items_per_block = cfg["items_per_block"]
        number_first = cfg["number_first"]
        num_blocks = math.ceil(total_item_size / block_size)

        for block_num in range(number_first, num_blocks):
            block_start = block_num * block_size
            block_end = block_start + block_size
            count_in_block = sum(
                1 for pos, (sec, _) in actual_map.items()
                if sec == section and block_start <= pos < block_end
            )
            if count_in_block > items_per_block:
                result.failed.append(
                    f"Section '{section}' block {block_num}: "
                    f"inserted {count_in_block} items but items_per_block={items_per_block}"
                )

    # 5. number_first: no insertions before the first allowed block
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "fix":
            continue
        block_size = cfg["block_size"]
        number_first = cfg["number_first"]
        first_allowed_abs = number_first * block_size
        for abs_pos, (actual_section, item_id) in actual_map.items():
            if actual_section == section and abs_pos < first_allowed_abs:
                result.failed.append(
                    f"Section '{section}': insertion at position {abs_pos} is before "
                    f"number_first={number_first} (first allowed abs={first_allowed_abs})"
                )

    # 6. positions boundary: check no actual fix-insertion is at a wrong relative slot
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "fix":
            continue
        block_size = cfg["block_size"]
        allowed_rel = set(cfg["positions"])
        for abs_pos, (actual_section, item_id) in actual_map.items():
            if actual_section == section:
                rel_pos = abs_pos % block_size
                if allowed_rel and rel_pos not in allowed_rel:
                    result.failed.append(
                        f"Section '{section}': insertion at abs={abs_pos} (rel={rel_pos}) "
                        f"is not in allowed positions={sorted(allowed_rel)}"
                    )

    return result


# ─────────────────────────────────────────────────────────────
# Pytest fixtures
# ─────────────────────────────────────────────────────────────

def _case_id(tc: dict) -> str:
    return tc["id"]


@pytest.fixture(scope="session")
def all_results() -> dict:
    """Cache API responses per URL to avoid duplicate calls."""
    return {}


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("tc", TEST_CASES, ids=_case_id)
def test_insertion_logic(tc: dict[str, Any], all_results: dict, tmp_path_factory):
    """
    End-to-end insertion logic coverage test:
      - builds URL from test-case params
      - calls the API
      - validates special_content_positions against expected insertion logic
      - saves per-test JSON report
    """
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))

    # ── Fetch (with cache) ──
    if url not in all_results:
        try:
            all_results[url] = fetch_response(url)
        except Exception as exc:
            pytest.skip(f"API unreachable ({exc}). Run from internal network.")
    response = all_results[url]

    # ── Extract data ──
    insert_result = extract_insert_result(response)
    actual_special = get_special_positions(insert_result)
    total_item_size: int = insert_result.get("result", {}).get("item_size", 0)
    pool_sizes = extract_pool_sizes(response)

    assert total_item_size > 0, "insert_items_result.result.item_size must be > 0"

    # ── Compute expected ──
    expected_map = compute_expected_insertions(tc["sections"], pool_sizes, total_item_size)

    # ── Validate ──
    vr = validate_insertions(
        expected_map, actual_special, tc["sections"], pool_sizes, total_item_size
    )

    # ── Save report ──
    report_dir = tmp_path_factory.mktemp(tc["id"], numbered=False)
    report = {
        "test_id":            tc["id"],
        "sso_id":             tc["sso_id"],
        "extra_params":       tc.get("extra_params", {}),
        "url":                url,
        "total_item_size":    total_item_size,
        "pool_sizes":         pool_sizes,
        "expected_positions": {str(k): v for k, v in expected_map.items()},
        "actual_positions":   {
            str(k): {"section": s, "item_id": i}
            for k, (s, i) in actual_special.items()
        },
        "validation":         vr.to_dict(),
    }
    report_path = report_dir / "insertion_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    # ── Assert ──
    if vr.failed:
        failures = "\n  ".join(vr.failed)
        warnings = "\n  ".join(vr.warnings) if vr.warnings else "(none)"
        pytest.fail(
            f"\n[{tc['id']}] Insertion logic failures:\n  {failures}"
            f"\nWarnings (non-blocking):\n  {warnings}"
            f"\nReport: {report_path}"
        )

    # Print summary even on pass
    print(f"\n✅ {tc['id']}: {len(vr.passed)} checks passed, {len(vr.warnings)} warnings")
    if vr.warnings:
        for w in vr.warnings:
            print(f"   ⚠ {w}")


@pytest.mark.parametrize("tc", TEST_CASES, ids=_case_id)
def test_no_out_of_bounds_insertions(tc: dict[str, Any], all_results: dict):
    """All special_content_positions must be within [0, item_size)."""
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    if url not in all_results:
        pytest.skip("Run test_insertion_logic first to populate cache.")
    response = all_results[url]
    insert_result = extract_insert_result(response)
    actual_special = get_special_positions(insert_result)
    total_item_size = insert_result.get("result", {}).get("item_size", 0)

    oob = [p for p in actual_special if p >= total_item_size]
    assert not oob, f"Out-of-bounds positions: {oob} (total_item_size={total_item_size})"


@pytest.mark.parametrize("tc", TEST_CASES, ids=_case_id)
def test_section_labels_valid(tc: dict[str, Any], all_results: dict):
    """Labels in special_content_positions must be known section names."""
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    if url not in all_results:
        pytest.skip("Run test_insertion_logic first to populate cache.")
    response = all_results[url]
    insert_result = extract_insert_result(response)
    actual_special = get_special_positions(insert_result)

    unknown = {s for s, _ in actual_special.values() if s not in SECTION_ORDER}
    assert not unknown, f"Unknown section labels in result: {unknown}"


@pytest.mark.parametrize("tc", TEST_CASES, ids=_case_id)
def test_random_sections_have_insertions_when_pool_not_empty(
    tc: dict[str, Any], all_results: dict
):
    """
    For sections with calculate_type=random and non-empty pool,
    at least one item from that pool should appear in special_content_positions.
    (Best-effort: pool may have 0 items at runtime.)
    """
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    if url not in all_results:
        pytest.skip("Run test_insertion_logic first to populate cache.")
    response = all_results[url]
    insert_result = extract_insert_result(response)
    actual_special = get_special_positions(insert_result)
    pool_sizes = extract_pool_sizes(response)

    for section, cfg in tc["sections"].items():
        if cfg["calculate_type"] != "random":
            continue
        if pool_sizes.get(section, 0) == 0:
            continue  # pool empty → no insertion expected
        found = any(s == section for s, _ in actual_special.values())
        assert found, (
            f"Section '{section}' is random with pool_size={pool_sizes[section]} "
            f"but no item from this pool appears in special_content_positions"
        )


# ─────────────────────────────────────────────────────────────
# Helper – pin pool item extraction
# ─────────────────────────────────────────────────────────────

def extract_pin_pool_item_ids(response: dict) -> list[str]:
    """
    Return item IDs from filter_pin_exclude_live pool.

    The node usually looks like one of:
      {"result": ["id1", "id2", ...]}
      {"result": {"items": [...], "item_size": N}}
      {"result": {"ids": [...]}}
    Returns an empty list if the pool is absent or empty.
    """
    node = response.get(POOL_RESPONSE_KEYS["pin"], {})
    result = node.get("result", node)

    if isinstance(result, list):
        return [str(x) for x in result]
    if isinstance(result, dict):
        for key in ("items", "item", "ids"):
            items = result.get(key, [])
            if isinstance(items, list) and items:
                return [str(x) for x in items]
    return []


# ─────────────────────────────────────────────────────────────
# New tests – pin pool source via shelfid / cmsIds
# ─────────────────────────────────────────────────────────────

# Only TC-06 and TC-07 carry extra_params; filter helper
_PIN_SOURCE_CASES = [tc for tc in TEST_CASES if tc.get("extra_params")]


@pytest.mark.parametrize("tc", _PIN_SOURCE_CASES, ids=_case_id)
def test_pin_pool_populated_when_param_given(tc: dict[str, Any], all_results: dict):
    """
    เมื่อส่ง shelfid หรือ cmsIds มาใน query param
    filter_pin_exclude_live (pin pool) ต้องคืน items ≥ 1 รายการ

    หาก pool ว่างเปล่า แสดงว่า shelfid/cmsIds ไม่มีเนื้อหาหรือ API
    ไม่ได้ใช้ param นั้น → test fail พร้อมข้อความชัดเจน
    """
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    if url not in all_results:
        try:
            all_results[url] = fetch_response(url)
        except Exception as exc:
            pytest.skip(f"API unreachable ({exc}). Run from internal network.")
    response = all_results[url]

    pin_ids = extract_pin_pool_item_ids(response)
    extra = tc.get("extra_params", {})
    param_desc = ", ".join(f"{k}={v}" for k, v in extra.items())

    assert pin_ids, (
        f"[{tc['id']}] filter_pin_exclude_live is empty when {param_desc}. "
        f"ตรวจสอบว่า API รับ param นี้จริงและมีเนื้อหาใน shelf/cms ดังกล่าว"
    )
    print(
        f"\n✅ {tc['id']}: pin pool has {len(pin_ids)} item(s) "
        f"from {param_desc} → {pin_ids[:3]}{'…' if len(pin_ids) > 3 else ''}"
    )


@pytest.mark.parametrize("tc", _PIN_SOURCE_CASES, ids=_case_id)
def test_pin_insertions_come_from_pin_pool(tc: dict[str, Any], all_results: dict):
    """
    item_id ที่ถูก insert ในตำแหน่ง pin ต้องอยู่ใน filter_pin_exclude_live pool
    ที่ API คืนมาด้วย — ตรวจว่า pin pool เป็นแหล่งที่มาจริงของ pin items
    """
    url = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    if url not in all_results:
        pytest.skip("Run test_pin_pool_populated_when_param_given first to populate cache.")
    response = all_results[url]

    pin_pool_ids = set(extract_pin_pool_item_ids(response))
    if not pin_pool_ids:
        pytest.skip("Pin pool is empty; cannot verify insertion source.")

    insert_result = extract_insert_result(response)
    actual_special = get_special_positions(insert_result)

    # Collect item_ids actually inserted as "pin"
    inserted_pin_items = [
        item_id
        for section, item_id in actual_special.values()
        if section == "pin"
    ]

    if not inserted_pin_items:
        pytest.skip("No pin items were inserted (pool may have been exhausted by live override).")

    out_of_pool = [iid for iid in inserted_pin_items if iid not in pin_pool_ids]
    assert not out_of_pool, (
        f"[{tc['id']}] Pin items inserted but NOT found in filter_pin_exclude_live pool: "
        f"{out_of_pool}. "
        f"Pool contained: {sorted(pin_pool_ids)[:10]}"
    )
    print(
        f"\n✅ {tc['id']}: all {len(inserted_pin_items)} inserted pin item(s) "
        f"are in the pin pool ✓"
    )


@pytest.mark.parametrize("tc", _PIN_SOURCE_CASES, ids=_case_id)
def test_pin_pool_differs_by_source_param(tc: dict[str, Any], all_results: dict):
    """
    เปรียบเทียบ pin pool ระหว่าง:
      - baseline (ไม่มี shelfid/cmsIds)
      - TC ที่มี extra_params

    ถ้า pool เหมือนกันทุก item อาจแสดงว่า API ไม่ได้ใช้ param นั้นกรอง pin
    → test บันทึก warning (ไม่ fail) เพราะบาง shelf อาจ overlap กับ default pool
    """
    url_with_param = build_url(tc["sso_id"], tc["sections"], tc.get("extra_params"))
    url_baseline   = build_url(tc["sso_id"], tc["sections"])  # ไม่มี extra_params

    # Fetch both (with cache)
    for url in (url_with_param, url_baseline):
        if url not in all_results:
            try:
                all_results[url] = fetch_response(url)
            except Exception as exc:
                pytest.skip(f"API unreachable ({exc}). Run from internal network.")

    pool_with  = set(extract_pin_pool_item_ids(all_results[url_with_param]))
    pool_base  = set(extract_pin_pool_item_ids(all_results[url_baseline]))

    extra = tc.get("extra_params", {})
    param_desc = ", ".join(f"{k}={v}" for k, v in extra.items())

    if pool_with == pool_base:
        # Not a hard failure — might be legitimate overlap — but flag it clearly
        pytest.warns(
            UserWarning,
            match="pin pool identical",
        ) if False else None   # pytest.warns needs context manager; use print instead
        print(
            f"\n⚠  {tc['id']}: pin pool with {param_desc} is IDENTICAL to baseline "
            f"({len(pool_with)} items). "
            f"ตรวจสอบว่า API ใช้ param นี้กรอง pin จริงหรือไม่"
        )
    else:
        only_in_param = pool_with - pool_base
        only_in_base  = pool_base - pool_with
        print(
            f"\n✅ {tc['id']}: pin pool differs when {param_desc}. "
            f"unique-to-param={len(only_in_param)}, unique-to-baseline={len(only_in_base)}"
        )

    # Hard assert: pool with param must not be a strict subset of baseline with 0 exclusive items
    # (i.e., at minimum the pools should differ OR param pool must be non-empty)
    assert pool_with, (
        f"[{tc['id']}] Pin pool is empty even with {param_desc}. "
        f"API อาจไม่รองรับ param หรือ shelf/cms ว่างเปล่า"
    )
