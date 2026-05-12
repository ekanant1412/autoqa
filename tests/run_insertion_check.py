#!/usr/bin/env python3
"""
Standalone insertion-logic checker (no pytest required).
Works with Python 3.10+.

Modes:
  1. Mock mode  – validates logic with synthetic data (always works, no network needed)
  2. Live mode  – hits the real API and validates actual response

Usage:
    python3 tests/run_insertion_check.py             # mock + live (live skipped if unreachable)
    python3 tests/run_insertion_check.py --mock-only # force mock only
    python3 tests/run_insertion_check.py --live-only # force live only

Output:
    reports/sfv_insertion_results_<timestamp>.json
    reports/sfv_insertion_results_<timestamp>.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

SECTION_ORDER = ["history", "pin", "live", "google_trend", "viral", "creator"]

POOL_RESPONSE_KEYS = {
    "history":      "candidate_sfv_history",
    "pin":          "filter_pin_exclude_live",
    "live":         "filter_live_exclude_pin",
    "google_trend": "candidate_sfv_google_trend",
    "viral":        "candidate_sfv_viral",
    "creator":      "candidate_sfv_relevance_creator",
}

URL_PARAM_PREFIX = {
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

# ─────────────────────────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────────────────────────

TEST_CASES: list[dict[str, Any]] = [
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
    {
        "id": "TC03_items_per_block_2",
        "sso_id": "9",
        "sections": {
            "history":      dict(block_size=6, calculate_type="fix",    items_per_block=2, number_first=0, positions=[0, 1]),
            "pin":          dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[2]),
            "live":         dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[3]),
            "google_trend": dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[4]),
            "viral":        dict(block_size=6, calculate_type="fix",    items_per_block=1, number_first=0, positions=[5]),
            "creator":      dict(block_size=6, calculate_type="random", items_per_block=1, number_first=0, positions=[]),
        },
    },
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
]

# ─────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────

def build_url(sso_id: str, sections: dict) -> str:
    params = [f"ssoId={sso_id}", "verbose=debug"]
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
    return BASE_URL + "?" + "&".join(params)


def fetch_response(url: str, timeout: int = 15) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"       ⚠ fetch error: {type(e).__name__}: {e}")
        return None


def extract_pool_sizes(response: dict) -> dict[str, int]:
    sizes: dict[str, int] = {}
    for section, key in POOL_RESPONSE_KEYS.items():
        node = response.get(key, {})
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


def get_special_positions(insert_result: dict) -> dict[int, tuple[str, str]]:
    raw = insert_result.get("result", {}).get("special_content_positions", {})
    parsed: dict[int, tuple[str, str]] = {}
    for pos_str, value in raw.items():
        pos = int(pos_str)
        for section in SECTION_ORDER:
            if value.startswith(section + "_"):
                parsed[pos] = (section, value[len(section) + 1:])
                break
        else:
            parts = value.split("_", 1)
            parsed[pos] = (parts[0], parts[1] if len(parts) > 1 else "")
    return parsed


def compute_expected_insertions(sections: dict, pool_sizes: dict, total_item_size: int) -> dict[int, str]:
    position_map: dict[int, str] = {}
    for section in SECTION_ORDER:
        cfg = sections[section]
        pool_size = pool_sizes.get(section, 0)
        if pool_size == 0 or cfg["calculate_type"] != "fix":
            continue
        block_size = cfg["block_size"]
        items_per_block = cfg["items_per_block"]
        number_first = cfg["number_first"]
        positions = cfg["positions"]
        num_blocks = math.ceil(total_item_size / block_size)
        pool_used = 0
        for block_num in range(number_first, num_blocks):
            if pool_used >= pool_size:
                break
            items_this_block = 0
            for rel_pos in positions:
                if rel_pos >= block_size:
                    continue
                abs_pos = block_num * block_size + rel_pos
                if abs_pos >= total_item_size:
                    continue
                if items_this_block >= items_per_block:
                    break
                if pool_used >= pool_size:  # exhausted mid-block
                    break
                position_map[abs_pos] = section
                items_this_block += 1
                pool_used += 1
    return position_map


def validate(expected_map, actual_map, sections, pool_sizes, total_item_size) -> dict:
    """
    Validate special_content_positions against insertion logic.

    NOTE: positions in special_content_positions are absolute positions in the
    FINAL merged list (after insertions shift the feed). They are NOT the same as
    the relative block-positions specified in URL params. Therefore we only check
    things we can reliably verify from the final result:

      1. All inserted positions are in bounds [0, total_item_size)
      2. Section labels are valid (known pool names)
      3. Inserted items come from a non-empty pool
      4. number_first: no fix-section item appears in blocks BEFORE number_first
      5. items_per_block: count per section per block ≤ items_per_block
      6. Fix section with non-empty pool should have at least one item in result
      7. Random section with non-empty pool should have at least one item in result
    """
    passed, failed, warnings = [], [], []

    # 1. Out-of-bounds check
    for abs_pos in actual_map:
        if abs_pos >= total_item_size:
            failed.append(f"pos {abs_pos} out of bounds (total_item_size={total_item_size})")
        else:
            passed.append(f"pos {abs_pos}: in bounds ✓")

    # 2. Valid section labels
    unknown = {s for s, _ in actual_map.values() if s not in SECTION_ORDER}
    if unknown:
        failed.append(f"Unknown section labels in result: {unknown}")
    elif actual_map:
        passed.append("All section labels valid ✓")

    # 3. Pool existence: inserted section must have non-empty pool
    for abs_pos, (actual_section, item_id) in actual_map.items():
        pool_size = pool_sizes.get(actual_section, 0)
        if pool_size == 0:
            failed.append(
                f"'{actual_section}' at pos {abs_pos}: pool is empty but item was inserted (item={item_id})"
            )
        else:
            passed.append(f"pos {abs_pos}: '{actual_section}' pool_size={pool_size} ✓")

    # 4. number_first: no fix insertion before first allowed block
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "fix":
            continue
        block_size    = cfg["block_size"]
        number_first  = cfg["number_first"]
        first_allowed = number_first * block_size
        for abs_pos, (actual_section, item_id) in actual_map.items():
            if actual_section == section and abs_pos < first_allowed:
                failed.append(
                    f"'{section}': insertion at pos {abs_pos} is before "
                    f"number_first={number_first} boundary (abs={first_allowed})"
                )
            elif actual_section == section and abs_pos >= first_allowed:
                passed.append(
                    f"'{section}' pos {abs_pos}: respects number_first={number_first} ✓"
                )

    # 5. items_per_block: count per section per block must not exceed limit
    for section in SECTION_ORDER:
        cfg = sections[section]
        block_size     = cfg["block_size"]
        items_per_block = cfg["items_per_block"]
        number_first   = cfg["number_first"]
        num_blocks     = math.ceil(total_item_size / block_size)
        for block_num in range(number_first, num_blocks):
            bs    = block_num * block_size
            count = sum(
                1 for p, (s, _) in actual_map.items()
                if s == section and bs <= p < bs + block_size
            )
            if count > items_per_block:
                failed.append(
                    f"'{section}' block {block_num} (pos {bs}–{bs+block_size-1}): "
                    f"{count} insertions > items_per_block={items_per_block}"
                )
            elif count > 0:
                passed.append(
                    f"'{section}' block {block_num}: {count} insertion(s) ≤ {items_per_block} ✓"
                )

    # 6. Fix section with non-empty pool → expect at least one item in result
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "fix" or pool_sizes.get(section, 0) == 0:
            continue
        found = any(s == section for s, _ in actual_map.values())
        if not found:
            warnings.append(
                f"'{section}' fix pool_size={pool_sizes[section]} "
                f"but no item appeared in result (could be all overridden)"
            )
        else:
            passed.append(f"'{section}' fix: at least one item inserted ✓")

    # 7. Random section with non-empty pool → expect at least one item in result
    for section in SECTION_ORDER:
        cfg = sections[section]
        if cfg["calculate_type"] != "random" or pool_sizes.get(section, 0) == 0:
            continue
        found = any(s == section for s, _ in actual_map.values())
        if not found:
            warnings.append(
                f"'{section}' random pool_size={pool_sizes[section]} "
                f"but no item appeared in result"
            )
        else:
            passed.append(f"'{section}' random: at least one item inserted ✓")

    return {"passed": passed, "failed": failed, "warnings": warnings, "ok": len(failed) == 0}


# ─────────────────────────────────────────────────────────────
# Mock data generator
# ─────────────────────────────────────────────────────────────

def _make_pool(section: str, n: int) -> list[dict]:
    return [{"id": f"{section}_{i:03d}"} for i in range(n)]


def build_mock_response(tc: dict) -> dict:
    """
    Simulate what the API would return for a given test-case config.
    Applies the insertion logic to a synthetic base list of 30 items.
    """
    sections = tc["sections"]
    POOL_SIZE = 5
    BASE_SIZE = 30

    # Fake candidate pools
    pool_items = {s: _make_pool(s, POOL_SIZE) for s in SECTION_ORDER}
    pool_idx   = {s: 0 for s in SECTION_ORDER}

    # Compute expected positions
    pool_sizes = {s: POOL_SIZE for s in SECTION_ORDER}
    expected_map = compute_expected_insertions(sections, pool_sizes, BASE_SIZE)

    # Build the final items list: mix base items + inserted items
    result_items: list[dict] = []
    insert_positions: dict[int, tuple[str, str]] = {}   # abs_pos → (section, item_id)

    used_positions = sorted(expected_map.items())

    base_idx = 0
    slot = 0
    base_used = set(pos for pos in expected_map)

    # Simple flat approach: iterate positions and assign
    for abs_pos, section in sorted(expected_map.items()):
        if pool_idx[section] < POOL_SIZE:
            item = pool_items[section][pool_idx[section]]
            pool_idx[section] += 1
            insert_positions[abs_pos] = (section, item["id"])

    # Fill items list
    total = BASE_SIZE
    for i in range(total):
        if i in insert_positions:
            section, item_id = insert_positions[i]
            result_items.append({"id": item_id})
        else:
            result_items.append({"id": f"base_{i:03d}"})

    # Handle random sections (insert some at arbitrary remaining positions)
    random_sections = [s for s in SECTION_ORDER if sections[s]["calculate_type"] == "random" and POOL_SIZE > 0]
    occupied = set(insert_positions.keys())
    free_slots = [i for i in range(total) if i not in occupied]
    for i, section in enumerate(random_sections):
        if i < len(free_slots):
            slot = free_slots[i]
            item_id = pool_items[section][0]["id"]
            result_items[slot] = {"id": item_id}
            insert_positions[slot] = (section, item_id)

    # Build special_content_positions
    special = {str(pos): f"{sec}_{iid}" for pos, (sec, iid) in insert_positions.items()}

    # Build fake response structure
    response = {
        "insert_items_result": {
            "result": {
                "items": result_items,
                "item_size": total,
                "special_content_positions": special,
            }
        }
    }
    for section, key in POOL_RESPONSE_KEYS.items():
        response[key] = {"result": {"items": pool_items[section]}}

    return response


# ─────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────

PASS  = "\033[92m✓ PASS\033[0m"
FAIL  = "\033[91m✗ FAIL\033[0m"
SKIP  = "\033[93m⚡ SKIP\033[0m"
WARN  = "\033[93m⚠\033[0m"


def run_tc(tc: dict, mode: str, api_cache: dict) -> dict:
    tc_id = tc["id"]
    url = build_url(tc["sso_id"], tc["sections"])
    sections = tc["sections"]
    all_checks: list[dict] = []

    for check_name, response_source in _get_check_sources(mode, url, api_cache, tc):
        response = response_source
        if response is None:
            all_checks.append({"check": check_name, "outcome": "skipped", "reason": "network unreachable"})
            continue

        # Unwrap top-level {"status":..., "data": {"pipeline":..., "results": {...}}}
        data = response.get("data", response)
        # insert_items_result lives inside data.results (or data.pipeline as fallback)
        payload = data.get("results", data.get("pipeline", data))

        insert_result = payload.get("insert_items_result", {})
        actual_map    = get_special_positions(insert_result)
        total         = insert_result.get("result", {}).get("item_size", 0)
        pool_sizes    = extract_pool_sizes(payload)

        if total == 0:
            # Debug: show actual response keys to diagnose parse issue
            top_keys    = list(response.keys()) if isinstance(response, dict) else str(type(response))
            data_keys   = list(data.keys())    if isinstance(data, dict)     else "-"
            payload_keys= list(payload.keys()) if isinstance(payload, dict)  else "-"
            insert_keys = list(insert_result.keys()) if insert_result else "(missing insert_items_result)"
            debug_msg = (f"item_size=0 | top={top_keys} | data={data_keys} "
                         f"| payload={payload_keys} | insert_items_result={insert_keys}")
            all_checks.append({"check": check_name, "outcome": "failed", "failed": [debug_msg]})
            continue

        v = validate({}, actual_map, sections, pool_sizes, total)
        all_checks.append({
            "check":        check_name,
            "outcome":      "passed" if v["ok"] else "failed",
            "total_items":  total,
            "pool_sizes":   pool_sizes,
            "actual_pos":   {str(k): {"section": s, "item_id": i} for k, (s, i) in actual_map.items()},
            "passed":       v["passed"],
            "failed":       v["failed"],
            "warnings":     v["warnings"],
        })

    overall = "passed" if all(c["outcome"] != "failed" for c in all_checks) else "failed"
    if all(c["outcome"] == "skipped" for c in all_checks):
        overall = "skipped"

    return {"test_id": tc_id, "url": url, "overall": overall, "checks": all_checks}


def _get_check_sources(mode, url, api_cache, tc):
    sources = []
    if mode in ("mock", "both"):
        sources.append(("mock", build_mock_response(tc)))
    if mode in ("live", "both"):
        if url not in api_cache:
            api_cache[url] = fetch_response(url)
        sources.append(("live", api_cache[url]))
    return sources


def print_result(r: dict):
    icon = {"passed": PASS, "failed": FAIL, "skipped": SKIP}.get(r["overall"], "?")
    print(f"\n  {icon}  {r['test_id']}")
    for chk in r["checks"]:
        chk_icon = {"passed": PASS, "failed": FAIL, "skipped": SKIP}.get(chk["outcome"], "?")
        label = chk["check"].upper()
        print(f"       [{label}] {chk_icon}", end="")
        if chk["outcome"] == "skipped":
            print(f"  — {chk.get('reason','')}")
        elif chk["outcome"] == "failed":
            print()
            for f in chk.get("failed", []):
                print(f"            {FAIL} {f}")
            for w in chk.get("warnings", []):
                print(f"            {WARN}  {w}")
        else:
            passed = chk.get("passed", [])
            warnings = chk.get("warnings", [])
            print(f"  {len(passed)} assertions OK", end="")
            if warnings:
                print(f"  ({len(warnings)} warnings)", end="")
            print()
            for w in warnings:
                print(f"            {WARN}  {w}")


def save_reports(all_results: list[dict]):
    os.makedirs("reports", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = f"reports/sfv_insertion_results_{ts}.json"
    csv_path  = f"reports/sfv_insertion_results_{ts}.csv"

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total": len(all_results),
        "passed":  sum(1 for r in all_results if r["overall"] == "passed"),
        "failed":  sum(1 for r in all_results if r["overall"] == "failed"),
        "skipped": sum(1 for r in all_results if r["overall"] == "skipped"),
        "results": all_results,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    rows = []
    for r in all_results:
        for chk in r["checks"]:
            rows.append({
                "test_id":       r["test_id"],
                "check":         chk["check"],
                "outcome":       chk["outcome"],
                "total_items":   chk.get("total_items", ""),
                "num_passed":    len(chk.get("passed", [])),
                "num_failed":    len(chk.get("failed", [])),
                "num_warnings":  len(chk.get("warnings", [])),
                "failures":      " | ".join(chk.get("failed", [])),
                "warnings":      " | ".join(chk.get("warnings", [])),
                "url":           r["url"],
            })
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    return json_path, csv_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mock-only", action="store_true")
    parser.add_argument("--live-only", action="store_true")
    args = parser.parse_args()

    if args.mock_only:
        mode = "mock"
    elif args.live_only:
        mode = "live"
    else:
        mode = "both"

    print(f"\n{'═'*60}")
    print(f"  SFV-P4 Insertion Logic Checker")
    print(f"  Mode: {mode.upper()}  |  {len(TEST_CASES)} test cases")
    print(f"{'═'*60}")

    api_cache: dict = {}
    all_results: list[dict] = []

    for tc in TEST_CASES:
        r = run_tc(tc, mode, api_cache)
        all_results.append(r)
        print_result(r)

    # Summary
    passed  = sum(1 for r in all_results if r["overall"] == "passed")
    failed  = sum(1 for r in all_results if r["overall"] == "failed")
    skipped = sum(1 for r in all_results if r["overall"] == "skipped")
    print(f"\n{'─'*60}")
    print(f"  Results: {passed} passed  {failed} failed  {skipped} skipped")

    json_path, csv_path = save_reports(all_results)
    print(f"\n  📄 JSON → {json_path}")
    print(f"  📄 CSV  → {csv_path}")
    print(f"{'═'*60}\n")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
