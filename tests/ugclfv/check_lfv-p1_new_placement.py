"""
check_lfv-p1_new_placement.py
──────────────────────────────
1. ยิง Universal API (cursor=1) แล้วดึง IDs จาก candidate_latest_ugc_lfv / candidate_latest_lfv
2. เอา IDs ไป query Metadata API ตรวจว่า:
     - ugc_lfv IDs มี content_type == "ugclfv"
     - lfv IDs มี content_type == "lfv"
     (ไม่พบใน metadata ถือว่า warn ไม่ fail)
3. เช็คว่า shuffle เกิดขึ้น: เปรียบเทียบ rerank_items_with_creator_diversity
   vs shuffle_items_with_creator_diversity ว่า order ต่างกัน
4. เช็ค insert_pin_candidates.reservedPositions vs slice_pin_globals IDs
5. วน cursor 1..MAX_CURSOR เก็บ merge_page IDs ทุก cursor แล้วเช็ค duplicate
6. เช็คว่า 5 IDs แรกของ fresh_data_limit_for_set_seen_items cursor N
   ปรากฏใน get_seen_item_redis (ยิงซ้ำ cursor N)
7. nologin ต้องไม่เก็บ seen item ใน Redis
8. เช็คว่า append_bucketizes_with_ratio มี per_source_count ตรงกับ ratio ที่กำหนด
   (node_latest_ugc_lfv_ratio)

รัน: python3 check_lfv-p1_new_placement.py
"""

import os
import sys
import json
import random
from datetime import datetime
from collections import defaultdict
from typing import Optional
import requests

# ─── Config ───────────────────────────────────────────────────────────────────

EXPECTED_UGC_LFV_RATIO  = 7   # Step 8: ratio ที่จะส่งเฉพาะตอนทดสอบ ratio เท่านั้น
_UGC_LFV_RATIO_PARAMS   = f"&node_latest_ugc_lfv_ratio={EXPECTED_UGC_LFV_RATIO}"

_BASE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/lfv-p1"
    "?verbose=debug"
    "&limit=7"
)

PROFILES = [
    {
        "name":        "lfv-p1",
        "base_url":    f"{_BASE_URL}&ssoId=1709302903",
        "nologin_url": f"{_BASE_URL}&ssoId=nologin",
    },
]

METADATA_URL = (
    "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

MAX_CURSOR = 5            # จำนวน cursor ที่จะเช็ค
SEEN_ITEM_INCREMENT = 5  # จำนวน IDs แรกจาก fresh_data_limit ที่ตรวจ seen per cursor

# Candidate node names
_CANDIDATE_UGC_LFV_NODE = "candidate_latest_ugc_lfv"
_CANDIDATE_LFV_NODE     = "candidate_latest_lfv"

# ─── Evidence setup ───────────────────────────────────────────────────────────

_RUN_ID       = datetime.now().strftime("%Y%m%d_%H%M%S")
_EVIDENCE_DIR = f"evidence_{_RUN_ID}"
os.makedirs(_EVIDENCE_DIR, exist_ok=True)

_current_profile: str = ""
_last_called_url: str = ""
_fail_curls:    dict[str, str] = {}   # step_key → curl string
_fail_details:  dict[str, str] = {}   # step_key → detail string (duplicate list ฯลฯ)


def _to_curl(url: str) -> str:
    """แปลง URL เป็น curl command สำหรับ debug"""
    return f'curl -s \\\n  "{url}"'


def _record_fail_curl(step_key: str, url: str = "") -> None:
    """เก็บ curl ของ step ที่ fail ไว้ใน _fail_curls (โชว์ใน assert message) และ print สำหรับ standalone"""
    target = url or _last_called_url
    if target:
        cmd = _to_curl(target)
        _fail_curls[step_key] = cmd
        print(f"\n🔧  curl for debug:\n    {cmd}\n")


def _save_evidence(filename: str, method: str, url: str,
                   res: requests.Response, payload: Optional[dict] = None) -> None:
    path = os.path.join(_EVIDENCE_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"Timestamp : {datetime.now().isoformat()}\n")
        f.write(f"Profile   : {_current_profile}\n")
        f.write(f"Method    : {method}\n")
        f.write(f"URL       : {url}\n")
        if payload:
            f.write("\n" + "─" * 60 + "\n")
            f.write("PAYLOAD\n")
            f.write("─" * 60 + "\n")
            f.write(json.dumps(payload, ensure_ascii=False, indent=2))
            f.write("\n")
        f.write("\n" + "─" * 60 + "\n")
        f.write(f"STATUS  {res.status_code} {res.reason}\n")
        f.write("─" * 60 + "\n")
        try:
            body = res.json()
            f.write(json.dumps(body, ensure_ascii=False, indent=2))
        except Exception:
            f.write(res.text)
        f.write("\n")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def sep(title, width=60):
    print("\n" + "═" * width)
    print(f" {title}")
    print("═" * width)

def banner(title, width=60):
    print("\n" + "█" * width)
    print(f"  {title}")
    print("█" * width)

def call_universal(base_url: str, cursor: int, step: str = "misc") -> dict:
    global _last_called_url
    url = f"{base_url}&cursor={cursor}"
    _last_called_url = url
    res = requests.get(url)
    fname = f"{_current_profile}_{step}_c{cursor}.log"
    _save_evidence(fname, "GET", url, res)
    print(f"    💾  evidence → {_EVIDENCE_DIR}/{fname}")
    if not res.ok:
        _record_fail_curl("http_error", url)
    res.raise_for_status()
    return res.json()

# ─── Parsers ──────────────────────────────────────────────────────────────────

def _parse_candidate_node_ids(body: dict, node_name: str) -> list[str]:
    """
    Parse IDs from a candidate node.
    Tries simple items list first, then falls back to ES aggregation pattern.
    Also handles {ids: [...]} structure.
    """
    result = (
        body.get("data", {}).get("results", {})
        .get(node_name, {}).get("result", {})
    )

    # Pattern 1: simple {items: [{id: ...}]} list
    items = result.get("items", [])
    if items:
        return [item["id"] for item in items if "id" in item]

    # Pattern 2: simple {ids: [...]} list
    ids_list = result.get("ids", [])
    if ids_list:
        return ids_list

    # Pattern 3: ES aggregation — data.aggregations.agg_latest.buckets
    data    = result.get("data", {})
    buckets = data.get("aggregations", {}).get("agg_latest", {}).get("buckets", [])
    ids: list[str] = []
    for bucket in buckets:
        # sub-level aggregation: sort_by_publish_date
        for h in bucket.get("sort_by_publish_date", {}).get("hits", {}).get("hits", []):
            item_id = h.get("_source", {}).get("id")
            if item_id:
                ids.append(item_id)

    if ids:
        return ids

    # Pattern 4: top-level hits.hits (some ES responses omit agg wrapper)
    for h in data.get("hits", {}).get("hits", []):
        item_id = h.get("_source", {}).get("id")
        if item_id:
            ids.append(item_id)

    return ids


def _parse_node_items(body: dict, node_name: str) -> list[str]:
    """Generic parser for nodes that return result.items[].id (e.g. rerank/shuffle)."""
    items = (
        body.get("data", {}).get("results", {})
        .get(node_name, {}).get("result", {}).get("items", [])
    )
    return [item["id"] for item in items if "id" in item]


def _parse_merge_ids(body: dict) -> list[str]:
    merge_items = (
        body.get("data", {}).get("results", {})
        .get("merge_page", {}).get("result", {}).get("items", [])
    )
    return [item["id"] for item in merge_items if "id" in item]


def _parse_slice_pagination_ids(body: dict) -> list[str]:
    """Parse IDs from slice_pagination node (IDs ที่ถูก serve ใน cursor นี้)."""
    items = (
        body.get("data", {}).get("results", {})
        .get("slice_pagination", {}).get("result", {}).get("items", [])
    )
    return [item["id"] for item in items if "id" in item]


def _parse_merge_seen_ids(body: dict) -> list[str]:
    """Parse IDs from merge_seen_and_prev_page_items node (seen pool ของ cursor นี้)."""
    items = (
        body.get("data", {}).get("results", {})
        .get("merge_seen_and_prev_page_items", {}).get("result", {}).get("items", [])
    )
    return [item["id"] for item in items if "id" in item]


def _parse_seen_item_ids(body: dict) -> list[str]:
    return (
        body.get("data", {}).get("results", {})
        .get("get_seen_item_redis", {}).get("result", {}).get("ids", [])
    )


def _parse_append_bucketizes_per_source_count(body: dict) -> dict[str, int]:
    """Return per_source_count dict from append_bucketizes_with_ratio node result."""
    result = (
        body.get("data", {}).get("results", {})
        .get("append_bucketizes_with_ratio", {}).get("result", {})
    )
    return result.get("per_source_count", {})


# ─── Step 1: Call Universal API cursor=1 ──────────────────────────────────────

def get_lfv_candidate_ids(base_url: str) -> tuple[dict[str, list[str]], dict]:
    """
    Returns:
        node_ids: { "candidate_latest_ugc_lfv": [...], "candidate_latest_lfv": [...] }
        body:     raw response body
    """
    sep("STEP 1 — Universal API (cursor=1)")
    print(f"URL: {base_url}&cursor=1\n")

    body     = call_universal(base_url, 1, step="step1")
    results_obj = body.get("data", {}).get("results", {})

    node_ids: dict[str, list[str]] = {}
    for node in [_CANDIDATE_UGC_LFV_NODE, _CANDIDATE_LFV_NODE]:
        ids = _parse_candidate_node_ids(body, node)
        node_ids[node] = ids
        if ids:
            print(f"  [{node}]  พบ {len(ids)} IDs:")
            for i in ids:
                print(f"    - {i}")
        else:
            present = node in results_obj
            print(f"  [{node}]  ⚠️  ไม่พบ IDs  (node {'มีอยู่' if present else 'ไม่มีใน results'})")

    total = sum(len(v) for v in node_ids.values())
    print(f"\nรวมทั้งหมด : {total} IDs")
    return node_ids, body


# ─── Step 2: Query Metadata API ───────────────────────────────────────────────

def check_content_type(node_ids: dict[str, list[str]]) -> bool:
    """
    Verify content_type for each candidate node:
      - candidate_latest_ugc_lfv → content_type must be "ugclfv"
      - candidate_latest_lfv     → content_type must be "lfv"
    Missing data in metadata → warn only (not fail).
    """
    sep("STEP 2 — Metadata API content_type check")
    print(f"URL: {METADATA_URL}\n")

    # Define expected content_type per node
    node_expected: dict[str, str] = {
        _CANDIDATE_UGC_LFV_NODE: "ugclfv",
        _CANDIDATE_LFV_NODE:     "lfv",
    }

    all_ids: list[str] = []
    id_to_expected: dict[str, str] = {}
    for node, ids in node_ids.items():
        expected_ct = node_expected.get(node, "unknown")
        for i in ids:
            if i not in id_to_expected:
                all_ids.append(i)
                id_to_expected[i] = expected_ct

    if not all_ids:
        print("⚠️  ไม่มี ID ให้ตรวจ — ข้าม Step 2")
        return True

    payload = {
        "parameters": {"id": all_ids, "fields": ["id", "content_type"]},
        "options": {"cache": False},
    }
    res = requests.post(METADATA_URL, json=payload, headers={"Content-Type": "application/json"})
    fname = f"{_current_profile}_step2_metadata.log"
    _save_evidence(fname, "POST", METADATA_URL, res, payload)
    print(f"    💾  evidence → {_EVIDENCE_DIR}/{fname}")
    res.raise_for_status()
    body = res.json()

    items     = body.get("items", [])
    found_map = {item["id"]: item.get("content_type") for item in items if item.get("id")}
    not_found = [i for i in all_ids if i not in found_map]

    all_pass = True

    for node, ids in node_ids.items():
        expected_ct = node_expected.get(node, "unknown")
        if not ids:
            # Empty candidate pool → warn only, not fail
            print(f"\n  [{node}]  ⚠️  ไม่มี IDs ใน candidate pool — ข้าม (warn only)")
            continue
        print(f"\n  [{node}] — expected content_type = \"{expected_ct}\"")
        correct  = []
        wrong    = []
        missing  = []
        for i in ids:
            if i not in found_map:
                missing.append(i)
            elif found_map[i] == expected_ct:
                correct.append(i)
            else:
                wrong.append((i, found_map[i]))

        for i in correct:
            print(f"    ✅  {i}  →  {expected_ct}")
        for i, ct in wrong:
            print(f"    ❌  {i}  →  {ct}  (ไม่ตรง)")
            all_pass = False
        for i in missing:
            print(f"    ❌  {i}  →  ไม่พบใน metadata")
            all_pass = False

    sep("SUMMARY content_type")
    print(f"Total IDs ที่ตรวจ : {len(all_ids)}")
    print(f"ไม่พบใน metadata  : {len(not_found)} (❌ fail)")
    print(f"\nResult : {'✅ PASS' if all_pass else '❌ FAIL — มี ID ที่ content_type ไม่ตรง หรือ ไม่พบใน metadata'}")
    if not all_pass:
        _record_fail_curl("content_type")
    return all_pass


# ─── Step 3: Check shuffle occurred ────────────────────────────────────────────

def check_shuffle_occurred(body: dict) -> bool:
    """
    Verify that creator diversity shuffle changed the item order.
    Compare ordered IDs of:
      rerank_items_with_creator_diversity  vs
      shuffle_items_with_creator_diversity
    If ANY position differs → shuffle occurred ✅
    """
    sep("STEP 3 — Shuffle verification (rerank vs shuffle)")

    rerank_ids  = _parse_node_items(body, "rerank_items_with_creator_diversity")
    shuffle_ids = _parse_node_items(body, "shuffle_items_with_creator_diversity")

    print(f"rerank_items_with_creator_diversity  : {len(rerank_ids)} items")
    print(f"shuffle_items_with_creator_diversity : {len(shuffle_ids)} items")

    if not rerank_ids:
        print("\n⚠️  ไม่พบ items ใน rerank_items_with_creator_diversity — ข้าม")
        return False
    if not shuffle_ids:
        print("\n⚠️  ไม่พบ items ใน shuffle_items_with_creator_diversity — ข้าม")
        return False

    # Compare order
    min_len  = min(len(rerank_ids), len(shuffle_ids))
    diffs    = [(pos, rerank_ids[pos], shuffle_ids[pos])
                for pos in range(min_len) if rerank_ids[pos] != shuffle_ids[pos]]

    print(f"\nComparing first {min_len} positions...")
    if diffs:
        print(f"✅  พบความแตกต่าง {len(diffs)} ตำแหน่ง — shuffle เกิดขึ้นแล้ว")
        for pos, r_id, s_id in diffs[:5]:
            print(f"    pos {pos:>3}:  rerank={r_id}  →  shuffle={s_id}")
        if len(diffs) > 5:
            print(f"    ... (และอีก {len(diffs) - 5} ตำแหน่ง)")
        result = True
    else:
        if len(rerank_ids) != len(shuffle_ids):
            print(f"✅  ขนาด list ต่างกัน ({len(rerank_ids)} vs {len(shuffle_ids)}) — shuffle เกิดขึ้นแล้ว")
            result = True
        else:
            print("❌  IDs ทุกตำแหน่งเหมือนกันทุกประการ — shuffle ไม่เกิดขึ้น")
            result = False

    sep("SUMMARY shuffle")
    print(f"Result : {'✅ PASS — shuffle เกิดขึ้น' if result else '❌ FAIL — shuffle ไม่เกิดขึ้น'}")
    if not result:
        _record_fail_curl("shuffle")
    return result


# ─── Step 4: Check pin reservedPositions vs slice_pin_globals ─────────────────

def check_pin_reserved_positions(body: dict) -> bool:
    sep("STEP 4 — Pin reservedPositions vs slice_pin_globals")

    results = body.get("data", {}).get("results", {})

    slice_ids    = results.get("slice_pin_globals", {}).get("result", {}).get("ids", [])
    slice_id_set = set(slice_ids)
    print(f"slice_pin_globals IDs ({len(slice_ids)}): {slice_ids}")

    reserved = results.get("insert_pin_candidates", {}).get("result", {}).get("reservedPositions", {})
    print(f"\ninsert_pin_candidates reservedPositions ({len(reserved)}):")
    reserved_ids = []
    for pos, val in sorted(reserved.items(), key=lambda x: int(x[0])):
        item_id = val.removeprefix("pin_")
        reserved_ids.append(item_id)
        print(f"    position {pos:>3} → {val}  (id: {item_id})")

    ok      = [i for i in reserved_ids if i in slice_id_set]
    not_ok  = [i for i in reserved_ids if i not in slice_id_set]
    missing = [i for i in slice_ids if i not in set(reserved_ids)]

    print()
    if not_ok:
        print(f"❌  reserved IDs ที่ไม่อยู่ใน slice_pin_globals ({len(not_ok)}):")
        for i in not_ok:
            print(f"    ✗  {i}")
    else:
        print("✅  reserved IDs ทุกตัวมาจาก slice_pin_globals")

    if missing:
        print(f"\nℹ️   slice_pin_globals IDs ที่ยังไม่ถูก reserve ({len(missing)}):")
        for i in missing:
            print(f"    -  {i}")

    sep("SUMMARY pin")
    print(f"slice_pin_globals IDs    : {len(slice_ids)}")
    print(f"reservedPositions IDs    : {len(reserved_ids)}")
    print(f"ตรงกับ slice_pin_globals : {len(ok)}")
    print(f"ไม่ตรง                   : {len(not_ok)}")
    all_pass = len(not_ok) == 0
    print(f"\nResult : {'✅ PASS' if all_pass else '❌ FAIL'}")
    if not all_pass:
        _record_fail_curl("pin")
    return all_pass


# ─── Fetch all cursor bodies ───────────────────────────────────────────────────

def fetch_all_bodies(base_url: str) -> dict[int, dict]:
    sep(f"Fetching cursor 1..{MAX_CURSOR}")
    bodies: dict[int, dict] = {}
    for cursor in range(1, MAX_CURSOR + 1):
        print(f"  ยิง cursor={cursor} ...", end=" ", flush=True)
        try:
            bodies[cursor] = call_universal(base_url, cursor, step="step5")
            print("OK")
        except Exception as e:
            print(f"ERROR: {e}")
    return bodies


# ─── Step 5: Duplicate check across cursors ───────────────────────────────────

def check_duplicates_across_cursors(bodies: dict[int, dict]) -> bool:
    sep(f"STEP 5 — Duplicate check (cursor 1..{MAX_CURSOR})")

    # ── 5a: cross-cursor duplicate ─────────────────────────────────────────────
    id_to_cursors: dict[str, list[int]] = defaultdict(list)
    for cursor, body in bodies.items():
        slice_ids = _parse_slice_pagination_ids(body)
        print(f"  cursor={cursor}  slice_pagination IDs: {len(slice_ids)}")
        for i in slice_ids:
            id_to_cursors[i].append(cursor)

    cross_dups = {i: cs for i, cs in id_to_cursors.items() if len(cs) > 1}

    # ── 5b: slice_pagination ∩ merge_seen_and_prev_page_items (same cursor) ────
    print()
    same_cursor_overlaps: list[str] = []
    for cursor, body in bodies.items():
        slice_ids = set(_parse_slice_pagination_ids(body))
        seen_ids  = set(_parse_merge_seen_ids(body))
        overlap   = slice_ids & seen_ids
        if overlap:
            msg = f"  cursor={cursor}  ❌  slice_pagination ∩ merge_seen ({len(overlap)} IDs): {sorted(overlap)}"
            print(msg)
            same_cursor_overlaps.append(msg)
        else:
            print(f"  cursor={cursor}  ✅  slice_pagination ไม่ซ้ำกับ merge_seen_and_prev_page_items")

    # ── SUMMARY ────────────────────────────────────────────────────────────────
    sep("SUMMARY duplicate")
    print(f"Unique IDs รวมทุก cursor      : {len(id_to_cursors)}")
    print(f"Cross-cursor duplicate IDs    : {len(cross_dups)}")
    print(f"Same-cursor overlap cursors   : {len(same_cursor_overlaps)}")

    all_pass = True
    detail_lines: list[str] = []

    if cross_dups:
        print(f"\n❌  Cross-cursor duplicate ({len(cross_dups)} IDs):")
        for i, cs in sorted(cross_dups.items(), key=lambda x: x[1]):
            line = f"    {i}  →  cursor {cs}"
            print(line)
            detail_lines.append(line)
        all_pass = False

    if same_cursor_overlaps:
        print(f"\n❌  Same-cursor overlap (merge_page ∩ merge_seen):")
        for msg in same_cursor_overlaps:
            detail_lines.append(msg)
        all_pass = False

    if all_pass:
        print("\n✅  ไม่พบ duplicate ในทุก cursor")
    else:
        _fail_details["dup"] = "\n".join(detail_lines)
        _record_fail_curl("dup")

    return all_pass


# ─── Step 6: seen items accumulation check ────────────────────────────────────

def check_seen_items_across_cursors(base_url: str, bodies: dict[int, dict]) -> bool:
    sep(f"STEP 6 — seen items accumulation (cursor N → N+1)")
    print(
        f"logic: {SEEN_ITEM_INCREMENT} IDs แรกของ slice_pagination[cursor N]\n"
        f"       ต้องปรากฏใน merge_seen_and_prev_page_items[cursor N+1]\n"
    )

    all_pass = True
    for cursor in range(1, MAX_CURSOR):   # cursor 1..MAX_CURSOR-1
        next_cursor = cursor + 1
        if cursor not in bodies:
            print(f"  ⚠️  ไม่มีข้อมูล cursor={cursor} — ข้าม")
            continue
        if next_cursor not in bodies:
            print(f"  ⚠️  ไม่มีข้อมูล cursor={next_cursor} — ข้าม")
            continue

        slice_ids = _parse_slice_pagination_ids(bodies[cursor])
        expected  = slice_ids[:SEEN_ITEM_INCREMENT]

        if not expected:
            print(f"  cursor={cursor}  ⚠️  ไม่พบ IDs ใน slice_pagination — ข้าม")
            all_pass = False
            continue

        merge_seen_ids = _parse_merge_seen_ids(bodies[next_cursor])
        merge_seen_set = set(merge_seen_ids)

        print(f"  cursor={cursor} → cursor={next_cursor}")
        print(f"    slice_pagination first {SEEN_ITEM_INCREMENT}          : {expected}")
        print(f"    merge_seen_and_prev_page_items ({len(merge_seen_ids)}) : {merge_seen_ids}")

        missing = [i for i in expected if i not in merge_seen_set]
        if missing:
            print(f"    ❌  ไม่พบใน merge_seen_and_prev_page_items ({len(missing)} IDs): {missing}")
            detail = (
                f"cursor {cursor}→{next_cursor}\n"
                f"    slice_pagination first {SEEN_ITEM_INCREMENT}          : {expected}\n"
                f"    merge_seen_and_prev_page_items ({len(merge_seen_ids)}) : {merge_seen_ids}\n"
                f"    missing                                               : {missing}"
            )
            _fail_details["seen"] = _fail_details.get("seen", "") + "\n" + detail
            _record_fail_curl("seen")
            all_pass = False
        else:
            print(f"    ✅  ครบทุก ID ({len(expected)} IDs)")

    sep("SUMMARY seen items")
    print(f"Result : {'✅ PASS' if all_pass else '❌ FAIL'}")
    return all_pass


# ─── Step 7: nologin must not accumulate seen items ───────────────────────────

def check_nologin_seen_item(nologin_url: str) -> bool:
    sep("STEP 7 — nologin ต้องไม่เก็บ seen item ใน Redis")
    print("logic: ยิง cursor 1..MAX_CURSOR ด้วย ssoId=nologin แล้วเช็คว่า get_seen_item_redis ต้องไม่มี IDs\n")

    all_pass = True
    for cursor in range(1, MAX_CURSOR + 1):
        print(f"  cursor={cursor} ...", end=" ", flush=True)
        try:
            body     = call_universal(nologin_url, cursor, step="step7_nologin")
            seen_ids = _parse_seen_item_ids(body)
            print("OK")
        except Exception as e:
            print(f"ERROR: {e}")
            all_pass = False
            continue

        if seen_ids:
            print(f"    ❌  พบ IDs ใน get_seen_item_redis ({len(seen_ids)} IDs) — ไม่ควรเก็บ seen สำหรับ nologin:")
            for i in seen_ids:
                print(f"        - {i}")
            _record_fail_curl("seen_nologin")
            all_pass = False
        else:
            print(f"    ✅  get_seen_item_redis ว่างเปล่า — ถูกต้อง")

    sep("SUMMARY nologin seen item")
    print(f"Result : {'✅ PASS — nologin ไม่เก็บ seen item' if all_pass else '❌ FAIL — nologin มีการเก็บ seen item'}")
    return all_pass


# ─── Step 8: append_bucketizes_with_ratio per_source_count ───────────────────

def check_ugc_lfv_ratio(base_url: str, expected_ratio: int = EXPECTED_UGC_LFV_RATIO) -> bool:
    sep(f"STEP 8 — append_bucketizes_with_ratio per_source_count (expected={expected_ratio})")
    random_sso = str(random.randint(1_000_000, 99_999_999))
    ratio_url  = f"{_BASE_URL}&ssoId={random_sso}{_UGC_LFV_RATIO_PARAMS}"
    print(
        f"logic: ยิง API พร้อม node_latest_ugc_lfv_ratio={expected_ratio}\n"
        f"       ssoId={random_sso} (random ทุกครั้งที่รัน)\n"
        f"       เช็ค append_bucketizes_with_ratio.per_source_count ว่า\n"
        f"       merge_bucketize_latest_ugc_lfv ได้ {expected_ratio} items\n"
    )
    print(f"URL: {ratio_url}&cursor=1\n")

    try:
        body = call_universal(ratio_url, 1, step="step8_ratio")
    except Exception as e:
        print(f"❌  ยิง API ไม่สำเร็จ: {e}")
        _record_fail_curl("ugc_lfv_ratio")
        return False

    per_source = _parse_append_bucketizes_per_source_count(body)

    if not per_source:
        print("⚠️  ไม่พบ per_source_count ใน append_bucketizes_with_ratio — node อาจไม่มีอยู่ใน results")
        _record_fail_curl("ugc_lfv_ratio")
        return False

    expected_keys = {"merge_bucketize_latest_ugc_lfv"}
    all_pass = True

    print("per_source_count ที่ได้:")
    for source, count in per_source.items():
        if source in expected_keys:
            ok     = count == expected_ratio
            status = "✅" if ok else "❌"
            print(f"    {status}  {source} = {count}  (expected {expected_ratio})")
            if not ok:
                all_pass = False
        else:
            print(f"    ℹ️   {source} = {count}  (extra source)")

    # เช็คว่ามี key ที่คาดหวังครบ
    missing_keys = expected_keys - set(per_source.keys())
    if missing_keys:
        for key in missing_keys:
            print(f"    ❌  ไม่พบ source '{key}' ใน per_source_count")
        all_pass = False

    sep("SUMMARY ugc_lfv_ratio")
    print(f"Expected ratio : {expected_ratio}")
    for source in sorted(expected_keys):
        count = per_source.get(source, "N/A")
        ok    = count == expected_ratio
        print(f"  {'✅' if ok else '❌'}  {source} = {count}")
    print(f"\nResult : {'✅ PASS — pool ได้ตาม ratio ที่กำหนด' if all_pass else '❌ FAIL — pool ไม่ตรงตาม ratio'}")
    if not all_pass:
        _record_fail_curl("ugc_lfv_ratio")
    return all_pass


# ─── Run all steps for one profile ────────────────────────────────────────────

def run_profile(profile: dict) -> dict[str, bool]:
    global _current_profile
    name     = profile["name"]
    base_url = profile["base_url"]
    _current_profile = name

    banner(f"PROFILE: {name}")

    results: dict[str, bool] = {}

    try:
        node_ids, body_1 = get_lfv_candidate_ids(base_url)

        # Step 2 — content_type check
        has_ids = any(v for v in node_ids.values())
        if has_ids:
            results["content_type"] = check_content_type(node_ids)
        else:
            print("\n[SKIP] ไม่มี ID จาก candidate nodes — ข้าม Step 2")
            results["content_type"] = False

        # Step 3 — shuffle check
        results["shuffle"] = check_shuffle_occurred(body_1)

        # Step 4 — pin check
        results["pin"] = check_pin_reserved_positions(body_1)

        # Step 5 — duplicate check across cursors
        bodies             = fetch_all_bodies(base_url)
        results["dup"]     = check_duplicates_across_cursors(bodies)

        # Step 6 — seen items accumulation
        results["seen"]    = check_seen_items_across_cursors(base_url, bodies)

        # Step 7 — nologin
        results["seen_nologin"] = check_nologin_seen_item(profile["nologin_url"])

    except requests.HTTPError as e:
        print(f"\n[ERROR] HTTP {e.response.status_code}: {e.response.text}")
        for key in ["content_type", "shuffle", "pin", "dup", "seen", "seen_nologin"]:
            results.setdefault(key, False)
    except Exception as e:
        print(f"\n[ERROR] {e}")

    return results


# ─── Pytest Test Class ────────────────────────────────────────────────────────

class TestLfvP1NewPlacement:
    """
    pytest รัน: pytest check_lfv-p1_new_placement.py -v
    standalone รัน: python3 check_lfv-p1_new_placement.py
    """
    _results:      Optional[dict] = None
    _fail_curls:   dict[str, str]         = {}
    _fail_details: dict[str, str]         = {}

    @classmethod
    def setup_class(cls):
        global _current_profile, _fail_curls, _fail_details
        _fail_curls   = {}
        _fail_details = {}
        profile = PROFILES[0]
        _current_profile = profile["name"]
        cls._results      = run_profile(profile)
        cls._fail_curls   = dict(_fail_curls)
        cls._fail_details = dict(_fail_details)

    def _curl(self, key: str) -> str:
        c = self._fail_curls.get(key, "")
        return f"\n\n🔧 curl:\n{c}" if c else ""

    def _detail(self, key: str) -> str:
        d = self._fail_details.get(key, "")
        return f"\n{d}" if d else ""

    def test_content_type(self):
        """[lfv-p1] ugc_lfv IDs ต้องมี content_type == ugclfv, lfv IDs ต้องมี content_type == lfv"""
        assert self._results["content_type"], \
            f"บาง ID มี content_type ไม่ตรง หรือ ไม่พบใน metadata{self._curl('content_type')}"

    def test_shuffle_occurred(self):
        """[lfv-p1] shuffle_items_with_creator_diversity ต้องมี order ต่างจาก rerank_items_with_creator_diversity"""
        assert self._results["shuffle"], \
            f"shuffle ไม่เกิดขึ้น — IDs ทุกตำแหน่งยังเหมือนเดิม{self._curl('shuffle')}"

    def test_pin_reserved_positions_match_slice_globals(self):
        """[lfv-p1] reservedPositions ของ insert_pin_candidates ต้องมาจาก slice_pin_globals ทั้งหมด"""
        assert self._results["pin"], \
            f"พบ reserved ID ที่ไม่อยู่ใน slice_pin_globals{self._curl('pin')}"

    def test_no_duplicate_ids_across_cursors(self):
        """[lfv-p1] ไม่มี duplicate ID ใน merge_page ข้ามทุก cursor"""
        assert self._results["dup"], \
            f"พบ duplicate ID ใน merge_page ระหว่าง cursor{self._detail('dup')}{self._curl('dup')}"

    def test_seen_items_accumulate_correctly(self):
        """[lfv-p1] 5 IDs แรกของ slice_pagination[cursor N] ต้องปรากฏใน merge_seen_and_prev_page_items[cursor N+1]"""
        assert self._results["seen"], \
            f"get_seen_item_redis ไม่ครบตาม merge_seen_and_prev_page_items{self._detail('seen')}{self._curl('seen')}"

    def test_nologin_does_not_store_seen_items(self):
        """[lfv-p1] ssoId=nologin ต้องไม่เก็บ seen item ใน Redis"""
        assert self._results["seen_nologin"], \
            f"nologin มีการเก็บ seen item ใน get_seen_item_redis{self._curl('seen_nologin')}"



# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n📁  Evidence directory → {_EVIDENCE_DIR}/\n")
    all_profile_results: dict[str, dict[str, bool]] = {}

    for profile in PROFILES:
        all_profile_results[profile["name"]] = run_profile(profile)

    # ─ Final summary across all profiles
    banner("FINAL SUMMARY", width=60)
    labels = {
        "content_type":    "Step 2 content_type        ",
        "shuffle":         "Step 3 shuffle             ",
        "pin":             "Step 4 pin                 ",
        "dup":             "Step 5 duplicate           ",
        "seen":            "Step 6 seen items          ",
        "seen_nologin":    "Step 7 nologin seen item   ",
        "ugc_lfv_ratio":   f"Step 8 ugc_lfv_ratio       (expected={EXPECTED_UGC_LFV_RATIO})",
    }
    overall_pass = True
    for profile_name, results in all_profile_results.items():
        print(f"\n  [{profile_name}]")
        for key, label in labels.items():
            ok = results.get(key, False)
            print(f"    {label} : {'✅ PASS' if ok else '❌ FAIL'}")
            if not ok:
                overall_pass = False

    print(f"\n{'═' * 60}")
    print(f"  Overall : {'✅ ALL PASS' if overall_pass else '❌ SOME TESTS FAILED'}")
    print(f"{'═' * 60}\n")

    sys.exit(0 if overall_pass else 1)
