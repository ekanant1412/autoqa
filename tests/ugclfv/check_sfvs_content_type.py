"""
check_sfvs_content_type.py
──────────────────────────
1. ยิง Universal API (cursor=1) แล้วดึง IDs จาก candidate_latest_sfv_series / candidate_tophit_sfv_series
2. เอา IDs ไป query Metadata API ตรวจว่าทุก ID มี content_type == "sfvseries"
3. เช็คว่า sfvseries IDs ปรากฏใน merge_page cursor=1 ไหม
4. เช็ค reservedPositions ของ insert_pin_candidates vs slice_pin_globals
5. วน cursor 1..MAX_CURSOR เก็บ merge_page IDs ทุก cursor แล้วเช็ค duplicate
6. เช็คว่า 5 IDs แรกของ logic_filter_overlap_items_pin_and_live cursor N ปรากฏใน get_seen_item_redis (ยิงซ้ำ cursor N)
7. nologin ต้องไม่เก็บ seen item ใน Redis
8. เช็คว่า append_bucketizes_with_ratio มี per_source_count ตรงกับ ratio ที่กำหนด
   (node_tophit_sfv_series_ratio / node_latest_sfv_series_ratio)

รัน: python3 check_sfvs_content_type.py
"""

import os
import sys
import json
from datetime import datetime
from collections import defaultdict
import requests

# ─── Config ───────────────────────────────────────────────────────────────────

_BASE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfvs-p1"
    "?verbose=debug"
    "&shelfId=zmEXe3EQnXDk"
)

PROFILES = [
    {
        "name":        "sfvs-p1",
        "base_url":    f"{_BASE_URL}&ssoId=13923222",
        "nologin_url": f"{_BASE_URL}&ssoId=nologin",
    },
]

METADATA_URL = (
    "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

MAX_CURSOR = 5            # จำนวน cursor ที่จะเช็ค
SEEN_ITEM_INCREMENT = 5  # จำนวน id ที่ logic_filter ส่งเข้า seen per cursor

# Step 8: ratio ที่ตั้งค่าไว้สำหรับ node_tophit_sfv_series_ratio / node_latest_sfv_series_ratio
EXPECTED_SFV_SERIES_RATIO = 7
_SFV_RATIO_PARAMS = (
    f"&node_tophit_sfv_series_ratio={EXPECTED_SFV_SERIES_RATIO}"
    f"&node_latest_sfv_series_ratio={EXPECTED_SFV_SERIES_RATIO}"
)

# ─── Evidence setup ───────────────────────────────────────────────────────────

_RUN_ID       = datetime.now().strftime("%Y%m%d_%H%M%S")
_EVIDENCE_DIR = f"evidence_{_RUN_ID}"
os.makedirs(_EVIDENCE_DIR, exist_ok=True)

_current_profile: str = ""


def _save_evidence(filename: str, method: str, url: str,
                   res: requests.Response, payload: dict | None = None) -> None:
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
    url = f"{base_url}&cursor={cursor}"
    res = requests.get(url)
    fname = f"{_current_profile}_{step}_c{cursor}.log"
    _save_evidence(fname, "GET", url, res)
    print(f"    💾  evidence → {_EVIDENCE_DIR}/{fname}")
    res.raise_for_status()
    return res.json()

# ─── Parsers ──────────────────────────────────────────────────────────────────

_SFVSERIES_CANDIDATE_NODES = [
    "candidate_latest_sfv_series",
    "candidate_tophit_sfv_series",
]


def _parse_candidate_node_ids(body: dict, node_name: str) -> list[str]:
    result = (
        body.get("data", {}).get("results", {})
        .get(node_name, {}).get("result", {})
    )
    # Try items list (simpler structure)
    items = result.get("items", [])
    if items:
        return [item["id"] for item in items if "id" in item]
    # Fallback: aggregation pattern
    data    = result.get("data", {})
    buckets = data.get("aggregations", {}).get("agg_latest", {}).get("buckets", [])
    ids: list[str] = []
    for bucket in buckets:
        for h in bucket.get("sort_by_publish_date", {}).get("hits", {}).get("hits", []):
            item_id = h.get("_source", {}).get("id")
            if item_id:
                ids.append(item_id)
    return ids


def _parse_sfvseries_ids(body: dict) -> dict[str, list[str]]:
    """Return {node_name: [id, ...]} for each sfvseries candidate node."""
    return {
        node: _parse_candidate_node_ids(body, node)
        for node in _SFVSERIES_CANDIDATE_NODES
    }

def _parse_merge_ids(body: dict) -> list[str]:
    merge_items = (
        body.get("data", {}).get("results", {})
        .get("merge_page", {}).get("result", {}).get("items", [])
    )
    return [item["id"] for item in merge_items if "id" in item]

def _parse_logic_filter_ids(body: dict) -> list[str]:
    items = (
        body.get("data", {}).get("results", {})
        .get("logic_filter_overlap_items_pin_and_live", {})
        .get("result", {}).get("items", [])
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

def get_sfvseries_ids(base_url: str) -> tuple[list[str], dict]:
    sep("STEP 1 — Universal API (cursor=1)")
    print(f"URL: {base_url}&cursor=1\n")

    body        = call_universal(base_url, 1, step="step1")
    node_ids    = _parse_sfvseries_ids(body)
    results_obj = body.get("data", {}).get("results", {})

    all_ids: list[str] = []
    seen: set[str]     = set()

    for node, ids in node_ids.items():
        if ids:
            print(f"  [{node}]  พบ {len(ids)} IDs:")
            for i in ids:
                print(f"    - {i}")
                if i not in seen:
                    seen.add(i)
                    all_ids.append(i)
        else:
            present = node in results_obj
            print(f"  [{node}]  ⚠️  ไม่พบ IDs  (node {'มีอยู่' if present else 'ไม่มีใน results'})")

    if not all_ids:
        print("\ndata.results keys:", list(results_obj.keys()))
        return [], body

    print(f"\nรวมทั้งหมด (dedup) : {len(all_ids)} IDs")
    return all_ids, body

# ─── Step 2: Query Metadata API ───────────────────────────────────────────────

def check_content_type(ids: list[str]) -> bool:
    sep("STEP 2 — Metadata API")
    print(f"URL: {METADATA_URL}\n")

    payload = {
        "parameters": {"id": ids, "fields": ["id", "content_type"]},
        "options": {"cache": False},
    }
    res = requests.post(METADATA_URL, json=payload, headers={"Content-Type": "application/json"})
    fname = f"{_current_profile}_step2_metadata.log"
    _save_evidence(fname, "POST", METADATA_URL, res, payload)
    print(f"    💾  evidence → {_EVIDENCE_DIR}/{fname}")
    res.raise_for_status()
    body = res.json()

    items = body.get("items", [])
    results = [
        {"id": item.get("id"), "content_type": item.get("content_type")}
        for item in items if item.get("id")
    ]

    sfvseries = [r for r in results if r["content_type"] == "sfvseries"]
    others    = [r for r in results if r["content_type"] != "sfvseries"]
    found_ids = {r["id"] for r in results}
    not_found = [i for i in ids if i not in found_ids]

    print(f"✅  content_type == \"sfvseries\"  ({len(sfvseries)} items)")
    for r in sfvseries:
        print(f"    ✓  {r['id']}")

    if others:
        print(f"\n❌  content_type != \"sfvseries\"  ({len(others)} items)")
        for r in others:
            print(f"    ✗  {r['id']}  →  {r['content_type']}")

    if not_found:
        print(f"\n⚠️   ไม่พบใน metadata  ({len(not_found)} items)")
        for i in not_found:
            print(f"    ?  {i}")

    sep("SUMMARY content_type")
    print(f"Total IDs    : {len(ids)}")
    print(f"Found        : {len(results)}")
    print(f"sfvseries    : {len(sfvseries)}")
    print(f"Not sfvseries: {len(others)}")
    print(f"Not found    : {len(not_found)}")

    all_pass = len(sfvseries) == len(ids) and not others and not not_found
    print(f"\nResult : {'✅ PASS — ทุก ID มี content_type == sfvseries' if all_pass else '❌ FAIL — มีบาง ID ที่ content_type ไม่ใช่ sfvseries'}")
    return all_pass

# ─── Step 3: Check merge_page cursor=1 ────────────────────────────────────────

def check_merge_page(sfvseries_ids: list[str], body: dict) -> bool:
    sep("STEP 3 — merge_page (cursor=1)")

    merge_ids = _parse_merge_ids(body)
    if not merge_ids:
        print("⚠️  ไม่พบ items ใน merge_page")
        return False

    print(f"merge_page มีทั้งหมด {len(merge_ids)} IDs (non-live)")

    sfvseries_set = set(sfvseries_ids)
    matched       = [i for i in merge_ids if i in sfvseries_set]
    not_matched   = [i for i in sfvseries_ids if i not in set(merge_ids)]

    if matched:
        print(f"\n✅  พบ sfvseries IDs ใน merge_page ({len(matched)} items):")
        for i in matched:
            print(f"    ✓  {i}")
    else:
        print("\n⚠️  ไม่พบ sfvseries IDs ใน merge_page เลย")

    if not_matched:
        print(f"\nℹ️   sfvseries IDs ที่ไม่อยู่ใน merge_page ({len(not_matched)} items):")
        for i in not_matched:
            print(f"    -  {i}")

    sep("SUMMARY merge_page")
    print(f"sfvseries candidate IDs : {len(sfvseries_ids)}")
    print(f"พบใน merge_page         : {len(matched)}")
    print(f"ไม่พบใน merge_page      : {len(not_matched)}")
    return len(matched) > 0

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

    id_to_cursors: dict[str, list[int]] = defaultdict(list)
    for cursor, body in bodies.items():
        merge_ids = _parse_merge_ids(body)
        print(f"  cursor={cursor}  merge_page IDs: {len(merge_ids)}")
        for i in merge_ids:
            id_to_cursors[i].append(cursor)

    duplicates = {i: cs for i, cs in id_to_cursors.items() if len(cs) > 1}

    sep("SUMMARY duplicate")
    print(f"Unique IDs รวมทุก cursor : {len(id_to_cursors)}")
    print(f"Duplicate IDs            : {len(duplicates)}")

    if duplicates:
        print(f"\n❌  พบ duplicate ({len(duplicates)} IDs):")
        for i, cs in sorted(duplicates.items(), key=lambda x: x[1]):
            print(f"    {i}  →  cursor {cs}")
    else:
        print("\n✅  ไม่พบ duplicate ในทุก cursor")

    return len(duplicates) == 0

# ─── Step 6: seen items accumulation check ────────────────────────────────────

def check_seen_items_across_cursors(base_url: str, bodies: dict[int, dict]) -> bool:
    sep(f"STEP 6 — seen items accumulation (cursor 1..{MAX_CURSOR}, increment={SEEN_ITEM_INCREMENT})")
    print("logic: ยิง req cursor N ซ้ำอีกครั้ง แล้วเช็ค get_seen_item_redis ว่ามี 5 IDs แรกของ logic_filter_overlap_items_pin_and_live[N] ไหม\n")

    all_pass = True
    for cursor in range(1, MAX_CURSOR + 1):
        if cursor not in bodies:
            print(f"  ⚠️  ไม่มีข้อมูล cursor={cursor} — ข้าม")
            continue

        filter_ids = _parse_logic_filter_ids(bodies[cursor])
        expected   = filter_ids[:SEEN_ITEM_INCREMENT]

        if not expected:
            print(f"  cursor={cursor}  ⚠️  ไม่พบ items ใน logic_filter_overlap_items_pin_and_live — ข้าม")
            all_pass = False
            continue

        print(f"  cursor={cursor}  ยิงซ้ำ ...", end=" ", flush=True)
        try:
            body2    = call_universal(base_url, cursor, step="step6_repeat")
            seen_ids = _parse_seen_item_ids(body2)
            seen_set = set(seen_ids)
            print("OK")
        except Exception as e:
            print(f"ERROR: {e}")
            all_pass = False
            continue

        print(f"    logic_filter first {SEEN_ITEM_INCREMENT} IDs              : {expected}")
        print(f"    get_seen_item_redis IDs ({len(seen_ids)} total) : {seen_ids}")

        missing = [i for i in expected if i not in seen_set]
        if missing:
            print(f"    ❌  ไม่พบใน get_seen_item_redis ({len(missing)} IDs): {missing}")
            all_pass = False
        else:
            print(f"    ✅  ครบทุก ID ({len(expected)} IDs)")

    sep("SUMMARY seen items")
    print(f"Result : {'✅ PASS' if all_pass else '❌ FAIL'}")
    return all_pass

# ─── Step 7: nologin must not accumulate seen items ───────────────────────────

def check_nologin_seen_item(nologin_url: str) -> bool:
    sep("STEP 7 — nologin ต้องไม่เก็บ seen item ใน Redis")
    print("logic: ยิง cursor 1 ด้วย ssoId=nologin แล้วเช็คว่า get_seen_item_redis ต้องไม่มี IDs\n")

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
            all_pass = False
        else:
            print(f"    ✅  get_seen_item_redis ว่างเปล่า — ถูกต้อง")

    sep("SUMMARY nologin seen item")
    print(f"Result : {'✅ PASS — nologin ไม่เก็บ seen item' if all_pass else '❌ FAIL — nologin มีการเก็บ seen item'}")
    return all_pass


# ─── Step 8: append_bucketizes_with_ratio per_source_count ───────────────────

def check_sfv_series_ratio(base_url: str, expected_ratio: int = EXPECTED_SFV_SERIES_RATIO) -> bool:
    sep(f"STEP 8 — append_bucketizes_with_ratio per_source_count (expected={expected_ratio})")
    ratio_url = f"{base_url}{_SFV_RATIO_PARAMS}"
    print(
        f"logic: ยิง API พร้อม node_tophit_sfv_series_ratio={expected_ratio}"
        f" และ node_latest_sfv_series_ratio={expected_ratio}\n"
        f"       แล้วเช็ค append_bucketizes_with_ratio.per_source_count ว่าทุก pool ได้ {expected_ratio} items\n"
    )
    print(f"URL: {ratio_url}&cursor=1\n")

    try:
        body = call_universal(ratio_url, 1, step="step8_ratio")
    except Exception as e:
        print(f"❌  ยิง API ไม่สำเร็จ: {e}")
        return False

    per_source = _parse_append_bucketizes_per_source_count(body)

    if not per_source:
        print("⚠️  ไม่พบ per_source_count ใน append_bucketizes_with_ratio — node อาจไม่มีอยู่ใน results")
        return False

    expected_keys = {
        "merge_bucketize_tophit_sfv_series",
        "merge_bucketize_latest_sfv_series",
    }
    all_pass = True

    print(f"per_source_count ที่ได้:")
    for source, count in per_source.items():
        ok = count == expected_ratio
        status = "✅" if ok else "❌"
        print(f"    {status}  {source} = {count}  (expected {expected_ratio})")
        if not ok:
            all_pass = False

    # เช็คว่ามีครบทั้งสอง source
    missing_keys = expected_keys - set(per_source.keys())
    if missing_keys:
        for key in missing_keys:
            print(f"    ❌  ไม่พบ source '{key}' ใน per_source_count")
        all_pass = False

    sep("SUMMARY sfv_series_ratio")
    print(f"Expected ratio per pool : {expected_ratio}")
    for source in sorted(expected_keys):
        count = per_source.get(source, "N/A")
        ok = count == expected_ratio
        print(f"  {'✅' if ok else '❌'}  {source} = {count}")
    print(f"\nResult : {'✅ PASS — ทุก pool ได้ตาม ratio ที่กำหนด' if all_pass else '❌ FAIL — มี pool ที่ไม่ตรงตาม ratio'}")
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
        ids, body_1 = get_sfvseries_ids(base_url)
        if not ids:
            print("\n[SKIP] ไม่มี ID จาก candidate sfvseries — ข้าม Step 2-3")
            results["content_type"] = False
            results["merge_page"]   = False
        else:
            results["content_type"] = check_content_type(ids)
            results["merge_page"]   = check_merge_page(ids, body_1)

        results["pin"]              = check_pin_reserved_positions(body_1)
        bodies                      = fetch_all_bodies(base_url)
        results["dup"]              = check_duplicates_across_cursors(bodies)
        results["seen"]             = check_seen_items_across_cursors(base_url, bodies)
        results["seen_nologin"]     = check_nologin_seen_item(profile["nologin_url"])
        results["sfv_series_ratio"] = check_sfv_series_ratio(base_url)

    except requests.HTTPError as e:
        print(f"\n[ERROR] HTTP {e.response.status_code}: {e.response.text}")
        results.setdefault("content_type",    False)
        results.setdefault("merge_page",      False)
        results.setdefault("pin",             False)
        results.setdefault("dup",             False)
        results.setdefault("seen",            False)
        results.setdefault("sfv_series_ratio", False)
    except Exception as e:
        print(f"\n[ERROR] {e}")

    return results

# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n📁  Evidence directory → {_EVIDENCE_DIR}/\n")
    all_profile_results: dict[str, dict[str, bool]] = {}

    for profile in PROFILES:
        all_profile_results[profile["name"]] = run_profile(profile)

    # ─ Final summary across all profiles
    banner("FINAL SUMMARY", width=60)
    labels = {
        "content_type":    "Step 2 content_type       ",
        "merge_page":      "Step 3 merge_page         ",
        "pin":             "Step 4 pin                ",
        "dup":             "Step 5 duplicate          ",
        "seen":            "Step 6 seen items         ",
        "seen_nologin":    "Step 7 nologin seen item  ",
        "sfv_series_ratio": f"Step 8 sfv_series_ratio   (expected={EXPECTED_SFV_SERIES_RATIO})",
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
