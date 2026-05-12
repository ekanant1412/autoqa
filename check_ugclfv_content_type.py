"""
check_ugclfv_content_type.py
─────────────────────────────
1. ยิง Universal API (cursor=1) แล้วดึง IDs จาก candidate_latest_ugc_lfv
2. เอา IDs ไป query Metadata API ตรวจว่าทุก ID มี content_type == "ugclfv"
3. เช็คว่า ugclfv IDs ปรากฏใน merge_page cursor=1 ไหม
4. เช็ค reservedPositions ของ insert_pin_candidates vs slice_pin_globals
5. วน cursor 1..MAX_CURSOR เก็บ merge_page IDs ทุก cursor แล้วเช็ค duplicate
6. เช็คว่า 5 IDs แรกของ logic_filter_overlap_items_pin_and_live cursor N ปรากฏใน get_seen_item_redis (ยิงซ้ำ cursor N)

รัน: python3 check_ugclfv_content_type.py
"""

import os
import sys
import json
from datetime import datetime
from collections import defaultdict
import requests

# ─── Config ───────────────────────────────────────────────────────────────────

_BASE_PARAMS = (
    "?shelfId=zmEXe3EQnXDk"
    "&total_candidates=400"
    "&language=th"
    "&pool_limit_category_items=40"
    "&ssoId=766"
    "&userId=null"
    "&pseudoId=null"
    "&limit=20"
    "&returnItemMetadata=false"
    "&isOnlyId=true"
    "&verbose=debug"
)

_HOST = "http://ai-universal-service-new-2.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"

PROFILES = [
    {
        "name": "sfv-p4",
        "base_url": f"{_HOST}/api/v1/universal/sfv-p4{_BASE_PARAMS}",
    },
    {
        "name": "sfv-p5",
        "base_url": f"{_HOST}/api/v1/universal/sfv-p5{_BASE_PARAMS}",
    },
]

METADATA_URL = (
    "http://ai-metadata-service.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
    "/metadata/all-view-data"
)

MAX_CURSOR = 5            # จำนวน cursor ที่จะเช็ค
SEEN_ITEM_INCREMENT = 5  # จำนวน id ที่ logic_filter ส่งเข้า seen per cursor

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

def _parse_ugclfv_ids(body: dict) -> list[str]:
    node_result = (
        body.get("data", {}).get("results", {})
        .get("candidate_latest_ugc_lfv", {})
        .get("result", {}).get("data", {})
    )
    if not node_result:
        return []
    buckets = node_result.get("aggregations", {}).get("agg_latest", {}).get("buckets", [])
    seen = set()
    ids = []
    for bucket in buckets:
        for h in bucket.get("sort_by_publish_date", {}).get("hits", {}).get("hits", []):
            item_id = h.get("_source", {}).get("id")
            if item_id and item_id not in seen:
                seen.add(item_id)
                ids.append(item_id)
    return ids

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

# ─── Step 1: Call Universal API cursor=1 ──────────────────────────────────────

def get_ugclfv_ids(base_url: str) -> tuple[list[str], dict]:
    sep("STEP 1 — Universal API (cursor=1)")
    print(f"URL: {base_url}&cursor=1\n")

    body = call_universal(base_url, 1, step="step1")
    ids = _parse_ugclfv_ids(body)

    if not ids:
        print("⚠️  ไม่พบ candidate_latest_ugc_lfv ใน data.results")
        results_keys = list(body.get("data", {}).get("results", {}).keys())
        print("data.results keys:", results_keys)
        return [], body

    print(f"พบ {len(ids)} IDs จาก aggregations (candidate_latest_ugc_lfv):")
    for i in ids:
        print(f"  - {i}")
    return ids, body

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

    ugclfv    = [r for r in results if r["content_type"] == "ugclfv"]
    others    = [r for r in results if r["content_type"] != "ugclfv"]
    found_ids = {r["id"] for r in results}
    not_found = [i for i in ids if i not in found_ids]

    print(f"✅  content_type == \"ugclfv\"  ({len(ugclfv)} items)")
    for r in ugclfv:
        print(f"    ✓  {r['id']}")

    if others:
        print(f"\n❌  content_type != \"ugclfv\"  ({len(others)} items)")
        for r in others:
            print(f"    ✗  {r['id']}  →  {r['content_type']}")

    if not_found:
        print(f"\n⚠️   ไม่พบใน metadata  ({len(not_found)} items)")
        for i in not_found:
            print(f"    ?  {i}")

    sep("SUMMARY content_type")
    print(f"Total IDs  : {len(ids)}")
    print(f"Found      : {len(results)}")
    print(f"ugclfv     : {len(ugclfv)}")
    print(f"Not ugclfv : {len(others)}")
    print(f"Not found  : {len(not_found)}")

    all_pass = len(ugclfv) == len(ids) and not others and not not_found
    print(f"\nResult : {'✅ PASS — ทุก ID มี content_type == ugclfv' if all_pass else '❌ FAIL — มีบาง ID ที่ content_type ไม่ใช่ ugclfv'}")
    return all_pass

# ─── Step 3: Check merge_page cursor=1 ────────────────────────────────────────

def check_merge_page(ugclfv_ids: list[str], body: dict) -> bool:
    sep("STEP 3 — merge_page (cursor=1)")

    merge_ids = _parse_merge_ids(body)
    if not merge_ids:
        print("⚠️  ไม่พบ items ใน merge_page")
        return False

    print(f"merge_page มีทั้งหมด {len(merge_ids)} IDs (non-live)")

    ugclfv_set  = set(ugclfv_ids)
    matched     = [i for i in merge_ids if i in ugclfv_set]
    not_matched = [i for i in ugclfv_ids if i not in set(merge_ids)]

    if matched:
        print(f"\n✅  พบ ugclfv IDs ใน merge_page ({len(matched)} items):")
        for i in matched:
            print(f"    ✓  {i}")
    else:
        print("\n⚠️  ไม่พบ ugclfv IDs ใน merge_page เลย")

    if not_matched:
        print(f"\nℹ️   ugclfv IDs ที่ไม่อยู่ใน merge_page ({len(not_matched)} items):")
        for i in not_matched:
            print(f"    -  {i}")

    sep("SUMMARY merge_page")
    print(f"ugclfv candidate IDs : {len(ugclfv_ids)}")
    print(f"พบใน merge_page      : {len(matched)}")
    print(f"ไม่พบใน merge_page   : {len(not_matched)}")
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

        print(f"    logic_filter first {SEEN_ITEM_INCREMENT} IDs   : {expected}")
        print(f"    get_seen_item_redis IDs               : {seen_ids}")

        missing = [i for i in expected if i not in seen_set]
        if missing:
            print(f"    ❌  ไม่พบใน get_seen_item_redis ({len(missing)} IDs): {missing}")
            all_pass = False
        else:
            print(f"    ✅  ครบทุก ID ({len(expected)} IDs)")

    sep("SUMMARY seen items")
    print(f"Result : {'✅ PASS' if all_pass else '❌ FAIL'}")
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
        ids, body_1 = get_ugclfv_ids(base_url)
        if not ids:
            print("\n[SKIP] ไม่มี ID จาก candidate_latest_ugc_lfv — ข้าม Step 2-3")
            results["content_type"] = False
            results["merge_page"]   = False
        else:
            results["content_type"] = check_content_type(ids)
            results["merge_page"]   = check_merge_page(ids, body_1)

        results["pin"]  = check_pin_reserved_positions(body_1)
        bodies          = fetch_all_bodies(base_url)
        results["dup"]  = check_duplicates_across_cursors(bodies)
        results["seen"] = check_seen_items_across_cursors(base_url, bodies)

    except requests.HTTPError as e:
        print(f"\n[ERROR] HTTP {e.response.status_code}: {e.response.text}")
        results.setdefault("content_type", False)
        results.setdefault("merge_page",   False)
        results.setdefault("pin",          False)
        results.setdefault("dup",          False)
        results.setdefault("seen",         False)
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
        "content_type": "Step 2 content_type",
        "merge_page":   "Step 3 merge_page  ",
        "pin":          "Step 4 pin         ",
        "dup":          "Step 5 duplicate   ",
        "seen":         "Step 6 seen items  ",
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
