"""
get_live_ids.py
───────────────
validate live items จาก node get_all_live_today และ merge_page ของ 7 endpoints

Test cases:
  [Endpoint]
    E1: HTTP 200
    E2: node ที่กำหนดต้องอยู่ใน response
    E3: response time ≤ RESPONSE_TIME_LIMIT วินาที

  [Item — ต่อทุก ActivityId]
    I1:  ActivityId not null
    I2:  SiteTags is list (not null)
    I3:  CoverImage not null
    I4:  is_portrait is bool (not string/null)
    I5:  tags == SiteTags
    I6:  thumb_horizontal == CoverImage
    I7:  is_portrait ถูกต้องตาม rule (SiteTags[Preview]=="Landscape" → false, else → true)
    I8:  ไม่มี ActivityId ซ้ำภายใน node เดียวกัน
    I9:  CoverImage เป็น URL ที่ valid (ขึ้นต้น http:// หรือ https://)
    I10: ActivityId เป็น positive integer
    I11: tags เป็น list (not null)

  [Cross-node — เฉพาะ endpoint ที่มีทั้งสอง node]
    C1: item ActivityId เดียวกันใน 2 node ต้องมี tags / thumb_horizontal / is_portrait ตรงกัน
    C2: ทุก ActivityId ใน merge_page ต้องอยู่ใน get_all_live_today ด้วย

  [Pagination — merge_page cursor 1-5]
    P1: ไม่มี ActivityId ซ้ำข้าม cursor (แต่ละ page ต้องมี items ไม่ซ้ำกัน)

รัน:  python get_live_ids.py
"""

import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

BASE = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn"
    ".int-ai-platform.gcp.dmp.true.th/api/v1/universal"
)

RESPONSE_TIME_LIMIT = 5.0  # วินาที

ENDPOINTS = [
    {"name": "lc-b1",
     "nodes": ["get_all_live_today"],   # ไม่มี merge_page
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
     "url": f"{BASE}/sfv-p5?shelfId=zmEXe3EQnXDk&total_candidates=400"
             "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
             "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
             "&verbose=debug&cursor=1&limit_seen_items=20"},
    {"name": "sfv-p4",
     "url": f"{BASE}/sfv-p4?shelfId=zmEXe3EQnXDk&total_candidates=400"
             "&language=th&pool_limit_category_items=40&ssoId=111&userId=null"
             "&pseudoId=null&limit=20&returnItemMetadata=false&isOnlyId=true"
             "&verbose=debug&cursor=1&limit_seen_items=20"},
]

NODES = ["get_all_live_today", "merge_page"]
CURSOR_RANGE = range(1, 6)   # cursor 1–5 สำหรับ pagination test


# ── HTTP fetch ──────────────────────────────────────────────────────────────
def fetch(url: str) -> tuple[int, float, Any]:
    try:
        t0 = time.monotonic()
        res = subprocess.run(
            ["curl", "-s", "-o", "-", "-w", "\n__STATUS__%{http_code}", url],
            capture_output=True, text=True, timeout=30,
        )
        elapsed = time.monotonic() - t0
        out = res.stdout
        *body_parts, status_line = out.rsplit("\n__STATUS__", 1)
        body = "\n__STATUS__".join(body_parts)
        status = int(status_line.strip()) if status_line.strip().isdigit() else 0
        return status, elapsed, json.loads(body)
    except Exception as e:
        return 0, 0.0, {"_error": str(e)}


# ── Node extraction ─────────────────────────────────────────────────────────
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


# ── Live item extraction ────────────────────────────────────────────────────
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


# ── Validation helpers ──────────────────────────────────────────────────────
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


def validate_item(item: dict) -> list[dict]:
    checks = []
    aid       = item.get("ActivityId") or item.get("activityId")
    site_tags = item.get("SiteTags")
    cover_img = item.get("CoverImage")
    tags      = item.get("tags")
    thumb_h   = item.get("thumb_horizontal")
    is_port   = item.get("is_portrait")

    # I1: ActivityId not null
    checks.append({
        "case": "I1: ActivityId not null",
        "passed": bool(aid),
        "detail": f"ActivityId={aid!r}",
    })

    # I2: SiteTags is list
    checks.append({
        "case": "I2: SiteTags is list",
        "passed": isinstance(site_tags, list),
        "detail": f"type={type(site_tags).__name__}",
    })

    # I3: CoverImage not null
    checks.append({
        "case": "I3: CoverImage not null",
        "passed": bool(cover_img),
        "detail": f"CoverImage={cover_img!r}",
    })

    # I4: is_portrait is bool
    checks.append({
        "case": "I4: is_portrait is bool",
        "passed": isinstance(is_port, bool),
        "detail": f"value={is_port!r} type={type(is_port).__name__}",
    })

    # I5: tags == SiteTags
    checks.append({
        "case": "I5: tags == SiteTags",
        "passed": tags == site_tags,
        "detail": (
            "OK" if tags == site_tags else
            f"tags={json.dumps(tags, ensure_ascii=False)}\n"
            f"           SiteTags={json.dumps(site_tags, ensure_ascii=False)}"
        ),
    })

    # I6: thumb_horizontal == CoverImage
    checks.append({
        "case": "I6: thumb_horizontal == CoverImage",
        "passed": thumb_h == cover_img,
        "detail": (
            "OK" if thumb_h == cover_img else
            f"thumb_horizontal={thumb_h}\n"
            f"           CoverImage      ={cover_img}"
        ),
    })

    # I7: is_portrait rule
    exp = expected_is_portrait(site_tags)
    preview_val = next(
        (t.get("Value") for t in (site_tags or [])
         if isinstance(t, dict) and t.get("Name") == "Preview"),
        "—",
    )
    checks.append({
        "case": "I7: is_portrait rule",
        "passed": is_port == exp,
        "detail": (
            f"OK (Preview={preview_val} → {exp})" if is_port == exp else
            f"expected={exp}, actual={is_port}, Preview={preview_val}"
        ),
    })

    # I9: CoverImage valid URL
    checks.append({
        "case": "I9: CoverImage valid URL",
        "passed": is_valid_url(cover_img),
        "detail": f"CoverImage={cover_img!r}",
    })

    # I10: ActivityId is positive integer
    aid_ok = isinstance(aid, (int, float)) and int(aid) > 0
    checks.append({
        "case": "I10: ActivityId is positive integer",
        "passed": aid_ok,
        "detail": f"ActivityId={aid!r} type={type(aid).__name__}",
    })

    # I11: tags is list
    checks.append({
        "case": "I11: tags is list",
        "passed": isinstance(tags, list),
        "detail": f"type={type(tags).__name__}",
    })

    return checks


def check_no_duplicates(items: list[dict]) -> dict:
    """I8: ไม่มี ActivityId ซ้ำภายใน node เดียวกัน"""
    ids = [str(i.get("ActivityId") or i.get("activityId")) for i in items]
    seen = set()
    dupes = []
    for aid in ids:
        if aid in seen:
            dupes.append(aid)
        seen.add(aid)
    return {
        "case": "I8: no duplicate ActivityId",
        "passed": len(dupes) == 0,
        "detail": f"duplicates={dupes}" if dupes else "OK",
    }


def cross_node_checks(items_live: list[dict], items_merge: list[dict]) -> list[dict]:
    map_live  = {str(i.get("ActivityId") or i.get("activityId")): i for i in items_live}
    map_merge = {str(i.get("ActivityId") or i.get("activityId")): i for i in items_merge}

    results = []

    # C1: item เดียวกันต้องมี fields ตรงกัน
    shared = sorted(set(map_live) & set(map_merge))
    if not shared:
        results.append({
            "case": "C1: consistent fields across nodes",
            "passed": None,   # None = SKIP
            "detail": "ไม่มี ActivityId ที่ซ้ำกันระหว่าง 2 node (ไม่สามารถเช็คได้)",
        })
    else:
        for aid in shared:
            a, b = map_live[aid], map_merge[aid]
            diffs = []
            for field in ("tags", "thumb_horizontal", "is_portrait"):
                if a.get(field) != b.get(field):
                    diffs.append(
                        f"{field}: get_all_live_today={a.get(field)!r} "
                        f"vs merge_page={b.get(field)!r}"
                    )
            results.append({
                "case": f"C1: {aid} consistent across nodes",
                "passed": len(diffs) == 0,
                "detail": "\n           ".join(diffs) if diffs else "OK",
            })

    # C2: ทุก ActivityId ใน merge_page ต้องอยู่ใน get_all_live_today
    if not map_merge:
        results.append({
            "case": "C2: merge_page IDs ⊆ get_all_live_today IDs",
            "passed": None,   # None = SKIP
            "detail": "merge_page ไม่มี live item (ไม่สามารถเช็คได้)",
        })
    else:
        extra = sorted(set(map_merge) - set(map_live))
        results.append({
            "case": "C2: merge_page IDs ⊆ get_all_live_today IDs",
            "passed": len(extra) == 0,
            "detail": f"IDs in merge_page but NOT in get_all_live_today: {extra}" if extra else "OK",
        })

    return results


# ── Pagination helpers ──────────────────────────────────────────────────────
def make_cursor_url(url: str, cursor: int) -> str:
    """แทนค่า cursor=N ใน URL; ถ้าไม่มีให้ append"""
    import re
    if re.search(r"[?&]cursor=\d+", url):
        return re.sub(r"(cursor=)\d+", rf"\g<1>{cursor}", url)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}cursor={cursor}"


def fetch_cursor_ids(ep: dict, cursor: int) -> tuple[int, list[str]]:
    """fetch cursor=N แล้วคืน (cursor, [ActivityId, ...]) จาก merge_page"""
    url = make_cursor_url(ep["url"], cursor)
    status, _, data = fetch(url)
    if status != 200:
        return cursor, []
    result = get_node_result(data, "merge_page")
    if result is None:
        return cursor, []
    items = collect_live_items(result)
    return cursor, [str(i.get("ActivityId") or i.get("activityId")) for i in items]


def run_pagination_test(ep: dict) -> dict:
    """
    P1: fetch merge_page cursor 1-5 พร้อมกัน
    ตรวจว่า ActivityId ไม่ซ้ำข้าม cursor
    """
    cursor_ids: dict[int, list[str]] = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_cursor_ids, ep, c): c for c in CURSOR_RANGE}
        for fut in as_completed(futures):
            cursor, ids = fut.result()
            cursor_ids[cursor] = ids

    # รวม ids ทุก cursor แล้วหา duplicate
    seen: dict[str, int] = {}   # aid → cursor ที่เจอครั้งแรก
    dupes: list[str] = []
    all_ids_count = 0
    for cursor in sorted(cursor_ids):
        for aid in cursor_ids[cursor]:
            all_ids_count += 1
            if aid in seen:
                dupes.append(f"{aid} (cursor {seen[aid]} & cursor {cursor})")
            else:
                seen[aid] = cursor

    summary = {c: len(ids) for c, ids in cursor_ids.items()}

    # ถ้าไม่มี live item เลยทุก cursor → SKIP
    if all_ids_count == 0:
        p1 = {
            "case": "P1: no duplicate ActivityId across cursor 1-5",
            "passed": None,   # None = SKIP
            "detail": "ทุก cursor ไม่มี live item (ไม่สามารถเช็คได้)",
        }
    else:
        p1 = {
            "case": "P1: no duplicate ActivityId across cursor 1-5",
            "passed": len(dupes) == 0,
            "detail": ("OK" if not dupes else
                       "duplicates:\n           " + "\n           ".join(dupes)),
        }

    return {"cursor_ids": cursor_ids, "summary": summary, "p1": p1}


# ── Per-endpoint processing ─────────────────────────────────────────────────
def process_endpoint(ep: dict) -> dict:
    status, elapsed, data = fetch(ep["url"])
    out = {
        "name":     ep["name"],
        "status":   status,
        "elapsed":  elapsed,
        "nodes":    {},
        "raw_items": {},
    }

    # E1, E3
    out["e1_passed"] = (status == 200)
    out["e3_passed"] = (elapsed <= RESPONSE_TIME_LIMIT)

    if not out["e1_passed"]:
        out["error"] = data.get("_error", f"HTTP {status}")
        return out

    nodes_to_check = ep.get("nodes", NODES)
    for node_name in nodes_to_check:
        result = get_node_result(data, node_name)

        # E2
        out["nodes"][node_name] = {
            "e2_passed": result is not None,
            "i8": None,
            "items": [],
        }
        if result is None:
            continue

        raw = collect_live_items(result)
        out["raw_items"][node_name] = raw

        # I8: no duplicates (node-level check)
        out["nodes"][node_name]["i8"] = check_no_duplicates(raw)

        out["nodes"][node_name]["items"] = [
            {"id": i.get("ActivityId") or i.get("activityId"),
             "checks": validate_item(i)}
            for i in raw
        ]

    # C1 + C2
    if "get_all_live_today" in out["raw_items"] and "merge_page" in out["raw_items"]:
        out["cross"] = cross_node_checks(
            out["raw_items"]["get_all_live_today"],
            out["raw_items"]["merge_page"],
        )

    return out


# ── Print helpers ───────────────────────────────────────────────────────────
PASS = "✅"
FAIL = "❌"
SKIP = "—"


def print_check(label: str, passed, detail: str = "", indent: int = 6):
    if passed is None:
        icon = "⏭️ "
    elif passed:
        icon = PASS
    else:
        icon = FAIL
    pad  = " " * indent
    line = f"{pad}{icon} {label}"
    if detail and detail not in ("OK", ""):
        line += f"\n{pad}     {detail}"
    print(line)


def tally(checks: list[dict], totals: list):
    # totals = [total, pass, fail, skip]
    for c in checks:
        if c["passed"] is None:
            totals[3] += 1   # skip
        else:
            totals[0] += 1
            if c["passed"]:
                totals[1] += 1
            else:
                totals[2] += 1


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f"Fetching {len(ENDPOINTS)} endpoints in parallel...\n")

    results = {}
    with ThreadPoolExecutor(max_workers=7) as ex:
        futures = {ex.submit(process_endpoint, ep): ep["name"] for ep in ENDPOINTS}
        for fut in as_completed(futures):
            r = fut.result()
            results[r["name"]] = r

    totals = [0, 0, 0, 0]   # [total, pass, fail, skip]

    for ep in ENDPOINTS:
        name = ep["name"]
        r    = results[name]
        print(f"{'═'*65}")
        print(f"  {name}")
        print(f"{'─'*65}")

        # E1
        e1 = {"case": f"E1: HTTP 200 (got {r['status']})", "passed": r["e1_passed"], "detail": ""}
        print_check(e1["case"], e1["passed"])
        tally([e1], totals)

        # E3
        e3 = {
            "case": f"E3: response time ≤ {RESPONSE_TIME_LIMIT}s (got {r['elapsed']:.2f}s)",
            "passed": r["e3_passed"],
            "detail": "",
        }
        print_check(e3["case"], e3["passed"])
        tally([e3], totals)

        if not r["e1_passed"]:
            print(f"      ❌  {r.get('error','')}")
            print()
            continue

        for node_name in ep.get("nodes", NODES):
            node = r["nodes"].get(node_name, {})
            print(f"\n  [{node_name}]")

            # E2
            e2 = {"case": "E2: node found in response", "passed": node.get("e2_passed", False), "detail": ""}
            print_check(e2["case"], e2["passed"])
            tally([e2], totals)
            if not e2["passed"]:
                continue

            # I8
            if node.get("i8"):
                print_check(node["i8"]["case"], node["i8"]["passed"], node["i8"]["detail"])
                tally([node["i8"]], totals)

            items = node.get("items", [])
            if not items:
                print(f"      {SKIP}  0 live items (pipeline filtered)")
                continue

            for item in items:
                print(f"\n    ActivityId {item['id']}")
                for chk in item["checks"]:
                    print_check(chk["case"], chk["passed"], chk["detail"], indent=6)
                tally(item["checks"], totals)

        # C1 + C2
        if "cross" in r:
            print(f"\n  [cross-node]")
            if not r["cross"]:
                print(f"      {SKIP}  ไม่มี ActivityId ที่ซ้ำกันใน 2 node")
            else:
                for cx in r["cross"]:
                    print_check(cx["case"], cx["passed"], cx["detail"])
                tally(r["cross"], totals)

        print()

    # ── Pagination tests (merge_page cursor 1-5) ──────────────────────────
    paged_eps = [ep for ep in ENDPOINTS if "merge_page" in ep.get("nodes", NODES)]

    if paged_eps:
        print(f"\n{'═'*65}")
        print(f"  PAGINATION TEST  —  merge_page cursor 1-{max(CURSOR_RANGE)}")
        print(f"{'═'*65}")
        print(f"  Fetching {len(paged_eps)} endpoints × {len(CURSOR_RANGE)} cursors in parallel...\n")

        pag_results = {}
        with ThreadPoolExecutor(max_workers=len(paged_eps)) as ex:
            pag_futures = {ex.submit(run_pagination_test, ep): ep["name"] for ep in paged_eps}
            for fut in as_completed(pag_futures):
                pr = fut.result()
                name = pag_futures[fut]
                pag_results[name] = pr

        for ep in paged_eps:
            name = ep["name"]
            pr   = pag_results[name]
            print(f"  {name}")
            print(f"{'─'*65}")

            # per-cursor item count
            for c in sorted(CURSOR_RANGE):
                cnt = pr["summary"].get(c, 0)
                tag = f"  (0 live items)" if cnt == 0 else ""
                print(f"    cursor {c}: {cnt} live item(s){tag}")

            print()
            print_check(pr["p1"]["case"], pr["p1"]["passed"], pr["p1"]["detail"])
            tally([pr["p1"]], totals)
            print()

    total, passed, failed, skipped = totals
    print(f"{'═'*65}")
    print(f"  Total checks : {total}  (+ {skipped} skipped)")
    print(f"  PASS         : {passed}")
    print(f"  FAIL         : {failed}")
    print(f"  SKIP         : {skipped}  (ไม่มี live item ให้เช็ค)")
    print(f"{'═'*65}")


if __name__ == "__main__":
    main()
