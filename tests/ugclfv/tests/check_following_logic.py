import pytest
import requests
from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any, Optional

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG (Global defaults)
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL     = "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
METADATA_URL = "http://ai-metadata-service.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
TIMEOUT      = 30

# ══════════════════════════════════════════════════════════════════════════════
# Logging helpers
# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 65

def log(msg):        print(msg)
def section(t):      log(f"\n{SEP}\n  {t}\n{SEP}")
def step(l, v=None): log(f"  ▶ {l}{(': ' + str(v)) if v is not None else ''}")
def ok(l):           log(f"  ✅ {l}")
def warn(l):         log(f"  ⚠️  {l}")
def fail(l):         log(f"  ❌ {l}")

# ── Evidence display helpers ──────────────────────────────────────────────────
DIV = "─" * 65

def ctx(label: str, value=""):
    """แสดง 1 row ของ data context (ส่วนบนสุดของแต่ละ test)"""
    log(f"     {label:<34}{value}")

def det(label: str, value=""):
    """แสดง detail ภายใน test block"""
    log(f"         {label:<22}{value}")

def fmt_ids(ids, max_show: int = 6) -> str:
    """Format id list — แสดง max_show ตัวแรก ถ้ายาวกว่านั้นบอก +N more"""
    if not ids:
        return "[]  (empty)"
    if len(ids) <= max_show:
        return f"{ids}  ({len(ids)} items)"
    preview = [repr(i) for i in ids[:max_show]]
    return f"[{', '.join(preview)}, ...]  ({len(ids)} items)"

def t_block(passed: bool, tid: str, desc: str):
    """เปิด test block — แสดง icon + test id + description"""
    icon = "✅" if passed else "❌"
    log(f"\n  {DIV}")
    log(f"  {icon} {tid}  {desc}")


# ══════════════════════════════════════════════════════════════════════════════
# Generic helpers
# ══════════════════════════════════════════════════════════════════════════════
def find_key(obj, key_name):
    """Recursive search — หา node ไม่ว่าจะซ้อนลึกแค่ไหน"""
    if not isinstance(obj, dict):
        return None
    if key_name in obj:
        return obj[key_name]
    for v in obj.values():
        found = find_key(v, key_name)
        if found is not None:
            return found
    return None


def extract_ids(node):
    """ดึง ids จาก node.result (รองรับทั้ง .ids และ .items[].id)"""
    if not node or "result" not in node:
        return []
    result = node["result"]
    if isinstance(result.get("ids"), list) and result["ids"]:
        return result["ids"]
    return [item["id"] for item in result.get("items", []) if "id" in item]


def call_api(endpoint: str, params: Dict[str, Any]) -> Dict:
    resp = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def call_metadata_following(ssoids: List[str], limit: int = 200) -> List[str]:
    """ดึง item ids จาก sfv_following_chanel สำหรับ ssoids ที่กำหนด
    คืนค่า list ของ id ทั้งหมดที่ followed ssoids นั้นมีอยู่"""
    payload = {
        "parameters": {
            "create_by_ssoid": [int(s) for s in ssoids],
            "limit": limit,
            "fields": ["id"],
            "language": "th",
        }
    }
    resp = requests.post(
        f"{METADATA_URL}/metadata/sfv_following_chanel",
        json=payload,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    # รองรับหลาย response shape
    items = (
        body.get("data")
        or body.get("items")
        or body.get("result", {}).get("items", [])
        or []
    )
    return [item["id"] for item in items if "id" in item]


def _detect_merge_phase(node) -> str:
    """ตรวจ phase ของ merge_page จาก structure ที่ได้รับ
    'following' — result.ids (flat list) → content จากคนที่ follow
    'fallback'  — result.items[].create_by_ssoid → fallback grouped
    'empty'     — ไม่มี content"""
    if not node or "result" not in node:
        return "empty"
    result = node["result"]
    if isinstance(result.get("ids"), list) and result["ids"]:
        return "following"
    items = result.get("items", [])
    if items and isinstance(items[0], dict) and "create_by_ssoid" in items[0]:
        return "fallback"
    return "empty"


# ══════════════════════════════════════════════════════════════════════════════
# Scenario dataclass
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ScenarioConfig:
    """Config สำหรับแต่ละ scenario — endpoint, params, node ที่จะ check"""
    endpoint:     str
    base_params:  Dict[str, Any]
    source_node:  str
    start_cursor: int = 1
    max_cursors:  int = 50


@dataclass
class Scenario:
    """หน่วยของ test scenario — มี config + build_fn + verify_fn เป็นของตัวเอง"""
    name:      str
    config:    ScenarioConfig
    build_fn:  Callable[["Scenario"], List[Dict[str, Any]]]
    verify_fn: Callable[[Dict[str, Any]], None]


# ══════════════════════════════════════════════════════════════════════════════
# Registry — เพิ่ม scenario ใหม่ที่นี่
# ══════════════════════════════════════════════════════════════════════════════
SCENARIO_REGISTRY: List[Scenario] = []


def register(scenario: Scenario) -> Scenario:
    """Register scenario เข้า registry — ใช้ได้ทั้ง direct call หรือ decorator"""
    SCENARIO_REGISTRY.append(scenario)
    return scenario


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: seen_pool (sliding window)
# ══════════════════════════════════════════════════════════════════════════════
def _seen_pool_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg        = scenario.config
    cases      = []
    prev_ids   = None
    seen_limit = int(cfg.base_params.get("limit_seen_items", 200))

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        # ดึง ids จาก source node
        node        = find_key(body, cfg.source_node)
        current_ids = (
            [item["id"] for item in node.get("result", {}).get("items", []) if "id" in item]
            if node else []
        )

        # ดึง seen ids
        seen_node = find_key(body, "redis_get_seen_item")
        seen_ids  = extract_ids(seen_node) if seen_node else []

        step(f"cursor={cursor} {cfg.source_node} all ids", current_ids)
        step(f"cursor={cursor} seen_ids count",            len(seen_ids))

        if prev_ids is not None:
            cases.append({
                "scenario":      scenario.name,
                "cursor_source": cursor - 1,
                "cursor_check":  cursor,
                "prev_ids":      prev_ids,
                "seen_ids":      seen_ids,
                "seen_limit":    seen_limit,
                "test_id":       f"{scenario.name}::src=cursor{cursor - 1}_chk=cursor{cursor}",
            })
            ok(f"cursor={cursor} → test case added (checking ids from cursor={cursor - 1})")
        else:
            ok(f"cursor={cursor} → first request, captured ids only")

        if not current_ids:
            ok(f"cursor={cursor} → {cfg.source_node}.items empty, stopping")
            break

        prev_ids = current_ids

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _seen_pool_verify(tc: Dict[str, Any]):
    cursor_source = tc["cursor_source"]
    cursor_check  = tc["cursor_check"]
    prev_ids      = tc["prev_ids"]
    seen_ids      = tc["seen_ids"]
    seen_limit    = tc["seen_limit"]

    n    = len(prev_ids)
    tail = seen_ids[-n:] if len(seen_ids) >= n else seen_ids

    # ── pre-compute ──────────────────────────────────────────────────────────
    missing    = [i for i in prev_ids if i not in seen_ids]
    tail_match = tail == prev_ids
    t3_pass    = len(seen_ids) <= seen_limit
    seen_set, dupes = set(), []
    for sid in seen_ids:
        if sid in seen_set: dupes.append(sid)
        else: seen_set.add(sid)

    t1_pass = not missing
    t2_pass = tail_match
    t4_pass = not dupes

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  cursor {cursor_source} → {cursor_check}")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx(f"prev_ids  (cursor {cursor_source})",  fmt_ids(prev_ids))
    ctx(f"seen_ids  (cursor {cursor_check})",   f"{len(seen_ids)} items  (limit: {seen_limit})")

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1", "prev_ids ทุกตัวต้องอยู่ใน seen")
    det("checked",  f"{n} ids")
    det("found",    f"{n - len(missing)} / {n}")
    if not t1_pass:
        det("⚠️  missing", missing)
    assert t1_pass, (
        f"[{tc['scenario']} src={cursor_source} chk={cursor_check}] "
        f"T1 FAIL — missing from seen: {missing}"
    )

    # ── T2 ───────────────────────────────────────────────────────────────────
    t_block(t2_pass, "T2", "prev_ids ต้องอยู่ท้าย seen ตามลำดับ (tail match)")
    det("expected tail", prev_ids)
    det("actual tail",   tail)
    assert t2_pass, (
        f"[{tc['scenario']} src={cursor_source} chk={cursor_check}] "
        f"T2 FAIL — expected={prev_ids}, got={tail}"
    )

    # ── T3 ───────────────────────────────────────────────────────────────────
    t_block(t3_pass, "T3", "seen size ต้องไม่เกิน limit")
    det("size",  f"{len(seen_ids)}")
    det("limit", f"{seen_limit}")
    assert t3_pass, (
        f"[{tc['scenario']} src={cursor_source} chk={cursor_check}] "
        f"T3 FAIL — seen size {len(seen_ids)} exceeds limit {seen_limit}"
    )

    # ── T4 ───────────────────────────────────────────────────────────────────
    t_block(t4_pass, "T4", "ไม่มี id ซ้ำใน seen")
    det("unique ids",  len(seen_set))
    det("duplicates",  len(dupes))
    if not t4_pass:
        det("⚠️  dupes", dupes)
    assert t4_pass, (
        f"[{tc['scenario']} src={cursor_source} chk={cursor_check}] "
        f"T4 FAIL — duplicates: {dupes}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1"
        f"   {'✅' if t2_pass else '❌'} T2"
        f"   {'✅' if t3_pass else '❌'} T3"
        f"   {'✅' if t4_pass else '❌'} T4")
    log(SEP)


# ── Register seen_pool ────────────────────────────────────────────────────────
register(Scenario(
    name="seen_pool",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "1913", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_seen_pool_build,
    verify_fn=_seen_pool_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: non_following  (ssoId=1714, nologin)
#   • merge_page.result.items → grouped by create_by_ssoid  ≠ flat ids
#   • redis_get_user_following → ต้องว่าง (nologin ไม่มี following)
#   • T1  following empty
#   • T2  merge_page items มี create_by_ssoid structure
#   • T3  prev ids อยู่ใน seen
#   • T4  prev ids อยู่ท้าย seen ตามลำดับ
#   • T5  seen ไม่เกิน limit
#   • T6  seen ไม่มี duplicate
# ══════════════════════════════════════════════════════════════════════════════
def _extract_grouped_ids(node) -> List[str]:
    """Flatten ids จาก merge_page ที่ items เป็น [{create_by_ssoid, items:[{id}]}]"""
    if not node or "result" not in node:
        return []
    ids = []
    for group in node["result"].get("items", []):
        for item in group.get("items", []):
            if "id" in item:
                ids.append(item["id"])
    return ids


def _non_following_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg        = scenario.config
    cases      = []
    prev_ids   = None
    seen_limit = int(cfg.base_params.get("limit_seen_items", 200))

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        # ดึง ids จาก merge_page (grouped structure)
        node        = find_key(body, cfg.source_node)
        current_ids = _extract_grouped_ids(node)
        raw_items   = node["result"].get("items", []) if node else []

        # ดึง following ids (ควรว่างเสมอสำหรับ nologin)
        following_node = find_key(body, "redis_get_user_following")
        following_ids  = extract_ids(following_node) if following_node else []

        # ดึง seen ids
        seen_node = find_key(body, "redis_get_seen_item")
        seen_ids  = extract_ids(seen_node) if seen_node else []

        step(f"cursor={cursor} {cfg.source_node} ids (flattened)", current_ids)
        step(f"cursor={cursor} following_ids",                     following_ids)
        step(f"cursor={cursor} seen_ids count",                    len(seen_ids))

        if prev_ids is not None:
            cases.append({
                "scenario":      scenario.name,
                "cursor_source": cursor - 1,
                "cursor_check":  cursor,
                "prev_ids":      prev_ids,
                "seen_ids":      seen_ids,
                "seen_limit":    seen_limit,
                "following_ids": following_ids,
                "raw_items":     raw_items,
                "test_id":       f"{scenario.name}::src=cursor{cursor - 1}_chk=cursor{cursor}",
            })
            ok(f"cursor={cursor} → test case added (checking ids from cursor={cursor - 1})")
        else:
            ok(f"cursor={cursor} → first request, captured ids only")

        if not current_ids:
            ok(f"cursor={cursor} → {cfg.source_node}.items empty, stopping")
            break

        prev_ids = current_ids

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _non_following_verify(tc: Dict[str, Any]):
    cursor_source = tc["cursor_source"]
    cursor_check  = tc["cursor_check"]
    seen_ids      = tc["seen_ids"]
    following_ids = tc["following_ids"]
    raw_items     = tc["raw_items"]

    # ── pre-compute ──────────────────────────────────────────────────────────
    bad_structure, empty_items = [], []
    for item in raw_items:
        ssoid     = item.get("create_by_ssoid", "")
        sub_items = item.get("items", [])
        if not ssoid:          bad_structure.append(item)
        elif not sub_items:    empty_items.append(ssoid)

    t1_pass = not following_ids
    t2_pass = not bad_structure and not empty_items
    t3_pass = len(seen_ids) == 0

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  cursor {cursor_source} → {cursor_check}")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("following_ids",        fmt_ids(following_ids) if following_ids else "[]  (empty)")
    ctx("merge_page groups",    f"{len(raw_items)} groups")
    ctx(f"seen_ids  (cursor {cursor_check})", f"{len(seen_ids)} items")

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1", "redis_get_user_following ต้องว่าง (non-following / nologin user)")
    det("result", "empty ✓" if t1_pass else f"found {len(following_ids)} ids — should be empty")
    if not t1_pass:
        det("⚠️  got", following_ids)
    assert t1_pass, (
        f"[{tc['scenario']} chk={cursor_check}] "
        f"T1 FAIL — following_ids should be empty, got: {following_ids}"
    )

    # ── T2 ───────────────────────────────────────────────────────────────────
    t_block(t2_pass, "T2", "ทุก group ใน merge_page ต้องมี create_by_ssoid และ items ไม่ว่าง")
    det("groups checked",   len(raw_items))
    det("valid",            len(raw_items) - len(bad_structure) - len(empty_items))
    det("bad structure",    f"{len(bad_structure)}" + (f"  ⚠️  {bad_structure}" if bad_structure else ""))
    det("empty items",      f"{len(empty_items)}" + (f"  ⚠️  {empty_items}" if empty_items else ""))
    assert not bad_structure, (
        f"[{tc['scenario']} chk={cursor_check}] "
        f"T2 FAIL — groups missing create_by_ssoid: {bad_structure}"
    )
    assert not empty_items, (
        f"[{tc['scenario']} chk={cursor_check}] "
        f"T2 FAIL — groups with empty items: {empty_items}"
    )

    # ── T3 ───────────────────────────────────────────────────────────────────
    t_block(t3_pass, "T3", "seen_ids ต้องว่าง (non-following ไม่มี seen tracking)")
    det("result", "empty ✓" if t3_pass else f"{len(seen_ids)} items — should be empty")
    if not t3_pass:
        det("⚠️  got", fmt_ids(seen_ids))
    assert t3_pass, (
        f"[{tc['scenario']} chk={cursor_check}] "
        f"T3 FAIL — seen_ids should be empty, got: {seen_ids}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1"
        f"   {'✅' if t2_pass else '❌'} T2"
        f"   {'✅' if t3_pass else '❌'} T3")
    log(SEP)


# ── Register non_following (ssoId=1714) ──────────────────────────────────────
register(Scenario(
    name="non_following_1714",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "1714", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_non_following_build,
    verify_fn=_non_following_verify,
))

# ── Register non_following (ssoId=nologin) ───────────────────────────────────
register(Scenario(
    name="non_following_nologin",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "nologin", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_non_following_build,
    verify_fn=_non_following_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: sfv_history  (ssoId=207535257)
#   • ตรวจว่า redis_get_sfv_history.result.ids ทั้งหมดอยู่ใน merge_page ไหม
#   • T1  history_ids ทั้งหมดต้องอยู่ใน merge_page ids
# ══════════════════════════════════════════════════════════════════════════════
def _sfv_history_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg              = scenario.config
    all_merge_ids    = []   # สะสม merge_ids ข้ามทุก cursor
    final_history_ids = []

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        # ดึง ids จาก merge_page (grouped structure: items[].create_by_ssoid + items[].items[].id)
        node      = find_key(body, cfg.source_node)
        merge_ids = _extract_grouped_ids(node)

        # ดึง history ids (ใช้ค่าล่าสุดที่ได้ — redis คืนค่าเดิมตลอด)
        history_node = find_key(body, "redis_get_sfv_history")
        if history_node and "result" in history_node:
            final_history_ids = history_node["result"].get("ids", [])

        step(f"cursor={cursor} merge_page ids ({len(merge_ids)} items)", merge_ids)
        step(f"cursor={cursor} history_ids count", len(final_history_ids))

        all_merge_ids.extend(merge_ids)
        ok(f"cursor={cursor} → accumulated merge_ids total: {len(all_merge_ids)}")

        if not merge_ids:
            ok(f"cursor={cursor} → merge_page.items empty, stopping")
            break

    # สร้าง 1 test case รวม — เทียบ history กับ merge_ids ทั้งหมดที่ paginate ได้
    history_limit = int(cfg.base_params.get("limit_sfv_history", 3))
    cases = []
    if all_merge_ids or final_history_ids:
        cases.append({
            "scenario":       scenario.name,
            "cursor":         "all",
            "merge_ids":      list(dict.fromkeys(all_merge_ids)),   # dedupe, preserve order
            "history_ids":    final_history_ids,
            "history_limit":  history_limit,
            "test_id":        f"{scenario.name}::all_cursors",
        })
        ok(f"→ 1 aggregated test case (merge_ids total={len(all_merge_ids)}, history={len(final_history_ids)}, history_limit={history_limit})")

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _sfv_history_verify(tc: Dict[str, Any]):
    cursor        = tc["cursor"]
    merge_ids     = tc["merge_ids"]
    history_ids   = tc["history_ids"]
    history_limit = tc.get("history_limit", 3)

    # ── pre-compute ──────────────────────────────────────────────────────────
    expected_found = min(history_limit, len(history_ids))
    found          = [i for i in history_ids if i in merge_ids]
    not_found      = [i for i in history_ids if i not in merge_ids]
    t1_pass        = len(found) >= expected_found

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  all cursors")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("history_ids  (redis_get_sfv_history)", fmt_ids(history_ids))
    ctx("merge_page ids  (all cursors)",        f"{len(merge_ids)} items")
    ctx("history_limit  (max shown in feed)",   history_limit)

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1",
            f"history_ids ต้องปรากฏใน merge_page อย่างน้อย {expected_found} จาก {len(history_ids)} ตัว")
    det("expected at least", f"{expected_found} of {len(history_ids)}")
    det("found in feed",     f"{len(found)} / {len(history_ids)}")
    det("not found",         f"{len(not_found)}"
                             + (f"  ⚠️  {not_found}" if not_found and not t1_pass else
                                f"  (allowed — exceeds limit)" if not_found else ""))
    assert t1_pass, (
        f"[{tc['scenario']} cursor={cursor}] "
        f"T1 FAIL — found {len(found)}/{len(history_ids)}, "
        f"expected at least {expected_found} (limit={history_limit})"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1")
    log(SEP)


# ── Register sfv_history ──────────────────────────────────────────────────────
register(Scenario(
    name="sfv_history_207535257",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "207535257", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_sfv_history_build,
    verify_fn=_sfv_history_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: following  (ssoId=9999, following > 1 person)
#   • merge_page ใช้ structure แบบ flat: result.ids = ["id1", "id2", ...]
#   • redis_get_user_following → ต้องมีคนที่ follow อยู่ (ไม่ว่าง)
#   • score มาจาก tf_serving_sfv.result.results (parallel กับ .targetIds)
#   • T1  following_ids ต้องไม่ว่าง
#   • T2  merge_page ต้องมี item id อย่างน้อย 1 ตัวข้ามทุก cursor
#   • T3  ไม่มี item id ซ้ำข้ามคนละ cursor (cross-cursor uniqueness)
#   • T4  merge_page.result.ids ต้องเรียงตาม tf_serving_sfv score จากมากไปน้อย
# ══════════════════════════════════════════════════════════════════════════════
def _following_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg                              = scenario.config
    all_ids_flat:      List[str]     = []
    ids_per_cursor:    Dict[int, List[str]] = {}
    score_per_cursor:  Dict[int, Dict[str, float]] = {}   # cursor → {id: score}
    final_following_ids              = []

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        # following ids
        following_node = find_key(body, "redis_get_user_following")
        if following_node:
            final_following_ids = extract_ids(following_node)

        # merge_page ids (flat)
        node        = find_key(body, cfg.source_node)
        current_ids = extract_ids(node)
        ids_per_cursor[cursor] = current_ids
        all_ids_flat.extend(current_ids)

        # tf_serving_sfv scores
        sfv_node = find_key(body, "tf_serving_sfv")
        if sfv_node and "result" in sfv_node:
            sfv_result  = sfv_node["result"]
            target_ids  = sfv_result.get("targetIds", [])
            scores      = sfv_result.get("results", [])
            score_per_cursor[cursor] = {
                tid: float(s) for tid, s in zip(target_ids, scores)
            }
            step(f"cursor={cursor} tf_serving_sfv scores", len(score_per_cursor[cursor]))

        step(f"cursor={cursor} merge_page ids", current_ids)
        ok(f"cursor={cursor} → captured {len(current_ids)} ids")

        if not current_ids:
            ok(f"cursor={cursor} → merge_page.ids empty, stopping")
            break

    cases = []
    if final_following_ids or all_ids_flat:
        cases.append({
            "scenario":        scenario.name,
            "cursor":          "all",
            "following_ids":   final_following_ids,
            "all_ids_flat":    all_ids_flat,
            "ids_per_cursor":  ids_per_cursor,
            "score_per_cursor": score_per_cursor,
            "test_id":         f"{scenario.name}::all_cursors",
        })
        ok(f"→ 1 aggregated test case "
           f"(following={len(final_following_ids)}, total ids={len(all_ids_flat)}, "
           f"cursors={len(ids_per_cursor)}, cursors_with_scores={len(score_per_cursor)})")

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _following_verify(tc: Dict[str, Any]):
    following_ids   = tc["following_ids"]
    all_ids_flat    = tc["all_ids_flat"]
    ids_per_cursor  = tc["ids_per_cursor"]
    score_per_cursor = tc["score_per_cursor"]

    # ── pre-compute T1-T3 ─────────────────────────────────────────────────────
    id_to_cursors: Dict[str, List[int]] = {}
    for cursor, ids in ids_per_cursor.items():
        for id_ in ids:
            id_to_cursors.setdefault(id_, []).append(cursor)
    dupes = {id_: c for id_, c in id_to_cursors.items() if len(c) > 1}

    t1_pass = bool(following_ids)
    t2_pass = len(all_ids_flat) > 0
    t3_pass = not dupes

    # ── pre-compute T4: score ordering per cursor ─────────────────────────────
    t4_failures: List[Dict] = []   # [{cursor, id_a, score_a, id_b, score_b}]
    for cursor, ids in ids_per_cursor.items():
        id_to_score = score_per_cursor.get(cursor, {})
        if not id_to_score:
            continue
        scored = [(id_, id_to_score[id_]) for id_ in ids if id_ in id_to_score]
        for j in range(len(scored) - 1):
            id_a, score_a = scored[j]
            id_b, score_b = scored[j + 1]
            if score_a < score_b:   # ถ้า score ต่อไปสูงกว่า = เรียงผิด
                t4_failures.append({
                    "cursor": cursor, "pos": j,
                    "id_a": id_a, "score_a": score_a,
                    "id_b": id_b, "score_b": score_b,
                })
    t4_pass = not t4_failures

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  all cursors  (flat result.ids)")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("following_ids  (redis_get_user_following)", fmt_ids(following_ids))
    ctx("total ids in feed",      f"{len(all_ids_flat)} items  across {len(ids_per_cursor)} cursors")
    ctx("cursors with sfv scores", len(score_per_cursor))

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1", "redis_get_user_following ต้องไม่ว่าง")
    det("result", f"{len(following_ids)} following users" if t1_pass else "empty — expected at least 1")
    assert t1_pass, f"[{tc['scenario']}] T1 FAIL — following_ids is empty"

    # ── T2 ───────────────────────────────────────────────────────────────────
    t_block(t2_pass, "T2", "feed ต้องมี item id (content จากคนที่ follow)")
    det("total ids", f"{len(all_ids_flat)} items")
    det("cursors",   f"{len(ids_per_cursor)}")
    if not t2_pass:
        det("⚠️  result", "feed is empty across all cursors")
    assert t2_pass, f"[{tc['scenario']}] T2 FAIL — merge_page returned 0 ids"

    # ── T3 ───────────────────────────────────────────────────────────────────
    t_block(t3_pass, "T3", "ไม่มี id ซ้ำข้ามคนละ cursor")
    det("total ids",  len(all_ids_flat))
    det("unique ids", len(id_to_cursors))
    det("duplicates", f"{len(dupes)}" + (f"  ⚠️  {list(dupes.keys())}" if dupes else ""))
    assert t3_pass, f"[{tc['scenario']}] T3 FAIL — duplicate ids: {dupes}"

    # ── T4 ───────────────────────────────────────────────────────────────────
    t_block(t4_pass, "T4",
            "merge_page.result.ids ต้องเรียงตาม tf_serving_sfv score จากมากไปน้อย")
    det("cursors checked",   len(score_per_cursor))
    det("ordering errors",   len(t4_failures))
    for f in t4_failures:
        det(f"  ⚠️  cursor {f['cursor']} pos {f['pos']}",
            f'"{f["id_a"]}" score={f["score_a"]:.6f}  >  '
            f'"{f["id_b"]}" score={f["score_b"]:.6f}  ← wrong order')
    if not score_per_cursor:
        det("note", "ไม่พบ tf_serving_sfv node ใน response — ข้ามการเช็ค score")
    assert t4_pass, (
        f"[{tc['scenario']}] T4 FAIL — score ordering errors: {t4_failures}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1"
        f"   {'✅' if t2_pass else '❌'} T2"
        f"   {'✅' if t3_pass else '❌'} T3"
        f"   {'✅' if t4_pass else '❌'} T4")
    log(SEP)


# ── Register following (ssoId=9999) ──────────────────────────────────────────
register(Scenario(
    name="following_9999",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "9999", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_following_build,
    verify_fn=_following_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: following_no_items  (ssoId=8888)
#   • follow ssoid ที่ไม่มีการสร้าง item เลย
#   • ssoid ที่ follow แต่ไม่มี item จะไม่ปรากฏใน merge_page เลย (ไม่ใช่ empty group)
#   • merge_page จะแสดง content จาก ssoid อื่น ๆ ที่ไม่ใช่ following
#   • T1  following_ids ต้องไม่ว่าง (มี following จริง)
#   • T2  ทุก group ใน merge_page ต้องมี items[] ไม่ว่าง (ไม่มี empty group)
#   • T3  following_ids ต้องไม่ปรากฏเป็น create_by_ssoid ใน merge_page เลย
#         (เพราะ followed ssoid ไม่มี item จึงต้องขาดหายไปจาก feed โดยสมบูรณ์)
# ══════════════════════════════════════════════════════════════════════════════
def _following_no_items_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg                 = scenario.config
    all_raw_groups:     List[Dict] = []   # ทุก group จาก merge_page ข้ามทุก cursor
    final_following_ids             = []

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        # ดึง following ids
        following_node = find_key(body, "redis_get_user_following")
        if following_node:
            final_following_ids = extract_ids(following_node)

        # เก็บ raw groups จาก merge_page
        node      = find_key(body, cfg.source_node)
        raw_items = node["result"].get("items", []) if node else []
        all_raw_groups.extend(raw_items)

        current_ids = _extract_grouped_ids(node)

        step(f"cursor={cursor} following_ids",       final_following_ids)
        step(f"cursor={cursor} groups in merge_page", len(raw_items))

        ok(f"cursor={cursor} → captured {len(raw_items)} groups")

        if not current_ids:
            ok(f"cursor={cursor} → merge_page.items empty, stopping")
            break

    cases = []
    if final_following_ids or all_raw_groups:
        cases.append({
            "scenario":        scenario.name,
            "cursor":          "all",
            "following_ids":   final_following_ids,
            "all_raw_groups":  all_raw_groups,
            "test_id":         f"{scenario.name}::all_cursors",
        })
        ok(f"→ 1 aggregated test case "
           f"(following={len(final_following_ids)}, total groups={len(all_raw_groups)})")

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _following_no_items_verify(tc: Dict[str, Any]):
    following_ids  = tc["following_ids"]
    all_raw_groups = tc["all_raw_groups"]

    # ── pre-compute ──────────────────────────────────────────────────────────
    empty_groups = [g.get("create_by_ssoid", "(missing)")
                    for g in all_raw_groups if not g.get("items")]
    feed_ssoids  = {g.get("create_by_ssoid", "") for g in all_raw_groups}
    leaked       = [sid for sid in following_ids if sid in feed_ssoids]

    t1_pass = bool(following_ids)
    t2_pass = not empty_groups
    t3_pass = not leaked

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  all cursors  (following ssoid has no items)")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("following_ids  (ssoids with no items)", fmt_ids(following_ids))
    ctx("merge_page groups  (all cursors)",      f"{len(all_raw_groups)} groups")
    ctx("ssoids seen in feed",                   f"{len(feed_ssoids)} unique ssoids")

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1", "redis_get_user_following ต้องไม่ว่าง")
    det("result", f"{len(following_ids)} following users" if t1_pass else "empty — expected at least 1")
    assert t1_pass, f"[{tc['scenario']}] T1 FAIL — following_ids is empty"

    # ── T2 ───────────────────────────────────────────────────────────────────
    t_block(t2_pass, "T2", "ทุก group ใน merge_page ต้องมี items[] ไม่ว่าง")
    det("groups checked",  len(all_raw_groups))
    det("empty groups",    f"{len(empty_groups)}" + (f"  ⚠️  {empty_groups}" if empty_groups else ""))
    assert t2_pass, (
        f"[{tc['scenario']}] T2 FAIL — groups with empty items: {empty_groups}"
    )

    # ── T3 ───────────────────────────────────────────────────────────────────
    t_block(t3_pass, "T3",
            "followed ssoid ต้องไม่ปรากฏใน feed เลย (ไม่มี item → ต้องขาดหายไปสมบูรณ์)")
    det("following ssoids",     len(following_ids))
    det("leaked into feed",     f"{len(leaked)}" + (f"  ⚠️  {leaked}" if leaked else ""))
    det("correctly absent",     len(following_ids) - len(leaked))
    assert t3_pass, (
        f"[{tc['scenario']}] T3 FAIL — followed ssoids appeared in feed: {leaked}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1"
        f"   {'✅' if t2_pass else '❌'} T2"
        f"   {'✅' if t3_pass else '❌'} T3")
    log(SEP)


# ── Register following_no_items (ssoId=8888) ─────────────────────────────────
register(Scenario(
    name="following_no_items_8888",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "8888", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_following_no_items_build,
    verify_fn=_following_no_items_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: following_exhausted  (ssoId=7777)
#   • call ครั้งที่ 1 (cursor=1): ดึง merge_page ids เทียบกับ metadata
#   • call ครั้งที่ 2 (cursor=1 เดิม): ids จาก call 1 ถูก track ใน seen แล้ว
#     → merge_page fallback เป็น grouped structure
#     → seen node มี ids จาก call 1
#   • T1  merge_page ids จาก call 1 ต้องเป็น subset ของ metadata ids
#   • T2  call 2 (cursor=1 ซ้ำ) ต้องแสดง fallback grouped structure ที่ถูกต้อง
#   • T3  ids จาก call 1 ต้องปรากฏใน seen node ของ call 2
# ══════════════════════════════════════════════════════════════════════════════
def _following_exhausted_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg = scenario.config

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    # ── Call 1 (cursor=1) — ดึง following_ids + merge_page ids ───────────────
    step("Call 1: cursor=1 (first time)")
    try:
        body_call1     = call_api(cfg.endpoint, {**cfg.base_params, "cursor": 1})
        following_node = find_key(body_call1, "redis_get_user_following")
        following_ids  = extract_ids(following_node) if following_node else []
        node_call1     = find_key(body_call1, cfg.source_node)
        first_ids      = extract_ids(node_call1)
        if not first_ids:
            first_ids = _extract_grouped_ids(node_call1)
    except Exception as e:
        warn(f"Call 1 failed: {e}")
        return []

    step("following_ids", following_ids)
    step("merge_page ids (call 1)", first_ids)

    if not following_ids:
        warn("following_ids is empty — stopping")
        return []

    # ── Metadata API — ดึง ids ทั้งหมดของ followed ssoid ────────────────────
    step("Fetching metadata from sfv_following_chanel")
    try:
        metadata_ids = call_metadata_following(following_ids)
    except Exception as e:
        warn(f"Metadata API failed: {e}")
        metadata_ids = []

    step("metadata_ids count", len(metadata_ids))

    # ── Call 2 (cursor=1 ซ้ำ) — เก็บ seen_ids + fallback groups ─────────────
    step("Call 2: cursor=1 (same request, repeated)")
    try:
        body_call2  = call_api(cfg.endpoint, {**cfg.base_params, "cursor": 1})
        node_call2  = find_key(body_call2, cfg.source_node)
        seen_node2  = find_key(body_call2, "redis_get_seen_item")
        seen_ids2   = extract_ids(seen_node2) if seen_node2 else []
        raw_items2  = node_call2["result"].get("items", []) if node_call2 else []
        fallback_groups = [g for g in raw_items2 if "create_by_ssoid" in g]
        transitioned    = bool(fallback_groups)
    except Exception as e:
        warn(f"Call 2 failed: {e}")
        seen_ids2, fallback_groups, transitioned = [], [], False

    step("seen_ids (call 2)", len(seen_ids2))
    step("fallback groups (call 2)", len(fallback_groups))
    ok(f"call 2 → {'fallback ✓' if transitioned else 'no fallback detected'}")

    cases = []
    if following_ids:
        cases.append({
            "scenario":        scenario.name,
            "cursor":          "call1_call2",
            "following_ids":   following_ids,
            "metadata_ids":    metadata_ids,
            "first_ids":       first_ids,
            "seen_ids2":       seen_ids2,
            "fallback_groups": fallback_groups,
            "transitioned":    transitioned,
            "test_id":         f"{scenario.name}::all_cursors",
        })
        ok(f"→ 1 test case  (first_ids={len(first_ids)}, metadata={len(metadata_ids)}, "
           f"seen_call2={len(seen_ids2)}, fallback_groups={len(fallback_groups)})")

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _following_exhausted_verify(tc: Dict[str, Any]):
    following_ids   = tc["following_ids"]
    metadata_ids    = tc["metadata_ids"]
    first_ids       = tc["first_ids"]
    seen_ids2       = tc["seen_ids2"]
    fallback_groups = tc["fallback_groups"]
    transitioned    = tc["transitioned"]

    # ── pre-compute ──────────────────────────────────────────────────────────
    metadata_set    = set(metadata_ids)
    seen_set2       = set(seen_ids2)
    not_in_metadata = [i for i in first_ids if i not in metadata_set]
    not_in_seen     = [i for i in first_ids if i not in seen_set2]

    bad_structure, empty_groups = [], []
    for group in fallback_groups:
        ssoid     = group.get("create_by_ssoid", "")
        sub_items = group.get("items", [])
        if not ssoid:       bad_structure.append(group)
        elif not sub_items: empty_groups.append(ssoid)

    t1_pass = not not_in_metadata
    t2_pass = transitioned and not bad_structure and not empty_groups
    t3_pass = not not_in_seen

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  call 1 vs metadata  +  call 2 (same cursor) seen check")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("following_ids",                        fmt_ids(following_ids))
    ctx("metadata_ids  (sfv_following_chanel)",  f"{len(metadata_ids)} items")
    ctx("merge_page ids  (call 1, cursor=1)",    fmt_ids(first_ids))
    ctx("seen_ids  (call 2, cursor=1 repeated)", f"{len(seen_ids2)} items")
    ctx("fallback groups  (call 2)",             f"{len(fallback_groups)} groups")

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1",
            "merge_page ids จาก call 1 ต้องเป็น subset ของ metadata ids")
    det("call 1 ids",        fmt_ids(first_ids))
    det("metadata ids",      f"{len(metadata_ids)} items")
    det("found in metadata", f"{len(first_ids) - len(not_in_metadata)} / {len(first_ids)}")
    det("not in metadata",   f"{len(not_in_metadata)}"
                             + (f"  ⚠️  {not_in_metadata}" if not_in_metadata else ""))
    assert t1_pass, (
        f"[{tc['scenario']}] T1 FAIL — ids not in metadata: {not_in_metadata}"
    )

    # ── T2 ───────────────────────────────────────────────────────────────────
    t_block(t2_pass, "T2",
            "call 2 (cursor=1 ซ้ำ) ต้อง fallback เป็น grouped structure (create_by_ssoid + items)")
    det("transitioned",   "yes ✓" if transitioned else "no ✗  ⚠️  no fallback groups found")
    det("groups checked", len(fallback_groups))
    det("bad structure",  f"{len(bad_structure)}" + (f"  ⚠️  {bad_structure}" if bad_structure else ""))
    det("empty items",    f"{len(empty_groups)}" + (f"  ⚠️  {empty_groups}" if empty_groups else ""))
    assert transitioned, (
        f"[{tc['scenario']}] T2 FAIL — call 2 did not return fallback grouped structure"
    )
    assert not bad_structure, (
        f"[{tc['scenario']}] T2 FAIL — groups missing create_by_ssoid: {bad_structure}"
    )
    assert not empty_groups, (
        f"[{tc['scenario']}] T2 FAIL — groups with empty items: {empty_groups}"
    )

    # ── T3 ───────────────────────────────────────────────────────────────────
    t_block(t3_pass, "T3",
            "ids จาก call 1 ต้องอยู่ใน seen node ของ call 2 (cursor=1 ซ้ำ)")
    det("call 1 ids",      fmt_ids(first_ids))
    det("found in seen",   f"{len(first_ids) - len(not_in_seen)} / {len(first_ids)}")
    det("missing from seen", f"{len(not_in_seen)}"
                             + (f"  ⚠️  {not_in_seen}" if not_in_seen else ""))
    assert t3_pass, (
        f"[{tc['scenario']}] T3 FAIL — ids missing from seen: {not_in_seen}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1"
        f"   {'✅' if t2_pass else '❌'} T2"
        f"   {'✅' if t3_pass else '❌'} T3")
    log(SEP)


# ── Register following_exhausted (ssoId=7777) ────────────────────────────────
register(Scenario(
    name="following_exhausted_7777",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "7777", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_following_exhausted_build,
    verify_fn=_following_exhausted_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Scenario: all_cursor_dup  (duplicate check across all cursors)
#   • paginate ทุก cursor แล้วสะสม ids จาก source_node
#   • T1  ไม่มี id ซ้ำข้ามคนละ cursor (cross-cursor uniqueness)
# ══════════════════════════════════════════════════════════════════════════════
def _extract_node_ids(node) -> List[str]:
    """Auto-detect — ลอง grouped structure ก่อน ถ้าว่างค่อย fallback flat"""
    grouped = _extract_grouped_ids(node)
    if grouped:
        return grouped
    return extract_ids(node)


def _all_cursor_dup_build(scenario: Scenario) -> List[Dict[str, Any]]:
    cfg            = scenario.config
    ids_per_cursor: Dict[int, List[str]] = {}
    all_ids_flat:   List[str]            = []

    section(f"[{scenario.name}] Building test cases — source node: {cfg.source_node}")

    for i in range(cfg.max_cursors):
        cursor = cfg.start_cursor + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cfg.endpoint, {**cfg.base_params, "cursor": cursor})
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        node        = find_key(body, cfg.source_node)
        current_ids = _extract_node_ids(node)

        step(f"cursor={cursor} {cfg.source_node} ids ({len(current_ids)} items)", current_ids)

        ids_per_cursor[cursor] = current_ids
        all_ids_flat.extend(current_ids)

        ok(f"cursor={cursor} → accumulated total: {len(all_ids_flat)}")

        if not current_ids:
            ok(f"cursor={cursor} → {cfg.source_node}.items empty, stopping")
            break

    cases = []
    if ids_per_cursor:
        cases.append({
            "scenario":        scenario.name,
            "cursor":          "all",
            "ids_per_cursor":  ids_per_cursor,
            "all_ids_flat":    all_ids_flat,
            "test_id":         f"{scenario.name}::all_cursors_dup_check",
        })
        ok(f"→ 1 aggregated test case "
           f"(total ids={len(all_ids_flat)} across {len(ids_per_cursor)} cursors)")

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


def _all_cursor_dup_verify(tc: Dict[str, Any]):
    ids_per_cursor = tc["ids_per_cursor"]
    all_ids_flat   = tc["all_ids_flat"]

    # ── pre-compute ──────────────────────────────────────────────────────────
    id_to_cursors: Dict[str, List[int]] = {}
    for cursor, ids in ids_per_cursor.items():
        for id_ in ids:
            id_to_cursors.setdefault(id_, []).append(cursor)
    dupes   = {id_: c for id_, c in id_to_cursors.items() if len(c) > 1}
    t1_pass = not dupes

    # ── header ───────────────────────────────────────────────────────────────
    section(f"TEST  {tc['scenario']}  │  all cursors  (cross-cursor duplicate check)")

    # ── context ───────────────────────────────────────────────────────────────
    log(f"\n  📊 CONTEXT")
    ctx("cursors checked",  len(ids_per_cursor))
    ctx("total ids",        len(all_ids_flat))
    ctx("unique ids",       len(id_to_cursors))

    # ── T1 ───────────────────────────────────────────────────────────────────
    t_block(t1_pass, "T1", "ไม่มี id ซ้ำข้ามคนละ cursor")
    det("total ids",   len(all_ids_flat))
    det("unique ids",  len(id_to_cursors))
    det("duplicates",  f"{len(dupes)}" + (f"  ⚠️  {list(dupes.keys())}" if dupes else ""))
    if dupes:
        for id_, cursors in dupes.items():
            det(f'  "{id_}"', f"appears in cursors {cursors}")
    assert t1_pass, (
        f"[{tc['scenario']} cursor=all] "
        f"T1 FAIL — duplicate ids found: {dupes}"
    )

    # ── result summary ────────────────────────────────────────────────────────
    log(f"\n  {DIV}")
    log(f"  RESULT   {'✅' if t1_pass else '❌'} T1")
    log(SEP)


# ── Register all_cursor_dup (ssoId=1913 — seen_pool user) ────────────────────
register(Scenario(
    name="all_cursor_dup_1913",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "1913", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_all_cursor_dup_build,
    verify_fn=_all_cursor_dup_verify,
))

# ── Register all_cursor_dup (ssoId=207535257 — sfv_history user) ─────────────
register(Scenario(
    name="all_cursor_dup_207535257",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "207535257", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_all_cursor_dup_build,
    verify_fn=_all_cursor_dup_verify,
))

# ── Register all_cursor_dup (ssoId=1714 — non_following user) ────────────────
register(Scenario(
    name="all_cursor_dup_1714",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "1714", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_all_cursor_dup_build,
    verify_fn=_all_cursor_dup_verify,
))

# ── Register all_cursor_dup (ssoId=nologin) ──────────────────────────────────
register(Scenario(
    name="all_cursor_dup_nologin",
    config=ScenarioConfig(
        endpoint="/api/v1/universal/fc-p1",
        base_params={"ssoId": "nologin", "verbose": "debug"},
        source_node="merge_page",
        start_cursor=1,
        max_cursors=50,
    ),
    build_fn=_all_cursor_dup_build,
    verify_fn=_all_cursor_dup_verify,
))


# ══════════════════════════════════════════════════════════════════════════════
# Build all test cases from all registered scenarios
# ══════════════════════════════════════════════════════════════════════════════
def _build_all() -> List[Dict[str, Any]]:
    all_cases = []
    for scenario in SCENARIO_REGISTRY:
        all_cases.extend(scenario.build_fn(scenario))
    return all_cases


_ALL_TEST_CASES = _build_all()


# ══════════════════════════════════════════════════════════════════════════════
# Pytest — parametrize + dispatch
# ══════════════════════════════════════════════════════════════════════════════
def pytest_generate_tests(metafunc):
    if "tc" in metafunc.fixturenames:
        ids = [
            c.get("test_id") or
            f"{c['scenario']}::src=cursor{c['cursor_source']}_chk=cursor{c['cursor_check']}"
            for c in _ALL_TEST_CASES
        ]
        metafunc.parametrize("tc", _ALL_TEST_CASES, ids=ids)


# ── Summary collector ─────────────────────────────────────────────────────────
_RESULTS: List[Dict[str, Any]] = []   # {"id": ..., "status": "PASS"|"FAIL", "reason": ...}


def pytest_runtest_logreport(report):
    """เก็บผล pass/fail ของแต่ละ test case"""
    if report.when != "call":
        return
    status = "PASS" if report.passed else "FAIL"
    reason = ""
    if report.failed and report.longrepr:
        # ดึงเฉพาะบรรทัดสุดท้ายของ assert message
        lines  = str(report.longrepr).strip().splitlines()
        reason = next((l.strip() for l in reversed(lines) if l.strip()), "")
    _RESULTS.append({"id": report.nodeid.split("::")[-1], "status": status, "reason": reason})


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Print summary table หลัง pytest รันเสร็จ"""
    if not _RESULTS:
        return

    passed = [r for r in _RESULTS if r["status"] == "PASS"]
    failed = [r for r in _RESULTS if r["status"] == "FAIL"]

    print(f"\n{SEP}")
    print(f"  TEST SUMMARY  ({len(passed)} passed / {len(failed)} failed / {len(_RESULTS)} total)")
    print(SEP)

    for r in _RESULTS:
        icon  = "✅" if r["status"] == "PASS" else "❌"
        label = f"  {icon} {r['status']}  {r['id']}"
        print(label)
        if r["reason"]:
            print(f"          ↳ {r['reason']}")

    print(SEP)


class TestAllScenarios:

    def test_verify(self, tc):
        """Dispatch ไปยัง verify_fn ของแต่ละ scenario โดยอัตโนมัติ"""
        scenario = next(s for s in SCENARIO_REGISTRY if s.name == tc["scenario"])
        scenario.verify_fn(tc)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))
