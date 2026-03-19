"""
test_sfv_p4_seen_pool.py
════════════════════════════════════════════════════════════════════════════════
Flow (sliding window):
  cursor=1  → call API → เก็บ top5 จาก logic_filter_overlap_items_pin_and_live
  cursor=2  → call API → เช็ค seen (top5 จาก cursor=1 ต้องอยู่ใน seen pool)
                        → เก็บ top5 ของ cursor=2 ต่อ
  cursor=3  → call API → เช็ค seen (top5 จาก cursor=2) ...
  หยุดเมื่อ logic_filter_overlap_items_pin_and_live.result.items ว่างเปล่า

วิธีรัน:
  pip install pytest requests
  pytest test_sfv_p4_seen_pool.py -v -s
════════════════════════════════════════════════════════════════════════════════
"""

import pytest
import requests

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL = "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
ENDPOINT = "/api/v1/universal/sfv-p4"

BASE_PARAMS = {
    "shelfId":                   "zmEXe3EQnXDk",
    "total_candidates":          400,
    "language":                  "th",
    "pool_limit_category_items": 40,
    "ssoId":                     "162",
    "userId":                    "null",
    "pseudoId":                  "null",
    "limit":                     100,
    "returnItemMetadata":        "false",
    "isOnlyId":                  "true",
    "verbose":                   "debug",
    "limit_seen_items":          200,
}

SOURCE_NODE  = "logic_filter_overlap_items_pin_and_live"  # node ที่ดึง top5
TOP_N        = 5
START_CURSOR = 1
MAX_CURSORS  = 50
TIMEOUT      = 30

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 65

def log(msg):        print(msg)
def section(t):      log(f"\n{SEP}\n  {t}\n{SEP}")
def step(l, v=None): log(f"  ▶ {l}{(': ' + str(v)) if v is not None else ''}")
def ok(l):           log(f"  ✅ {l}")
def warn(l):         log(f"  ⚠️  {l}")
def fail(l):         log(f"  ❌ {l}")


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


def call_api(cursor):
    params = {**BASE_PARAMS, "cursor": cursor}
    resp   = requests.get(f"{BASE_URL}{ENDPOINT}", params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def get_source_top5(body):
    """ดึง top5 จาก logic_filter_overlap_items_pin_and_live.result.items"""
    node = find_key(body, SOURCE_NODE)
    if not node:
        return []
    items = node.get("result", {}).get("items", [])
    return [item["id"] for item in items[:TOP_N] if "id" in item]


def get_seen_ids(body):
    """ดึง seen ids จาก get_seen_item_redis"""
    node = find_key(body, "get_seen_item_redis")
    return extract_ids(node) if node else []


# ══════════════════════════════════════════════════════════════════════════════
# Build test cases — sliding window
# ══════════════════════════════════════════════════════════════════════════════
def build_test_cases():
    """
    วน cursor ตั้งแต่ START_CURSOR:
      - request แรก  : เก็บ top5 จาก SOURCE_NODE (ยังไม่มี seen ให้เช็ค)
      - request ถัดไป: เช็ค seen จาก top5 ก่อนหน้า แล้วเก็บ top5 ใหม่ต่อ
    หยุดเมื่อ SOURCE_NODE.result.items ว่าง
    """
    cases      = []
    top5_prev  = None
    seen_limit = int(BASE_PARAMS.get("limit_seen_items", 200))

    section(f"Building test cases — source node: {SOURCE_NODE}")

    for i in range(MAX_CURSORS):
        cursor = START_CURSOR + i
        step(f"Calling cursor={cursor}")

        try:
            body = call_api(cursor)
        except Exception as e:
            warn(f"cursor={cursor} request failed: {e} — stopping")
            break

        top5_current = get_source_top5(body)
        seen_ids     = get_seen_ids(body)

        step(f"cursor={cursor} {SOURCE_NODE} top5", top5_current)
        step(f"cursor={cursor} seen_ids count",      len(seen_ids))

        if not top5_current:
            ok(f"cursor={cursor} → {SOURCE_NODE}.items empty, stopping")
            break

        if top5_prev is not None:
            cases.append({
                "cursor_source": cursor - 1,
                "cursor_check":  cursor,
                "top5_prev":     top5_prev,
                "seen_ids":      seen_ids,
                "seen_limit":    seen_limit,
            })
            ok(f"cursor={cursor} → test case added (checking top5 from cursor={cursor - 1})")
        else:
            ok(f"cursor={cursor} → first request, captured top5 only")

        top5_prev = top5_current

    log(f"\n  📋 Total test cases: {len(cases)}")
    log(SEP)
    return cases


_TEST_CASES = build_test_cases()


def pytest_generate_tests(metafunc):
    if "tc" in metafunc.fixturenames:
        ids = [
            f"src=cursor{c['cursor_source']}_chk=cursor{c['cursor_check']}"
            for c in _TEST_CASES
        ]
        metafunc.parametrize("tc", _TEST_CASES, ids=ids)


# ══════════════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════════════
class TestSeenPool:

    def test_seen_pool(self, tc):
        """
        เช็คว่า top5 จาก SOURCE_NODE ของ cursor_source
        อยู่ใน seen pool ของ cursor_check
        """
        cursor_source = tc["cursor_source"]
        cursor_check  = tc["cursor_check"]
        top5_prev     = tc["top5_prev"]
        seen_ids      = tc["seen_ids"]
        seen_limit    = tc["seen_limit"]

        section(f"TEST  source=cursor{cursor_source}  check=cursor{cursor_check}")
        step(f"top5 from {SOURCE_NODE} at cursor={cursor_source}", top5_prev)
        step(f"seen_ids at cursor={cursor_check}",                 seen_ids)
        step("seen count",                                         len(seen_ids))
        step("seen_limit",                                         seen_limit)

        tail = seen_ids[-TOP_N:] if len(seen_ids) >= TOP_N else seen_ids

        # ── T1: top5 ต้องมีอยู่ใน seen ────────────────────────────
        log(f"\n  [T1] Top {TOP_N} from cursor={cursor_source} must exist in seen at cursor={cursor_check}")
        missing = [i for i in top5_prev if i not in seen_ids]
        for i in top5_prev:
            if i in seen_ids: ok(f'"{i}" in seen ✓')
            else:             fail(f'"{i}" NOT in seen')

        assert not missing, (
            f"[src={cursor_source} chk={cursor_check}] T1 FAIL — "
            f"missing from seen: {missing}"
        )

        # ── T2: top5 ต้องอยู่ท้ายสุดตามลำดับ ──────────────────────
        log(f"\n  [T2] Top {TOP_N} must be at END of seen list in order")
        step("expected tail", top5_prev)
        step("actual tail  ", tail)
        tail_match = tail == top5_prev
        if tail_match: ok("Tail matches ✓")
        else:          fail(f"Tail mismatch!\n    expected: {top5_prev}\n    got     : {tail}")

        assert tail_match, (
            f"[src={cursor_source} chk={cursor_check}] T2 FAIL — "
            f"expected={top5_prev}, got={tail}"
        )

        # ── T3: seen ต้องไม่เกิน limit ─────────────────────────────
        log(f"\n  [T3] seen size ({len(seen_ids)}) <= limit ({seen_limit})")
        if len(seen_ids) <= seen_limit: ok(f"{len(seen_ids)} <= {seen_limit} ✓")
        else:                           fail(f"{len(seen_ids)} > {seen_limit}")

        assert len(seen_ids) <= seen_limit, (
            f"[src={cursor_source} chk={cursor_check}] T3 FAIL — "
            f"seen size {len(seen_ids)} exceeds limit {seen_limit}"
        )

        # ── T4: ไม่มี duplicate ─────────────────────────────────────
        log(f"\n  [T4] No duplicates in seen_ids")
        seen_set, dupes = set(), []
        for sid in seen_ids:
            if sid in seen_set: dupes.append(sid)
            else: seen_set.add(sid)
        if not dupes: ok("No duplicates ✓")
        else:         fail(f"Duplicates: {dupes}")

        assert not dupes, (
            f"[src={cursor_source} chk={cursor_check}] T4 FAIL — "
            f"duplicates: {dupes}"
        )

        # ── Summary ─────────────────────────────────────────────────
        log(f"\n  {'─' * 65}")
        log(f"  top5 (src cursor={cursor_source}) : {top5_prev}")
        log(f"  seen tail (chk cursor={cursor_check}) : {tail}")
        log(f"  T1 {'✅' if not missing else '❌'}  "
            f"T2 {'✅' if tail_match else '❌'}  "
            f"T3 {'✅' if len(seen_ids) <= seen_limit else '❌'}  "
            f"T4 {'✅' if not dupes else '❌'}")
        log(SEP)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))