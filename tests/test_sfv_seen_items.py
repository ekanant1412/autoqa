"""
test_sfv_seen_items.py
======================
ทดสอบ seen-item consistency ข้าม cursor สำหรับ 3 DAGs:
  sfv-p4 | sfv-p5 | sfd-p1

วิธีรัน:
    pytest test_sfv_seen_items.py --ssoId 1468                          # ทุก DAG (default)
    pytest test_sfv_seen_items.py --ssoId 1468 --dag sfv-p4             # DAG เดียว
    pytest test_sfv_seen_items.py --ssoId 1468 --dag sfv-p4,sfv-p5     # เลือกหลาย DAG
    pytest test_sfv_seen_items.py --ssoId 1468 --dag all --max-cursors 10 -v

Evidence output:
    reports/sfv_seen_items/sso{ID}_{timestamp}/
        {dag}/
            summary.json
            cursor_1_evidence.json
            cursor_2_evidence.json
            ...

Logic ที่ตรวจสอบ (ทุก DAG, ทุก cursor):
    [Check 1] set_seen_items_redis.result.ids
              == logic_filter_overlap_items_pin_and_live.result.items[:5] (ยกเว้น pin_*/live_*)
               ∪ insert_relevance_creator_candidates.result.reservedPositions (ยกเว้น pin_*/live_*)

    cursor >= 2:
    [Check 2] get_seen_item_redis.result.items[].id
              == set_seen_items_redis.ids ของ cursor ก่อนหน้า

    [Check 3] ( merge_seen_and_prev_page_items.result.items[].id
               ∪ logic_filter_overlap_items_pin_and_live.result.items[:5]
               ∪ insert_relevance_creator_candidates.result.reservedPositions (ยกเว้น pin_*/live_*) )
              == set_seen_items_redis.result.ids
"""

import json
import os
import pytest
import requests
from datetime import datetime
from typing import Optional

# ──────────────────────────── DAG config ──────────────────────────────────────

_HOST = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn"
    ".int-ai-platform.gcp.dmp.true.th/api/v1/universal"
)

DEFAULT_PARAMS = {
    "shelfId": "zmEXe3EQnXDk",
    "total_candidates": 400,
    "language": "th",
    "pool_limit_category_items": 40,
    "userId": "null",
    "pseudoId": "null",
    "returnItemMetadata": "false",
    "isOnlyId": "true",
    "verbose": "debug",
    "limit": 20,
}

DAG_CONFIGS: dict[str, dict] = {
    "sfv-p4": {"base_url": f"{_HOST}/sfv-p4", "params": DEFAULT_PARAMS},
    "sfv-p5": {"base_url": f"{_HOST}/sfv-p5", "params": DEFAULT_PARAMS},
    "sfd-p1": {"base_url": f"{_HOST}/sfd-p1", "params": DEFAULT_PARAMS},
}

ALL_DAGS = list(DAG_CONFIGS.keys())


def _parse_dag_option(raw: Optional[str]) -> list[str]:
    """'all' หรือ None → ทุก DAG, 'sfv-p4,sfd-p1' → ['sfv-p4','sfd-p1']"""
    if not raw or raw.strip().lower() == "all":
        return list(ALL_DAGS)
    dags = [d.strip() for d in raw.split(",") if d.strip()]
    unknown = [d for d in dags if d not in DAG_CONFIGS]
    if unknown:
        raise ValueError(f"Unknown DAG(s): {unknown}. Valid: {ALL_DAGS}")
    return dags


# ──────────────────────────── helpers ─────────────────────────────────────────

def fetch_cursor(dag_name: str, sso_id: str, cursor: int) -> dict:
    cfg = DAG_CONFIGS[dag_name]
    params = {**cfg["params"], "ssoId": sso_id, "cursor": cursor}
    resp = requests.get(cfg["base_url"], params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


_KNOWN_NODES = {
    "logic_filter_overlap_items_pin_and_live",
    "insert_relevance_creator_candidates",
    "set_seen_items_redis",
    "get_seen_item_redis",
    "merge_seen_and_prev_page_items",
    "get_final_result_redis",
}


def _is_nodes_dict(d: dict) -> bool:
    return bool(_KNOWN_NODES & d.keys())


def find_nodes(data: dict) -> Optional[dict]:
    candidate_paths = [
        ["data", "results"],
        [],
        ["nodes"],
        ["data"],
        ["data", "nodes"],
        ["debug", "nodes"],
        ["result", "nodes"],
        ["results"],
    ]
    for path in candidate_paths:
        node = data
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
        if node and isinstance(node, dict) and _is_nodes_dict(node):
            return node

    for val in data.values():
        if isinstance(val, dict):
            if _is_nodes_dict(val):
                return val
            for val2 in val.values():
                if isinstance(val2, dict) and _is_nodes_dict(val2):
                    return val2
    return None


def extract_items_ids(node: Optional[dict]) -> set:
    if not node:
        return set()
    return {
        item["id"]
        for item in node.get("result", {}).get("items", [])
        if isinstance(item, dict) and "id" in item
    }


def extract_result_ids(node: Optional[dict]) -> set:
    if not node:
        return set()
    return set(node.get("result", {}).get("ids", []))


def extract_reserved_positions_ids(node: Optional[dict]) -> set:
    if not node:
        return set()
    reserved = node.get("result", {}).get("reservedPositions", {})
    ids = set()
    for val in reserved.values():
        if not isinstance(val, str) or "_" not in val:
            continue
        if val.startswith("pin_") or val.startswith("live_"):
            continue
        ids.add(val.rsplit("_", 1)[-1])
    return ids


def extract_items_ids_limit(node: Optional[dict], limit: int = 5) -> set:
    if not node:
        return set()
    items = node.get("result", {}).get("items", [])
    return {
        item["id"]
        for item in items[:limit]
        if isinstance(item, dict) and "id" in item
    }


def _check_result(set_a: set, set_b: set) -> dict:
    passed = set_a == set_b
    return {
        "passed": passed,
        "count_expected": len(set_a),
        "count_actual":   len(set_b),
        "missing_in_actual": sorted(set_a - set_b),
        "extra_in_actual":   sorted(set_b - set_a),
    }


def diff_msg(set_a: set, set_b: set, label_a: str, label_b: str) -> str:
    lines = [f"{label_a} ({len(set_a)}) != {label_b} ({len(set_b)})"]
    missing = set_a - set_b
    extra   = set_b - set_a
    if missing:
        lines.append(f"  ขาดใน {label_b}: {sorted(missing)[:20]}")
    if extra:
        lines.append(f"  เกินใน {label_b}: {sorted(extra)[:20]}")
    return "\n".join(lines)


def _save_json(path: str, data: object):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ──────────────────────────── per-DAG cache builder ───────────────────────────

def _build_dag_cache(
    dag_name: str,
    sso_id: str,
    max_cursors: int,
    base_evidence_dir: str,
) -> dict[int, dict]:
    dag_dir = os.path.join(base_evidence_dir, dag_name)
    cache: dict[int, dict] = {}
    summary_rows = []
    api_call_count = 0

    for cursor in range(1, max_cursors + 1):
        try:
            data = fetch_cursor(dag_name, sso_id, cursor)
            api_call_count += 1
            print(f"\n[{dag_name}] API call #{api_call_count} cursor={cursor}")
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 404:
                print(f"\n[{dag_name}] cursor {cursor}: 404 — หยุด (ไม่มีข้อมูลเพิ่ม)")
                break
            pytest.fail(f"[{dag_name}] ดึงข้อมูล cursor {cursor} ล้มเหลว (HTTP {status}): {exc}")
        except Exception as exc:
            pytest.fail(f"[{dag_name}] ดึงข้อมูล cursor {cursor} ไม่สำเร็จ: {exc}")

        _save_json(os.path.join(dag_dir, f"cursor_{cursor}_raw.json"), data)

        nodes = find_nodes(data)
        assert nodes is not None, (
            f"[{dag_name}] cursor {cursor}: ไม่พบ nodes — keys: {list(data.keys())[:10]}"
        )

        logic_filter_ids  = extract_items_ids_limit(
            nodes.get("logic_filter_overlap_items_pin_and_live"), limit=5
        )
        insert_relev_ids  = extract_reserved_positions_ids(
            nodes.get("insert_relevance_creator_candidates")
        )
        set_seen_ids      = extract_result_ids(nodes.get("set_seen_items_redis"))
        get_seen_ids      = extract_items_ids(nodes.get("get_seen_item_redis"))
        merge_seen_ids    = extract_items_ids(nodes.get("merge_seen_and_prev_page_items"))
        final_result_ids  = extract_items_ids(nodes.get("get_final_result_redis"))
        current_combined  = logic_filter_ids | insert_relev_ids

        prev_set_seen_ids     = cache[cursor - 1]["set_seen_ids"]     if cursor > 1 else set()
        prev_final_result_ids = cache[cursor - 1]["final_result_ids"] if cursor > 1 else set()

        skip_check1     = bool(final_result_ids)
        check1          = None if skip_check1 else _check_result(current_combined, set_seen_ids)
        check2          = _check_result(prev_set_seen_ids, get_seen_ids) if cursor >= 2 else None
        check3_combined = merge_seen_ids | logic_filter_ids | insert_relev_ids
        check3          = _check_result(check3_combined, set_seen_ids) if cursor >= 2 else None
        check4          = _check_result(set(), get_seen_ids) if cursor == 1 else None

        overlap6        = merge_seen_ids & logic_filter_ids
        check6          = _check_result(set(), overlap6) if cursor >= 2 else None

        merge_extra          = merge_seen_ids - get_seen_ids if cursor >= 2 else set()
        check5_seen_subset   = _check_result(get_seen_ids, get_seen_ids & merge_seen_ids) if cursor >= 2 else None
        check5_extra_source  = _check_result(set(), merge_extra - final_result_ids) if cursor >= 2 else None

        evidence = {
            "dag":    dag_name,
            "sso_id": sso_id,
            "cursor": cursor,
            "nodes": {
                "logic_filter_overlap_items_pin_and_live": {
                    "ids": sorted(logic_filter_ids), "count": len(logic_filter_ids),
                },
                "insert_relevance_creator_candidates": {
                    "ids": sorted(insert_relev_ids), "count": len(insert_relev_ids),
                    "note": "ดึงจาก reservedPositions — ตัด pool_name prefix (ยกเว้น pin_*/live_*)",
                },
                "current_combined": {
                    "ids": sorted(current_combined), "count": len(current_combined),
                },
                "set_seen_items_redis": {
                    "ids": sorted(set_seen_ids), "count": len(set_seen_ids),
                },
                "get_seen_item_redis": {
                    "ids": sorted(get_seen_ids), "count": len(get_seen_ids),
                },
                "set_seen_items_redis_prev_cursor": {
                    "ids": sorted(prev_set_seen_ids), "count": len(prev_set_seen_ids),
                    "note": f"set_seen_items_redis ของ cursor {cursor - 1}" if cursor > 1 else "cursor 1 ไม่มี cursor ก่อนหน้า",
                },
                "merge_seen_and_prev_page_items": {
                    "ids": sorted(merge_seen_ids), "count": len(merge_seen_ids),
                },
                "get_final_result_redis": {
                    "ids": sorted(final_result_ids), "count": len(final_result_ids),
                },
                "check3_combined": {
                    "ids": sorted(check3_combined), "count": len(check3_combined),
                    "note": "merge_seen ∪ logic_filter(5) ∪ insert_relevance",
                },
            },
            "checks": {
                "check1_set_seen_equals_combined": {
                    "description": "set_seen.ids == logic_filter(5) ∪ insert_relevance",
                    "skipped": skip_check1,
                    "skip_reason": "get_final_result_redis มี item" if skip_check1 else None,
                    **(check1 if check1 is not None else {}),
                },
                "check2_get_seen_equals_prev_set_seen": {
                    "description": f"get_seen_redis(cursor {cursor}) == set_seen_redis(cursor {cursor - 1})",
                    "skipped": cursor < 2,
                    **(check2 if check2 is not None else {"note": "skip — cursor 1"}),
                },
                "check3_merge_seen_equals_set_seen": {
                    "description": "(merge_seen ∪ logic_filter(5) ∪ insert_relevance) == set_seen.ids",
                    "skipped": cursor < 2,
                    **(check3 if check3 is not None else {"note": "skip — cursor 1"}),
                },
                "check4_get_seen_empty_on_cursor1": {
                    "description": "get_seen_redis ต้องว่างที่ cursor 1",
                    "skipped": cursor != 1,
                    **(check4 if check4 is not None else {"note": "skip — ใช้เฉพาะ cursor 1"}),
                },
                "check5_merge_seen_composition": {
                    "description": "get_seen ⊆ merge_seen AND (merge_seen − get_seen) ⊆ final_result",
                    "skipped": cursor < 2,
                    "check5a_seen_subset":  check5_seen_subset  or {"note": "skip"},
                    "check5b_extra_source": check5_extra_source or {"note": "skip"},
                },
                "check6_merge_seen_not_in_logic_filter": {
                    "description": "merge_seen ∩ logic_filter == ∅",
                    "skipped": cursor < 2,
                    "overlap_ids": sorted(overlap6) if cursor >= 2 else [],
                    **(check6 if check6 is not None else {"note": "skip — cursor 1"}),
                },
            },
        }
        _save_json(os.path.join(dag_dir, f"cursor_{cursor}_evidence.json"), evidence)

        summary_rows.append({
            "cursor": cursor,
            "check1": "SKIP" if skip_check1 else ("PASS" if check1["passed"] else "FAIL"),
            "check2": "SKIP" if cursor < 2  else ("PASS" if check2["passed"] else "FAIL"),
            "check3": "SKIP" if cursor < 2  else ("PASS" if check3["passed"] else "FAIL"),
            "check4": "SKIP" if cursor != 1 else ("PASS" if check4["passed"] else "FAIL"),
            "check5": "SKIP" if cursor < 2  else (
                "PASS" if (check5_seen_subset["passed"] and check5_extra_source["passed"]) else "FAIL"
            ),
            "check6": "SKIP" if cursor < 2  else ("PASS" if check6["passed"] else "FAIL"),
            "combined_count":    len(current_combined),
            "set_seen_count":    len(set_seen_ids),
            "get_seen_count":    len(get_seen_ids),
            "merge_seen_count":  len(merge_seen_ids),
        })

        cache[cursor] = {
            "dag":                    dag_name,
            "sso_id":                 sso_id,
            "cursor":                 cursor,
            "logic_filter_ids":       logic_filter_ids,
            "insert_relev_ids":       insert_relev_ids,
            "set_seen_ids":           set_seen_ids,
            "get_seen_ids":           get_seen_ids,
            "merge_seen_ids":         merge_seen_ids,
            "final_result_ids":       final_result_ids,
            "current_combined":       current_combined,
            "prev_set_seen_ids":      prev_set_seen_ids,
            "check3_combined":        check3_combined,
            "skip_check1":            skip_check1,
            "prev_final_result_ids":  prev_final_result_ids,
            "overlap6":               overlap6 if cursor >= 2 else set(),
            "merge_extra":            merge_extra if cursor >= 2 else set(),
            "check5_seen_subset":     check5_seen_subset,
            "check5_extra_source":    check5_extra_source,
        }

        if not current_combined:
            break

    overall = all(
        r["check1"] in ("PASS", "SKIP")
        and r["check2"] in ("PASS", "SKIP")
        and r["check3"] in ("PASS", "SKIP")
        and r["check4"] in ("PASS", "SKIP")
        and r["check5"] in ("PASS", "SKIP")
        and r["check6"] in ("PASS", "SKIP")
        for r in summary_rows
    )
    print(f"\n[{dag_name}] total API calls = {api_call_count}")

    _save_json(
        os.path.join(dag_dir, "summary.json"),
        {
            "dag":             dag_name,
            "sso_id":          sso_id,
            "timestamp":       datetime.now().isoformat(timespec="seconds"),
            "overall_passed":  overall,
            "total_api_calls": api_call_count,
            "cursors":         summary_rows,
        },
    )
    return cache


# ──────────────────────────── session fixtures ────────────────────────────────

@pytest.fixture(scope="session")
def evidence_dir(pytestconfig):
    sso_id = pytestconfig.getoption("--ssoId") or os.environ.get("SFV_SSO_ID") or "unknown"
    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    path   = os.path.join("reports", "sfv_seen_items", f"sso{sso_id}_{ts}")
    os.makedirs(path, exist_ok=True)
    return path


@pytest.fixture(scope="session")
def cursor_cache(pytestconfig, evidence_dir):
    sso_id = pytestconfig.getoption("--ssoId") or os.environ.get("SFV_SSO_ID")
    if not sso_id:
        pytest.fail("ต้องระบุ --ssoId หรือ env var SFV_SSO_ID")

    dag_option    = pytestconfig.getoption("--dag") or os.environ.get("SFV_DAG", "all")
    max_cursors   = int(pytestconfig.getoption("--max-cursors") or os.environ.get("SFV_MAX_CURSORS", "5"))
    selected_dags = _parse_dag_option(dag_option)

    all_cache: dict[str, dict[int, dict]] = {}
    for dag_name in selected_dags:
        print(f"\n{'='*60}\n[DAG] {dag_name}\n{'='*60}")
        all_cache[dag_name] = _build_dag_cache(dag_name, sso_id, max_cursors, evidence_dir)

    return all_cache


# ──────────────────────────── dynamic parametrize ─────────────────────────────

def pytest_generate_tests(metafunc):
    if "dag_name" in metafunc.fixturenames:
        dag_option    = metafunc.config.getoption("--dag", default=None) or os.environ.get("SFV_DAG", "all")
        selected_dags = _parse_dag_option(dag_option)
        metafunc.parametrize("dag_name", selected_dags, ids=selected_dags)

    if "cursor_num" in metafunc.fixturenames:
        max_cursors = int(metafunc.config.getoption("--max-cursors", default=None) or os.environ.get("SFV_MAX_CURSORS", "5"))
        fn          = metafunc.function.__name__

        if fn in ("test_check1_set_seen_equals_combined",
                  "test_check4_get_seen_empty_on_cursor1"):
            metafunc.parametrize("cursor_num", [1], ids=["cursor-1"])

        elif fn == "test_set_seen_redis_special_and_first5":
            metafunc.parametrize(
                "cursor_num",
                list(range(1, max_cursors + 1)),
                ids=[f"cursor-{i}" for i in range(1, max_cursors + 1)],
            )

        elif fn in ("test_set_seen_redis_prev_equals_get_seen",
                    "test_merge_seen_all_items_from_prev_cursor"):
            metafunc.parametrize(
                "cursor_num",
                list(range(2, max_cursors + 1)),
                ids=[f"cursor-{i}" for i in range(2, max_cursors + 1)],
            )

        else:
            metafunc.parametrize(
                "cursor_num",
                list(range(1, max_cursors + 1)),
                ids=[f"cursor-{i}" for i in range(1, max_cursors + 1)],
            )


# ──────────────────────────── test cases ──────────────────────────────────────

class TestSfvSeenItems:

    def test_check1_set_seen_equals_combined(self, dag_name, cursor_num, cursor_cache):
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch (items หมดก่อน)")
        if data["skip_check1"]:
            pytest.skip(f"[{dag_name}] cursor {cursor_num}: get_final_result_redis มี item")
        combined = data["current_combined"]
        set_seen = data["set_seen_ids"]
        assert combined == set_seen, diff_msg(combined, set_seen,
            f"[{dag_name}] logic_filter∪insert_relevance (cursor {cursor_num})", "set_seen_items_redis")

    def test_check2_get_seen_equals_accumulated(self, dag_name, cursor_num, cursor_cache):
        if cursor_num < 2:
            pytest.skip("Check 2 เริ่มตั้งแต่ cursor 2")
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert data["prev_set_seen_ids"] == data["get_seen_ids"], diff_msg(
            data["prev_set_seen_ids"], data["get_seen_ids"],
            f"[{dag_name}] set_seen(cursor {cursor_num-1})", f"get_seen(cursor {cursor_num})")

    def test_check3_merge_seen_equals_set_seen(self, dag_name, cursor_num, cursor_cache):
        if cursor_num < 2:
            pytest.skip("Check 3 เริ่มตั้งแต่ cursor 2")
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert data["check3_combined"] == data["set_seen_ids"], diff_msg(
            data["check3_combined"], data["set_seen_ids"],
            f"[{dag_name}] merge_seen∪logic_filter∪insert_relevance (cursor {cursor_num})",
            f"set_seen_items_redis (cursor {cursor_num})")

    def test_check4_get_seen_empty_on_cursor1(self, dag_name, cursor_num, cursor_cache):
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert data["get_seen_ids"] == set(), (
            f"[{dag_name}] cursor 1: get_seen_item_redis ควรว่างเปล่า "
            f"แต่พบ {len(data['get_seen_ids'])} items")

    def test_check5_merge_seen_composition(self, dag_name, cursor_num, cursor_cache):
        if cursor_num < 2:
            pytest.skip("Check 5 เริ่มตั้งแต่ cursor 2")
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        c5a = data["check5_seen_subset"]
        c5b = data["check5_extra_source"]
        assert c5a["passed"], f"[{dag_name}] cursor {cursor_num} [5a]: {c5a['missing_in_actual'][:10]}"
        assert c5b["passed"], f"[{dag_name}] cursor {cursor_num} [5b]: {c5b['extra_in_actual'][:10]}"

    def test_check6_merge_seen_not_in_logic_filter(self, dag_name, cursor_num, cursor_cache):
        if cursor_num < 2:
            pytest.skip("Check 6 เริ่มตั้งแต่ cursor 2")
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert not data["overlap6"], (
            f"[{dag_name}] cursor {cursor_num}: พบ {len(data['overlap6'])} seen items ใน logic_filter")

    def test_set_seen_redis_special_and_first5(self, dag_name, cursor_num, cursor_cache):
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        if data["skip_check1"]:
            pytest.skip(f"[{dag_name}] cursor {cursor_num}: get_final_result_redis มี item")
        assert data["current_combined"] == data["set_seen_ids"], diff_msg(
            data["current_combined"], data["set_seen_ids"],
            f"[{dag_name}] logic_filter(5)∪insert_relevance (cursor {cursor_num})",
            f"set_seen_items_redis (cursor {cursor_num})")

    def test_set_seen_redis_prev_equals_get_seen(self, dag_name, cursor_num, cursor_cache):
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert data["prev_set_seen_ids"] == data["get_seen_ids"], diff_msg(
            data["prev_set_seen_ids"], data["get_seen_ids"],
            f"[{dag_name}] set_seen_items_redis (cursor {cursor_num-1})",
            f"get_seen_item_redis  (cursor {cursor_num})")

    def test_merge_seen_all_items_from_prev_cursor(self, dag_name, cursor_num, cursor_cache):
        data = cursor_cache.get(dag_name, {}).get(cursor_num)
        if data is None:
            pytest.skip(f"[{dag_name}] cursor {cursor_num} ไม่ถูก fetch")
        assert data["check3_combined"] == data["set_seen_ids"], diff_msg(
            data["check3_combined"], data["set_seen_ids"],
            f"[{dag_name}] merge_seen∪logic_filter(5)∪insert_relevance (cursor {cursor_num})",
            f"set_seen_items_redis (cursor {cursor_num})")
