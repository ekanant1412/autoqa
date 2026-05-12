import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9584"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
        ),
    },
    {
        "name": "sfv-p6",
        "url": (
            "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p6"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=99999999.99999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&limit=3"
            "&limit_seen_item=2"
        ),
    },
]
TIMEOUT = 30

FINAL_NODE = "final_result"
POOLS = [
    "candidate_latest_sfv",
    "candidate_latest_ugc_sfv",
    "candidate_tophit_sfv",
    "candidate_tophit_ugc_sfv",
]

# evidence output (ให้ conftest zip ได้)
REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)


def call(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def assert_valid_universal_response(resp: Dict[str, Any]):
    """
    Guard: ensure API returned real universal graph, not error payload.
    ถ้าเป็น error format (ไม่มี data/results) จะ fail แบบอ่านรู้เรื่อง
    """
    if not isinstance(resp, dict):
        raise AssertionError("Response is not JSON object")

    # service error payload มักมี keys แบบ {status, message, items}
    if "data" not in resp:
        msg = resp.get("message", "unknown error")
        raise AssertionError(f"Universal API error response (no data): {msg}")

    data = resp.get("data")
    if not isinstance(data, dict):
        raise AssertionError("Response.data is not dict")

    if "results" not in data or not isinstance(data.get("results"), dict):
        # บางที error อาจซ่อนอยู่ใน message
        msg = resp.get("message") or data.get("message") or "missing data.results"
        raise AssertionError(f"Invalid universal response (missing data.results): {msg}")


def deep_find_node(obj: Any, target: str) -> Optional[Dict[str, Any]]:
    """Find node dict by key name or by {'name': target} recursively."""
    if isinstance(obj, dict):
        if obj.get("name") == target:
            return obj
        if target in obj and isinstance(obj[target], dict):
            return obj[target]
        for v in obj.values():
            found = deep_find_node(v, target)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = deep_find_node(v, target)
            if found:
                return found
    return None


def extract_final_ids(resp: Dict[str, Any]) -> List[str]:
    node = deep_find_node(resp, FINAL_NODE)
    if not node:
        raise AssertionError(f"Cannot find node '{FINAL_NODE}'")

    res = node.get("result", {})
    ids = res.get("ids")
    if not isinstance(ids, list):
        raise AssertionError(f"Node '{FINAL_NODE}' has no result.ids list")

    out = [x for x in ids if isinstance(x, str) and x.strip()]
    if not out:
        raise AssertionError("final_result.ids is empty")
    return out


def extract_ids_from_any_node(node: Dict[str, Any]) -> Set[str]:
    """
    รองรับหลายรูปแบบ:
    1) node.result.ids: [str]
    2) node.result.items: [{id:...}] หรือ [str]
    3) ES aggregations/hits ใน node.result.data
    """
    if not isinstance(node, dict):
        return set()

    res = node.get("result")
    if not isinstance(res, dict):
        return set()

    # 1) result.ids
    ids = res.get("ids")
    if isinstance(ids, list):
        return {x for x in ids if isinstance(x, str) and x.strip()}

    # 2) result.items
    items = res.get("items")
    if isinstance(items, list):
        out: Set[str] = set()
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("id"), str) and it["id"].strip():
                out.add(it["id"].strip())
            elif isinstance(it, str) and it.strip():
                out.add(it.strip())
        if out:
            return out

    out: Set[str] = set()

    # 3/4) ES in result.data
    data = res.get("data")
    if isinstance(data, dict):
        # hits.hits
        hits = data.get("hits")
        if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
            for h in hits["hits"]:
                if not isinstance(h, dict):
                    continue
                src = h.get("_source")
                if isinstance(src, dict) and isinstance(src.get("id"), str) and src["id"].strip():
                    out.add(src["id"].strip())
                elif isinstance(h.get("_id"), str) and h["_id"].strip():
                    _id = h["_id"].strip()
                    out.add(_id.split("th-")[-1] if _id.startswith("th-") else _id)

        # aggregations.*.buckets.*.hits.hits[]._source.id
        aggs = data.get("aggregations")
        if isinstance(aggs, dict):
            for agg_obj in aggs.values():
                if not isinstance(agg_obj, dict):
                    continue
                buckets = agg_obj.get("buckets")
                if not isinstance(buckets, list):
                    continue

                for b in buckets:
                    if not isinstance(b, dict):
                        continue

                    for v in b.values():
                        if not isinstance(v, dict):
                            continue
                        hh = v.get("hits")
                        if not isinstance(hh, dict):
                            continue
                        hits_list = hh.get("hits")
                        if not isinstance(hits_list, list):
                            continue
                        for h in hits_list:
                            if isinstance(h, dict):
                                src = h.get("_source", {})
                                if isinstance(src, dict) and isinstance(src.get("id"), str) and src["id"].strip():
                                    out.add(src["id"].strip())

    return out


def run_check(placement: Dict[str, Any]) -> Dict[str, Any]:
    name = placement["name"]
    url = placement["url"]

    art_dir = f"{REPORT_DIR}/{TEST_KEY}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_json_path = f"{art_dir}/universal_response_{ts}.json"
    summary_json_path = f"{art_dir}/pool_breakdown_summary_{ts}.json"
    log_path = f"{art_dir}/run_{ts}.log"

    def _log(msg: str):
        line = f"{datetime.now().isoformat(timespec='seconds')} | {msg}"
        print(line)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    _log(f"TEST={TEST_KEY} placement={name}")
    _log(f"URL={url}")

    try:
        resp = call(url)
    except Exception as e:
        return {"placement": name, "status": "ERROR", "error": str(e)}

    # save raw response for evidence
    with open(raw_json_path, "w", encoding="utf-8") as f:
        json.dump(resp, f, ensure_ascii=False, indent=2)
    _log(f"Saved raw response: {raw_json_path}")

    # ✅ Guard: ถ้า response เป็น error payload จะ fail ตรงนี้
    try:
        assert_valid_universal_response(resp)
    except AssertionError as e:
        return {"placement": name, "status": "ERROR", "error": str(e)}

    try:
        final_ids = extract_final_ids(resp)
    except AssertionError as e:
        return {"placement": name, "status": "ERROR", "error": str(e)}

    _log(f"final_result.ids={len(final_ids)}")

    pool_sets: Dict[str, Set[str]] = {}
    for p in POOLS:
        node = deep_find_node(resp, p)
        pool_sets[p] = extract_ids_from_any_node(node) if node else set()
        _log(f"pool={p} extracted_ids={len(pool_sets[p])}")

    id_to_pools = {fid: [p for p, s in pool_sets.items() if fid in s] for fid in final_ids}
    unknown = [fid for fid, src in id_to_pools.items() if not src]

    allowed_unknown = int(len(final_ids) * 0.05)  # <= 5%
    errors = []
    if len(final_ids) == 0:
        errors.append("final_result.ids is empty")
    if len(unknown) > allowed_unknown:
        errors.append(f"Too many UNKNOWN: {len(unknown)}/{len(final_ids)} (allowed={allowed_unknown})")

    status = "PASS" if not errors else "FAIL"

    summary = {
        "test_key": TEST_KEY,
        "placement": name,
        "status": status,
        "final_count": len(final_ids),
        "unknown_count": len(unknown),
        "allowed_unknown": allowed_unknown,
        "unknown_sample": unknown[:20],
        "errors": errors,
        "count_by_pool": {
            p: sum(1 for fid in final_ids if p in id_to_pools[fid]) for p in POOLS
        },
    }

    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    _log(f"[{name}] {status}: final={len(final_ids)} unknown={len(unknown)}")
    _log(f"Saved summary: {summary_json_path}")

    return summary


def _assert_result(summary: Dict[str, Any]):
    assert summary.get("status") != "ERROR", (
        f"[{summary.get('placement')}] error: {summary.get('error')}"
    )
    assert not summary["errors"], (
        f"[{summary['placement']}] check failed:\n" + "\n".join(summary["errors"])
    )


def test_verify_final_result_all_cdd_sfv_p7():
    """DMPREC-9584 sfv-p7: final_result ids traced to known pools"""
    _assert_result(run_check(PLACEMENTS[0]))


def test_verify_final_result_all_cdd_sfv_p6():
    """DMPREC-9584 sfv-p6: final_result ids traced to known pools"""
    _assert_result(run_check(PLACEMENTS[1]))


def test_verify_final_result_all_cdd():
    """รันทั้ง p7 + p6 ในครั้งเดียว"""
    test_verify_final_result_all_cdd_sfv_p7()
    test_verify_final_result_all_cdd_sfv_p6()


if __name__ == "__main__":
    for pl in PLACEMENTS:
        result = run_check(pl)
        if result.get("status") == "ERROR":
            print(f"  error: {result.get('error')}")
