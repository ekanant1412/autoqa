import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9584"

URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7"
    "?shelfId=Kaw6MLVzPWmo"
    "&total_candidates=200"
    "&pool_limit_category_items=50"
    "&language=th"
    "&limit=100"
    "&userId=null"
    "&pseudoId=null"
    "&cursor=1"
    "&ga_id=100118391.0851155978"
    "&ssoId=22092422"
    "&is_use_live=true"
    "&verbose=debug"
)
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
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

TS = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_JSON_PATH = f"{ART_DIR}/universal_response_{TS}.json"
SUMMARY_JSON_PATH = f"{ART_DIR}/pool_breakdown_summary_{TS}.json"
LOG_PATH = f"{ART_DIR}/run_{TS}.log"


def tlog(msg: str):
    line = f"{datetime.now().isoformat(timespec='seconds')} | {msg}"
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def call(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=TIMEOUT)
    tlog(f"HTTP={r.status_code}")
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


def run_check() -> Dict[str, Any]:
    tlog(f"TEST={TEST_KEY}")
    tlog(f"URL={URL}")

    resp = call(URL)

    # save raw response for evidence
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(resp, f, ensure_ascii=False, indent=2)
    tlog(f"Saved raw response: {RAW_JSON_PATH}")

    # ✅ Guard: ถ้า response เป็น error payload จะ fail ตรงนี้
    assert_valid_universal_response(resp)

    final_ids = extract_final_ids(resp)
    tlog(f"final_result.ids={len(final_ids)}")

    pool_sets: Dict[str, Set[str]] = {}
    for p in POOLS:
        node = deep_find_node(resp, p)
        pool_sets[p] = extract_ids_from_any_node(node) if node else set()
        tlog(f"pool={p} extracted_ids={len(pool_sets[p])}")

    id_to_pools = {fid: [p for p, s in pool_sets.items() if fid in s] for fid in final_ids}
    unknown = [fid for fid, src in id_to_pools.items() if not src]

    # ✅ เงื่อนไขผ่าน/ไม่ผ่าน (ปรับได้)
    assert len(final_ids) > 0
    allowed_unknown = int(len(final_ids) * 0.05)  # <= 5%
    assert len(unknown) <= allowed_unknown, f"Too many UNKNOWN: {len(unknown)}/{len(final_ids)} (allowed={allowed_unknown})"

    summary = {
        "test_key": TEST_KEY,
        "final_count": len(final_ids),
        "unknown_count": len(unknown),
        "allowed_unknown": allowed_unknown,
        "unknown_sample": unknown[:20],
        "count_by_pool": {
            p: sum(1 for fid in final_ids if p in id_to_pools[fid]) for p in POOLS
        },
    }

    with open(SUMMARY_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    tlog(f"Saved summary: {SUMMARY_JSON_PATH}")

    return summary


def test_DMPREC_9584():
    result = run_check()
    print("RESULT:", result)


if __name__ == "__main__":
    print(run_check())
