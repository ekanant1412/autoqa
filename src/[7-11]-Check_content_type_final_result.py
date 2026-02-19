import requests
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from typing import Any, Dict, List, Optional, Set
from datetime import datetime
import os

# ===================== CONFIG =====================
URL = "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th/api/v1/universal/sfv-p7?shelfId=Kaw6MLVzPWmo&total_candidates=200&pool_limit_category_items=50&language=th&limit=100&userId=null&pseudoId=null&cursor=1&ga_id=100118391.0851155978&ssoId=22092422&is_use_live=true&verbose=debug"
TIMEOUT = 30

FINAL_NODE = "final_result"

# ใส่ชื่อ node ที่มีอยู่จริงใน response ของคุณ
POOLS = [
    "candidate_latest_sfv",
    "candidate_latest_ugc_sfv",
    "candidate_tophit_sfv",
    "candidate_tophit_ugc_sfv",
]
# ==================================================

TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"final_result_pool_breakdown_{TS}.log"

def log(msg: str):
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def call(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

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
        raise ValueError(f"Cannot find node '{FINAL_NODE}'")
    res = node.get("result", {})
    ids = res.get("ids")
    if not isinstance(ids, list):
        raise ValueError(f"Node '{FINAL_NODE}' has no result.ids list")
    return [x for x in ids if isinstance(x, str) and x]

def extract_ids_from_any_node(node: Dict[str, Any]) -> Set[str]:
    """
    รองรับหลายรูปแบบ:
    1) node.result.ids: [str]
    2) node.result.items: [{id:...}] หรือ [str]
    3) ES aggregations:
       - buckets[].sort_by_publish_date.hits.hits[]._source.id
       - buckets[].sort_by_hit_count.hits.hits[]._source.id
       - buckets[].<any>.hits.hits[]._source.id
    4) ES hits:
       - result.data.hits.hits[]._source.id
       - result.data.hits.hits[]._id (fallback)
    """
    if not isinstance(node, dict):
        return set()

    res = node.get("result")
    if not isinstance(res, dict):
        return set()

    # 1) result.ids
    ids = res.get("ids")
    if isinstance(ids, list):
        return {x for x in ids if isinstance(x, str) and x}

    # 2) result.items
    items = res.get("items")
    if isinstance(items, list):
        out: Set[str] = set()
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("id"), str):
                out.add(it["id"])
            elif isinstance(it, str) and it:
                out.add(it)
        if out:
            return out

    out: Set[str] = set()

    # ES in result.data
    data = res.get("data")
    if isinstance(data, dict):
        # 4) hits.hits
        hits = data.get("hits")
        if isinstance(hits, dict) and isinstance(hits.get("hits"), list):
            for h in hits["hits"]:
                if not isinstance(h, dict):
                    continue
                src = h.get("_source")
                if isinstance(src, dict) and isinstance(src.get("id"), str):
                    out.add(src["id"])
                elif isinstance(h.get("_id"), str) and h["_id"]:
                    _id = h["_id"]
                    out.add(_id.split("th-")[-1] if _id.startswith("th-") else _id)

        # 3) aggregations.*.buckets
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

                    # common keys
                    for key in ("sort_by_publish_date", "sort_by_hit_count"):
                        v = b.get(key)
                        if isinstance(v, dict):
                            hh = v.get("hits")
                            if isinstance(hh, dict) and isinstance(hh.get("hits"), list):
                                for h in hh["hits"]:
                                    if isinstance(h, dict):
                                        src = h.get("_source", {})
                                        if isinstance(src, dict) and isinstance(src.get("id"), str):
                                            out.add(src["id"])

                    # fallback: scan any dict in bucket that contains hits.hits
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
                                if isinstance(src, dict) and isinstance(src.get("id"), str):
                                    out.add(src["id"])

    return out

if __name__ == "__main__":
    log("=== FINAL RESULT -> POOL BREAKDOWN ===")
    log(f"URL: {URL}")
    log(f"Log file: {os.path.abspath(LOG_FILE)}\n")

    # 1) ยิง API เพื่อดึง response
    resp = call(URL)

    # 2) final ids
    final_ids = extract_final_ids(resp)
    log(f"final_result.ids: {len(final_ids)}")

    # 3) ดึง ids ของแต่ละ pool
    pool_sets: Dict[str, Set[str]] = {}
    for p in POOLS:
        node = deep_find_node(resp, p)
        if not node:
            log(f"[WARN] node not found: {p}")
            pool_sets[p] = set()
            continue

        s = extract_ids_from_any_node(node)
        pool_sets[p] = s
        log(f"pool '{p}' extracted ids: {len(s)}")

    # 4) map final_id -> pools
    id_to_pools: Dict[str, List[str]] = {}
    for fid in final_ids:
        sources = [p for p, s in pool_sets.items() if fid in s]
        id_to_pools[fid] = sources

    # 5) Summary count by pool
    log("\n--- COUNT BY POOL (within final_result) ---")
    for p in POOLS:
        cnt = sum(1 for fid in final_ids if p in id_to_pools[fid])
        log(f"{p}: {cnt}")

    unknown = [fid for fid in final_ids if len(id_to_pools[fid]) == 0]
    multi = [fid for fid in final_ids if len(id_to_pools[fid]) > 1]

    log(f"\nUNKNOWN source (not found in any listed pool): {len(unknown)}")
    if unknown:
        log("  sample unknown: " + ", ".join(unknown[:30]))

    log(f"\nMULTI source (found in >1 pool): {len(multi)}")
    if multi:
        for fid in multi[:30]:
            log(f"  {fid} -> {id_to_pools[fid]}")

    # 6) Details: IDs by pool (ตามลำดับใน final_result)
    log("\n--- DETAILS: IDs BY POOL (order follows final_result) ---")
    for p in POOLS:
        ids_in_pool = [fid for fid in final_ids if p in id_to_pools[fid]]
        log(f"\n[{p}] count={len(ids_in_pool)}")
        for fid in ids_in_pool:
            log(f"  {fid}")

    # 7) Details: final_id -> pools
    log("\n--- DETAILS: FINAL ID -> SOURCE POOL(S) ---")
    for fid in final_ids:
        src = id_to_pools[fid]
        log(f"{fid} -> {src if src else ['UNKNOWN']}")

    log("\n✅ DONE")
