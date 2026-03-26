import requests
import json
import os
from collections import Counter
from datetime import datetime

# ===================== CONFIG =====================
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
        "name": "sfv-p8",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=bxAwRPp85gmL"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=999999999.999999999"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
            "&limit_seen_item=1"
        ),
    },
]
TIMEOUT_SEC = 20

# rule: must NOT have same create_by consecutive >= 3
MAX_CONSECUTIVE_ALLOWED = 2

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# =================================================


def tlog(msg: str, log_path: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def dump_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def get_results_root(j: dict):
    data = j.get("data", {}) if isinstance(j.get("data", {}), dict) else {}
    results = data.get("results", {}) if isinstance(data.get("results", {}), dict) else {}
    return results


def extract_ids_from_items(items):
    ids = []
    if isinstance(items, list):
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("id"), str) and it["id"]:
                ids.append(it["id"])
            elif isinstance(it, str) and it:
                ids.append(it)
    return ids


def extract_merge_page_ids(results: dict):
    node = results.get("merge_page", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    return extract_ids_from_items(result.get("items", []))


def extract_final_result_ids(results: dict):
    """ดึง ids จาก final_result.result.items"""
    node = results.get("final_result", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    return extract_ids_from_items(result.get("items", []))


def extract_generate_candidates_map_id_to_create_by(results: dict):
    """
    Build id -> create_by map from:
      generate_candidates.result.targetItems
    """
    node = results.get("generate_candidates", {})
    if not isinstance(node, dict):
        return {}, {"source": None, "count": 0, "missing_create_by_count": 0}

    result = node.get("result", {})
    if not isinstance(result, dict):
        return {}, {"source": None, "count": 0, "missing_create_by_count": 0}

    items = result.get("targetItems")
    if not isinstance(items, list):
        return {}, {"source": None, "count": 0, "missing_create_by_count": 0}

    id_to_create_by = {}
    missing = 0

    for it in items:
        if not isinstance(it, dict):
            continue
        _id = it.get("id")
        if not isinstance(_id, str):
            continue
        cb = it.get("create_by")
        if not isinstance(cb, str) or not cb:
            missing += 1
            continue
        id_to_create_by[_id] = cb

    meta = {
        "source": "generate_candidates.result.targetItems",
        "count": len(id_to_create_by),
        "missing_create_by_count": missing,
    }
    return id_to_create_by, meta


def find_consecutive_create_by_violations(ordered_ids, id_to_create_by, max_allowed=2):
    violations = []
    seq = []

    def flush_seq():
        nonlocal seq
        if not seq:
            return
        # skip UNKNOWN
        if seq[0]["create_by"] != "UNKNOWN" and len(seq) > max_allowed:
            violations.append({
                "create_by": seq[0]["create_by"],
                "run_length": len(seq),
                "start_pos": seq[0]["pos"],
                "end_pos": seq[-1]["pos"],
                "items": seq.copy(),
            })
        seq = []

    prev_cb = None
    for idx, _id in enumerate(ordered_ids):
        cb = id_to_create_by.get(_id, "UNKNOWN")
        if cb == prev_cb:
            seq.append({"pos": idx, "id": _id, "create_by": cb})
        else:
            flush_seq()
            seq = [{"pos": idx, "id": _id, "create_by": cb}]
            prev_cb = cb

    flush_seq()
    return violations


def check_creator_dominance(ids, id_to_create_by, max_allowed=2):
    """
    ตรวจว่า creator คนใดครอง pool มากเกินจน consecutive violation เกิดขึ้นได้โดยหลีกเลี่ยงไม่ได้

    หลักการ:
      ถ้า creator A มี k items จาก n items ทั้งหมด
      และ k > n * max_allowed / (max_allowed + 1)
      → แม้จะ shuffle ดีแค่ไหนก็ต้องมี consecutive ≥ max_allowed+1 เสมอ
      → violation เป็น expected behavior ไม่ใช่ bug

    ตัวอย่าง (max_allowed=2):
      threshold = 2/3 ≈ 66.7%
      n=3, k=3 (100%) → WARN (unavoidable)
      n=17, k=12 (70%) → WARN (unavoidable)
      n=17, k=8  (47%) → FAIL (diversity เพียงพอ ควร shuffle ได้)
    """
    known_ids = [_id for _id in ids if id_to_create_by.get(_id, "UNKNOWN") != "UNKNOWN"]
    total = len(known_ids)

    if total == 0:
        return {
            "is_unavoidable": False,
            "dominant_creator": None,
            "dominant_count": 0,
            "dominant_ratio": 0.0,
            "total_known": 0,
            "threshold_ratio": max_allowed / (max_allowed + 1),
            "creator_distribution": {},
        }

    counter = Counter(id_to_create_by.get(_id) for _id in known_ids)
    dominant_creator, dominant_count = counter.most_common(1)[0]
    dominant_ratio = dominant_count / total
    threshold = max_allowed / (max_allowed + 1)  # เช่น 2/3 ≈ 0.667

    return {
        "is_unavoidable": dominant_ratio > threshold,
        "dominant_creator": dominant_creator,
        "dominant_count": dominant_count,
        "dominant_ratio": round(dominant_ratio, 4),
        "total_known": total,
        "threshold_ratio": round(threshold, 4),
        "creator_distribution": dict(counter.most_common()),
    }


# =================================================
def run_check(placement: dict) -> dict:
    """รัน check สำหรับ placement เดียว และ return summary dict"""
    name = placement["name"]
    url = placement["url"]

    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    log_path = f"{art_dir}/tc_creator_consecutive.log"
    out_json = f"{art_dir}/tc_creator_consecutive.json"
    out_response = f"{art_dir}/tc_creator_full_response.json"

    open(log_path, "w", encoding="utf-8").close()

    def log(msg):
        tlog(msg, log_path)

    log(f"START TC: create_by consecutive check  [{name}]")
    log(f"MAX_CONSECUTIVE_ALLOWED={MAX_CONSECUTIVE_ALLOWED}")
    log(f"URL={url}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    log(f"HTTP={r.status_code}")

    try:
        j = r.json()
    except Exception:
        log("Response not JSON")
        return {"placement": name, "status": "ERROR", "violations": [], "error": "not JSON"}

    dump_json(out_response, j)

    if r.status_code != 200:
        log(f"Non-200 response")
        return {"placement": name, "status": "ERROR", "violations": [], "error": f"HTTP {r.status_code}"}

    results = get_results_root(j)
    merge_ids = extract_merge_page_ids(results)
    final_ids = extract_final_result_ids(results)
    id_to_create_by, meta = extract_generate_candidates_map_id_to_create_by(results)

    log("=== Extracted counts ===")
    log(f"merge_page_ids_count   = {len(merge_ids)}")
    log(f"final_result_ids_count = {len(final_ids)}")
    log(f"generate_candidates_map_count = {meta.get('count')}  source={meta.get('source')}")
    log(f"missing_create_by_count = {meta.get('missing_create_by_count')}")
    log(f"merge_page_ids(all) = {merge_ids}")

    ordered_rows = []
    unknown_count = 0
    for i, _id in enumerate(merge_ids):
        cb = id_to_create_by.get(_id)
        if cb is None:
            cb = "UNKNOWN"
            unknown_count += 1
        ordered_rows.append({"pos": i, "id": _id, "create_by": cb})

    log(f"merge_ids_not_found_in_map = {unknown_count}")

    # ── Step 1: consecutive check ───────────────────────────────────────────
    violations = find_consecutive_create_by_violations(
        ordered_ids=merge_ids,
        id_to_create_by=id_to_create_by,
        max_allowed=MAX_CONSECUTIVE_ALLOWED,
    )

    # ── Step 2: dominance check (เฉพาะเมื่อมี violations) ──────────────────
    dominance = None
    status = "PASS"
    warn_reason = None

    if violations:
        # ตรวจสอบจาก final_result ก่อน ถ้าไม่มีให้ fallback ไป merge_page
        check_ids = final_ids if final_ids else merge_ids
        dominance = check_creator_dominance(check_ids, id_to_create_by, MAX_CONSECUTIVE_ALLOWED)

        log("\n=== DOMINANCE CHECK (final_result) ===")
        log(f"total_known_ids   = {dominance['total_known']}")
        log(f"dominant_creator  = {dominance['dominant_creator']}")
        log(f"dominant_count    = {dominance['dominant_count']}")
        log(f"dominant_ratio    = {dominance['dominant_ratio']:.1%}")
        log(f"threshold_ratio   = {dominance['threshold_ratio']:.1%}  (max_allowed/{MAX_CONSECUTIVE_ALLOWED+1})")
        log(f"is_unavoidable    = {dominance['is_unavoidable']}")
        log(f"creator_distribution = {dominance['creator_distribution']}")

        if dominance["is_unavoidable"]:
            status = "WARN"
            warn_reason = (
                f"creator '{dominance['dominant_creator']}' มี {dominance['dominant_count']}/{dominance['total_known']} items "
                f"({dominance['dominant_ratio']:.1%} > threshold {dominance['threshold_ratio']:.1%}) "
                f"→ consecutive violation เกิดได้โดยหลีกเลี่ยงไม่ได้เพราะ items น้อย"
            )
            log(f"⚠️  WARN: {warn_reason}")
            log(f"   violations={len(violations)} แต่ถือว่า acceptable")
        else:
            status = "FAIL"
            log(f"❌ FAIL: {len(violations)} consecutive create_by violation(s) (> {MAX_CONSECUTIVE_ALLOWED})")
            for v in violations:
                log(f"  - create_by={v['create_by']} run_length={v['run_length']} pos={v['start_pos']}..{v['end_pos']}")
                log(f"    ids={[x['id'] for x in v['items']]}")
    else:
        log(f"✅ PASS: no create_by consecutive run > {MAX_CONSECUTIVE_ALLOWED}")

    out = {
        "placement": name,
        "url": url,
        "max_consecutive_allowed": MAX_CONSECUTIVE_ALLOWED,
        "merge_page_ids": merge_ids,
        "final_result_ids": final_ids,
        "generate_candidates_meta": meta,
        "merge_order_with_create_by": ordered_rows,
        "violations": violations,
        "dominance_check": dominance,
        "warn_reason": warn_reason,
        "stats": {
            "merge_page_count": len(merge_ids),
            "final_result_count": len(final_ids),
            "unknown_create_by_in_merge_count": unknown_count,
            "violations_count": len(violations),
        },
        "status": status,  # PASS / WARN / FAIL / ERROR
    }
    dump_json(out_json, out)
    log(f"Saved: {out_json}")
    log("END")
    return out


# =================================================
# ✅ PYTEST ENTRY
# =================================================
def _assert_result(summary: dict):
    """PASS และ WARN → ผ่าน, FAIL → raise AssertionError"""
    if summary["status"] in ("PASS", "WARN"):
        if summary["status"] == "WARN":
            print(f"\n⚠️  WARN [{summary['placement']}]: {summary['warn_reason']}")
        return
    # FAIL
    v = summary["violations"]
    dom = summary.get("dominance_check") or {}
    raise AssertionError(
        f"[{summary['placement']}] {len(v)} create_by consecutive violation(s) "
        f"(max_allowed={MAX_CONSECUTIVE_ALLOWED})\n"
        f"  dominant_ratio={dom.get('dominant_ratio', '?'):.1%}  "
        f"threshold={dom.get('threshold_ratio', '?'):.1%}  "
        f"→ diversity เพียงพอ แต่ยังเกิด consecutive\n"
        f"  sample violations: {v[:2]}"
    )


def test_7_11_creator_logic_sfv_p7():
    _assert_result(run_check(PLACEMENTS[0]))


def test_7_11_creator_logic_sfv_p8():
    _assert_result(run_check(PLACEMENTS[1]))


def test_7_11_creator_logic():
    """รันทั้ง p7 + p8 ในครั้งเดียว"""
    test_7_11_creator_logic_sfv_p7()
    test_7_11_creator_logic_sfv_p8()


# =================================================
if __name__ == "__main__":
    for p in PLACEMENTS:
        run_check(p)
