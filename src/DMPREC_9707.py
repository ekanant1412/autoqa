import requests
import json
from datetime import datetime
import os

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9707"

PLACEMENTS = [
    {
        "name": "sfv-p4",
        "url": (
            "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p4"
            "?shelfId=zmEXe3EQnXDk"
            "&total_candidates=100"
            "&language=th"
            "&ssoId=22092422"
            "&userId=null"
            "&pseudoId=null"
            "&limit=100"
            "&returnItemMetadata=false"
            "&isOnlyId=true"
            "&verbose=debug"
            "&cursor=1"
        ),
    },
    {
        "name": "sfv-p5",
        "url": (
            "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p5"
            "?shelfId=zmEXe3EQnXDk"
            "&total_candidates=100"
            "&language=th"
            "&ssoId=22092422"
            "&userId=null"
            "&pseudoId=null"
            "&limit=100"
            "&returnItemMetadata=false"
            "&isOnlyId=true"
            "&verbose=debug"
            "&cursor=1"
        ),
    },
]

TIMEOUT_SEC = 20
MAX_SEEN = 200   # default limit ของ redis
TOP_N = 5        # จำนวนที่เช็ค (5 อันดับแรก)
RUNS = 50        # จำนวนรอบที่รัน


REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)


# =================================================
def tlog(msg: str, log_path: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


def get_results_root(j: dict) -> dict:
    data = j.get("data", {})
    return data.get("results", {}) if isinstance(data, dict) else {}


def extract_slice_pagination_ids(results: dict) -> list:
    node = results.get("slice_pagination", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


def extract_seen_item_ids(results: dict) -> list:
    node = results.get("get_seen_item_redis", {})
    if not isinstance(node, dict):
        return []
    result = node.get("result", {})
    if not isinstance(result, dict):
        return []
    ids = result.get("ids", [])
    if ids:
        return [str(i) for i in ids if i]
    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


# =================================================
def run_check(placement: dict) -> dict:
    name = placement["name"]
    url = placement["url"]

    placement_dir = f"{ART_DIR}/{name}"
    os.makedirs(placement_dir, exist_ok=True)

    log_txt = f"{placement_dir}/seen_check.log"
    result_json = f"{placement_dir}/seen_check_result.json"

    open(log_txt, "w", encoding="utf-8").close()

    def log(msg):
        tlog(msg, log_txt)

    log(f"TEST={TEST_KEY}  PLACEMENT={name}")
    log(f"URL={url}")
    log(f"RUNS={RUNS}  TOP_N={TOP_N}  MAX_SEEN={MAX_SEEN}")

    all_issues = []
    run_results = []

    for run in range(1, RUNS + 1):
        log(f"\n{'='*50}")
        log(f"RUN {run}/{RUNS}")
        log(f"{'='*50}")

        # REQUEST 1
        r1 = requests.get(url, timeout=TIMEOUT_SEC)
        log(f"[REQ1] HTTP={r1.status_code}")
        r1.raise_for_status()
        j1 = r1.json()
        results1 = get_results_root(j1)

        # save เฉพาะ run แรกและ run สุดท้าย
        if run == 1 or run == RUNS:
            with open(f"{placement_dir}/response_req1_run{run}.json", "w", encoding="utf-8") as f:
                json.dump(j1, f, ensure_ascii=False, indent=2)

        all_pagination_ids = extract_slice_pagination_ids(results1)
        top_n_ids = all_pagination_ids[:TOP_N]
        seen_ids_r1 = extract_seen_item_ids(results1)
        seen_count_r1 = len(seen_ids_r1)
        is_full = seen_count_r1 >= MAX_SEEN
        expected_evicted = seen_ids_r1[:TOP_N] if is_full else []

        log(f"slice_pagination top_{TOP_N} : {top_n_ids}")
        log(f"seen_before                  : {seen_count_r1} {'⚠️ FULL' if is_full else ''}")

        # REQUEST 2
        r2 = requests.get(url, timeout=TIMEOUT_SEC)
        log(f"[REQ2] HTTP={r2.status_code}")
        r2.raise_for_status()
        j2 = r2.json()
        results2 = get_results_root(j2)

        if run == 1 or run == RUNS:
            with open(f"{placement_dir}/response_req2_run{run}.json", "w", encoding="utf-8") as f:
                json.dump(j2, f, ensure_ascii=False, indent=2)

        seen_ids_r2 = extract_seen_item_ids(results2)
        seen_set_r2 = set(seen_ids_r2)
        log(f"seen_after                   : {len(seen_ids_r2)}")

        # VALIDATE
        run_issues = []

        # CHECK 1: seen ไม่เกิน MAX_SEEN
        if len(seen_ids_r2) > MAX_SEEN:
            msg = f"[run{run}] seen count={len(seen_ids_r2)} exceeds MAX_SEEN={MAX_SEEN}"
            log(f"  ❌ CHECK1 FAIL: {msg}")
            run_issues.append(msg)
        else:
            log(f"  ✅ CHECK1 PASS: seen={len(seen_ids_r2)} <= {MAX_SEEN}")

        # CHECK 2: top 5 ต้องอยู่ใน seen req2
        missing_top_n = [pid for pid in top_n_ids if pid not in seen_set_r2]
        found_top_n = [pid for pid in top_n_ids if pid in seen_set_r2]

        for pid in top_n_ids:
            icon = "✅" if pid in seen_set_r2 else "❌"
            log(f"  {icon} {pid}")

        if missing_top_n:
            msg = f"[run{run}] missing top_{TOP_N} in seen: {missing_top_n}"
            log(f"  ❌ CHECK2 FAIL: {msg}")
            run_issues.append(msg)
        else:
            log(f"  ✅ CHECK2 PASS: all top_{TOP_N} found in seen")

        # CHECK 3: eviction
        if is_full:
            still_in = [eid for eid in expected_evicted if eid in seen_set_r2]
            evicted = [eid for eid in expected_evicted if eid not in seen_set_r2]
            log(f"  evicted={len(evicted)}/{TOP_N}  still_in={len(still_in)}/{TOP_N}")
            if still_in:
                msg = f"[run{run}] {len(still_in)} oldest not evicted: {still_in}"
                log(f"  ❌ CHECK3 FAIL: {msg}")
                run_issues.append(msg)
            else:
                log(f"  ✅ CHECK3 PASS: oldest {TOP_N} evicted correctly")
        else:
            log(f"  SKIP CHECK3: seen not full ({seen_count_r1} < {MAX_SEEN})")

        run_status = "FAIL" if run_issues else "PASS"
        log(f"RUN {run} STATUS: {'✅ PASS' if run_status == 'PASS' else '❌ FAIL'}")

        run_results.append({
            "run": run,
            "seen_before": seen_count_r1,
            "seen_after": len(seen_ids_r2),
            "top_n_ids": top_n_ids,
            "found_top_n": found_top_n,
            "missing_top_n": missing_top_n,
            "expected_evicted": expected_evicted,
            "status": run_status,
            "issues": run_issues,
        })

        all_issues.extend(run_issues)

    # =================================================
    # FINAL SUMMARY
    # =================================================
    pass_runs = sum(1 for r in run_results if r["status"] == "PASS")
    fail_runs = sum(1 for r in run_results if r["status"] == "FAIL")
    status = "FAIL" if all_issues else "PASS"

    log(f"\n{'='*50}")
    log(f"=== FINAL SUMMARY  placement={name} ===")
    log(f"{'='*50}")
    log(f"total runs   : {RUNS}")
    log(f"✅ pass runs : {pass_runs}")
    log(f"❌ fail runs : {fail_runs}")
    log(f"total issues : {len(all_issues)}")
    log(f"STATUS       : {'✅ PASS' if status == 'PASS' else '❌ FAIL'}")
    if all_issues:
        for iss in all_issues[:20]:
            log(f"  ⚠️  {iss}")

    result = {
        "test_key": TEST_KEY,
        "placement": name,
        "url": url,
        "runs": RUNS,
        "top_n": TOP_N,
        "max_seen": MAX_SEEN,
        "pass_runs": pass_runs,
        "fail_runs": fail_runs,
        "total_issues": len(all_issues),
        "all_issues": all_issues,
        "run_details": run_results,
        "status": status,
    }

    with open(result_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    log(f"\nSaved: {result_json}")
    log(f"Saved: {log_txt}")

    if status == "FAIL":
        raise AssertionError(
            f"{TEST_KEY} [{name}] FAIL: fail_runs={fail_runs}/{RUNS} issues={len(all_issues)}"
        )

    return result


# =================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =================================================
def test_DMPREC_9707_sfv_p4():
    result = run_check(PLACEMENTS[0])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| seen_before={result['seen_before_count']}",
          f"| seen_after={result['seen_after_count']}",
          f"| missing_top{TOP_N}={len(result['missing_in_seen_r2'])}")


def test_DMPREC_9707_sfv_p5():
    result = run_check(PLACEMENTS[1])
    print("RESULT:", result["status"],
          f"| placement={result['placement']}",
          f"| seen_before={result['seen_before_count']}",
          f"| seen_after={result['seen_after_count']}",
          f"| missing_top{TOP_N}={len(result['missing_in_seen_r2'])}")
    
def test_DMPREC_9707():
    test_DMPREC_9707_sfv_p4()
    test_DMPREC_9707_sfv_p5()


if __name__ == "__main__":
    failures = []
    for p in PLACEMENTS:
        try:
            run_check(p)
        except AssertionError as e:
            failures.append(str(e))

    if failures:
        raise AssertionError("\n".join(failures))