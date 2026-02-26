import requests
import json
from datetime import datetime
import os
from collections import Counter

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-SEEN"

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
RUNS = 50

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
    result = node.get("result", {}) if isinstance(node, dict) else {}
    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


def extract_seen_item_ids(results: dict) -> list:
    node = results.get("get_seen_item_redis", {})
    result = node.get("result", {}) if isinstance(node, dict) else {}

    ids = result.get("ids", [])
    if ids:
        return [str(i) for i in ids if i]

    items = result.get("items", [])
    return [it["id"] for it in items if isinstance(it, dict) and "id" in it]


def find_duplicates(ids: list) -> list:
    counter = Counter(ids)
    return [k for k, v in counter.items() if v > 1]


# =================================================
def run_check(placement: dict):

    name = placement["name"]
    url = placement["url"]

    placement_dir = f"{ART_DIR}/{name}"
    os.makedirs(placement_dir, exist_ok=True)

    log_txt = f"{placement_dir}/seen_validation.log"
    result_json = f"{placement_dir}/seen_validation_result.json"

    open(log_txt, "w").close()

    def log(msg):
        tlog(msg, log_txt)

    log(f"TEST={TEST_KEY} | placement={name}")
    log(f"URL={url}")
    log(f"RUNS={RUNS}")

    all_issues = []
    run_results = []

    for run in range(1, RUNS + 1):

        log(f"\n{'='*60}")
        log(f"RUN {run}/{RUNS}")
        log(f"{'='*60}")

        r = requests.get(url, timeout=TIMEOUT_SEC)
        log(f"HTTP={r.status_code}")
        r.raise_for_status()

        j = r.json()
        results = get_results_root(j)

        if run == 1 or run == RUNS:
            with open(f"{placement_dir}/response_run{run}.json","w",encoding="utf-8") as f:
                json.dump(j, f, indent=2, ensure_ascii=False)

        slice_ids = extract_slice_pagination_ids(results)
        seen_ids = extract_seen_item_ids(results)

        slice_set = set(slice_ids)
        seen_set = set(seen_ids)

        run_issues = []

        log(f"slice_count={len(slice_ids)}")
        log(f"seen_count={len(seen_ids)}")

        # =================================================
        # CHECK 1: seen must NOT appear in slice
        # =================================================
        intersection = list(slice_set.intersection(seen_set))

        if intersection:
            msg = f"[run{run}] seen items appear in slice: {intersection[:20]}"
            log(f"❌ CHECK1 FAIL: {msg}")
            run_issues.append(msg)
        else:
            log("✅ CHECK1 PASS: slice excludes seen items")

        # =================================================
        # CHECK 2: no duplicate in slice
        # =================================================
        dup_slice = find_duplicates(slice_ids)

        if dup_slice:
            msg = f"[run{run}] duplicate in slice: {dup_slice[:20]}"
            log(f"❌ CHECK2 FAIL: {msg}")
            run_issues.append(msg)
        else:
            log("✅ CHECK2 PASS: no duplicates in slice")

        # =================================================
        # CHECK 3: no duplicate in seen
        # =================================================
        dup_seen = find_duplicates(seen_ids)

        if dup_seen:
            msg = f"[run{run}] duplicate in seen: {dup_seen[:20]}"
            log(f"❌ CHECK3 FAIL: {msg}")
            run_issues.append(msg)
        else:
            log("✅ CHECK3 PASS: no duplicates in seen")

        status = "FAIL" if run_issues else "PASS"
        log(f"RUN STATUS: {status}")

        run_results.append({
            "run": run,
            "slice_count": len(slice_ids),
            "seen_count": len(seen_ids),
            "intersection": intersection,
            "duplicate_slice": dup_slice,
            "duplicate_seen": dup_seen,
            "status": status,
            "issues": run_issues,
        })

        all_issues.extend(run_issues)

    # =================================================
    # FINAL SUMMARY
    # =================================================
    pass_runs = sum(1 for r in run_results if r["status"] == "PASS")
    fail_runs = RUNS - pass_runs
    final_status = "FAIL" if all_issues else "PASS"

    log("\n" + "="*60)
    log("FINAL SUMMARY")
    log("="*60)
    log(f"PASS RUNS : {pass_runs}")
    log(f"FAIL RUNS : {fail_runs}")
    log(f"TOTAL ISSUES : {len(all_issues)}")
    log(f"STATUS : {final_status}")

    result = {
        "test_key": TEST_KEY,
        "placement": name,
        "runs": RUNS,
        "pass_runs": pass_runs,
        "fail_runs": fail_runs,
        "status": final_status,
        "issues": all_issues,
        "run_details": run_results,
    }

    with open(result_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    log(f"Saved: {result_json}")

    if final_status == "FAIL":
        raise AssertionError(
            f"{TEST_KEY} [{name}] FAIL {fail_runs}/{RUNS}"
        )

    return result


# =================================================
if __name__ == "__main__":
    failures = []
    for p in PLACEMENTS:
        try:
            run_check(p)
        except AssertionError as e:
            failures.append(str(e))

    if failures:
        raise AssertionError("\n".join(failures))