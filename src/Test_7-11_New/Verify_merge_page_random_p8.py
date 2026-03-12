import requests
import json
import os
from datetime import datetime
from collections import Counter

# ===================== CONFIG =====================
PLACEMENT = {
    "name": "sfv-p8",
    "url": (
        "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
        "/api/v1/universal/sfv-p8"
        "?shelfId=Kaw6MLVzPWmo"
        "&total_candidates=200"
        "&pool_limit_category_items=100"
        "&language=th&pool_tophit_date=365"
        "&userId=null&pseudoId=null"
        "&cursor=1&ga_id=142448494.084991337978"
        "&is_use_live=true&verbose=debug&pool_latest_date=365"
        "&partner_id=AN9PjZR1wEol"
        "&limit=3"
        "&limit_seen_item=10"
    ),
}

RUNS = 10
TOP_K = 50
TIMEOUT = 20

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# =================================================


def _log(msg: str, log_path: str):
    line = f"{datetime.now()} | {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def extract_ids(resp_json: dict) -> list:
    try:
        return [
            x["id"]
            for x in resp_json["data"]["results"]["merge_page"]["result"]["items"]
            if isinstance(x, dict) and "id" in x
        ]
    except Exception:
        return []


# ── Similarity Metrics ────────────────────────────

def jaccard(a: list, b: list) -> float:
    sa, sb = set(a), set(b)
    return len(sa & sb) / max(1, len(sa | sb))


def kendall_similarity(a: list, b: list):
    pos_b = {x: i for i, x in enumerate(b)}
    common = [x for x in a if x in pos_b]
    n = len(common)
    if n < 5:
        return None
    seq = [pos_b[x] for x in common]
    inv = sum(
        1
        for i in range(n)
        for j in range(i + 1, n)
        if seq[i] > seq[j]
    )
    total = n * (n - 1) // 2
    return 1.0 - inv / total if total else None


# ── Output Helpers ────────────────────────────────

def write_all_ids(run_lists: list, path: str):
    with open(path, "w", encoding="utf-8") as f:
        for r, ids in enumerate(run_lists, 1):
            f.write(f"\n===== RUN {r} =====\n")
            for i, _id in enumerate(ids, 1):
                f.write(f"{i:03d} {_id}\n")


def write_csv(run_lists: list, path: str):
    max_rank = min(len(x) for x in run_lists)
    headers = ["rank"] + [f"run_{i}" for i in range(1, len(run_lists) + 1)]
    rows = [",".join(headers)]
    for r in range(max_rank):
        row = [str(r + 1)] + [run[r] for run in run_lists]
        rows.append(",".join(row))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))


# =================================================

def run_check(placement: dict) -> dict:
    name = placement["name"]
    url = placement["url"]

    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    log_path     = f"{art_dir}/tc_merge_random.log"
    all_ids_path = f"{art_dir}/tc_merge_random_all_ids.txt"
    csv_path     = f"{art_dir}/tc_merge_random_runs.csv"
    json_path    = f"{art_dir}/tc_merge_random.json"

    open(log_path, "w").close()

    def log(msg):
        _log(msg, log_path)

    log(f"START RANDOM PROOF TEST  [{name}]")
    log(f"URL={url}")
    log(f"RUNS={RUNS}  TOP_K={TOP_K}")

    run_lists = []

    for i in range(1, RUNS + 1):
        log(f"RUN {i}")

        try:
            r = requests.get(url, timeout=TIMEOUT)
            data = r.json()

            ids = extract_ids(data)

            log(f"items={len(ids)}")

            for idx, _id in enumerate(ids, 1):
                log(f"{idx:03d} {_id}")

            if ids:
                run_lists.append(ids[:TOP_K])

        except Exception as e:
            log(f"ERROR: {e}")

    if len(run_lists) < 2:
        return {
            "placement": name,
            "status": "ERROR",
            "error": f"only {len(run_lists)} successful run(s)",
        }

    # similarity
    pairwise = []
    j_scores = []
    k_scores = []

    for i in range(len(run_lists)):
        for j in range(i + 1, len(run_lists)):
            jac = jaccard(run_lists[i], run_lists[j])
            kend = kendall_similarity(run_lists[i], run_lists[j])

            pairwise.append((i + 1, j + 1, jac, kend))

            j_scores.append(jac)

            if kend is not None:
                k_scores.append(kend)

    # sticky position
    max_rank = min(len(x) for x in run_lists)

    sticky = sum(
        1
        for pos in range(max_rank)
        if Counter(lst[pos] for lst in run_lists).most_common(1)[0][1]
        / len(run_lists)
        >= 0.8
    )

    avg_j = round(sum(j_scores) / len(j_scores), 3) if j_scores else 0
    avg_k = round(sum(k_scores) / len(k_scores), 3) if k_scores else 0

    verdict = "PASS_RANDOM_ENOUGH"

    fail_reasons = []

    if k_scores and avg_k > 0.9:
        fail_reasons.append(f"avg_kendall={avg_k} > 0.9")

    if sticky >= 5:
        fail_reasons.append(f"sticky_positions={sticky} >= 5")

    if fail_reasons:
        verdict = "FAIL_NOT_RANDOM"

    summary = {
        "placement": name,
        "runs": len(run_lists),
        "avg_jaccard": avg_j,
        "avg_kendall": avg_k,
        "sticky_positions": sticky,
        "VERDICT": verdict,
        "fail_reasons": fail_reasons,
    }

    # save outputs
    write_all_ids(run_lists, all_ids_path)
    write_csv(run_lists, csv_path)

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2)

    log(f"RESULT = {summary}")

    return summary


# =================================================
# PYTEST
# =================================================

def _assert_result(summary: dict):

    assert summary.get("status") != "ERROR", (
        f"[{summary['placement']}] run failed: {summary.get('error')}"
    )

    assert summary["VERDICT"] == "PASS_RANDOM_ENOUGH", (
        f"[{summary['placement']}] results NOT random enough. "
        f"avg_jaccard={summary['avg_jaccard']} "
        f"avg_kendall={summary['avg_kendall']} "
        f"sticky={summary['sticky_positions']} "
        f"Reasons: {summary['fail_reasons']}"
    )


def test_7_11_merge_page_random_sfv_p8():
    _assert_result(run_check(PLACEMENT))


# =================================================

if __name__ == "__main__":
    run_check(PLACEMENT)