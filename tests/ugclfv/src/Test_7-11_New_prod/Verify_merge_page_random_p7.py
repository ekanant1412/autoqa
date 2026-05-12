import random
import requests
import json
import os
from datetime import datetime
from collections import Counter

# ===================== CONFIG =====================
PLACEMENT = {
    "name": "sfv-p7",
    # ga_id ถูกแทนด้วย {ga_id} → สร้างใหม่แต่ละ run เพื่อ bypass cache
    "url_template": (
        "http://ai-universal-service-711.prod-gcp-ai-bn.ai-platform.gcp.dmp.true.th"
        "/api/v1/universal/sfv-p7"
        "?shelfId=zmEXe3EQnXDk"
        "&total_candidates=200"
        "&pool_limit_category_items=100"
        "&language=th&pool_tophit_date=365"
        "&userId=null&pseudoId=null"
        "&cursor=1&ga_id={ga_id}"
        "&is_use_live=true&verbose=debug&pool_latest_date=365"
        "&limit=3"
    ),
}

# ga_id prefix สำหรับ cold-start fake users (ไม่มีประวัติใน bumblebee)
# รูปแบบ: 999999999.XXXXXXXXX  → แต่ละ run ใช้ random suffix ต่างกัน
GA_ID_PREFIX = "999999999"

RUNS = 10
TOP_K = 50
TIMEOUT = 20

LAST_ID_KEYS   = [f"last_id_{i}"   for i in range(5)]   # last_id_0 … last_id_4
LAST_TAGS_KEYS = [f"last_tags_{i}" for i in range(5)]   # last_tags_0 … last_tags_4

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)
# =================================================


def _log(msg: str, log_path: str):
    line = f"{datetime.now()} | {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ── Extractor helpers ─────────────────────────────

def extract_ids(resp_json: dict) -> list:
    """ดึง item id ออกจาก merge_page"""
    try:
        return [
            x["id"]
            for x in resp_json["data"]["results"]["merge_page"]["result"]["items"]
            if isinstance(x, dict) and "id" in x
        ]
    except Exception:
        return []


def _find_key_recursive(obj, target_key):
    """
    ค้นหา key ใน nested dict/list แบบ recursive
    คืน value ของ key แรกที่เจอ หรือ None ถ้าไม่เจอ
    """
    if isinstance(obj, dict):
        if target_key in obj:
            return obj[target_key]
        for v in obj.values():
            found = _find_key_recursive(v, target_key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_key_recursive(item, target_key)
            if found is not None:
                return found
    return None


def extract_user_feature_result(resp_json: dict) -> dict:
    """
    ดึง external_user_feature.result จาก verbose=debug response
    ใช้ recursive search เพื่อรองรับ structure ทุกรูปแบบ
    """
    feature = _find_key_recursive(resp_json, "external_user_feature")
    if isinstance(feature, dict):
        result = feature.get("result")
        if isinstance(result, dict):
            return result
    return {}


def is_all_null(feature_result: dict) -> bool:
    """
    คืน True ถ้า last_id_0-4 และ last_tags_0-4 ทุกค่าเป็น null / "" / None
    """
    keys_to_check = LAST_ID_KEYS + LAST_TAGS_KEYS
    for key in keys_to_check:
        val = feature_result.get(key)
        # ถือว่า null = None, "", หรือไม่มี key
        if val not in (None, "", "null"):
            return False
    return True


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

def _make_url(placement: dict) -> str:
    """สร้าง URL พร้อม random ga_id ใหม่ทุกครั้ง เพื่อ bypass bumblebee cache"""
    suffix = random.randint(100_000_000, 999_999_999)
    ga_id  = f"{GA_ID_PREFIX}.{suffix}"
    return placement["url_template"].format(ga_id=ga_id), ga_id


def run_check(placement: dict) -> dict:
    name = placement["name"]

    art_dir = f"{REPORT_DIR}/{name}"
    os.makedirs(art_dir, exist_ok=True)

    log_path     = f"{art_dir}/tc_merge_random.log"
    all_ids_path = f"{art_dir}/tc_merge_random_all_ids.txt"
    csv_path     = f"{art_dir}/tc_merge_random_runs.csv"
    json_path    = f"{art_dir}/tc_merge_random.json"

    open(log_path, "w").close()

    def log(msg):
        _log(msg, log_path)

    log(f"START RANDOM PROOF TEST (null last_id/last_tags)  [{name}]")
    log(f"url_template={placement['url_template']}")
    log(f"RUNS={RUNS}  TOP_K={TOP_K}  (unique ga_id per run)")

    run_lists          = []
    skipped_not_null   = 0   # runs ที่ user มี history → ข้าม
    null_confirmed_runs = 0  # runs ที่ผ่าน null-check

    for i in range(1, RUNS + 1):
        url, ga_id = _make_url(placement)
        log(f"RUN {i}  ga_id={ga_id}")
        try:
            r    = requests.get(url, timeout=TIMEOUT)
            data = r.json()

            # ── step 1: ตรวจ last_id / last_tags ──────────────────
            feature_result = extract_user_feature_result(data)

            if feature_result:
                last_id_vals   = {k: feature_result.get(k) for k in LAST_ID_KEYS}
                last_tags_vals = {k: feature_result.get(k) for k in LAST_TAGS_KEYS}
                all_null       = is_all_null(feature_result)

                log(f"  last_id   : {last_id_vals}")
                log(f"  last_tags : {last_tags_vals}")
                log(f"  all_null  : {all_null}")

                if not all_null:
                    log(f"  → SKIP: user has history (last_id/last_tags not all null)")
                    skipped_not_null += 1
                    continue
                else:
                    log(f"  → PASS null-check: merge_page must be random")
                    null_confirmed_runs += 1
            else:
                # ไม่พบ feature_result ใน response → log แต่ยังนับ run
                log(f"  WARN: external_user_feature.result not found in response")

            # ── step 2: ดึง ids จาก merge_page ───────────────────
            ids = extract_ids(data)
            log(f"  items={len(ids)}")
            for idx, _id in enumerate(ids, 1):
                log(f"  {idx:03d} {_id}")

            if ids:
                run_lists.append(ids[:TOP_K])

        except Exception as e:
            log(f"ERROR: {e}")

    if len(run_lists) < 2:
        return {
            "placement": name,
            "status": "ERROR",
            "error": (
                f"only {len(run_lists)} successful run(s) with null last_id/last_tags. "
                f"skipped_not_null={skipped_not_null}"
            ),
        }

    # ── similarity analysis ────────────────────────
    pairwise = []
    j_scores = []
    k_scores = []

    for i in range(len(run_lists)):
        for j in range(i + 1, len(run_lists)):
            jac  = jaccard(run_lists[i], run_lists[j])
            kend = kendall_similarity(run_lists[i], run_lists[j])

            pairwise.append((i + 1, j + 1, jac, kend))
            j_scores.append(jac)
            if kend is not None:
                k_scores.append(kend)

    # sticky positions
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

    verdict      = "PASS_RANDOM_ENOUGH"
    fail_reasons = []

    if k_scores and avg_k > 0.9:
        fail_reasons.append(f"avg_kendall={avg_k} > 0.9")

    if sticky >= 5:
        fail_reasons.append(f"sticky_positions={sticky} >= 5")

    if fail_reasons:
        verdict = "FAIL_NOT_RANDOM"

    summary = {
        "placement"            : name,
        "runs_total"           : RUNS,
        "runs_null_confirmed"  : null_confirmed_runs,
        "runs_skipped_not_null": skipped_not_null,
        "runs_used"            : len(run_lists),
        "avg_jaccard"          : avg_j,
        "avg_kendall"          : avg_k,
        "sticky_positions"     : sticky,
        "VERDICT"              : verdict,
        "fail_reasons"         : fail_reasons,
    }

    # save outputs
    write_all_ids(run_lists, all_ids_path)
    write_csv(run_lists, csv_path)

    with open(json_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log(f"RESULT = {summary}")
    return summary


# =================================================
# PYTEST
# =================================================

def _assert_result(summary: dict):
    assert summary.get("status") != "ERROR", (
        f"[{summary['placement']}] run failed: {summary.get('error')}"
    )

    assert summary["runs_null_confirmed"] > 0, (
        f"[{summary['placement']}] No run had all-null last_id/last_tags. "
        f"Cannot verify randomness for cold-start users."
    )

    assert summary["VERDICT"] == "PASS_RANDOM_ENOUGH", (
        f"[{summary['placement']}] merge_page results NOT random enough "
        f"(when last_id_0-4 & last_tags_0-4 are all null). "
        f"avg_jaccard={summary['avg_jaccard']} "
        f"avg_kendall={summary['avg_kendall']} "
        f"sticky={summary['sticky_positions']} "
        f"Reasons: {summary['fail_reasons']}"
    )


def test_7_11_merge_page_random_sfv_p7():
    _assert_result(run_check(PLACEMENT))


# =================================================

if __name__ == "__main__":
    run_check(PLACEMENT)
