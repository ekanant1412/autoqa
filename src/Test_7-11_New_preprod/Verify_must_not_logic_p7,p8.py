import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

import requests

# ===================================
# CONFIG
# ===================================
TEST_KEY = "DMPREC-9613"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=60"
            "&language=th"
            "&pool_tophit_date=365"
            "&limit=100"
            "&userId=null"
            "&pseudoId=null"
            "&cursor=1"
            "&ga_id=999999999.999999999"
            "&is_use_live=true"
            "&verbose=debug"
            "&pool_latest_date=365"
        ),
    },
    {
        "name": "sfv-p8",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=60"
            "&language=th"
            "&pool_tophit_date=365"
            "&limit=100"
            "&userId=null"
            "&pseudoId=null"
            "&cursor=1"
            "&ga_id=999999999.999999999"
            "&is_use_live=true"
            "&verbose=debug"
            "&pool_latest_date=365"
        ),
    },
]

RUNS = 20
TIMEOUT = 20
SLEEP_SEC = 0.2

# ✅ เพิ่ม: target bucketize nodes (ยืมจาก DMPREC-9589)
TARGET_NODES = [
    "bucketize_tophit_sfv",
    "bucketize_latest_sfv",
    "bucketize_latest_ugc_sfv",
    "bucketize_tophit_ugc_sfv",
]

BANNED_IDS: Set[str] = {
    "wj5GOky1mkYx", "yVb5Mwjx55yK", "09pGq8RbEv1K", "5GMzjgY2VO8A",
    "xo4XD2gbaJpo", "2OramNWW42mM", "lk1xWMlyB3pq", "rGRzA9EdZRLG",
    "QXzdvLe5y5yq", "nY4NvWQvwyZ5", "8GYgr8R94B5l",
    "npXJYPV0LPWb", "G5pjqMJekWyP", "YoQPY686MqbN", "a13LzAMLLVNg",
    "YVdk7XjNLn5p", "0XNNmQDmN6LX", "jl1Oq5YMADw0", "WkZlKxJoW3wq",
    "4OeVdaXapd5l", "LVQ1o5Wd8QdV", "6kMBqG4BMD3y", "8y506nqnAqkj",
    "GXzmkeobYDq6", "0xoVO2apADp1"
}

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str, log_path: str) -> None:
    line = f"{_now()} | {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ===================================
# HELPERS
# ===================================

# ✅ เพิ่ม: ยืมจาก DMPREC-9589
def deep_find_nodes(obj: Any, target_names: List[str]) -> List[Dict[str, Any]]:
    found: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if obj.get("name") in target_names:
            found.append(obj)
        for v in obj.values():
            found.extend(deep_find_nodes(v, target_names))
    elif isinstance(obj, list):
        for it in obj:
            found.extend(deep_find_nodes(it, target_names))
    return found


# ✅ เพิ่ม: ยืมจาก DMPREC-9589
def iter_items_from_result(result: Any) -> List[Tuple[str, Dict[str, Any]]]:
    out: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(result, list):
        for it in result:
            if isinstance(it, dict):
                out.append(("_list_", it))
        return out
    if isinstance(result, dict):
        for bucket_name, v in result.items():
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        out.append((str(bucket_name), it))
        return out
    return out


# ✅ เปลี่ยน: จากเดิมดึง merge_page → ดึงจาก bucketize nodes แทน
def extract_bucketize_ids(json_data: Dict[str, Any]) -> List[str]:
    nodes = deep_find_nodes(json_data, TARGET_NODES)
    ids: List[str] = []
    for node in nodes:
        result = node.get("result")
        if result is None:
            continue
        for _, it in iter_items_from_result(result):
            item_id = it.get("id") or it.get("content_id") or it.get("_id")
            if item_id and str(item_id).strip():
                ids.append(str(item_id).strip())
    return ids


# ===================================
# REPORT WRITERS
# ===================================
def write_all_ids(runs: List[Dict[str, Any]], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        for r in runs:
            f.write(f"\n===== RUN {r['run']} =====\n")
            f.write(f"extracted={r['extracted_count']}\n")
            if r["found_banned"]:
                f.write(f"FOUND_BANNED={','.join(r['found_banned'])}\n")
            for i, _id in enumerate(r["ids"], 1):
                f.write(f"{i:03d} {_id}\n")


def write_html(summary: Dict[str, Any], runs: List[Dict[str, Any]], out_path: str) -> None:
    html: List[str] = []
    html.append("<html><head><meta charset='utf-8'>")
    html.append("<style>")
    html.append("body{font-family:Arial, sans-serif} table{border-collapse:collapse}")
    html.append("td,th{border:1px solid #ccc;padding:6px;font-size:12px;vertical-align:top}")
    html.append(".fail{background:#ffe5e5} .pass{background:#e7ffe7}")
    html.append("</style></head><body>")
    html.append(f"<h2>Exclude Validation Report: {TEST_KEY} [{summary['placement']}]</h2>")
    html.append("<h3>Summary</h3><table>")
    for k, v in summary.items():
        html.append(f"<tr><th>{k}</th><td>{v}</td></tr>")
    html.append("</table>")
    html.append("<h3>Runs</h3><table>")
    html.append("<tr><th>Run</th><th>Extracted</th><th>Found banned</th><th>Sample IDs</th></tr>")
    for r in runs:
        klass = "fail" if r["found_banned"] else "pass"
        sample = "<br>".join(r["ids"][:20])
        found = "<br>".join(r["found_banned"]) if r["found_banned"] else "-"
        html.append(
            f"<tr class='{klass}'>"
            f"<td>{r['run']}</td>"
            f"<td>{r['extracted_count']}</td>"
            f"<td>{found}</td>"
            f"<td>{sample}</td>"
            f"</tr>"
        )
    html.append("</table></body></html>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("".join(html))


# ===================================
# MAIN CHECK
# ===================================
def run_check(placement: dict) -> Dict[str, Any]:
    name = placement["name"]
    url = placement["url"]

    placement_dir = f"{ART_DIR}/{name}"
    os.makedirs(placement_dir, exist_ok=True)

    out_log     = f"{placement_dir}/tc_exclude_bucketize.log"
    out_html    = f"{placement_dir}/tc_exclude_bucketize_report.html"
    out_json    = f"{placement_dir}/tc_exclude_bucketize_summary.json"
    out_all_ids = f"{placement_dir}/tc_exclude_bucketize_all_ids.txt"

    open(out_log, "w", encoding="utf-8").close()

    def _log(msg):
        log(msg, out_log)

    _log(f"TEST={TEST_KEY}  PLACEMENT={name}")
    _log("START EXCLUDE VALIDATION (source: bucketize nodes)")

    runs_result: List[Dict[str, Any]] = []
    total_found: Set[str] = set()
    total_extracted = 0

    for run in range(1, RUNS + 1):
        _log(f"RUN {run}")

        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        # ✅ เปลี่ยน: เรียก extract_bucketize_ids แทน extract_merge_ids
        ids = extract_bucketize_ids(data)
        id_set = set(ids)
        found = sorted(id_set.intersection(BANNED_IDS))

        if found:
            total_found.update(found)

        total_extracted += len(ids)
        runs_result.append({
            "run": run,
            "extracted_count": len(ids),
            "found_banned": found,
            "ids": ids,
        })

        _log(f"extracted={len(ids)} found_banned={len(found)}")
        if found:
            _log(f"FOUND_BANNED: {found}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    summary = {
        "test_key": TEST_KEY,
        "placement": name,
        "url": url,
        "runs": RUNS,
        "timeout_sec": TIMEOUT,
        "sleep_sec": SLEEP_SEC,
        "target_nodes": TARGET_NODES,  # ✅ เพิ่ม: บันทึก nodes ที่ใช้
        "avg_extracted_per_run": round(total_extracted / max(1, RUNS), 2),
        "total_unique_banned_found": len(total_found),
        "banned_found_list": sorted(total_found),
        "status": "FAIL" if total_found else "PASS",
    }

    write_all_ids(runs_result, out_all_ids)
    write_html(summary, runs_result, out_html)

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "runs": runs_result}, f, indent=2, ensure_ascii=False)

    _log("DONE")
    _log(f"HTML REPORT -> {out_html}")

    if total_found:
        raise AssertionError(
            f"{TEST_KEY} [{name}] FAIL: found banned IDs in bucketize nodes: {sorted(total_found)}"
        )

    return summary


# ===================================
# ✅ PYTEST ENTRY (Xray mapping)
# ===================================
def test_verify_must_not_logic_sfv_p7():
    summary = run_check(PLACEMENTS[0])
    print("RESULT:", summary["status"],
          f"| placement={summary['placement']}",
          f"| banned_found={summary['total_unique_banned_found']}")


def test_verify_must_not_logic_sfv_p8():
    summary = run_check(PLACEMENTS[1])
    print("RESULT:", summary["status"],
          f"| placement={summary['placement']}",
          f"| banned_found={summary['total_unique_banned_found']}")
    
def test_verify_must_not_logic():
    test_verify_must_not_logic_sfv_p7()
    test_verify_must_not_logic_sfv_p8()


if __name__ == "__main__":
    failures = []
    for p in PLACEMENTS:
        try:
            run_check(p)
        except AssertionError as e:
            failures.append(str(e))

    if failures:
        raise AssertionError("\n".join(failures))