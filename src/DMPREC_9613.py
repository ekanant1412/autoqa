import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import requests

# ===================================
# CONFIG
# ===================================
TEST_KEY = "DMPREC-9613"

URL = (
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
    "&ga_id=100118391.0851155978"
    "&is_use_live=true"
    "&verbose=debug"
    "&pool_latest_date=365"
)

RUNS = 20
TIMEOUT = 20
SLEEP_SEC = 0.2  # เว้นจังหวะยิง (กัน cache/กัน rate)

BANNED_IDS: Set[str] = {
    "wj5GOky1mkYx","yVb5Mwjx55yK","09pGq8RbEv1K","5GMzjgY2VO8A",
    "xo4XD2gbaJpo","2OramNWW42mM","lk1xWMlyB3pq","rGRzA9EdZRLG",
    "QXzdvLe5y5yq","nY4NvWQvwyZ5","KRZ0EyX7X6bW","8GYgr8R94B5l",
    "npXJYPV0LPWb","G5pjqMJekWyP","YoQPY686MqbN","a13LzAMLLVNg",
    "YVdk7XjNLn5p","0XNNmQDmN6LX","jl1Oq5YMADw0","WkZlKxJoW3wq",
    "4OeVdaXapd5l","LVQ1o5Wd8QdV","6kMBqG4BMD3y"
}

# ===================================
# REPORT OUTPUT
# ===================================
REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

OUT_LOG = f"{ART_DIR}/tc_exclude_merge_page.log"
OUT_HTML = f"{ART_DIR}/tc_exclude_merge_page_report.html"
OUT_JSON = f"{ART_DIR}/tc_exclude_merge_page_summary.json"
OUT_ALL_IDS = f"{ART_DIR}/tc_exclude_merge_page_all_ids.txt"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    line = f"{_now()} | {msg}"
    print(line)
    with open(OUT_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ===================================
# HELPERS
# ===================================
def deep_find(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = deep_find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = deep_find(it, key)
            if r is not None:
                return r
    return None


def extract_merge_ids(json_data: Dict[str, Any]) -> List[str]:
    """
    Extract IDs from merge_page.result.items:
      - CMS item: item["id"]
      - LIVE wrapper: item["items"][*]["Id"]  (เก็บเป็น string)
    """
    merge_page = deep_find(json_data, "merge_page")
    if not isinstance(merge_page, dict):
        return []

    items = merge_page.get("result", {}).get("items", [])
    if not isinstance(items, list):
        return []

    ids: List[str] = []

    for it in items:
        if not isinstance(it, dict):
            continue

        # CMS item id
        if isinstance(it.get("id"), str) and it["id"].strip():
            ids.append(it["id"].strip())

        # LIVE wrapper ids
        if isinstance(it.get("items"), list):
            for live in it["items"]:
                if isinstance(live, dict) and live.get("Id") is not None:
                    ids.append(str(live["Id"]).strip())

    return [x for x in ids if x]


# ===================================
# REPORT WRITERS
# ===================================
def write_all_ids(runs: List[Dict[str, Any]]) -> None:
    with open(OUT_ALL_IDS, "w", encoding="utf-8") as f:
        for r in runs:
            f.write(f"\n===== RUN {r['run']} =====\n")
            f.write(f"extracted={r['extracted_count']}\n")
            if r["found_banned"]:
                f.write(f"FOUND_BANNED={','.join(r['found_banned'])}\n")
            for i, _id in enumerate(r["ids"], 1):
                f.write(f"{i:03d} {_id}\n")


def write_html(summary: Dict[str, Any], runs: List[Dict[str, Any]]) -> None:
    html: List[str] = []
    html.append("<html><head><meta charset='utf-8'>")
    html.append("<style>")
    html.append("body{font-family:Arial, sans-serif} table{border-collapse:collapse}")
    html.append("td,th{border:1px solid #ccc;padding:6px;font-size:12px;vertical-align:top}")
    html.append(".fail{background:#ffe5e5} .pass{background:#e7ffe7}")
    html.append("</style></head><body>")

    html.append(f"<h2>Exclude Validation Report: {TEST_KEY}</h2>")

    html.append("<h3>Summary</h3><table>")
    for k, v in summary.items():
        html.append(f"<tr><th>{k}</th><td>{v}</td></tr>")
    html.append("</table>")

    html.append("<h3>Runs</h3>")
    html.append("<table>")
    html.append("<tr><th>Run</th><th>Extracted</th><th>Found banned</th><th>Sample IDs</th></tr>")

    for r in runs:
        klass = "fail" if r["found_banned"] else "pass"
        sample = "<br>".join(r["ids"][:20])  # โชว์แค่ 20 ตัวแรกกันยาว
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

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write("".join(html))


# ===================================
# MAIN CHECK
# ===================================
def run_exclude_check() -> Dict[str, Any]:
    # reset log each run of pytest (ให้ไฟล์ใหม่เสมอ)
    open(OUT_LOG, "w", encoding="utf-8").close()

    log(f"TEST={TEST_KEY}")
    log("START EXCLUDE VALIDATION")

    runs_result: List[Dict[str, Any]] = []
    total_found: Set[str] = set()
    total_extracted = 0

    for run in range(1, RUNS + 1):
        log(f"RUN {run}")

        resp = requests.get(URL, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        ids = extract_merge_ids(data)
        id_set = set(ids)

        found = sorted(id_set.intersection(BANNED_IDS))
        if found:
            total_found.update(found)

        total_extracted += len(ids)

        runs_result.append(
            {
                "run": run,
                "extracted_count": len(ids),
                "found_banned": found,
                "ids": ids,
            }
        )

        log(f"extracted={len(ids)} found_banned={len(found)}")
        if found:
            log(f"FOUND_BANNED: {found}")

        if SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    summary = {
        "test_key": TEST_KEY,
        "url": URL,
        "runs": RUNS,
        "timeout_sec": TIMEOUT,
        "sleep_sec": SLEEP_SEC,
        "avg_extracted_per_run": round(total_extracted / max(1, RUNS), 2),
        "total_unique_banned_found": len(total_found),
        "banned_found_list": sorted(total_found),
        "status": "FAIL" if total_found else "PASS",
    }

    write_all_ids(runs_result)
    write_html(summary, runs_result)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "runs": runs_result}, f, indent=2, ensure_ascii=False)

    log("DONE")
    log(f"HTML REPORT -> {OUT_HTML}")

    if total_found:
        raise AssertionError(f"{TEST_KEY} FAIL: found banned IDs in merge_page: {sorted(total_found)}")

    return summary


# ===================================
# ✅ PYTEST ENTRY (Xray mapping)
# ===================================
def test_DMPREC_9613():
    result = run_exclude_check()
    print("RESULT:", result["status"])