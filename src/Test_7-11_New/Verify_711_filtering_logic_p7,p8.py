import json
import os
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9586"

PLACEMENTS = [
    {
        "name": "sfv-p7",
        "test_key": "DMPREC-9586-P7",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p7"
            "?shelfId=BJq5rZqYzjgJ"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=802218391.0851147978"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
        ),
    },
    {
        "name": "sfv-p8",
        "test_key": "DMPREC-9586-P8",
        "url": (
            "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
            "/api/v1/universal/sfv-p8"
            "?shelfId=Kaw6MLVzPWmo"
            "&total_candidates=200"
            "&pool_limit_category_items=100"
            "&language=th&pool_tophit_date=365"
            "&userId=null&pseudoId=null"
            "&cursor=1&ga_id=802218391.0851337978"
            "&is_use_live=true&verbose=debug&pool_latest_date=365"
            "&partner_id=AN9PjZR1wEol"
            "&limit=3"
            "&limit_seen_item=1"
        ),
    },
]

TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "25"))

REPORT_DIR = "reports"
ART_DIR = os.path.join(REPORT_DIR, TEST_KEY)
os.makedirs(ART_DIR, exist_ok=True)

# =====================================================
# 1) KEYWORDS: ต้องเจออย่างน้อย 1 คำใน (title/tags/article_category)
# =====================================================
KEYWORDS = [
    "7-11",
    "เซเว่น",
    "เซเว่นอีเลเว่น",
    "711",
    "รีวิวของกินเซเว่น",
    "7eleven",
    "รีวิวเซเว่น",
    "7Eleven",
    "ของกิน711",
    "7-Eleven",
    "EZYGO",
    "คูปองเซเว่น",
    "ของกิน711",
]

# =====================================================
# 2) 4 Nodes ที่ต้องตรวจ
# =====================================================
TARGET_NODES = [
    "bucketize_tophit_sfv",
    "bucketize_latest_sfv",
    "bucketize_latest_ugc_sfv",
    "bucketize_tophit_ugc_sfv",
]


# =====================================================
# Paths / Logging
# =====================================================
def make_paths(test_key: str, placement_name: str):
    safe_name = placement_name.replace("/", "_")
    base = f"{test_key}_{safe_name}"

    raw_path = os.path.join(ART_DIR, f"{base}_universal_debug_response.json")
    report_path = os.path.join(ART_DIR, f"{base}_bucketize_711_keyword_report.json")
    log_path = os.path.join(ART_DIR, f"{base}_bucketize_711_keyword.log")

    return raw_path, report_path, log_path


def tlog(log_path: str, msg: str):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(msg)


# =====================================================
# Normalization (กัน case/space/dash)
# =====================================================
def normalize(text: Any) -> str:
    if text is None:
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def join_field(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return " ".join(normalize(x) for x in v if x is not None)
    return normalize(v)


def contains_keyword(text: Any) -> Tuple[bool, str]:
    t = normalize(text)
    for kw in KEYWORDS:
        k = normalize(kw)
        if k and k in t:
            return True, kw
    return False, ""


# =====================================================
# Find nodes by "name"
# =====================================================
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


# =====================================================
# Validate one bucketize node
# =====================================================
def validate_node(node: Dict[str, Any]) -> Dict[str, Any]:
    node_name = node.get("name", "(no_name)")
    result = node.get("result")

    report = {
        "node": node_name,
        "total_items": 0,
        "failed_items": 0,
        "fail_samples": [],
        "note": "",
    }

    if not isinstance(result, dict) or not result:
        report["note"] = "result is empty (0 items) - treated as OK"
        return report

    for bucket_name, items in result.items():
        if not isinstance(items, list):
            continue

        for it in items:
            if not isinstance(it, dict):
                continue

            report["total_items"] += 1

            item_id = it.get("id") or it.get("content_id") or it.get("_id") or "(no_id)"
            title = it.get("title", "")
            tags = it.get("tags")
            cats = it.get("article_category")

            hit_title, kw_title = contains_keyword(title)
            hit_tags, kw_tags = contains_keyword(join_field(tags))
            hit_cat, kw_cat = contains_keyword(join_field(cats))

            ok = hit_title or hit_tags or hit_cat

            if not ok:
                report["failed_items"] += 1
                if len(report["fail_samples"]) < 15:
                    report["fail_samples"].append({
                        "bucket": bucket_name,
                        "id": item_id,
                        "title": title,
                        "tags": tags,
                        "article_category": cats,
                        "reason": "No keyword found in title/tags/article_category",
                    })

    return report


# =====================================================
def run_single_check(cfg: Dict[str, Any]) -> Dict[str, Any]:
    name = cfg["name"]
    test_key = cfg.get("test_key", TEST_KEY)
    url = cfg["url"]

    raw_path, report_path, log_path = make_paths(test_key, name)
    open(log_path, "w", encoding="utf-8").close()

    tlog(log_path, f"TEST={test_key}")
    tlog(log_path, f"PLACEMENT={name}")
    tlog(log_path, f"URL={url}")
    tlog(log_path, f"TIMEOUT_SEC={TIMEOUT_SEC}")

    r = requests.get(url, timeout=TIMEOUT_SEC)
    tlog(log_path, f"HTTP={r.status_code}")
    r.raise_for_status()

    data = r.json()

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tlog(log_path, f"Saved raw response: {raw_path}")

    nodes = deep_find_nodes(data, TARGET_NODES)
    found_names = {n.get("name") for n in nodes if isinstance(n, dict)}
    missing_nodes = [n for n in TARGET_NODES if n not in found_names]

    tlog(log_path, f"Found target nodes: {sorted(found_names)}")
    tlog(log_path, f"Missing nodes: {missing_nodes}")

    node_reports: List[Dict[str, Any]] = []
    total_all = 0
    failed_all = 0

    for node in sorted(nodes, key=lambda x: x.get("name", "")):
        rep = validate_node(node)
        node_reports.append(rep)
        total_all += rep["total_items"]
        failed_all += rep["failed_items"]

        tlog(
            log_path,
            f"node={rep['node']} total_items={rep['total_items']} failed_items={rep['failed_items']}"
        )

    result = {
        "test_key": test_key,
        "placement": name,
        "url": url,
        "keywords": KEYWORDS,
        "target_nodes": TARGET_NODES,
        "missing_nodes": missing_nodes,
        "total_items_all": total_all,
        "failed_all": failed_all,
        "node_reports": node_reports,
        "status": "FAIL" if failed_all > 0 else "PASS",
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    tlog(log_path, f"Saved report: {report_path}")
    tlog(log_path, f"STATUS={result['status']} failed_all={failed_all}")

    if failed_all > 0:
        sample = []
        for rep in node_reports:
            for row in rep.get("fail_samples", []):
                sample.append(f"{rep['node']}[{row['bucket']}] id={row['id']}")
                if len(sample) >= 5:
                    break
            if len(sample) >= 5:
                break

        raise AssertionError(
            f"{test_key} FAIL [{name}]: found {failed_all} item(s) without keyword. sample={sample}"
        )

    return result


def run_all_checks() -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []

    for cfg in PLACEMENTS:
        try:
            result = run_single_check(cfg)
            all_results.append(result)
        except Exception as e:
            failed_result = {
                "test_key": cfg.get("test_key", TEST_KEY),
                "placement": cfg.get("name"),
                "url": cfg.get("url"),
                "keywords": KEYWORDS,
                "target_nodes": TARGET_NODES,
                "status": "FAIL",
                "error": str(e),
            }
            all_results.append(failed_result)

            _, report_path, log_path = make_paths(
                cfg.get("test_key", TEST_KEY),
                cfg.get("name", "unknown")
            )

            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(failed_result, f, ensure_ascii=False, indent=2)

            tlog(log_path, f"ERROR={str(e)}")

    failed = [x for x in all_results if x["status"] == "FAIL"]
    if failed:
        raise AssertionError(
            "Some placements failed:\n" +
            json.dumps(failed, ensure_ascii=False, indent=2)
        )

    return all_results


# =====================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =====================================================
def test_DMPREC_9586():
    results = run_all_checks()
    print("RESULTS:", json.dumps(results, ensure_ascii=False, indent=2))


# =====================================================
if __name__ == "__main__":
    results = run_all_checks()
    print(json.dumps(results, ensure_ascii=False, indent=2))