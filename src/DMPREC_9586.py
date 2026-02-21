import json
import os
import re
import unicodedata
from typing import Any, Dict, List, Tuple

import requests

# ===================== CONFIG =====================
TEST_KEY = "DMPREC-9586"

DEFAULT_URL = (
    "http://ai-universal-service-711.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/sfv-p7"
    "?shelfId=BJq5rZqYzjgJ"
    "&total_candidates=300"
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

URL = os.getenv("URL", DEFAULT_URL)
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "25"))

REPORT_DIR = "reports"
ART_DIR = f"{REPORT_DIR}/{TEST_KEY}"
os.makedirs(ART_DIR, exist_ok=True)

RAW_PATH = f"{ART_DIR}/universal_debug_response.json"
REPORT_PATH = f"{ART_DIR}/bucketize_711_keyword_report.json"

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

            hit_title, _ = contains_keyword(title)
            hit_tags, _ = contains_keyword(join_field(tags))
            hit_cat, _ = contains_keyword(join_field(cats))

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
def run_check() -> Dict[str, Any]:
    r = requests.get(URL, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()

    # Save raw response (evidence)
    with open(RAW_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    nodes = deep_find_nodes(data, TARGET_NODES)
    found_names = {n.get("name") for n in nodes if isinstance(n, dict)}
    missing_nodes = [n for n in TARGET_NODES if n not in found_names]

    node_reports: List[Dict[str, Any]] = []
    total_all = 0
    failed_all = 0

    for node in sorted(nodes, key=lambda x: x.get("name", "")):
        rep = validate_node(node)
        node_reports.append(rep)
        total_all += rep["total_items"]
        failed_all += rep["failed_items"]

    result = {
        "test_key": TEST_KEY,
        "url": URL,
        "keywords": KEYWORDS,
        "target_nodes": TARGET_NODES,
        "missing_nodes": missing_nodes,
        "total_items_all": total_all,
        "failed_all": failed_all,
        "node_reports": node_reports,
        "status": "FAIL" if failed_all > 0 else "PASS",
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # ทำให้ pytest fail จริงถ้าเจอ violation
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
            f"{TEST_KEY} FAIL: found {failed_all} item(s) without keyword. sample={sample}"
        )

    return result

# =====================================================
# ✅ PYTEST ENTRY (Xray mapping)
# =====================================================
def test_DMPREC_9586():
    result = run_check()
    print("RESULT:", result["status"], "failed_all=", result["failed_all"])

# =====================================================
if __name__ == "__main__":
    run_check()
