import json
import os
import re
import sys
import unicodedata
from typing import Any, Dict, List, Tuple

import requests

# =====================================================
# 1) URL (ใช้ของคุณเป็น default แต่ override ได้ด้วย env URL)
# =====================================================
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

# =====================================================
# 2) KEYWORDS: ต้องเจออย่างน้อย 1 คำใน (title/tags/article_category)
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
# 3) 4 Nodes ที่ต้องตรวจ
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

    # normalize unicode (เช่น full-width)
    text = unicodedata.normalize("NFKC", text)

    # normalize dash ( - / – / — / − )
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")

    # collapse spaces
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
    found = []
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
        "fail_samples": [],   # เก็บแค่บางส่วนไว้แสดง
        "note": "",
    }

    # result ว่าง = OK (0 items) — ถ้าคุณอยากให้ถือว่า fail บอกได้เดี๋ยวเพิ่ม flag
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
# Main
# =====================================================
def main():
    print("Fetching URL:")
    print(URL)
    print("Timeout:", TIMEOUT_SEC, "sec")

    r = requests.get(URL, timeout=TIMEOUT_SEC)
    print("HTTP:", r.status_code)
    r.raise_for_status()
    data = r.json()

    # Save raw response (เผื่อแนบ evidence / debug)
    os.makedirs("artifacts", exist_ok=True)
    raw_path = "artifacts/universal_debug_response.json"
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved raw response:", raw_path)

    nodes = deep_find_nodes(data, TARGET_NODES)
    found_names = {n.get("name") for n in nodes if isinstance(n, dict)}
    missing_nodes = [n for n in TARGET_NODES if n not in found_names]

    node_reports = []
    total_all = 0
    failed_all = 0

    for node in sorted(nodes, key=lambda x: x.get("name", "")):
        rep = validate_node(node)
        node_reports.append(rep)
        total_all += rep["total_items"]
        failed_all += rep["failed_items"]

    print("\n========== 7-11 Keyword Check (Bucketize Nodes) ==========")
    print("Keywords:", ", ".join(KEYWORDS))
    print("")

    for rep in node_reports:
        print(f"NODE: {rep['node']}")
        print(f"  total_items  : {rep['total_items']}")
        print(f"  failed_items : {rep['failed_items']}")
        if rep["note"]:
            print(f"  note         : {rep['note']}")
        if rep["failed_items"] > 0:
            print("  FAIL samples:")
            for row in rep["fail_samples"]:
                print(f"    - [{row['bucket']}] id={row['id']} title={str(row['title'])[:100]!r}")
        print("")

    if missing_nodes:
        print("⚠️ Missing nodes (not found in response JSON):", ", ".join(missing_nodes))
        print("   (ไม่ถือว่า fail ถ้า response ไม่ได้ include node นั้นใน verbose/debug)\n")

    # Save report
    report_path = "artifacts/bucketize_711_keyword_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "url": URL,
                "keywords": KEYWORDS,
                "target_nodes": TARGET_NODES,
                "missing_nodes": missing_nodes,
                "total_items_all": total_all,
                "failed_all": failed_all,
                "node_reports": node_reports,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print("Saved report:", report_path)

    # Fail for CI if actual violation exists
    if failed_all > 0:
        print("\n❌ FAIL: พบ item ที่ไม่มี keyword ใน title/tags/article_category")
        sys.exit(1)

    print("\n✅ PASS: ทุก item ที่ออกมา มี keyword อย่างน้อย 1 คำใน 3 field")
    sys.exit(0)

if __name__ == "__main__":
    main()