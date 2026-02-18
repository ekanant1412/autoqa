import json
import re
import os
import csv
from urllib.parse import urlparse, parse_qs, unquote
import requests
import pytest

COLLECTION_PATH = "Test.postman_collection.json"
TIMEOUT = 20

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# =====================================================
# UNIVERSAL RESPONSE → EXTRACT IDS
# =====================================================
def deep_find_first(obj, keys):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and v is not None:
                return v
            found = deep_find_first(v, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = deep_find_first(it, keys)
            if found is not None:
                return found
    return None


def extract_item_ids(universal_resp_json):
    # case 1: ids array
    ids = deep_find_first(universal_resp_json, {"ids"})
    if isinstance(ids, list) and ids:
        return [x for x in ids if isinstance(x, str) and x.strip()]

    # case 2: items / targetItems
    items = deep_find_first(universal_resp_json, {"items", "targetItems"})
    if isinstance(items, list):
        out = []
        for it in items:
            if isinstance(it, dict) and it.get("id"):
                out.append(it["id"])
        return out

    # fallback regex
    s = json.dumps(universal_resp_json)
    return re.findall(r'"id"\s*:\s*"([^"]+)"', s)


# =====================================================
# METADATA → FIND ITEM BY ID
# =====================================================
def find_item_obj_by_id(obj, item_id):
    if isinstance(obj, dict):
        if obj.get("id") == item_id:
            return obj
        for v in obj.values():
            found = find_item_obj_by_id(v, item_id)
            if found:
                return found
    elif isinstance(obj, list):
        for it in obj:
            found = find_item_obj_by_id(it, item_id)
            if found:
                return found
    return None


def extract_content_type_for_id(meta_json, item_id):
    # 1) ถ้า response เป็น item ตรงๆ
    if isinstance(meta_json, dict):
        # บาง API ไม่มี id หรือใช้ field อื่น → ไม่ต้องบังคับว่าต้อง match id
        direct = (
            meta_json.get("content_type")
            or meta_json.get("contentType")
            or meta_json.get("content-type")
        )
        if direct:
            return direct

        # บางที object ตรงๆ แต่ id ชื่ออื่น ลอง map เพิ่มได้ตามจริง
        # เช่น meta_json.get("content_id") / meta_json.get("item_id") ฯลฯ

    # 2) ถ้าเป็นโครงสร้างซ้อนกัน ให้หา object ด้วย id ก่อน
    item_obj = find_item_obj_by_id(meta_json, item_id)
    if isinstance(item_obj, dict):
        return (
            item_obj.get("content_type")
            or item_obj.get("contentType")
            or item_obj.get("content-type")
        )

    # 3) สุดท้าย: คุ้ยหา key content_type ที่ไหนก็ได้ (ไม่ผูกกับ id)
    def deep_find_first_key(obj, keys):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in keys and v is not None:
                    return v
                found = deep_find_first_key(v, keys)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = deep_find_first_key(it, keys)
                if found is not None:
                    return found
        return None

    return deep_find_first_key(meta_json, {"content_type", "contentType", "content-type"})



# =====================================================
# EXPECTED FROM REQUEST NAME
# Supports:
#   content_type: 'sfv'
#   content_type='sfv'
#   content_type: sfv
#   content_type=sfv
# =====================================================
_CT_PATTERNS = [
    r"content_type\s*:\s*'([^']+)'",
    r'content_type\s*:\s*"([^"]+)"',
    r"content_type\s*=\s*'([^']+)'",
    r'content_type\s*=\s*"([^"]+)"',
    r"content_type\s*:\s*([A-Za-z0-9_,-]+)",
    r"content_type\s*=\s*([A-Za-z0-9_,-]+)",
]


def extract_expected_from_name(name: str):
    if not name:
        return None
    for pat in _CT_PATTERNS:
        m = re.search(pat, name, flags=re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return None


# =====================================================
# BUILD TEST CASES FROM POSTMAN COLLECTION
# =====================================================
def build_cases_from_collection(path):
    with open(path, "r", encoding="utf-8") as f:
        col = json.load(f)

    cases = []
    for it in col.get("item", []):
        name = it.get("name", "Unnamed")
        raw_url = it.get("request", {}).get("url", {}).get("raw")
        if not raw_url:
            continue

        expected = extract_expected_from_name(name)
        if not expected:
            continue

        parsed = urlparse(raw_url)
        qs = parse_qs(parsed.query)

        metadata_item_url = (qs.get("metadata_item_url", [None])[0] or "").strip()
        metadata_item_url = unquote(metadata_item_url)

        if not metadata_item_url:
            continue

        cases.append(
            {
                "name": name,
                "universal_url": raw_url,
                "expected": expected,
                "metadata_url": metadata_item_url.rstrip("/"),
            }
        )

    return cases


CASES = build_cases_from_collection(COLLECTION_PATH)


def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-\.]+", "_", s.strip())
    return s[:180] if len(s) > 180 else s


def write_csv(path, rows, headers):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# =====================================================
# TEST
# =====================================================
@pytest.mark.parametrize("case", CASES, ids=lambda c: c["name"])
def test_content_type(case):
    print("\n" + "=" * 110)
    print("CASE:", case["name"])
    print("Expected(from name):", case["expected"])
    print("Universal URL:", case["universal_url"])
    print("Metadata base:", case["metadata_url"])

    # ---- call universal API ----
    r = requests.get(case["universal_url"], timeout=TIMEOUT)
    assert r.status_code == 200

    universal_json = r.json()
    item_ids = extract_item_ids(universal_json)
    assert item_ids, "No IDs found from universal response"

    # ---- validate metadata for ALL IDs ----
    rows = []
    mismatches = []
    missing = []
    ct_count = {}

    for idx, item_id in enumerate(item_ids, start=1):
        # try /{id}
        url_a = f"{case['metadata_url']}/{item_id}"
        ra = requests.get(url_a, timeout=TIMEOUT)

        if ra.status_code == 200:
            meta_json = ra.json()
            meta_hit = "path"
            meta_status = 200
        else:
            # fallback ?id=
            url_b = f"{case['metadata_url']}?id={item_id}"
            rb = requests.get(url_b, timeout=TIMEOUT)
            meta_status = rb.status_code
            assert rb.status_code == 200, f"Metadata lookup failed for {item_id}: {rb.status_code}"
            meta_json = rb.json()
            meta_hit = "query"

        actual = extract_content_type_for_id(meta_json, item_id)
        expected = case["expected"]

        if actual is None:
            status = "MISSING"
            missing.append(item_id)
            actual_str = ""
        else:
            actual_str = str(actual).strip()
            ct_count[actual_str] = ct_count.get(actual_str, 0) + 1
            if actual_str != expected:
                status = "MISMATCH"
                mismatches.append((item_id, actual_str))
            else:
                status = "PASS"

        rows.append(
            {
                "index": idx,
                "item_id": item_id,
                "expected_content_type": expected,
                "actual_content_type": actual_str,
                "status": status,
                "metadata_lookup": meta_hit,
                "metadata_http_status": meta_status,
            }
        )

    # ---- PRINT CLEAR TABLE (console) ----
    print("\n--- ITEMS (first 50) ---")
    print("index | status    | item_id           | expected | actual")
    print("-" * 110)
    for r in rows[:50]:
        print(
            f"{str(r['index']).rjust(5)} | "
            f"{r['status'].ljust(9)} | "
            f"{r['item_id'][:16].ljust(16)} | "
            f"{r['expected_content_type'][:20].ljust(20)} | "
            f"{(r['actual_content_type'] or '-')[:30]}"
        )
    if len(rows) > 50:
        print(f"... and {len(rows) - 50} more items")

    # ---- SUMMARY ----
    print("\n===== SUMMARY =====")
    print("Total IDs:", len(item_ids))
    print("PASS:", sum(1 for r in rows if r["status"] == "PASS"))
    print("MISSING:", len(missing))
    print("MISMATCH:", len(mismatches))

    if ct_count:
        # show distribution of actual content_type
        top = sorted(ct_count.items(), key=lambda x: (-x[1], x[0]))
        print("\nActual content_type distribution:")
        for k, v in top[:20]:
            print(f" - {k}: {v}")
        if len(top) > 20:
            print(" ...")

    if missing:
        print("Missing sample:", missing[:10])
    if mismatches:
        print("Mismatch sample:", mismatches[:10])

    # ---- EXPORT REPORT (CSV + JSON) ----
    base = safe_filename(case["name"])
    csv_path = os.path.join(REPORT_DIR, f"{base}.content_type_report.csv")
    json_path = os.path.join(REPORT_DIR, f"{base}.content_type_report.json")

    write_csv(
        csv_path,
        rows,
        headers=[
            "index",
            "item_id",
            "expected_content_type",
            "actual_content_type",
            "status",
            "metadata_lookup",
            "metadata_http_status",
        ],
    )
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "case_name": case["name"],
                "expected": case["expected"],
                "universal_url": case["universal_url"],
                "metadata_url": case["metadata_url"],
                "total_ids": len(item_ids),
                "missing": missing,
                "mismatches": mismatches,
                "rows": rows,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print("\nReport saved:")
    print("-", csv_path)
    print("-", json_path)

    # ---- ASSERT ----
    assert not mismatches, f"Mismatch found: {mismatches[:20]}"
