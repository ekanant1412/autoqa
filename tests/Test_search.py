#!/usr/bin/env python3
"""
BASELINE vs CANDIDATE Ordering Test
======================================
เปรียบเทียบ ordering ของ search results ระหว่าง:
  - BASELINE  : ai-raas-api / text_search (POST + Authorization)
  - CANDIDATE : ai-universal-service-new / placements (GET)
"""

import json
import os
import time
import traceback
from datetime import datetime
import xml.etree.ElementTree as ET

import requests

try:
    import openpyxl
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except ImportError:
    print("กรุณาติดตั้ง openpyxl ก่อน: pip install openpyxl")
    raise SystemExit(1)

XRAY_TEST_KEY = "DMPREC-9833"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
REPORT_DIR = os.path.join(ROOT_DIR, "reports")

# ===================================================================
# CONFIG
# ===================================================================
BASELINE_URL = (
    "https://ai-raas-api.trueid-preprod.net"
    "/personalize-rcom/v2/search-api/api/v5/text_search"
)
BASELINE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Authorization": "5b18e526f0463656f7c4329f90b7ecef9dc546aeb6adad28e911ba82",
}

CANDIDATE_URL = (
    "http://ai-universal-service-new.preprod-gcp-ai-bn.int-ai-platform.gcp.dmp.true.th"
    "/api/v1/universal/text_search"
)
CANDIDATE_PARAMS_BASE = {
    "userId": "1",
    "pseudoId": "1",
    "cursor": "1",
    "limit": "100",
    "top_k": "450",
    "ssoId": "101316473",
}

TOP_N = 100
DELAY = 1

KEYWORDS = [
    "มวยไทย",
    " joker",
    " Joker",
    " JOKER",
    " 1",
    " @",
    " ",
    "๒",
    "/",
]

TYPES = [
    "movie",
    "series",
    "livetv",
    "top_results",
    "watch",
    "sfv",
    "privilege",
    "sfvseries",
    "read",
    "channel",
    "ecommerce",
    "game",
]


def escape_attr(text):
    return "" if text is None else str(text)


def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {
            "_raw_text": resp.text[:1000] if hasattr(resp, "text") else "",
            "_json_error": "response is not valid JSON",
        }


def ensure_report_dir():
    os.makedirs(REPORT_DIR, exist_ok=True)
    print(f"DEBUG cwd        = {os.getcwd()}")
    print(f"DEBUG REPORT_DIR = {os.path.abspath(REPORT_DIR)}")


def create_junit_report(results: list, output_path: str, test_key: str):
    testsuite = ET.Element(
        "testsuite",
        name="Baseline vs Candidate Ordering Test",
        tests=str(len(results)),
        failures=str(sum(1 for r in results if not r["passed"])),
        errors="0",
    )

    for r in results:
        tc_name = f'{test_key} | kw={repr(r["search_keyword"])} | type={r["type"]}'
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            classname="baseline_vs_candidate",
            name=tc_name,
            time=str((r["bl_ms"] + r["cd_ms"]) / 1000.0),
        )

        props = ET.SubElement(testcase, "properties")
        ET.SubElement(props, "property", name="test_key", value=test_key)
        ET.SubElement(props, "property", name="keyword", value=escape_attr(r["search_keyword"]))
        ET.SubElement(props, "property", name="type", value=escape_attr(r["type"]))
        ET.SubElement(props, "property", name="bl_status", value=escape_attr(r["bl_status"]))
        ET.SubElement(props, "property", name="cd_status", value=escape_attr(r["cd_status"]))
        ET.SubElement(props, "property", name="matched_positions", value=str(r["matched_positions"]))
        ET.SubElement(props, "property", name="compared_n", value=str(r["compared_n"]))

        if not r["passed"]:
            failure = ET.SubElement(
                testcase,
                "failure",
                message=escape_attr(r["fail_reason"]) or "Ordering mismatch",
            )
            failure.text = (
                f"keyword={repr(r['search_keyword'])}, type={r['type']}\n"
                f"fail_reason={r['fail_reason']}\n"
                f"baseline_top5={r['bl_ids_top5']}\n"
                f"candidate_top5={r['cd_ids_top5']}\n"
                f"bl_status={r['bl_status']}, cd_status={r['cd_status']}\n"
            )

    testsuites = ET.Element("testsuites")
    testsuites.append(testsuite)

    tree = ET.ElementTree(testsuites)
    ET.indent(tree, space="  ", level=0)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)
    print(f"✅ JUnit report saved → {output_path}")


def build_test_cases():
    return [(kw, t) for kw in KEYWORDS for t in TYPES]


def call_baseline(keyword: str, type_val: str) -> dict:
    body = {
        "debug": False,
        "no_cache_vector": False,
        "search_keyword": keyword,
        "top_k": "450",
        "type": type_val,
    }
    try:
        resp = requests.post(BASELINE_URL, headers=BASELINE_HEADERS, json=body, timeout=30)
        rj = safe_json(resp)

        items = None
        if isinstance(rj, dict):
            for key in ("search_results", "results", "items", "data"):
                if key in rj and isinstance(rj[key], list):
                    items = rj[key]
                    break
        elif isinstance(rj, list):
            items = rj

        items = items or []
        ids = [x["id"] for x in items if isinstance(x, dict) and x.get("id")][:TOP_N]
        return {
            "status": resp.status_code,
            "ids": ids,
            "count": len(ids),
            "error": "",
            "raw": str(rj)[:400],
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "ids": [],
            "count": 0,
            "error": str(e)[:200],
            "raw": "",
        }


def call_candidate(keyword: str, type_val: str) -> dict:
    params = {
        **CANDIDATE_PARAMS_BASE,
        "search_keyword": keyword,
        "type": type_val,
    }
    try:
        resp = requests.get(CANDIDATE_URL, params=params, timeout=30)
        rj = safe_json(resp)

        items = rj.get("items") if isinstance(rj, dict) else []
        items = items or []
        ids = [x["id"] for x in items if isinstance(x, dict) and x.get("id")][:TOP_N]

        return {
            "status": resp.status_code,
            "ids": ids,
            "count": len(ids),
            "error": "",
            "raw": str(rj)[:400],
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "ids": [],
            "count": 0,
            "error": str(e)[:200],
            "raw": "",
        }


def run_test(keyword: str, type_val: str, idx: int, total: int) -> dict:
    test_name = f"CANDIDATE matches BASELINE ordering type : {type_val} (top {TOP_N})"
    print(f"[{idx:03d}/{total}] kw={repr(keyword):<14} type={type_val:<14}", end="")

    t0 = time.time()
    bl = call_baseline(keyword, type_val)
    bl_ms = int((time.time() - t0) * 1000)
    time.sleep(DELAY)

    t0 = time.time()
    cd = call_candidate(keyword, type_val)
    cd_ms = int((time.time() - t0) * 1000)

    if bl["status"] != 200:
        passed = False
        fail_reason = f"BASELINE HTTP {bl['status']}: {bl['error']}"
    elif cd["status"] != 200:
        passed = False
        fail_reason = f"CANDIDATE HTTP {cd['status']}: {cd['error']}"
    elif not bl["ids"] and not cd["ids"]:
        passed = True
        fail_reason = ""
    elif not bl["ids"]:
        passed = False
        fail_reason = "BASELINE returned 0 results (CANDIDATE has results)"
    elif not cd["ids"]:
        passed = False
        fail_reason = "CANDIDATE returned 0 results (BASELINE has results)"
    else:
        n = min(TOP_N, len(bl["ids"]), len(cd["ids"]))
        passed = (bl["ids"][:n] == cd["ids"][:n])
        if not passed:
            mismatches = [
                (i + 1, bl["ids"][i], cd["ids"][i])
                for i in range(n)
                if bl["ids"][i] != cd["ids"][i]
            ]
            first_diff = mismatches[0] if mismatches else ("?", "?", "?")
            fail_reason = (
                f"{len(mismatches)}/{n} mismatches; "
                f"first diff at pos {first_diff[0]}: "
                f"BL={first_diff[1]} vs CD={first_diff[2]}"
            )
        else:
            fail_reason = ""

    label = "✅ PASS" if passed else "❌ FAIL"
    print(f"→ {label}  BL={bl_ms}ms({bl['count']}r) CD={cd_ms}ms({cd['count']}r)")

    if bl["ids"] and cd["ids"]:
        n = min(TOP_N, len(bl["ids"]), len(cd["ids"]))
        matched_positions = sum(1 for i in range(n) if bl["ids"][i] == cd["ids"][i])
    else:
        matched_positions = 0

    return {
        "test_no": idx,
        "test_name": test_name,
        "search_keyword": keyword,
        "type": type_val,
        "bl_status": bl["status"],
        "bl_ms": bl_ms,
        "bl_count": bl["count"],
        "bl_ids_top5": str(bl["ids"][:5]),
        "bl_error": bl["error"],
        "cd_status": cd["status"],
        "cd_ms": cd_ms,
        "cd_count": cd["count"],
        "cd_ids_top5": str(cd["ids"][:5]),
        "cd_error": cd["error"],
        "passed": passed,
        "fail_reason": fail_reason,
        "matched_positions": matched_positions,
        "compared_n": min(TOP_N, len(bl["ids"]), len(cd["ids"])),
    }


def create_excel_report(results: list, output_path: str):
    wb = openpyxl.Workbook()
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed

    c_blue = "1565C0"
    c_white = "FFFFFF"
    c_green = "00C851"
    c_red = "FF4444"
    c_orange = "FF6D00"
    c_lgre = "E8F5E9"
    c_lyel = "FFFDE7"

    def fill(h):
        return PatternFill(start_color=h, end_color=h, fill_type="solid")

    def border():
        s = Side(style="thin")
        return Border(left=s, right=s, top=s, bottom=s)

    center = Alignment(horizontal="center", vertical="center")
    wrap = Alignment(vertical="center", wrap_text=True)

    ws = wb.active
    ws.title = "Test Results"

    ws.merge_cells("A1:N1")
    c = ws["A1"]
    c.value = "BASELINE vs CANDIDATE Ordering Test Report"
    c.font = Font(size=15, bold=True, color=c_white)
    c.fill = fill(c_blue)
    c.alignment = center
    ws.row_dimensions[1].height = 28

    meta = [
        ("Run Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Top N Compared", str(TOP_N)),
        ("Total Tests", total, "Passed", passed),
        ("Failed", failed, "Pass Rate", f"{passed/total*100:.1f}%" if total else "0%"),
    ]
    for ri, (k1, v1, k2, v2) in enumerate(meta, 2):
        for col, val in [(1, k1), (2, v1), (5, k2), (6, v2)]:
            cell = ws.cell(row=ri, column=col, value=val)
            if col in (1, 5):
                cell.font = Font(bold=True)

    headers = [
        "#", "Test Name (keyword / type)", "Keyword", "Type",
        "BL Status", "BL Time(ms)", "BL Results",
        "CD Status", "CD Time(ms)", "CD Results",
        "Matched / N", "Result", "Fail Reason", "BASELINE top-5 IDs",
    ]
    hdr_row = 5
    for col, h in enumerate(headers, 1):
        cell = ws.cell(hdr_row, col, h)
        cell.fill = fill(c_blue)
        cell.font = Font(bold=True, color=c_white)
        cell.alignment = center
        cell.border = border()

    for r in results:
        row = hdr_row + r["test_no"]
        row_fill = fill(c_lgre) if r["passed"] else fill(c_lyel)
        matched_label = f'{r["matched_positions"]}/{r["compared_n"]}' if r["compared_n"] else "-"
        vals = [
            r["test_no"],
            f'{repr(r["search_keyword"])} / {r["type"]}',
            r["search_keyword"],
            r["type"],
            r["bl_status"], r["bl_ms"], r["bl_count"],
            r["cd_status"], r["cd_ms"], r["cd_count"],
            matched_label,
            "PASS" if r["passed"] else "FAIL",
            r["fail_reason"],
            r["bl_ids_top5"],
        ]
        for col, val in enumerate(vals, 1):
            cell = ws.cell(row, col, val)
            cell.fill = row_fill
            cell.border = border()
            cell.alignment = wrap if col in (2, 13, 14) else center
            if col == 12:
                cell.font = Font(bold=True, color=c_green if r["passed"] else c_red)

    for i, w in enumerate([4, 32, 14, 14, 10, 11, 10, 10, 11, 10, 12, 8, 50, 50], 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    wb.save(output_path)
    print(f"✅ Excel report saved → {output_path}")


def main():
    print("=" * 72)
    print("  BASELINE vs CANDIDATE Ordering Test Runner")
    print(f"  BASELINE  : {BASELINE_URL}")
    print(f"  CANDIDATE : {CANDIDATE_URL}")
    print(f"  Top N     : {TOP_N}")
    print(f"  Start     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    ensure_report_dir()

    cases = build_test_cases()
    total = len(cases)
    print(f"  Total test cases: {total}  (each case = 2 API calls)\n")

    results = []
    for i, (kw, t) in enumerate(cases, 1):
        result = run_test(kw, t, i, total)
        results.append(result)
        time.sleep(DELAY)

    passed = sum(1 for r in results if r["passed"])
    failed = total - passed

    print("\n" + "=" * 72)
    print(f"  FINAL : {passed}/{total} PASSED  |  {failed} FAILED")
    print(f"  Rate  : {passed/total*100:.1f}%")
    print("=" * 72)

    xlsx_path = os.path.join(REPORT_DIR, "Baseline_vs_Candidate_Report.xlsx")
    json_path = os.path.join(REPORT_DIR, "Baseline_vs_Candidate_Results.json")
    junit_path = os.path.join(REPORT_DIR, "junit.xml")

    print(f"DEBUG xlsx_path  = {os.path.abspath(xlsx_path)}")
    print(f"DEBUG json_path  = {os.path.abspath(json_path)}")
    print(f"DEBUG junit_path = {os.path.abspath(junit_path)}")

    create_excel_report(results, xlsx_path)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON results saved → {json_path}")

    create_junit_report(results, junit_path, XRAY_TEST_KEY)

    print("DEBUG files in reports:", os.listdir(REPORT_DIR))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        raise