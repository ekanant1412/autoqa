import os
import re
import json
import zipfile
import pytest
from datetime import datetime

TESTKEY_RE = re.compile(r"(DMPREC[-_]\d+)")


def _get_test_key(nodeid: str):
    m = TESTKEY_RE.search(nodeid)
    if not m:
        return None
    return m.group(1).replace("_", "-")


def _zip_folder(src_dir: str, zip_path: str):
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for fn in files:
                fp = os.path.join(root, fn)
                rel = os.path.relpath(fp, src_dir)
                z.write(fp, rel)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    # สร้าง evidence ตอนจบ test call เท่านั้น
    if rep.when != "call":
        return

    key = _get_test_key(item.nodeid)
    if not key:
        return

    # แต่ละเคสของคุณเขียน output อยู่ใน reports/<DMPREC-xxxx>/ อยู่แล้ว
    src_dir = os.path.join("reports", key)
    ev_dir = os.path.join("reports", "evidence", key)
    os.makedirs(ev_dir, exist_ok=True)

    # เก็บ meta ของ test run
    meta = {
        "test_key": key,
        "nodeid": item.nodeid,
        "outcome": rep.outcome,
        "duration_sec": getattr(rep, "duration", None),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    with open(os.path.join(ev_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # ถ้าไม่มี output folder ก็ยัง zip meta อย่างเดียว
    if os.path.isdir(src_dir):
        zip_path = os.path.join(ev_dir, "evidence.zip")
        _zip_folder(src_dir, zip_path)
    else:
        # zip meta.json อย่างเดียว
        zip_path = os.path.join(ev_dir, "evidence.zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(os.path.join(ev_dir, "meta.json"), "meta.json")


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    """
    สร้าง summary report รวมทุก DMPREC
    """
    os.makedirs("reports", exist_ok=True)

    # เก็บจาก results cache ของ pytest
    # วิธีง่าย: อ่านจาก junit.xml ภายหลังได้ แต่ตรงนี้ทำ summary แบบสั้นๆ
    summary = {
        "exitstatus": exitstatus,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "note": "Use reports/junit.xml for canonical per-test results",
    }

    with open("reports/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open("reports/summary.md", "w", encoding="utf-8") as f:
        f.write("# Regression Summary\n\n")
        f.write(f"- Exit status: {exitstatus}\n")
        f.write(f"- Generated at: {summary['generated_at']}\n")
        f.write("\nArtifacts:\n")
        f.write("- reports/junit.xml\n- reports/report.html\n- reports/evidence/<DMPREC-xxxx>/evidence.zip\n")
