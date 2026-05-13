import os
import re
import json
import zipfile
import pytest
import xml.etree.ElementTree as ET
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


# ── sfv-p4 seen-item test options ─────────────────────────────────────────────

def pytest_addoption(parser):
    parser.addoption(
        "--ssoId",
        action="store",
        default=None,
        help="SSO ID สำหรับ test run นี้ (ใช้กับ test_sfv_seen_items.py)",
    )
    parser.addoption(
        "--max-cursors",
        action="store",
        default="5",
        help="จำนวน cursor สูงสุดที่จะทดสอบ (default: 5)",
    )
    parser.addoption(
        "--save-responses",
        action="store_true",
        default=False,
        help="บันทึก raw JSON response ทุก cursor ลงไฟล์",
    )
    parser.addoption(
        "--dag",
        action="store",
        default="all",
        help=(
            "DAG ที่จะทดสอบ: 'all' (default) หรือชื่อ DAG คั่นด้วย comma "
            "เช่น 'sfv-p4' หรือ 'sfv-p4,sfv-p5,sfd-p1'"
        ),
    )


# ── Xray / evidence hooks (เดิมจาก coftest.py) ────────────────────────────────

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    if rep.when != "call":
        return

    key = _get_test_key(item.nodeid)
    if not key:
        return

    src_dir = os.path.join("reports", key)
    ev_dir = os.path.join("reports", "evidence", key)
    os.makedirs(ev_dir, exist_ok=True)

    meta = {
        "test_key": key,
        "nodeid": item.nodeid,
        "outcome": rep.outcome,
        "duration_sec": getattr(rep, "duration", None),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    with open(os.path.join(ev_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    if os.path.isdir(src_dir):
        zip_path = os.path.join(ev_dir, "evidence.zip")
        _zip_folder(src_dir, zip_path)
    else:
        zip_path = os.path.join(ev_dir, "evidence.zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(os.path.join(ev_dir, "meta.json"), "meta.json")


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    os.makedirs("reports", exist_ok=True)

    junit_path = "reports/junit.xml"
    if os.path.exists(junit_path):
        tree = ET.parse(junit_path)
        root = tree.getroot()
        for testcase in root.iter("testcase"):
            name = testcase.get("name", "")
            m = TESTKEY_RE.search(name)
            if m:
                key = m.group(1).replace("_", "-")
                props = testcase.find("properties")
                if props is None:
                    props = ET.SubElement(testcase, "properties")
                prop = ET.SubElement(props, "property")
                prop.set("name", "test_key")
                prop.set("value", key)
        tree.write(junit_path, encoding="unicode", xml_declaration=True)
