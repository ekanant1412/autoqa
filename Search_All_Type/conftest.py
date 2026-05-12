"""
conftest.py — Evidence collection for Search_All_Type test suites
- evidence fixture  : ใช้ใน Verify_search_711.py (url, data_sample, item_count)
- pytest hook       : บันทึก outcome (passed/failed + เหตุผล) ทุก test ลง JSON
- ผลลัพธ์          : reports/evidence/<test_id>.json ต่อ 1 test case
"""

import json
import os
import pytest

# ─── Evidence fixture ───────────────────────────────────────────────────────

@pytest.fixture
def evidence(request):
    """Fixture สำหรับ test ที่ต้องการบันทึก url / data_sample / item_count"""
    data = {
        "test_name":   request.node.name,
        "test_nodeid": request.node.nodeid,
    }
    yield data
    # บันทึกทันทีหลัง test body จบ (ก่อน teardown)
    _save_evidence(request.node.nodeid, data)


# ─── Hook: บันทึก outcome ทุก test ─────────────────────────────────────────

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    if call.when != "call":
        return

    evidence_dir = os.path.join("reports", "evidence")
    os.makedirs(evidence_dir, exist_ok=True)

    safe_name = (
        item.nodeid
        .replace("/", "_")
        .replace("::", "__")
        .replace(" ", "_")
        .replace("[", "_")
        .replace("]", "_")
    )
    filepath = os.path.join(evidence_dir, f"{safe_name}.json")

    # โหลด evidence ที่ fixture เขียนไว้ก่อนหน้า (ถ้ามี)
    existing = {}
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = {}

    # เติมข้อมูล outcome
    existing.setdefault("test_name",   item.name)
    existing.setdefault("test_nodeid", item.nodeid)
    existing["outcome"]  = rep.outcome          # "passed" | "failed" | "error"
    existing["duration"] = round(getattr(rep, "duration", 0) or 0, 3)

    if rep.failed and rep.longrepr:
        existing["failure_reason"] = str(rep.longrepr)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)


# ─── Helper ─────────────────────────────────────────────────────────────────

def _save_evidence(nodeid: str, data: dict):
    evidence_dir = os.path.join("reports", "evidence")
    os.makedirs(evidence_dir, exist_ok=True)

    safe_name = (
        nodeid
        .replace("/", "_")
        .replace("::", "__")
        .replace(" ", "_")
        .replace("[", "_")
        .replace("]", "_")
    )
    filepath = os.path.join(evidence_dir, f"{safe_name}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
