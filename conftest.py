import json
import os
import re
import pytest
from datetime import datetime, timezone

# ============================================================
# Evidence collection
# ============================================================
_evidence: list = []


def record_evidence(entry: dict):
    """เรียกจาก test เพื่อบันทึก evidence"""
    _evidence.append(entry)


@pytest.fixture(scope="module")
def evidence_collector():
    """fixture ให้ test ใช้ record ข้อมูลจริงที่ได้"""
    entries = []
    yield entries
    _evidence.extend(entries)


# ============================================================
# Auto evidence capture per test case
# ============================================================
_test_results: list = []


def _safe_filename(name: str) -> str:
    """แปลง test name ให้เป็น filename ที่ใช้งานได้"""
    name = re.sub(r"[^\w\-.]", "_", name)
    return name[:120]  # จำกัดความยาว


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    if call.when != "call":
        return

    # รวบรวม params ถ้ามี
    params = {}
    if hasattr(item, "callspec"):
        params = {k: str(v) for k, v in item.callspec.params.items()}

    # ดึง error message ถ้า fail
    error_msg = None
    if report.failed and report.longrepr:
        error_msg = str(report.longrepr).strip()[-2000:]

    skip_reason = None
    if report.skipped and report.longrepr:
        skip_reason = str(report.longrepr).strip()

    entry = {
        "test_id":     item.nodeid,
        "test_name":   item.name,
        "params":      params,
        "outcome":     report.outcome,
        "duration_s":  round(report.duration, 3),
        "error":       error_msg,
        "skip_reason": skip_reason,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }
    _test_results.append(entry)

    # ── save ไฟล์แยกทันทีหลัง test จบ ──────────────────────────
    os.makedirs("reports/evidence", exist_ok=True)
    fname = _safe_filename(item.name) + ".json"
    fpath = os.path.join("reports", "evidence", fname)
    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    # ── evidence_report.json (legacy) ──────────────────────────
    if _evidence:
        report = {
            "generated_at":       datetime.now(timezone.utc).isoformat(),
            "total_combinations": len(_evidence),
            "results":            _evidence,
        }
        path = "evidence_report.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\n📄 Evidence report saved → {path}")

    # ── per-test summary JSON ───────────────────────────────────
    if not _test_results:
        return

    os.makedirs("reports", exist_ok=True)

    passed  = [r for r in _test_results if r["outcome"] == "passed"]
    failed  = [r for r in _test_results if r["outcome"] == "failed"]
    skipped = [r for r in _test_results if r["outcome"] == "skipped"]

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total":   len(_test_results),
        "passed":  len(passed),
        "failed":  len(failed),
        "skipped": len(skipped),
        "results": _test_results,
    }

    out = "reports/test_evidence.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n📄 Test evidence saved → {out}  "
          f"(passed={len(passed)}, failed={len(failed)}, skipped={len(skipped)})")
    print(f"📁 Per-test files → reports/evidence/ ({len(_test_results)} files)")
