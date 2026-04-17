"""
conftest.py — pytest configuration สำหรับ Metadata API parameter tests

Features:
  1. pytest_pyfunc_call  — handle (passed, msg) return pattern ของทุก test function
  2. _patch_spanner      — autouse fixture: patch compare_with_spanner ให้ print
                           full API IDs + capture structured comparison data
  3. pytest_runtest_makereport — collect result + comparison per test
  4. pytest_sessionfinish     — save evidence_report.json + per-test JSON

Output (generated in reports/):
  reports/test_evidence.json        ← full summary (import Xray via REST)
  reports/evidence/<test>.json      ← per-test JSON พร้อม API IDs + comparison
  src/results/junit_report.xml      ← JUnit XML → import Xray UI
"""

import json
import os
import re
import inspect
import pytest
from datetime import datetime, timezone

# ─── Result stores (session-scoped) ───────────────────────────────────────────
_evidence:              list = []   # legacy evidence_report.json
_test_results:          dict = {}   # nodeid → full result dict
_comparison_store:      dict = {}   # nodeid → [comparison_dicts]  (set at fixture teardown)
_failed_skipped_urls:   list = []   # {"test_name", "url", "outcome"} สำหรับ card_type tests


# ─── Legacy helpers (kept for compatibility) ──────────────────────────────────
def record_evidence(entry: dict):
    _evidence.append(entry)


@pytest.fixture(scope="module")
def evidence_collector():
    entries = []
    yield entries
    _evidence.extend(entries)


def _safe_filename(name: str) -> str:
    name = re.sub(r"[^\w\-.]", "_", name)
    return name[:120]


# ═══════════════════════════════════════════════════════════════════════════════
# 1.  ENSURE OUTPUT DIRS EXIST
# ═══════════════════════════════════════════════════════════════════════════════
def pytest_configure(config):
    root = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(os.path.join(root, "src",     "results"),  exist_ok=True)
    os.makedirs(os.path.join(root, "reports", "evidence"), exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 2.  HANDLE (passed, msg) RETURN PATTERN  →  pytest pass / fail
# ═══════════════════════════════════════════════════════════════════════════════
@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """
    ทุก test function ในโปรเจกต์นี้ return (bool, str).
    Hook นี้แปลง False → pytest.fail() และ True → pass โดยอัตโนมัติ
    โดยไม่ต้องแก้ไขไฟล์ test แต่ละไฟล์
    """
    sig      = inspect.signature(pyfuncitem.obj)
    testargs = {k: pyfuncitem.funcargs[k]
                for k in sig.parameters if k in pyfuncitem.funcargs}

    result = pyfuncitem.obj(**testargs)

    if isinstance(result, tuple) and len(result) == 2:
        passed, msg = result
        if not passed:
            pytest.fail(msg, pytrace=False)

    return True   # บอก pytest ว่า call ถูก handle แล้ว


# ═══════════════════════════════════════════════════════════════════════════════
# 3.  PATCH compare_with_spanner  →  capture API IDs + comparison details
# ═══════════════════════════════════════════════════════════════════════════════
@pytest.fixture(autouse=True)
def _patch_spanner_evidence(request):
    """
    Autouse fixture สำหรับทุก test:
    - ถ้า module มี compare_with_spanner → patch มัน
    - Patched version จะ print full API IDs list และ store comparison details
    - Restore ของเดิมหลัง test จบ (teardown)
    """
    module = request.module
    if not hasattr(module, "compare_with_spanner"):
        yield
        return

    nodeid    = request.node.nodeid
    comp_list = []
    original  = module.compare_with_spanner

    def _patched(label: str, api_ids: list) -> tuple:
        # ─── print full API ID list เป็น evidence ───────────
        print(f"\n       📋 API IDs [{label}]  ({len(api_ids)} items)")
        for _id in api_ids:
            print(f"            {_id}")

        result          = original(label, api_ids)
        _ok, _msg, det  = result

        comp_list.append({
            "label":         label,
            "api_ids":       api_ids,
            "api_count":     det.get("api_count",     len(api_ids)),
            "spanner_count": det.get("spanner_count", 0),
            "match":         det.get("match",         0),
            "only_api":      det.get("only_api",      []),
            "only_spanner":  det.get("only_spanner",  []),
        })
        return result

    module.compare_with_spanner = _patched
    yield
    module.compare_with_spanner = original        # restore
    _comparison_store[nodeid]   = comp_list        # บันทึก comparison data


# ═══════════════════════════════════════════════════════════════════════════════
# 4.  COLLECT RESULTS  +  WRITE PER-TEST JSON
# ═══════════════════════════════════════════════════════════════════════════════
@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report  = outcome.get_result()
    nodeid  = item.nodeid

    # ── จับ URL สำหรับ card_type_ordering tests ที่ fail/skip ──────────────
    if report.outcome in ("failed", "skipped") and call.when == "call":
        try:
            if hasattr(item, "callspec") and "api_response" in item.callspec.params:
                tc = item.callspec.params["api_response"]
                if isinstance(tc, dict) and "url" in tc:
                    _failed_skipped_urls.append({
                        "test_name": item.name,
                        "url":       tc["url"],
                        "outcome":   report.outcome,
                    })
        except Exception:
            pass

    # ── หลัง call phase: เก็บผล basic ──────────────────────────────────────
    if call.when == "call":
        params = {}
        if hasattr(item, "callspec"):
            params = {k: str(v) for k, v in item.callspec.params.items()}

        err_msg     = None
        skip_reason = None
        if report.failed  and report.longrepr:
            err_msg     = str(report.longrepr).strip()[-3000:]
        if report.skipped and report.longrepr:
            skip_reason = str(report.longrepr).strip()

        # derive service name from filename
        fname   = os.path.basename(str(item.fspath))
        service = re.sub(r"^test_|_parameters\.py$", "", fname)

        _test_results[nodeid] = {
            "test_id":     nodeid,
            "test_name":   item.name,
            "service":     service,
            "params":      params,
            "outcome":     report.outcome,        # "passed" / "failed" / "skipped"
            "duration_s":  round(report.duration, 3),
            "error":       err_msg,
            "skip_reason": skip_reason,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "comparisons": [],                     # เติมหลัง teardown
        }

    # ── หลัง teardown phase: fixture ถูก restore แล้ว → เติม comparison + save ──
    elif call.when == "teardown" and nodeid in _test_results:
        comparisons = _comparison_store.get(nodeid, [])
        _test_results[nodeid]["comparisons"] = comparisons

        # ─── write per-test JSON (complete at this point) ────
        root  = os.path.dirname(os.path.abspath(str(item.fspath)))
        # put evidence relative to autoqa/ root
        ev_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "reports", "evidence"
        )
        os.makedirs(ev_dir, exist_ok=True)
        fname = _safe_filename(item.name) + ".json"
        with open(os.path.join(ev_dir, fname), "w", encoding="utf-8") as f:
            json.dump(_test_results[nodeid], f, ensure_ascii=False, indent=2)


# ═══════════════════════════════════════════════════════════════════════════════
# 5.  SESSION FINISH  →  SAVE SUMMARY JSON
# ═══════════════════════════════════════════════════════════════════════════════
@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    root = os.path.dirname(os.path.abspath(__file__))

    # ── legacy evidence_report.json ─────────────────────────────────────────
    if _evidence:
        with open(os.path.join(root, "evidence_report.json"), "w", encoding="utf-8") as f:
            json.dump({
                "generated_at":       datetime.now(timezone.utc).isoformat(),
                "total_combinations": len(_evidence),
                "results":            _evidence,
            }, f, ensure_ascii=False, indent=2)

    if not _test_results:
        return

    results_list = list(_test_results.values())
    passed  = [r for r in results_list if r["outcome"] == "passed"]
    failed  = [r for r in results_list if r["outcome"] == "failed"]
    skipped = [r for r in results_list if r["outcome"] == "skipped"]

    # ── group by service ─────────────────────────────────────────────────────
    by_service: dict = {}
    for ev in results_list:
        svc = ev.get("service", "unknown")
        by_service.setdefault(svc, []).append({
            "test_name":   ev["test_name"],
            "outcome":     ev["outcome"],
            "error":       ev["error"],
            "duration_s":  ev["duration_s"],
            "comparisons": ev["comparisons"],
        })

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total":      len(results_list),
        "passed":     len(passed),
        "failed":     len(failed),
        "skipped":    len(skipped),
        "by_service": by_service,
        "results":    results_list,
    }

    os.makedirs(os.path.join(root, "reports"), exist_ok=True)
    summary_path = os.path.join(root, "reports", "test_evidence.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n{'═'*62}")
    print(f"  📊 Results    : {len(passed)}/{len(results_list)} passed"
          f"  ({len(failed)} failed, {len(skipped)} skipped)")
    print(f"  📄 JUnit XML  : src/results/junit_report.xml   ← import Xray")
    print(f"  📋 Evidence   : reports/test_evidence.json")
    print(f"  📁 Per-test   : reports/evidence/  ({len(results_list)} files)")
    print(f"{'═'*62}")


# ═══════════════════════════════════════════════════════════════════════════════
# 6.  TERMINAL SUMMARY  →  แสดง URL ของ card_type tests ที่ FAIL / SKIP
# ═══════════════════════════════════════════════════════════════════════════════
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """แสดง URL ของ card_type_ordering test cases ที่ fail และ skip ตอนสรุปท้าย"""
    failed_entries  = [e for e in _failed_skipped_urls if e["outcome"] == "failed"]
    skipped_entries = [e for e in _failed_skipped_urls if e["outcome"] == "skipped"]

    if not failed_entries and not skipped_entries:
        return

    terminalreporter.write_sep("=", "Card Type Test — Failed / Skipped URLs")

    if failed_entries:
        terminalreporter.write_line(f"\n❌  FAILED ({len(failed_entries)} cases):")
        for e in failed_entries:
            terminalreporter.write_line(f"    {e['test_name']}")
            terminalreporter.write_line(f"    → {e['url']}")
            terminalreporter.write_line("")

    if skipped_entries:
        terminalreporter.write_line(f"⏭️   SKIPPED ({len(skipped_entries)} cases):")
        for e in skipped_entries:
            terminalreporter.write_line(f"    {e['test_name']}")
            terminalreporter.write_line(f"    → {e['url']}")
            terminalreporter.write_line("")

    terminalreporter.write_sep("=", "")
