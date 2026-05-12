"""
Optional conftest extension for insertion tests.
Saves a consolidated JSON + CSV summary of all test results.

Usage: place this file as conftest.py OR import from existing conftest.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime

import pytest


_RESULTS: list[dict] = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.when != "call":
        return
    if "sfv_insertion" not in item.nodeid:
        return

    # Extract test-case id from parametrize marker
    tc_id = ""
    for mark in item.iter_markers("parametrize"):
        pass  # will parse from nodeid
    if "[" in item.nodeid:
        tc_id = item.nodeid.split("[", 1)[1].rstrip("]")

    _RESULTS.append(
        {
            "test_name": item.name,
            "test_case_id": tc_id,
            "outcome": rep.outcome,          # "passed" | "failed" | "skipped"
            "duration_sec": round(getattr(rep, "duration", 0), 3),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "failure_message": (
                str(rep.longrepr) if rep.outcome == "failed" else ""
            ),
        }
    )


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    if not _RESULTS:
        return

    os.makedirs("reports", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = f"reports/sfv_insertion_results_{ts}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "exit_status": exitstatus,
                "total": len(_RESULTS),
                "passed": sum(1 for r in _RESULTS if r["outcome"] == "passed"),
                "failed": sum(1 for r in _RESULTS if r["outcome"] == "failed"),
                "skipped": sum(1 for r in _RESULTS if r["outcome"] == "skipped"),
                "results": _RESULTS,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    # CSV
    csv_path = f"reports/sfv_insertion_results_{ts}.csv"
    fieldnames = [
        "test_name", "test_case_id", "outcome",
        "duration_sec", "timestamp", "failure_message",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(_RESULTS)

    print(f"\n📊 Insertion test report saved:")
    print(f"   JSON → {json_path}")
    print(f"   CSV  → {csv_path}")
