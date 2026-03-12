"""
run_all.py  –  รันทุก test suite ในที่เดียว
Usage:  python run_all.py
"""

import importlib
import os
import sys
import time
import traceback
from datetime import datetime

# ===================== CONFIG =====================
TEST_MODULES = [
    ("DMPREC_autocomplete", [
        "test_autocomplete_returns_suggestions_for_valid_keyword",
        "test_autocomplete_returns_empty_for_non_matching_keyword",
        "test_autocomplete_works_with_partial_keyword",
        "test_autocomplete_works_with_full_keyword",
        "test_autocomplete_triggers_at_correct_input_length",
        "test_autocomplete_result_limit",
        "test_autocomplete_no_duplicate_suggestions",
    ]),
    ("DMPREC_9584", ["test_DMPREC_9584"]),
    ("DMPREC_9585", ["test_DMPREC_9585"]),
    ("DMPREC_9586", ["test_DMPREC_9586"]),
    ("DMPREC_9587", ["test_DMPREC_9587"]),
    ("DMPREC_9588", ["test_DMPREC_9588"]),
    ("DMPREC_9589", ["test_DMPREC_9589"]),
    ("DMPREC_9590", ["test_DMPREC_9590"]),
    ("DMPREC_9613", ["test_DMPREC_9613"]),
    ("DMPREC_9700", ["test_DMPREC_9700"]),
]

REPORT_DIR = "reports"
os.makedirs(REPORT_DIR, exist_ok=True)

RUN_LOG = f"{REPORT_DIR}/run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


# =================================================
def tlog(msg: str, f=None):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}"
    print(line)
    target = f or open(RUN_LOG, "a", encoding="utf-8")
    target.write(line + "\n")
    if not f:
        target.close()


def run_all():
    # ให้ Python หา module ใน folder เดียวกับ script
    src_dir = os.path.dirname(os.path.abspath(__file__))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    results = []

    with open(RUN_LOG, "w", encoding="utf-8") as log_f:
        tlog("=" * 70, log_f)
        tlog(f"  RUN ALL TESTS  –  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", log_f)
        tlog("=" * 70, log_f)

        for module_name, test_fns in TEST_MODULES:
            tlog(f"\n▶ Module: {module_name}", log_f)

            try:
                mod = importlib.import_module(module_name)
            except Exception as e:
                err = f"  ❌ IMPORT ERROR: {e}"
                tlog(err, log_f)
                for fn_name in test_fns:
                    results.append({
                        "module": module_name,
                        "test": fn_name,
                        "status": "ERROR",
                        "error": str(e),
                        "elapsed": 0,
                    })
                continue

            for fn_name in test_fns:
                fn = getattr(mod, fn_name, None)
                if fn is None:
                    tlog(f"  ⚠️  {fn_name}: NOT FOUND", log_f)
                    results.append({
                        "module": module_name,
                        "test": fn_name,
                        "status": "NOT_FOUND",
                        "error": "function not found",
                        "elapsed": 0,
                    })
                    continue

                tlog(f"  ▷ {fn_name}", log_f)
                t0 = time.time()
                try:
                    fn()
                    elapsed = round(time.time() - t0, 2)
                    tlog(f"  ✅ PASS  ({elapsed}s)", log_f)
                    results.append({
                        "module": module_name,
                        "test": fn_name,
                        "status": "PASS",
                        "error": "",
                        "elapsed": elapsed,
                    })
                except Exception as e:
                    elapsed = round(time.time() - t0, 2)
                    tb = traceback.format_exc()
                    tlog(f"  ❌ FAIL  ({elapsed}s): {e}", log_f)
                    log_f.write(tb + "\n")
                    results.append({
                        "module": module_name,
                        "test": fn_name,
                        "status": "FAIL",
                        "error": str(e),
                        "elapsed": elapsed,
                    })

        # ===================== SUMMARY =====================
        tlog("\n" + "=" * 70, log_f)
        tlog("  SUMMARY", log_f)
        tlog("=" * 70, log_f)

        passed  = [r for r in results if r["status"] == "PASS"]
        failed  = [r for r in results if r["status"] == "FAIL"]
        errors  = [r for r in results if r["status"] in ("ERROR", "NOT_FOUND")]
        total   = len(results)

        col_mod  = 26
        col_test = 52
        col_stat = 10
        col_time = 8

        header = f"{'MODULE':<{col_mod}}{'TEST':<{col_test}}{'STATUS':<{col_stat}}{'TIME':>{col_time}}"
        tlog(header, log_f)
        tlog("-" * len(header), log_f)

        for r in results:
            icon = "✅" if r["status"] == "PASS" else ("❌" if r["status"] == "FAIL" else "⚠️ ")
            line = (
                f"{r['module']:<{col_mod}}"
                f"{r['test']:<{col_test}}"
                f"{icon} {r['status']:<{col_stat - 2}}"
                f"{r['elapsed']:>{col_time}.2f}s"
            )
            tlog(line, log_f)

        tlog("-" * len(header), log_f)
        tlog(
            f"Total={total}  PASS={len(passed)}  FAIL={len(failed)}  ERROR={len(errors)}"
            f"  Elapsed={sum(r['elapsed'] for r in results):.2f}s",
            log_f,
        )

        overall = "✅ ALL PASS" if not failed and not errors else f"❌ {len(failed) + len(errors)} FAILED"
        tlog(f"\nOVERALL: {overall}", log_f)
        tlog(f"Log saved: {RUN_LOG}", log_f)

        if failed:
            tlog("\nFailed tests:", log_f)
            for r in failed:
                tlog(f"  ❌ {r['module']}::{r['test']}", log_f)
                tlog(f"     {r['error']}", log_f)

    return results, (not failed and not errors)


# =================================================
if __name__ == "__main__":
    _, all_ok = run_all()
    sys.exit(0 if all_ok else 1)
