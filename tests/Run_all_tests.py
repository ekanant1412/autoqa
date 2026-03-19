"""
Run_all_tests.py
════════════════════════════════════════════════════════════════════════════════
วางไฟล์นี้ไว้ใน folder เดียวกับ 4 test files แล้วรัน:
  python ./tests/Run_all_tests.py
════════════════════════════════════════════════════════════════════════════════
"""

import subprocess
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── resolve path จาก location ของไฟล์นี้เสมอ ────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

TEST_FILES = [
    os.path.join(SCRIPT_DIR, "check_pp2vspp5_changeid_run.py"),
    os.path.join(SCRIPT_DIR, "check_pp2vspp5_changeid.py"),
    os.path.join(SCRIPT_DIR, "check_pp2vspp5.py"),
    os.path.join(SCRIPT_DIR, "check_seen_fix_bug.py"),
]

PYTEST_ARGS = ["-v", "-s"]
LOG_DIR     = os.path.join(SCRIPT_DIR, "logs")

# ══════════════════════════════════════════════════════════════════════════════
SEP = "═" * 70
sep = "─" * 70

def log(msg): print(msg, flush=True)


def run_pytest(filepath: str) -> dict:
    os.makedirs(LOG_DIR, exist_ok=True)
    filename = os.path.basename(filepath)
    log_path = os.path.join(LOG_DIR, filename.replace(".py", ".log"))
    cmd      = [sys.executable, "-m", "pytest", filepath] + PYTEST_ARGS

    start_time = datetime.now()
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"FILE  : {filepath}\nSTART : {start_time}\n{'='*70}\n\n")
        proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True)

    end_time = datetime.now()
    elapsed  = round((end_time - start_time).total_seconds(), 1)

    summary = ""
    with open(log_path, "r", encoding="utf-8") as f:
        for line in reversed(f.readlines()):
            line = line.strip()
            if "passed" in line or "failed" in line or "error" in line:
                summary = line
                break

    return {
        "file":       filename,
        "returncode": proc.returncode,
        "elapsed":    elapsed,
        "log_path":   log_path,
        "summary":    summary,
        "start_time": start_time.strftime("%H:%M:%S.%f")[:-3],
        "end_time":   end_time.strftime("%H:%M:%S.%f")[:-3],
    }


def main():
    log(f"\n{SEP}")
    log(f"  🚀 Running {len(TEST_FILES)} test files concurrently")
    log(f"  Start      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  Script dir : {SCRIPT_DIR}")
    log(SEP)

    for f in TEST_FILES:
        status = "✅ found" if os.path.exists(f) else "❌ NOT FOUND"
        log(f"  {status}  {f}")

    missing = [f for f in TEST_FILES if not os.path.exists(f)]
    if missing:
        log(f"\n  ❌ Missing files — check paths above")
        sys.exit(1)

    log(f"\n  Logs: {LOG_DIR}/")
    log(sep)

    results     = {}
    total_start = datetime.now()

    with ThreadPoolExecutor(max_workers=len(TEST_FILES)) as executor:
        futures = {executor.submit(run_pytest, f): f for f in TEST_FILES}
        for future in as_completed(futures):
            result = future.result()
            results[result["file"]] = result
            icon = "✅" if result["returncode"] == 0 else "❌"
            log(f"  {icon} DONE  {result['file']:45}  {result['elapsed']}s  → {result['log_path']}")

    total_elapsed = round((datetime.now() - total_start).total_seconds(), 1)

    log(f"\n{SEP}")
    log(f"  📊 SUMMARY")
    log(SEP)
    log(f"  {'FILE':<45}  {'START':12}  {'END':12}  {'ELAPSED':>8}  RESULT")
    log(sep)

    passed_count = failed_count = 0
    for filepath in TEST_FILES:
        r    = results[os.path.basename(filepath)]
        icon = "✅ PASS" if r["returncode"] == 0 else "❌ FAIL"
        log(f"  {r['file']:<45}  {r['start_time']:12}  {r['end_time']:12}  {str(r['elapsed'])+'s':>8}  {icon}")
        log(f"    └─ {r['summary']}")
        if r["returncode"] == 0: passed_count += 1
        else:                    failed_count += 1

    log(sep)
    log(f"  Total elapsed : {total_elapsed}s  (all ran concurrently)")
    log(f"  Passed        : {passed_count}/{len(TEST_FILES)}")
    log(f"  Failed        : {failed_count}/{len(TEST_FILES)}")
    log(f"  Logs dir      : {LOG_DIR}/")
    log(SEP)

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()