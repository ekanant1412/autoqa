#!/bin/bash
# run_metadata_tests.sh
# รัน Metadata API parameter tests ทั้งหมด + export JUnit XML + evidence JSON
#
# Usage:
#   ./run_metadata_tests.sh                     # รันทุก test
#   ./run_metadata_tests.sh -k "privilege"       # กรองเฉพาะ privilege tests
#   ./run_metadata_tests.sh -k "sfv and not sfvseries"
#   ./run_metadata_tests.sh src/test_gameitem_parameters.py
#
# Output:
#   src/results/junit_report.xml    ← import Xray UI (Test Execution)
#   reports/test_evidence.json      ← full summary (import Xray REST API)
#   reports/evidence/<test>.json    ← per-test evidence พร้อม API IDs + comparison

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── สร้าง output directories ────────────────────────────────────────────────
mkdir -p src/results reports/evidence

echo "════════════════════════════════════════════════════════════════"
echo "  🚀 Metadata API Parameter Tests"
echo "  📁 Working dir : $SCRIPT_DIR"
echo "════════════════════════════════════════════════════════════════"

# ── รัน pytest (addopts จาก pytest.ini จะถูกใช้อัตโนมัติ) ──────────────────
python3 -m pytest "$@"

EXIT_CODE=$?

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  📄 JUnit XML  : src/results/junit_report.xml"
echo "  📋 Evidence   : reports/test_evidence.json"
echo "  📁 Per-test   : reports/evidence/"
echo "════════════════════════════════════════════════════════════════"

exit $EXIT_CODE
