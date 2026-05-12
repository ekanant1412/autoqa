# Atlas-RAAS Placement Automation Tests

Automated test suite for the **Placement Store** feature of Atlas-RAAS Dashboard.

- **Framework:** Playwright (Python) + pytest
- **Coverage:** 27 test cases (from `placement_store_xray_import.csv`)
- **Target URL:** `http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th/dashboard`

---

## Project Structure

```
atlas-raas-tests/
├── conftest.py                  # Shared pytest fixtures
├── pytest.ini                   # pytest configuration
├── requirements.txt             # Python dependencies
├── pages/
│   ├── dashboard_page.py        # Page Object: Placements list
│   └── placement_page.py        # Page Object: Create/Update Placement form
├── tests/
│   └── test_placement.py        # All 27 test cases
└── reports/                     # HTML reports (auto-generated)
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Authentication
This system requires a login session. Before running tests, either:

**Option A – Save auth state (recommended)**
```bash
playwright codegen http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th/dashboard \
  --save-storage=auth.json
```
Then add to `conftest.py`:
```python
context = browser.new_context(storage_state="auth.json")
```

**Option B – Run headed and log in manually**
```bash
pytest --headed --slowmo 1000
```

---

## Running Tests

| Command | Description |
|---|---|
| `pytest` | Run all 27 tests (headless) |
| `pytest --headed` | Run with browser visible |
| `pytest -m high` | High priority tests only |
| `pytest -m "high or medium"` | High + Medium priority |
| `pytest --slowmo 500` | Slow motion (500ms delay) |
| `pytest -k "TC01 or TC26"` | Run specific test cases |
| `pytest -n 4` | Parallel execution (4 workers) |
| `pytest --html=reports/report.html` | Generate HTML report |

---

## Test Cases Coverage

| TC | Summary | Priority |
|---|---|---|
| TC-01 | Create new placement | Medium |
| TC-02 | Placement name is required | **High** |
| TC-03 | Placement name accepts valid characters | Medium |
| TC-04 | Duplicate placement name handling | **High** |
| TC-05 | Update existing placement option | **High** |
| TC-06 | Dropdown opens when clicking split by field | Low |
| TC-07 | Multiple split selection | Medium |
| TC-08 | Add experiment | Medium |
| TC-09 | Experiment name can be entered | Medium |
| TC-10 | Force ID optional field | Medium |
| TC-11 | Force ID accepts valid value | Low |
| TC-12 | Select DAG | **High** |
| TC-13 | DAG selection saved after reload | **High** |
| TC-14 | Invalid DAG cannot be used | **High** |
| TC-15 | DAG field is required | **High** |
| TC-16 | Total ratio = 100 | **High** |
| TC-17 | Total ratio < 100 blocks save | **High** |
| TC-18 | Roll Out button behavior | Medium |
| TC-19 | Delete experiment | Medium |
| TC-20 | View Code button | Medium |
| TC-21 | Paste code after View Code | Medium |
| TC-22 | Traffic contribution bar updates | Medium |
| TC-23 | Experiment traffic display | Low |
| TC-24 | Total traffic distribution | Medium |
| TC-25 | Reset button | Medium |
| TC-26 | Save Placement success | **High** |
| TC-27 | Save disabled when form invalid | **High** |

---

## Notes

- Selectors use flexible fallback strategies to handle class name variations.
- If a test is skipped, check the skip reason — it usually means a prerequisite was not met.
- Update `VALID_DAG` in `test_placement.py` to use a real DAG URL from your environment.
