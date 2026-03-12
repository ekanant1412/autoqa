"""
conftest.py - Pytest fixtures for Atlas-RAAS Placement automation tests
Framework: Playwright (Python) + pytest-playwright
"""

import os
import pytest
from playwright.sync_api import Page, expect

BASE_URL = "http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th"
PLACEMENT_URL = f"{BASE_URL}/dashboard"
AUTH_FILE = "auth.json"


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "smoke: Run smoke tests only")
    config.addinivalue_line("markers", "regression: Run full regression suite")
    config.addinivalue_line("markers", "high: High priority test cases")
    config.addinivalue_line("markers", "medium: Medium priority test cases")
    config.addinivalue_line("markers", "low: Low priority test cases")


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    """Inject saved auth session if auth.json exists."""
    if os.path.exists(AUTH_FILE):
        return {**browser_context_args, "storage_state": AUTH_FILE}
    return browser_context_args


@pytest.fixture(scope="session")
def base_url():
    return BASE_URL


@pytest.fixture
def dashboard_page(page: Page):
    """Navigate to dashboard and return Page object."""
    page.goto(PLACEMENT_URL)
    page.wait_for_selector("input[placeholder='Search all fields...']", timeout=20000)
    return page


@pytest.fixture
def placement_create_page(page: Page):
    """Navigate to the Create Placement page via the + Create button."""
    page.goto(PLACEMENT_URL)
    page.wait_for_selector("input[placeholder='Search all fields...']", timeout=20000)
    page.locator("button:has-text('Create')").last.click()
    # Wait for placement name input to confirm form is loaded
    page.wait_for_selector("input[placeholder='Enter placement']", timeout=15000)
    return page
