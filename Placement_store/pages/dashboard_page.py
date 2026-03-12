"""
Page Object Model: Dashboard / Placements List Page
URL: /dashboard
"""

from playwright.sync_api import Page, expect, Locator


class DashboardPage:
    """Page Object for the main Dashboard / Placements list page."""

    def __init__(self, page: Page):
        self.page = page
        self.url = "http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th/dashboard"

        # Locators
        self.heading = page.get_by_text("Placements", exact=False)
        self.create_button = page.locator("button:has-text('Create')").last
        self.search_field = page.get_by_placeholder("Search all fields...")
        self.show_running_checkbox = page.get_by_label("Show running experiments")
        self.filter_all = page.get_by_role("button", name="All")
        self.filter_v1 = page.get_by_role("button", name="v1")
        self.filter_v2 = page.get_by_role("button", name="v2")
        self.placement_table = page.locator("table, [class*='table'], [class*='list']")

    def navigate(self):
        self.page.goto(self.url)
        # Wait for Placements list heading or search bar to appear
        self.page.wait_for_selector("input[placeholder='Search all fields...']", timeout=20000)

    def click_create(self):
        self.create_button.wait_for(timeout=10000)
        self.create_button.click()
        # Wait for the placement name input to appear (more reliable than text match)
        self.page.wait_for_selector("input[placeholder='Enter placement']", timeout=15000)

    def search_placement(self, name: str):
        self.search_field.fill(name)
        self.page.wait_for_timeout(800)

    def get_placement_row(self, name: str) -> Locator:
        return self.page.get_by_text(name, exact=True)

    def is_placement_visible(self, name: str) -> bool:
        return self.page.get_by_text(name, exact=True).is_visible()
