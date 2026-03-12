"""
Page Object Model: Placement Detail / Create Page
URL: /dashboard (after clicking + Create)
"""

from typing import Optional, List
from playwright.sync_api import Page, expect, Locator


class PlacementPage:
    """Page Object for Create / Update Placement form."""

    def __init__(self, page: Page):
        self.page = page

        # --- Placement section ---
        self.radio_create_new = page.get_by_label("Create new")
        self.radio_update_existing = page.get_by_label("Update existing")
        self.placement_input = page.get_by_placeholder("Enter placement")

        # --- Experiment section ---
        self.add_experiment_button = page.get_by_role("button", name="Add Experiment")
        self.view_code_button = page.get_by_role("button", name="View Code")
        self.view_performance_button = page.get_by_role("button", name="View Performance")

        # --- Traffic section ---
        self.total_traffic_label = page.get_by_text("Total Traffic Distribution")

        # --- Bottom actions ---
        self.reset_button = page.get_by_role("button", name="Reset")
        self.save_button = page.get_by_role("button", name="Save Placement")
        self.back_button = page.get_by_text("Back")

    # ------------------------------------------------------------------ #
    # Placement Name  (Ant Design input — needs click + press_sequentially)
    # ------------------------------------------------------------------ #
    def enter_placement_name(self, name: str):
        self.placement_input.click()
        self.placement_input.fill("")
        self.placement_input.press_sequentially(name, delay=30)

    def clear_placement_name(self):
        self.placement_input.click()
        self.placement_input.select_text()
        self.placement_input.press("Backspace")

    # ------------------------------------------------------------------ #
    # User group split by
    # ------------------------------------------------------------------ #
    def open_split_by_dropdown(self):
        """Click the Ant Design Select dropdown for User group split by."""
        self.page.locator(".ant-select-selector").first.click()
        self.page.wait_for_timeout(400)

    def select_split_by_value(self, value: str):
        self.open_split_by_dropdown()
        self.page.get_by_role("option", name=value).click()

    # ------------------------------------------------------------------ #
    # Experiments
    # ------------------------------------------------------------------ #
    def count_experiment_cards(self) -> int:
        """Count by number of DAG URL inputs."""
        return self.page.locator("input[placeholder='URL']").count()

    def fill_dag(self, index: int, dag_url: str):
        dag_inputs = self.page.locator("input[placeholder='URL']").all()
        if index < len(dag_inputs):
            dag_inputs[index].click()
            dag_inputs[index].fill(dag_url)

    def _set_number_input(self, locator, value: int):
        """Reliably set Ant Design number input using fill() which triggers React events."""
        locator.scroll_into_view_if_needed()
        locator.click()
        locator.fill(str(value))
        locator.press("Escape")  # commit value without moving focus to wrong field

    def fill_ratio(self, index: int, value: int):
        """Fill ratio input at given index."""
        all_num = self.page.locator("input[type='number']").all()
        ratio_inputs = all_num[1:]  # skip experiment count input
        if index < len(ratio_inputs):
            self._set_number_input(ratio_inputs[index], value)

    def fill_force_id(self, index: int, value: str):
        force_inputs = self.page.locator("input[placeholder='Enter value']").all()
        if index < len(force_inputs):
            force_inputs[index].click()
            force_inputs[index].press_sequentially(value, delay=30)

    def click_add_experiment(self):
        self.add_experiment_button.click()
        self.page.wait_for_timeout(500)

    def click_roll_out(self, index: int):
        buttons = self.page.get_by_role("button", name="Roll Out").all()
        if index < len(buttons):
            buttons[index].click(force=True)

    def hover_and_delete_experiment(self, index: int):
        """Hover the experiment card to reveal delete icon, then click it."""
        dag_inputs = self.page.locator("input[placeholder='URL']").all()
        if index < len(dag_inputs):
            # Hover over the card to make delete icon visible
            dag_inputs[index].hover()
            self.page.wait_for_timeout(300)
        # Click the delete SVG button
        delete_btns = self.page.locator("[data-icon='delete']").all()
        if index < len(delete_btns):
            delete_btns[index].click(force=True)

    # ------------------------------------------------------------------ #
    # Actions
    # ------------------------------------------------------------------ #
    def click_view_code(self):
        self.view_code_button.click()

    def click_reset(self):
        self.reset_button.click()
        self.page.wait_for_timeout(500)

    def click_save(self):
        self.save_button.scroll_into_view_if_needed()
        self.page.wait_for_timeout(300)
        self.save_button.click()

    def is_save_enabled(self) -> bool:
        return self.save_button.is_enabled()

    def select_update_existing(self, placement_name: str):
        self.radio_update_existing.click()
        # After switching, an Ant Design Select appears for existing placements
        self.page.locator(".ant-select-selector").first.click()
        self.page.wait_for_timeout(400)
        self.page.get_by_role("option", name=placement_name).click()

    # ------------------------------------------------------------------ #
    # Helper: fill all required fields for a valid placement
    # ------------------------------------------------------------------ #
    def fill_valid_placement(
        self,
        name: str,
        dag_urls: Optional[list] = None,
        ratios: Optional[dict] = None,
    ):
        """
        Fill all required fields:
        - placement name
        - DAG URL for each experiment card (each card needs its own URL)
        - ratios that sum to 100
        """
        if dag_urls is None:
            dag_urls = [
                "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p4?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
                "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p5?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
                "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p6?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
                "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p7?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
            ]
        if ratios is None:
            ratios = {"AA": 25, "AB": 25, "Control": 25, "OOE": 25}

        self.enter_placement_name(name)

        # Count how many experiment cards exist
        num_cards = self.page.locator("input[placeholder='URL']").count()
        ratio_values = list(ratios.values())

        for idx in range(num_cards):
            # Re-query fresh each iteration to avoid stale locators after DOM update
            dag_inputs = self.page.locator("input[placeholder='URL']").all()

            # Ratio inputs: only number inputs inside a container that also has a URL input
            # This reliably excludes the Experiment count field at the top
            ratio_inputs = self.page.locator(
                "div:has(input[placeholder='URL']) input[type='number']"
            ).all()

            # Fill DAG for this experiment
            url = dag_urls[idx] if idx < len(dag_urls) else dag_urls[-1]
            dag_inputs[idx].click()
            dag_inputs[idx].fill(url)
            self.page.wait_for_timeout(200)

            # Fill Ratio for this experiment
            if idx < len(ratio_inputs) and idx < len(ratio_values):
                self._set_number_input(ratio_inputs[idx], ratio_values[idx])
                self.page.wait_for_timeout(200)
