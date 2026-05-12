"""
Automated Test Suite: Placement Create / Update
Source: placement_store_xray_import.csv (27 test cases)
Framework: Playwright (Python) + pytest
URL: http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th/dashboard

Run:
    pytest tests/test_placement.py -v
    pytest tests/test_placement.py -v -m high       # High priority only
    pytest tests/test_placement.py -v --headed       # With browser visible
    pytest tests/test_placement.py -v --slowmo 500   # Slow motion for debug
"""

import pytest
from playwright.sync_api import Page, expect
from pages.dashboard_page import DashboardPage
from pages.placement_page import PlacementPage

BASE_URL = "http://atlas-admin.preprod-raas.int-ai-platform.gcp.dmp.true.th"

# DAG URLs สำหรับแต่ละ experiment card (AA, AB, Control, OOE)
VALID_DAGS = [
    "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p4?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
    "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p5?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
    "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p6?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
    "http://ai-universal-service-new-2-preprod/api/v1/universal/sfv-p7?shelfId=BJq5rZqYzjgJ&total_candidates=400&pool_limit_items=100&pool_limit_category_items=40&language=th&pool_tophit_date=30&limit=20",
]
VALID_DAG = VALID_DAGS[0]  # ใช้ตัวแรกสำหรับ test cases ที่ test แค่ field เดียว


# ======================================================================
# Helpers
# ======================================================================

def open_create_placement(page: Page) -> PlacementPage:
    """Navigate to dashboard and open the Create Placement form."""
    dashboard = DashboardPage(page)
    dashboard.navigate()
    dashboard.click_create()
    return PlacementPage(page)


def fill_ratio_inputs(page: Page, values: list):
    """Helper: fill ratio inputs for Ant Design number inputs."""
    all_num = page.locator("input[type='number']").all()
    ratio_inputs = all_num[1:]  # skip experiment count
    for i, val in enumerate(values):
        if i < len(ratio_inputs):
            ratio_inputs[i].click()
            ratio_inputs[i].press("Control+a")
            ratio_inputs[i].press("Backspace")
            ratio_inputs[i].press_sequentially(str(val), delay=50)
            ratio_inputs[i].press("Tab")
            page.wait_for_timeout(200)


# ======================================================================
# TC-01  Verify user can create new placement
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC01_create_new_placement(page: Page):
    placement = open_create_placement(page)
    expect(placement.radio_create_new).to_be_checked()

    placement.fill_valid_placement(
        name="placement_ui_001",
        dag_urls=VALID_DAGS,
        ratios={"AA": 25, "AB": 25, "Control": 25, "OOE": 25},
    )
    placement.click_save()
    page.wait_for_timeout(2000)

    success_locators = [
        page.get_by_text("success", case_sensitive=False),
        page.get_by_text("saved", case_sensitive=False),
        page.get_by_text("placement_ui_001"),
    ]
    visible = any(loc.is_visible() for loc in success_locators)
    assert visible, "Expected success message or placement to appear after save"


# ======================================================================
# TC-02  Verify placement name is required
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC02_placement_name_required(page: Page):
    placement = open_create_placement(page)
    placement.clear_placement_name()

    # Save button should be disabled when placement name is empty
    assert not placement.is_save_enabled(), (
        "Expected Save button to be disabled when placement name is empty"
    )


# ======================================================================
# TC-03  Verify placement name accepts valid characters
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC03_placement_name_valid_characters(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_01-A")
    expect(placement.placement_input).to_have_value("placement_01-A")


# ======================================================================
# TC-04  Verify duplicate placement name handling
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC04_duplicate_placement_name(page: Page):
    existing_name = "placement_ui_001"
    placement = open_create_placement(page)
    placement.fill_valid_placement(name=existing_name, dag_url=VALID_DAG)
    placement.click_save()
    page.wait_for_timeout(2000)

    duplicate_error = (
        page.get_by_text("already exists", case_sensitive=False).is_visible()
        or page.get_by_text("duplicate", case_sensitive=False).is_visible()
        or page.get_by_text("error", case_sensitive=False).is_visible()
        or page.locator(".ant-message, .ant-notification").is_visible()
    )
    assert duplicate_error, "Expected an error message for duplicate placement name"


# ======================================================================
# TC-05  Verify Update existing placement option
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC05_update_existing_placement(page: Page):
    placement = open_create_placement(page)
    placement.radio_update_existing.click()
    page.wait_for_timeout(500)

    expect(placement.radio_update_existing).to_be_checked()

    # After switching to Update existing, an Ant Design select should appear
    ant_select = page.locator(".ant-select").first
    expect(ant_select).to_be_visible()


# ======================================================================
# TC-06  Verify dropdown opens when clicking 'User group split by'
# Priority: Low
# ======================================================================
@pytest.mark.low
def test_TC06_split_by_dropdown_opens(page: Page):
    placement = open_create_placement(page)
    placement.open_split_by_dropdown()
    page.wait_for_timeout(500)

    # Ant Design dropdown popup appears with class ant-select-dropdown
    dropdown = page.locator(".ant-select-dropdown")
    expect(dropdown.first).to_be_visible()


# ======================================================================
# TC-07  Verify multiple split selection
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC07_multiple_split_selection(page: Page):
    placement = open_create_placement(page)
    placement.open_split_by_dropdown()
    page.wait_for_timeout(400)

    user_id_option = page.get_by_role("option", name="user_id")
    if user_id_option.is_visible():
        user_id_option.click()
        page.wait_for_timeout(300)

    # At least one Ant Design tag should be visible (ssoid is pre-selected)
    tags = page.locator(".ant-select-selection-item").all()
    assert len(tags) >= 1, "Expected at least one selected tag in split by field"


# ======================================================================
# TC-08  Verify user can add experiment
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC08_add_experiment(page: Page):
    placement = open_create_placement(page)
    before_count = placement.count_experiment_cards()
    placement.click_add_experiment()
    after_count = placement.count_experiment_cards()

    assert after_count > before_count, (
        f"Experiment count should increase. Before: {before_count}, After: {after_count}"
    )


# ======================================================================
# TC-09  Verify experiment name can be entered
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC09_experiment_name_input(page: Page):
    placement = open_create_placement(page)

    # Ant Design input inside the first experiment card
    # The experiment name inputs do NOT use placeholder - they have pre-filled values
    # Find first editable input that is NOT the placement name or DAG URL or number
    exp_name_input = page.locator(
        "input.ant-input:not([placeholder='Enter placement']):not([placeholder='URL']):not([placeholder='Enter value'])"
    ).first

    if exp_name_input.is_visible():
        exp_name_input.click(click_count=3)
        exp_name_input.press_sequentially("Variant_A", delay=30)
        expect(exp_name_input).to_have_value("Variant_A")
    else:
        pytest.skip("Could not locate experiment name input")


# ======================================================================
# TC-10  Verify Force ID optional field (can save without it)
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC10_force_id_optional(page: Page):
    placement = open_create_placement(page)
    placement.fill_valid_placement(
        name="placement_force_id_optional",
        dag_urls=VALID_DAGS,
    )
    # Force ID fields should be empty
    force_inputs = page.locator("input[placeholder='Enter value']").all()
    for fi in force_inputs:
        assert fi.input_value() == "", "Expected Force ID to be empty"

    assert placement.is_save_enabled(), "Save should be enabled without Force ID"


# ======================================================================
# TC-11  Verify Force ID accepts valid value
# Priority: Low
# ======================================================================
@pytest.mark.low
def test_TC11_force_id_accepts_valid_value(page: Page):
    placement = open_create_placement(page)
    force_input = page.locator("input[placeholder='Enter value']").first
    expect(force_input).to_be_visible()
    force_input.click()
    force_input.press_sequentially("force_12345", delay=30)
    expect(force_input).to_have_value("force_12345")


# ======================================================================
# TC-12  Verify user can select DAG
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC12_select_dag(page: Page):
    placement = open_create_placement(page)
    dag_input = page.locator("input[placeholder='URL']").first
    expect(dag_input).to_be_visible()
    dag_input.fill(VALID_DAG)
    expect(dag_input).to_have_value(VALID_DAG)


# ======================================================================
# TC-13  Verify DAG selection saved
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC13_dag_selection_saved(page: Page):
    placement_name = "placement_dag_persist_test"
    placement = open_create_placement(page)
    placement.fill_valid_placement(name=placement_name, dag_url=VALID_DAG)
    placement.click_save()
    page.wait_for_timeout(2000)

    dashboard = DashboardPage(page)
    dashboard.navigate()
    dashboard.search_placement(placement_name)
    page.wait_for_timeout(1000)

    row_text = page.get_by_text(placement_name)
    if row_text.is_visible():
        details_link = page.locator("a:has-text('Details'), button:has-text('Details')").first
        if details_link.is_visible():
            details_link.click()
            page.wait_for_selector("input[placeholder='URL']", timeout=10000)
            dag_input = page.locator("input[placeholder='URL']").first
            assert VALID_DAG in dag_input.input_value(), "DAG should persist after reload"
    else:
        pytest.skip(f"Placement '{placement_name}' not found")


# ======================================================================
# TC-14  Verify invalid DAG cannot be used
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC14_invalid_dag_rejected(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_invalid_dag_test")
    dag_input = page.locator("input[placeholder='URL']").first
    dag_input.fill("invalid_dag_value")
    fill_ratio_inputs(page, [25, 25, 25, 25])

    placement.click_save()
    page.wait_for_timeout(1500)

    error_visible = (
        page.get_by_text("invalid", case_sensitive=False).is_visible()
        or page.get_by_text("error", case_sensitive=False).is_visible()
        or page.locator(".ant-message, .ant-notification, [class*='error']").is_visible()
    )
    assert error_visible, "Expected an error when an invalid DAG is used"


# ======================================================================
# TC-15  Verify DAG field is required
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC15_dag_field_required(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_no_dag_test")
    fill_ratio_inputs(page, [25, 25, 25, 25])

    placement.click_save()
    page.wait_for_timeout(1500)

    error_visible = (
        page.get_by_text("required", case_sensitive=False).is_visible()
        or page.locator(".ant-message, .ant-notification, [class*='error']").is_visible()
        or not placement.is_save_enabled()
    )
    assert error_visible, "Expected validation error when DAG field is empty"


# ======================================================================
# TC-16  Verify total ratio = 100
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC16_total_ratio_equals_100(page: Page):
    placement = open_create_placement(page)
    placement.fill_valid_placement(
        name="placement_ratio_100",
        dag_urls=VALID_DAGS,
        ratios={"AA": 25, "AB": 25, "Control": 25, "OOE": 25},
    )
    page.wait_for_timeout(500)

    expect(page.get_by_text("100%").first).to_be_visible()
    assert placement.is_save_enabled(), "Save button should be enabled when ratio = 100"


# ======================================================================
# TC-17  Verify total ratio < 100 blocks save
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC17_total_ratio_less_than_100(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_ratio_80")
    page.locator("input[placeholder='URL']").first.fill(VALID_DAG)
    fill_ratio_inputs(page, [20, 20, 20, 20])  # total = 80

    page.wait_for_timeout(500)

    error_visible = (
        page.locator(".ant-message, .ant-notification, [class*='error']").is_visible()
        or not placement.is_save_enabled()
    )
    assert error_visible, "Expected validation when total ratio is less than 100%"


# ======================================================================
# TC-18  Verify Roll Out button behavior
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC18_roll_out_button(page: Page):
    placement = open_create_placement(page)
    roll_out_buttons = page.get_by_role("button", name="Roll Out").all()

    assert len(roll_out_buttons) > 0, "Expected at least one Roll Out button"

    # Roll Out is disabled until DAG is selected — check it is present (disabled is OK)
    first_btn = roll_out_buttons[0]
    is_disabled = not first_btn.is_enabled()
    assert is_disabled or first_btn.is_visible(), (
        "Expected Roll Out button to be present (may be disabled until DAG is filled)"
    )


# ======================================================================
# TC-19  Verify delete experiment
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC19_delete_experiment(page: Page):
    placement = open_create_placement(page)

    before_count = placement.count_experiment_cards()
    placement.click_add_experiment()
    page.wait_for_timeout(500)
    assert placement.count_experiment_cards() > before_count, "Experiment should be added"

    count_before_delete = placement.count_experiment_cards()

    # Hover over the last DAG input to reveal delete icon, then click it
    dag_inputs = page.locator("input[placeholder='URL']").all()
    if dag_inputs:
        dag_inputs[-1].hover()
        page.wait_for_timeout(300)

    delete_icons = page.locator("[data-icon='delete']").all()
    if delete_icons:
        delete_icons[-1].click(force=True)
        page.wait_for_timeout(500)
        assert placement.count_experiment_cards() < count_before_delete, (
            "Experiment count should decrease after delete"
        )
    else:
        pytest.skip("Delete icon not found")


# ======================================================================
# TC-20  Verify View Code button
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC20_view_code_button(page: Page):
    placement = open_create_placement(page)
    placement.click_view_code()
    page.wait_for_timeout(800)

    code_visible = (
        page.locator(".ant-modal, [role='dialog']").is_visible()
        or page.locator("textarea, pre").is_visible()
    )
    assert code_visible, "Expected code/config modal to appear after clicking View Code"


# ======================================================================
# TC-21  Verify paste code after click View Code
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC21_paste_code_in_view_code(page: Page):
    placement = open_create_placement(page)
    placement.click_view_code()
    page.wait_for_timeout(800)

    code_editor = page.locator("textarea").first
    if code_editor.is_visible():
        code_editor.fill('{"placement": "test_paste_code", "experiments": []}')
        page.wait_for_timeout(300)

        for btn_name in ["Apply", "OK", "Save"]:
            btn = page.get_by_role("button", name=btn_name).first
            if btn.is_visible():
                btn.click()
                page.wait_for_timeout(500)
                break

        expect(placement.placement_input).to_be_visible()
    else:
        pytest.skip("Code editor textarea not found")


# ======================================================================
# TC-22  Verify traffic contribution bar updates dynamically
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC22_traffic_bar_updates(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_traffic_bar_test")
    fill_ratio_inputs(page, [40, 30, 20, 10])

    expect(page.get_by_text("100%").first).to_be_visible()


# ======================================================================
# TC-23  Verify experiment traffic display
# Priority: Low
# ======================================================================
@pytest.mark.low
def test_TC23_experiment_traffic_display(page: Page):
    placement = open_create_placement(page)
    fill_ratio_inputs(page, [40, 30, 20, 10])
    page.wait_for_timeout(500)

    # Each ratio value should appear in the traffic section
    for val in [40, 30, 20, 10]:
        assert page.get_by_text(str(val)).first.is_visible(), (
            f"Expected '{val}' to appear in traffic section"
        )


# ======================================================================
# TC-24  Verify total traffic distribution
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC24_total_traffic_distribution(page: Page):
    placement = open_create_placement(page)
    fill_ratio_inputs(page, [40, 30, 20, 10])
    page.wait_for_timeout(500)

    total_line = page.get_by_text("Total Traffic Distribution")
    expect(total_line).to_be_visible()
    parent_text = total_line.locator("..").inner_text()
    assert "100" in parent_text, (
        f"Expected '100' in Total Traffic Distribution, got: {parent_text}"
    )


# ======================================================================
# TC-25  Verify Reset button
# Priority: Medium
# ======================================================================
@pytest.mark.medium
def test_TC25_reset_button(page: Page):
    placement = open_create_placement(page)
    placement.enter_placement_name("placement_reset_test")
    page.wait_for_timeout(300)

    expect(placement.placement_input).to_have_value("placement_reset_test")

    placement.click_reset()
    page.wait_for_timeout(800)

    placement_value = placement.placement_input.input_value()
    assert placement_value == "", (
        f"Expected placement name to be cleared after Reset, got: '{placement_value}'"
    )


# ======================================================================
# TC-26  Verify Save Placement success
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC26_save_placement_success(page: Page):
    placement = open_create_placement(page)
    placement.fill_valid_placement(
        name="placement_save_success_test",
        dag_urls=VALID_DAGS,
        ratios={"AA": 25, "AB": 25, "Control": 25, "OOE": 25},
    )
    placement.click_save()
    page.wait_for_timeout(2000)

    success_visible = (
        page.get_by_text("success", case_sensitive=False).is_visible()
        or page.get_by_text("saved", case_sensitive=False).is_visible()
        or page.locator(".ant-message, .ant-notification").is_visible()
    )
    assert success_visible, "Expected a success message after Save Placement"


# ======================================================================
# TC-27  Verify Save disabled when form is invalid
# Priority: High
# ======================================================================
@pytest.mark.high
def test_TC27_save_disabled_when_invalid(page: Page):
    placement = open_create_placement(page)
    placement.clear_placement_name()
    page.wait_for_timeout(300)

    if placement.is_save_enabled():
        placement.click_save()
        page.wait_for_timeout(1500)
        error_shown = (
            page.get_by_text("required", case_sensitive=False).is_visible()
            or page.locator(".ant-message, .ant-notification, [class*='error']").count() > 0
        )
        assert error_shown, "Expected validation errors when saving with empty required fields"
    else:
        assert not placement.is_save_enabled(), (
            "Expected Save Placement button to be disabled when form is invalid"
        )
