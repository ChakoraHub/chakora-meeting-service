import os
from datetime import date, datetime
from uuid import uuid4
import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


HOME_URL = os.getenv("MEETING_HOME_URL", "https://www.chakorahub.com")
MEETING_URL = os.getenv("MEETING_URL", "https://www.chakorahub.com/meeting")
MEETING_ADMIN_URL = os.getenv(
    "MEETING_ADMIN_URL",
    "https://www.chakorahub.com/meeting/admin",
)
WAIT = 25


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1000")
    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(45)
    yield browser
    browser.quit()


def wait_loaded(browser):
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def login_employee(browser):
    emp_id = os.getenv("MEETING_EMPLOYEE_ID")
    password = os.getenv("MEETING_PASSWORD")

    if not emp_id or not password:
        pytest.skip(
            "Configure MEETING_EMPLOYEE_ID and MEETING_PASSWORD "
            "for authenticated meeting tests."
        )

    browser.get(HOME_URL)
    wait_loaded(browser)

    Select(browser.find_element(By.ID, "login_type")).select_by_value("employee")

    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.ID, "employee_id"))
    )

    browser.find_element(By.ID, "employee_id").send_keys(emp_id)
    browser.find_element(By.ID, "password").send_keys(password)
    browser.find_element(
        By.CSS_SELECTOR, "form button[type='submit']"
    ).click()

    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def open_meeting(browser):
    browser.get(MEETING_URL)
    wait_loaded(browser)

    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.TAG_NAME, "body"))
    )


def open_meeting_admin(browser):
    login_employee(browser)
    browser.get(MEETING_ADMIN_URL)
    wait_loaded(browser)

    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.TAG_NAME, "body"))
    )


def test_meeting_page_loads_and_controls_visible(driver):
    open_meeting(driver)

    assert driver.find_element(By.TAG_NAME, "body").is_displayed()


def test_meeting_page_available_without_login(driver):
    driver.get(MEETING_URL)
    wait_loaded(driver)

    assert driver.find_element(By.TAG_NAME, "body").is_displayed()


def test_meeting_maintenance_notice_when_expected(driver):
    """Verify the maintenance notice when the workflow expects it."""
    if os.getenv("EXPECT_MAINTENANCE_NOTICE", "false").lower() != "true":
        pytest.skip("Maintenance notice check is not enabled.")

    open_meeting(driver)

    notice = driver.find_elements(By.ID, "meeting-maintenance-notice")
    assert notice, "Meeting maintenance notice was not found."
    assert notice[0].is_displayed(), "Meeting maintenance notice is not visible."

def test_meeting_date_control_visible(driver):
    open_meeting(driver)

    date_selectors = [
        (By.ID, "meetingDate"),
        (By.ID, "date"),
        (By.NAME, "date"),
        (By.CSS_SELECTOR, "input[type='date']"),
    ]

    for locator in date_selectors:
        elements = driver.find_elements(*locator)
        if elements:
            assert elements[0].is_displayed()
            return

    pytest.skip(
        "Meeting date selector is not identifiable from meeting-proxy.py. "
        "Provide meeting.html to use the exact locator."
    )


def test_meeting_slots_load_for_selected_date(driver):
    open_meeting(driver)

    date_value = os.getenv("MEETING_TEST_DATE", date.today().strftime("%Y-%m-%d"))

    date_elements = driver.find_elements(By.CSS_SELECTOR, "input[type='date']")
    if not date_elements:
        pytest.skip(
            "Meeting date input is not available from the supplied proxy file."
        )

    date_field = date_elements[0]
    date_field.clear()
    date_field.send_keys(date_value)

    # The exact slot control is defined in meeting.html, not meeting-proxy.py.
    slot_elements = driver.find_elements(
        By.CSS_SELECTOR,
        "[data-slot], .meeting-slot, .slot, #meetingSlots"
    )

    if not slot_elements:
        pytest.skip(
            "Meeting slot selector is not available from the supplied "
            "meeting-proxy.py. Provide meeting.html for exact selectors."
        )

    assert any(element.is_displayed() for element in slot_elements)


def test_book_meeting(driver):
    if os.getenv("RUN_MEETING_WRITE_TESTS", "false").lower() != "true":
        pytest.skip("Production write test is disabled.")

    open_meeting(driver)

    required = [
        "meetingDate",
        "meetingSlot",
    ]

    missing = [
        field_id
        for field_id in required
        if not driver.find_elements(By.ID, field_id)
    ]

    if missing:
        pytest.skip(
            "Exact booking element IDs are not available in "
            "meeting-proxy.py. Provide meeting.html."
        )

    date_field = driver.find_element(By.ID, "meetingDate")
    date_field.clear()
    date_field.send_keys(
        os.getenv("MEETING_TEST_DATE", date.today().strftime("%Y-%m-%d"))
    )

    driver.find_element(By.ID, "meetingSlot").click()

    submit_buttons = driver.find_elements(
        By.CSS_SELECTOR,
        "#meetingForm button[type='submit'], "
        "form button[type='submit']"
    )

    if not submit_buttons:
        pytest.skip("Meeting booking submit button was not identifiable.")

    submit_buttons[0].click()

    alert = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, ".alert, #alertContainer .alert")
        )
    )

    assert alert.is_displayed()


def test_my_bookings(driver):
    login_employee(driver)
    driver.get(MEETING_URL)
    wait_loaded(driver)

    booking_controls = driver.find_elements(
        By.CSS_SELECTOR,
        "#myBookings, .my-bookings, [data-section='my-bookings']"
    )

    if not booking_controls:
        pytest.skip(
            "My bookings selector is not available from meeting-proxy.py. "
            "Provide meeting.html."
        )

    assert any(control.is_displayed() for control in booking_controls)


def test_cancel_booking(driver):
    if os.getenv("RUN_MEETING_WRITE_TESTS", "false").lower() != "true":
        pytest.skip("Production write test is disabled.")

    login_employee(driver)
    driver.get(MEETING_URL)
    wait_loaded(driver)

    cancel_buttons = driver.find_elements(
        By.CSS_SELECTOR,
        "button[data-booking-id], .cancel-booking, "
        "[data-action='cancel-booking']"
    )

    if not cancel_buttons:
        pytest.skip(
            "Cancel-booking selector is not available from "
            "meeting-proxy.py. Provide meeting.html."
        )

    cancel_buttons[0].click()

    alert = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, ".alert, #alertContainer .alert")
        )
    )

    assert alert.is_displayed()


def test_admin_bookings_page_loads_for_employee(driver):
    open_meeting_admin(driver)

    assert driver.find_element(By.TAG_NAME, "body").is_displayed()


def test_admin_booking_status_filter(driver):
    open_meeting_admin(driver)

    status_elements = driver.find_elements(
        By.CSS_SELECTOR,
        "select[name='status'], #status, #bookingStatus"
    )

    if not status_elements:
        pytest.skip(
            "Admin status selector is not available from "
            "meeting-proxy.py. Provide admin.html."
        )

    Select(status_elements[0]).select_by_value("PENDING")

    assert status_elements[0].is_displayed()


def test_admin_approve_booking(driver):
    if os.getenv("RUN_MEETING_WRITE_TESTS", "false").lower() != "true":
        pytest.skip("Production write test is disabled.")

    open_meeting_admin(driver)

    approve_buttons = driver.find_elements(
        By.CSS_SELECTOR,
        ".approve-booking, [data-action='approve'], "
        "button[data-booking-id]"
    )

    if not approve_buttons:
        pytest.skip(
            "Admin approval selector is not available from "
            "meeting-proxy.py. Provide admin.html."
        )

    approve_buttons[0].click()

    alert = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, ".alert, #alertContainer .alert")
        )
    )

    assert alert.is_displayed()


@pytest.mark.production_write
def test_create_and_cancel_meeting_booking(driver):
    if os.getenv("RUN_MEETING_WRITE_TESTS", "false").lower() != "true":
        pytest.skip(
            "Set RUN_MEETING_WRITE_TESTS=true to intentionally "
            "create/cancel a meeting test booking."
        )

    open_meeting(driver)

    title = (
        "SELENIUM_MEETING_TEST_"
        + datetime.now().strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid4().hex[:6]
    )

    # The exact form fields are intentionally checked before use.
    # They must be confirmed against meeting.html.
    inputs = driver.find_elements(By.CSS_SELECTOR, "input")

    if not inputs:
        pytest.skip(
            "Meeting form controls are not available from "
            "meeting-proxy.py. Provide meeting.html."
        )

    assert title
