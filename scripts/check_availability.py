import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Driver setup (reused from your scraper) ===
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

# === DB Connection ===
def get_db_engine():
    return create_engine("postgresql://postgres:postgres@real_estate_db:5432/real_estate")

# === Core Check Logic ===
def check_url_availability(url: str, source: str) -> Tuple[bool, str]:
    driver = None
    try:
        logger.info(f"🔍 Checking URL: {url} [{source}]")
        driver = create_driver()
        driver.get(url)
        time.sleep(3)

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()

        if source == 'batdongsan':
            if soup.find('span', class_='pr-expired__title'):
                logger.info(f"❌ Expired marker found. {url}")
                return False, 'expired'
            if 'tin đã hết hạn' in page_text or 'tin không tồn tại' in page_text:
                logger.info(f"❌ Text indicates expired or not found. {url}")
                return False, 'expired'
            if not soup.find('div', class_='re__pr-short-info-item'):
                logger.warning(f"⚠️ Missing key content. {url}")
                return False, 'missing_content'

        elif source == 'nhatot':
            if soup.find('div', class_='NotFound_notFoundWrapper__2_cFc') or soup.find('div', class_='NotFound_content__KtIbC') or soup.find('div', class_='NotFound_buttons__fUQ5F'):
                logger.info(f"❌ NotFound block present. {url}")
                return False, 'not_found'
            if 'Tin đăng không còn tồn tại' in page_text or 'Tin đăng đã hết hạn' in page_text:
                logger.info(f"❌ Text indicates expired or not found. {url}")
                return False, 'expired'
            if soup.find('div', class_='neutral no-button s10kknc6'):
                logger.info(f"❌ NotFound block present. {url}")
                return False, 'expired'

        logger.info(f"✅ Still available. {url}")
        return True, 'available'

    except Exception as e:
        logger.error(f"❗ Error: {e} {url}")
        return False, f'error_{str(e)}'
    finally:
        if driver:
            driver.quit()

# === Process Batch ===
# def process_batch(urls: List[Dict]) -> List[Dict]:
#     results = []
#     for item in urls:
#         is_available, status = check_url_availability(item['url'], item['source'])
#         results.append({
#             'url_id': item['url_id'],
#             'is_available': is_available,
#             'status': status,
#             'updated_at': datetime.now()
#         })
#         time.sleep(1)
#     return results

# === Process Batch with Threading ===
def process_batch(urls: List[Dict], max_workers: int = 5) -> List[Dict]:
    results = []

    def task(item):
        is_available, status = check_url_availability(item['url'], item['source'])
        return {
            'url_id': item['url_id'],
            'is_available': is_available,
            'status': status,
            'updated_at': datetime.now()
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(task, item): item for item in urls}
        for future in as_completed(future_to_item):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Threaded task failed: {e}")

    return results
# === Update DB ===
def update_database(engine, results: List[Dict]):
    try:
        with engine.begin() as conn:
            for result in results:
                conn.execute(text("""
                    UPDATE real_estate
                    SET is_available = :is_available,
                        updated_at = :updated_at
                    WHERE url_id = :url_id
                """), result)
    except Exception as e:
        logger.error(f"❗ Failed to update DB: {e}")
        raise

# === Main ===
def check_availability():
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT url_id, url, source FROM real_estate WHERE is_available = TRUE
            """))
            urls = [dict(row) for row in result]

        logger.info(f"📦 Found {len(urls)} active listings.")

        batch_size = 50
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            results = process_batch(batch)
            update_database(engine, results)
            logger.info(f"✅ Batch {i//batch_size + 1} processed.")

        logger.info("🎉 Finished checking all listings.")
        return True

    except Exception as e:
        logger.error(f"❗ Top-level error: {e}")
        return False

# === Test Single ===
def test_single_url(url: str, source: str):
    is_available, status = check_url_availability(url, source)
    print(f"\n✅ Test URL: {url}")
    print(f"   → Available: {is_available}")
    print(f"   → Status: {status}")

# if __name__ == "__main__":
#     # For testing individual listings
#     # test_single_url("https://batdongsan.com.vn/...prxxxx", "batdongsan")

#     # To check all listings in DB
#     check_availability()


if __name__ == "__main__":
    # Test individual URLs
    test_single_url("https://batdongsan.com.vn/nha-dat-ban-ha-noi/ban-can-ho-chung-cu-duong-le-quang-dao-phuong-me-tri-prj-the-matrix-one/ban-87m2-4-5-ty-113m2-6-1-ty-cc-view-tang-nha-noi-that-dep-pr38160950", "batdongsan")
    print("--------------------------------")
    test_single_url("https://batdongsan.com.vn/ban-dat-duong-van-canh-xa-van-canh-1/ban-mat-qh-rong-40m-vi-tri-sieu-dep-ket-noi-g-tiem-nang-tang-gia-cao-pr43159345", "batdongsan")
    print("--------------------------------")
    test_single_url("https://www.nhatot.com/mua-ban-bat-dong-san/112303173.htm", "nhatot")

    # To run the full availability check, uncomment:
    # check_availability()
