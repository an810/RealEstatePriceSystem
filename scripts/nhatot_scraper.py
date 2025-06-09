from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd
import os
import tempfile
import shutil
from threading import Thread, Lock
from queue import Queue
import logging
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

# Constants
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Define output directories
AIRFLOW_DATA_DIR = '/opt/airflow/data/crawled'
LOCAL_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/crawled')

# Create output directories if they don't exist
def ensure_directories_exist():
    """Create output directories if they don't exist"""
    directories = [AIRFLOW_DATA_DIR, LOCAL_DATA_DIR]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")

# Call this function at the beginning
ensure_directories_exist()

def save_data(lock, url_data, data):
    # Determine which directory to use based on environment
    if os.path.exists(AIRFLOW_DATA_DIR):
        base_dir = AIRFLOW_DATA_DIR
    else:
        base_dir = LOCAL_DATA_DIR
        
    url_tsv_file = os.path.join(base_dir, "nhatot_url.tsv")
    data_tsv_file = os.path.join(base_dir, "nhatot.tsv")

    url_df = pd.DataFrame([url_data])
    data_df = pd.DataFrame([data])

    def write_with_header(df, file_path):
        write_header = not os.path.exists(file_path) or os.path.getsize(file_path) == 0
        with lock:
            df.to_csv(file_path, sep='\t', index=False, mode='a', header=write_header)

    if url_data is not None:
        write_with_header(url_df, url_tsv_file)
    if data is not None:
        write_with_header(data_df, data_tsv_file)

def extract_id_from_url(url):
    """Extract item ID from URL"""
    match = re.search(r'/(\d+)\.htm', url)
    return match.group(1) if match else None

def load_crawled_ids(file_path):
    """Load already crawled IDs from TSV file"""
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            return set()
        
        df = pd.read_csv(file_path, sep='\t')
        if 'id' not in df.columns:
            return set()
            
        return set(df["id"].astype(str))
    except Exception as e:
        logging.warning(f"Error loading crawled IDs: {e}")
        return set()

def create_driver(user_data_dir):
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
    # chrome_options.add_argument(f"--user-data-dir={user_data_dir}")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=chrome_options)

def scrape_one_url(args):
    """Scrape a single URL"""
    url, lock, crawled_ids = args
    item_id = extract_id_from_url(url)
    if item_id in crawled_ids:
        print(f"[Skipped] Already crawled: {url}")
        return

    print(f"[Scraping] {url}")
    user_data_dir = tempfile.mkdtemp()
    driver = None
    try:
        driver = create_driver(user_data_dir)
        driver.get(url)
        
        # Wait for page load
        time.sleep(random.uniform(2, 4))
        
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # Check for error pages
        if 'Please try again later!' in soup.find('body').get_text():
            driver.refresh()
            time.sleep(random.uniform(2, 4))
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')

        if soup.find('div', class_='NotFound_warning__dwnf5'):
            return

        # Try to click "Xem thêm" button if exists
        try:
            button = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Xem thêm')]"))
            )
            driver.execute_script("arguments[0].click();", button)
            time.sleep(1)
        except TimeoutException:
            logging.info(f"No 'Xem thêm' button found on {url}")

        # Prepare URL data
        url_data = {
            "id": item_id,
            "url": url,
            "status": 0,
            "source_id": 3,  # Unique ID for nhatot
            "source_name": "nhatot.com",
            "posted_date": "",
            "due_date": "",
            "updated_date": datetime.now().strftime('%Y-%m-%d')
        }

        # Prepare property data
        data = {
            "title": None,
            "url_id": url_data["id"],
            "area": None,
            "width": None,
            "price": None,
            "direction": None,
            "number_of_bedrooms": None,
            "number_of_toilets": None,
            "furniture": None,
            "legal": None,
            "lat": None,
            "lon": None,
            "address": None,
            "district": None,
            "province": None
        }

        # Extract title - updated selector
        title_tag = soup.find('div', class_='cd9gm5n')
        if title_tag:
            title_h1 = title_tag.find('h1')
            if title_h1:
                data["title"] = title_h1.text.strip()

        # Extract price - updated selector
        price_span = soup.find('b', class_='pyhk1dv')
        data["price"] = price_span.text if price_span else None

        # Extract address - updated selector
        address_div = soup.find('div', class_='sf0dbrp r9vw5if')
        if address_div:
            address_span = address_div.find('span', class_='bwq0cbs')
            if address_span:
                data["address"] = address_span.text.strip()

        # Extract province and district
        if data["address"]:
            match_city = re.search(r',\s*([^,]+)$', data["address"])
            if match_city:
                data["province"] = match_city.group(1).strip()
            match_district = re.search(r'(Quận|Huyện|Thị xã)\s*([^,]+)', data["address"])
            if match_district:
                data["district"] = match_district.group(2).strip()

        # Extract coordinates
        for tag in soup.find_all('script'):
            text = str(tag)
            if "longitude" in text and "latitude" in text:
                lon_match = re.search(r'longitude":([\d\.\-]+)', text)
                lat_match = re.search(r'latitude":([\d\.\-]+)', text)
                if lon_match and lat_match:
                    data["lon"] = lon_match.group(1)
                    data["lat"] = lat_match.group(1)
                break

        # Extract property specifications - updated selector
        specs_items = soup.find_all('div', class_='col-xs-6 abzctes')
        label_map = {
            "Diện tích đất": "area",
            "Chiều ngang": "width",
            "Hướng cửa chính": "direction",
            "Số phòng ngủ": "number_of_bedrooms",
            "Số phòng vệ sinh": "number_of_toilets",
            "Tình trạng nội thất": "furniture",
            "Giấy tờ pháp lý": "legal"
        }

        for item in specs_items:
            label_div = item.find('div', class_='a4ep88f')
            value_div = item.find('strong', class_='a3jfi3v')
            if label_div and value_div:
                label = label_div.text.strip()
                value = value_div.text.strip()
                key = label_map.get(label)
                if key:
                    data[key] = value

        url_data["status"] = 1 if data["title"] else 0
        if not data["title"]:
            data = None

        save_data(lock, url_data, data)

    except Exception as e:
        print(f"[Unhandled Error] Failed to scrape {url}: {e}")
    finally:
        if driver:
            driver.quit()
        shutil.rmtree(user_data_dir)

def scrape_data():
    """Main function to scrape all URLs"""
    try:
        ensure_directories_exist()
        
        # Read URLs from file
        file_path = os.path.join(DATA_DIR, 'nhatot_links.txt')
        with open(file_path, 'r', encoding='utf-8') as file:
            urls = [line.strip() for line in file if line.strip() and 'nhatot.com' in line]

        logging.info(f"Found {len(urls)} URLs to scrape")

        # Use threading instead of multiprocessing
        lock = Lock()
        crawled_ids = load_crawled_ids(os.path.join(DATA_DIR, "hn_nhatot_url.tsv"))
        url_queue = Queue()
        
        # Add URLs to queue
        for url in urls:
            url_queue.put(url)
            
        def worker():
            while not url_queue.empty():
                try:
                    url = url_queue.get()
                    item_id = extract_id_from_url(url)
                    if item_id in crawled_ids:
                        logging.info(f"[Skipped] Already crawled: {url}")
                        continue
                    
                    try:
                        scrape_one_url((url, lock, crawled_ids))
                        crawled_ids.add(item_id)
                    except Exception as e:
                        logging.error(f"Error scraping {url}: {e}")
                finally:
                    url_queue.task_done()
        
        # Create and start threads
        threads = []
        for _ in range(5):  # Number of concurrent threads
            thread = Thread(target=worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
            
        # Wait for all URLs to be processed
        url_queue.join()
        
        logging.info("✅ Scraping completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error in scrape_data: {e}")
        return False

if __name__ == "__main__":
    # This block only runs if the script is executed directly
    scrape_data()