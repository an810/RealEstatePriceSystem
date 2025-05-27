from selenium import webdriver
from selenium.webdriver.chrome.options import Options
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

def ensure_directories_exist():
    """Create output directories if they don't exist"""
    directories = [AIRFLOW_DATA_DIR, LOCAL_DATA_DIR]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")

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

def save_data(lock, url_data, data):
    # Determine which directory to use based on environment
    if os.path.exists(AIRFLOW_DATA_DIR):
        base_dir = AIRFLOW_DATA_DIR
    else:
        base_dir = LOCAL_DATA_DIR
        
    url_tsv_file = os.path.join(base_dir, "hn_batdongsan_url.tsv")
    data_tsv_file = os.path.join(base_dir, "hn_batdongsan.tsv")

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


def convert_date_format(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def extract_id_from_url(url: str):
    match = re.search(r'pr\d+', url)
    return match.group(0) if match else None


def create_driver(user_data_dir):
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
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

        max_retries = 5
        success = False
        soup = None

        for attempt in range(max_retries):
            try:
                driver.get(url)
            except Exception as e:
                print(f"[Error] Failed to connect to {url}: {e}")
                return
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if soup.find('div', class_='re__pr-short-info-item js__pr-config-item'):
                success = True
                break
            print(f"[Retry {attempt+1}] Retrying load for {url}")
            time.sleep(1)

        if not success:
            print(f"[Timeout] Failed to load key content: {url}")
            return

        url_data = {
            "id": item_id,
            "url": url,
            "status": 0,
            "source_id": 1,
            "source_name": "batdongsan.com.vn",
            "posted_date": "",
            "due_date": "",
            "updated_date": datetime.now().strftime('%Y-%m-%d')
        }

        data = {
            "title": None,
            "url_id": item_id,
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

        for item in soup.find_all('div', class_='re__pr-short-info-item js__pr-config-item'):
            title_span = item.find('span', class_='title')
            value_span = item.find('span', class_='value')
            if title_span and value_span:
                title = title_span.text.strip()
                value = value_span.text.strip()
                if title == "Ngày đăng":
                    url_data["posted_date"] = convert_date_format(value)
                elif title == "Ngày hết hạn":
                    url_data["due_date"] = convert_date_format(value)

        address = soup.find('span', class_='re__pr-short-description js__pr-address')
        data["address"] = address.text.strip() if address else None

        map_section = soup.find('div', class_='re__section re__pr-map js__section js__li-other')
        if map_section:
            iframe = map_section.find('iframe', {'data-src': True})
            if iframe and iframe.has_attr('data-src'):
                match = re.search(r'q=([-+]?\d*\.\d+),([-+]?\d*\.\d+)', iframe['data-src'])
                if match:
                    data["lat"] = match.group(1)
                    data["lon"] = match.group(2)

        for tab in soup.find_all('a', class_='re__link-se'):
            level = tab.get('level')
            if level == "2":
                data["province"] = tab.text.strip()
            elif level == "3":
                data["district"] = tab.text.strip()
            elif level == "4":
                data["title"] = tab.text.strip()

        specs = soup.find('div', class_='re__pr-specs-content-v2 js__other-info')
        if specs:
            for item in specs.find_all('div', class_='re__pr-specs-content-item'):
                title = item.find('span', class_='re__pr-specs-content-item-title').text.strip()
                value = item.find('span', class_='re__pr-specs-content-item-value').text.strip()
                key_map = {
                    "Diện tích": "area",
                    "Mặt tiền": "width",
                    "Mức giá": "price",
                    "Hướng nhà": "direction",
                    "Số phòng ngủ": "number_of_bedrooms",
                    "Số phòng tắm, vệ sinh": "number_of_toilets",
                    "Nội thất": "furniture",
                    "Pháp lý": "legal"
                }
                if title in key_map:
                    data[key_map[title]] = value

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
    """Main scraping function that can be called by the Airflow DAG"""
    try:
        ensure_directories_exist()
        
        # Determine which input file to use based on environment
        if os.path.exists(AIRFLOW_DATA_DIR):
            input_file = os.path.join(AIRFLOW_DATA_DIR, "hn_batdongsan_links.txt")
        else:
            input_file = os.path.join(LOCAL_DATA_DIR, "hn_batdongsan_links.txt")
            
        # Read links from file
        with open(input_file, 'r', encoding='utf-8') as f:
            urls = f.read().splitlines()

        logging.info(f"Found {len(urls)} URLs to scrape")

        # Use threading instead of multiprocessing
        lock = Lock()
        crawled_ids = load_crawled_ids(os.path.join(AIRFLOW_DATA_DIR if os.path.exists(AIRFLOW_DATA_DIR) else LOCAL_DATA_DIR, "batdongsan_url.tsv"))
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