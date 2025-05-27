from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import time
import random
import logging
import os
import concurrent.futures
from typing import List
from threading import Lock

class BatDongSanScraper:
    
    def __init__(
        self,
        output_file: str = "/opt/airflow/data/crawled/batdongsan_links.txt",
        error_file: str = "/opt/airflow/data/crawled/batdongsan_error_links.txt",
        base_url: str = "https://batdongsan.com.vn/nha-dat-ban-ha-noi",
        filter_path: str = "?gtn=7-ty&gcn=30-ty",
        max_pages: int = 3058,
        save_interval: int = 10,
        retry_attempts: int = 5,
        retry_delay: int = 3,
        max_workers: int = 5
    ):
        
        self.output_file = output_file
        self.error_file = error_file
        self.base_url = base_url
        self.filter_path = filter_path
        self.max_pages = max_pages
        self.save_interval = save_interval
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.max_workers = max_workers
        self.unscraped_links = set()
        self.links_lock = Lock()
        self.error_urls = []
        self.error_urls_lock = Lock()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        os.makedirs(os.path.dirname(error_file), exist_ok=True)
        
        # Initialize logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("BatDongSanScraper")
    
    def _setup_chrome_options(self) -> Options:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Use Chromium binary if specified in environment
        if os.environ.get('CHROME_BIN'):
            chrome_options.binary_location = os.environ.get('CHROME_BIN')
            
        return chrome_options
    
    def _create_webdriver(self):
        """Create a WebDriver instance that works on different architectures."""
        # Create a new ChromeOptions object each time
        chrome_options = self._setup_chrome_options()
        
        try:
            # Try to use the ChromeDriver from environment variable
            if os.environ.get('CHROMEDRIVER_PATH'):
                service = Service(executable_path=os.environ.get('CHROMEDRIVER_PATH'))
                return webdriver.Chrome(service=service, options=chrome_options)
            
            # Try to use the default Chrome driver
            return webdriver.Chrome(options=chrome_options)
        except Exception as e:
            self.logger.error(f"Error creating WebDriver: {e}")
            # Try to use undetected-chromedriver as a fallback
            try:
                import undetected_chromedriver as uc
                # Create a new options object for undetected-chromedriver
                uc_options = uc.ChromeOptions()
                uc_options.add_argument("--headless")
                uc_options.add_argument("--no-sandbox")
                uc_options.add_argument("--disable-dev-shm-usage")
                return uc.Chrome(options=uc_options)
            except ImportError:
                self.logger.error("undetected-chromedriver not installed. Please install it with: pip install undetected-chromedriver")
                raise
    
    def get_page_with_retries(self, driver: webdriver.Chrome, url: str, retries: int = 10, delay: int = 10) -> str:
        for attempt in range(retries): 
            try:
                driver.get(url)
                return driver.page_source
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(delay)
        raise Exception(f"Failed to load page after {retries} attempts: {url}")
    
    def clear_file(self, file_path: str) -> None:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.truncate(0)
    
    def load_existing_links(self) -> None:
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r', encoding='utf-8') as existing_links_file:
                existing_links = existing_links_file.read().splitlines()
                self.unscraped_links.update(existing_links)
                self.logger.info(f"Loaded {len(existing_links)} existing links")
    
    def save_links(self) -> None:
        self.clear_file(self.output_file)
        with open(self.output_file, 'w', encoding='utf-8') as linksfile:
            for link in self.unscraped_links:
                linksfile.write(link + '\n')
        self.logger.info(f"Saved {len(self.unscraped_links)} links to {self.output_file}")
    
    def scrape_page(self, page_index: int) -> List[str]:
        """Scrape a single page and return any error URLs."""
        error_urls = []
        driver = None
        full_path = f"{self.base_url}/p{page_index}{self.filter_path}"
        
        try:
            driver = self._create_webdriver()
            self.logger.info(f"Scraping page {page_index}: {full_path}")
            html_content = self.get_page_with_retries(driver, full_path)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all product links
            links = soup.find_all('a', class_='js__product-link-for-product-id')
            self.logger.info(f"Found {len(links)} links on page {page_index}")
            
            if len(links) == 0:
                error_urls.append(full_path)
                with open(self.error_file, 'a', encoding='utf-8') as ef:
                    ef.write(full_path + '\n')
                self.logger.warning(f"No links found on page {page_index}, added to error file")
            
            # Use a lock to safely update the shared set
            with self.links_lock:
                for link in links:
                    if link.get('data-product-id') == "0":
                        continue
                    href = link.get('href')
                    self.unscraped_links.add(self.base_url + href)
                
        except Exception as e:
            self.logger.error(f"Error scraping page {page_index}: {e}")
            error_urls.append(full_path)
        finally:
            if driver:
                driver.quit()
                
        return error_urls
    
    def scrape_page_wrapper(self, page_index: int) -> None:
        """Wrapper function for thread pool executor."""
        error_urls = self.scrape_page(page_index)
        if error_urls:
            with self.error_urls_lock:
                self.error_urls.extend(error_urls)
    
    def scrape_all_pages(self):
        """Scrape all pages using multiple threads."""
        self.load_existing_links()
        self.error_urls = []
        
        # Create a thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all page scraping tasks
            futures = {executor.submit(self.scrape_page_wrapper, page_index): page_index 
                      for page_index in range(1, self.max_pages + 1)}
            
            # Process completed tasks and save progress periodically
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                page_index = futures[future]
                try:
                    future.result()  # This will raise any exceptions that occurred
                    completed += 1
                    
                    # Save progress periodically
                    if completed % self.save_interval == 0:
                        self.save_links()
                        self.logger.info(f"Progress saved at {completed}/{self.max_pages} pages")
                        
                except Exception as e:
                    self.logger.error(f"Error processing page {page_index}: {e}")
        
        # Final save
        self.save_links()
        
        # Save error URLs
        if self.error_urls:
            with open(self.error_file, 'w', encoding='utf-8') as ef:
                for url in self.error_urls:
                    ef.write(url + '\n')
            self.logger.info(f"Saved {len(self.error_urls)} error URLs to {self.error_file}")
    
    def retry_error_urls(self):
        """Retry scraping URLs that previously failed using multiple threads."""
        if not os.path.exists(self.error_file):
            self.logger.info("No error file found. Nothing to retry.")
            return
            
        with open(self.error_file, 'r', encoding='utf-8') as ef:
            error_urls = ef.read().splitlines()
            
        if not error_urls:
            self.logger.info("No error URLs to retry.")
            return
            
        self.logger.info(f"Retrying {len(error_urls)} error URLs...")
        remaining_errors = []
        remaining_errors_lock = Lock()
        
        def retry_url(url):
            """Retry a single URL with multiple attempts."""
            for attempt in range(self.retry_attempts):
                self.logger.info(f"Retrying ({attempt+1}/{self.retry_attempts}): {url}")
                driver = None
                try:
                    driver = self._create_webdriver()
                    html_content = self.get_page_with_retries(driver, url)
                    soup = BeautifulSoup(html_content, 'html.parser')
                    links = soup.find_all('a', class_='js__product-link-for-product-id')
                    
                    if links:
                        self.logger.info(f"✅ Success on attempt {attempt+1}: Found {len(links)} links.")
                        with self.links_lock:
                            for link in links:
                                if link.get('data-product-id') == "0":
                                    continue
                                href = link.get('href')
                                self.unscraped_links.add(self.base_url + href)
                        return True
                    else:
                        self.logger.warning("No links found.")
                        
                except Exception as e:
                    self.logger.error(f"Error fetching {url}: {e}")
                finally:
                    if driver:
                        driver.quit()
                        
                if attempt < self.retry_attempts - 1:
                    time.sleep(random.uniform(1, self.retry_delay))
            
            # If we get here, all attempts failed
            with remaining_errors_lock:
                remaining_errors.append(url)
            return False
        
        # Use a thread pool to retry URLs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all retry tasks
            futures = {executor.submit(retry_url, url): url for url in error_urls}
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error retrying {url}: {e}")
                    with remaining_errors_lock:
                        remaining_errors.append(url)
                
        # Write updated error file
        with open(self.error_file, 'w', encoding='utf-8') as ef:
            for url in remaining_errors:
                ef.write(url + '\n')
                
        self.logger.info(f"Retry complete. {len(remaining_errors)} failed pages remaining in {self.error_file}.")
        
        # Save all collected links
        self.save_links()
    
    def run(self):
        self.scrape_all_pages()
        self.retry_error_urls()
        self.logger.info("Scraping process completed.")


def scrape_links(
    output_file: str = "/opt/airflow/data/crawled/batdongsan_links.txt",
    error_file: str = "/opt/airflow/data/crawled/batdongsan_error_links.txt",
    base_url: str = "https://batdongsan.com.vn/nha-dat-ban",
    filter_path: str = "?gtn=7-ty&gcn=30-ty",
    max_pages: int = 3000,
    max_workers: int = 5
):
    scraper = BatDongSanScraper(
        output_file=output_file,
        error_file=error_file,
        base_url=base_url,
        filter_path=filter_path,
        max_pages=max_pages,
        max_workers=max_workers
    )
    scraper.run()


if __name__ == "__main__":
    scrape_links()
