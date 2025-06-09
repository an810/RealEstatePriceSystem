from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os
import logging

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from batdongsan_save_link import BatDongSanScraper
from nhatot_save_link import NhatotScraper
from batdongsan_scraper import scrape_data as batdongsan_scrape_data
from nhatot_scraper import scrape_data as nhatot_scrape_data
from clean_data import clean_data
from save_data import save_to_database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'real_estate_scraping',
    default_args=default_args,
    description='DAG for scraping and processing real estate data from multiple sources',
    schedule_interval='0 0 * * *',  # Schedule every day at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraper', 'real_estate'],
)

# Task 1: Save BatDongSan Links
def save_batdongsan_links_task():
    try:
        scraper = BatDongSanScraper(
            output_file="/opt/airflow/data/crawled/batdongsan_links.txt",
            error_file="/opt/airflow/data/crawled/batdongsan_error_links.txt",
            max_pages=100,  # Limit pages for testing
            save_interval=10
        )
        scraper.run()
        logger.info("Successfully completed save_batdongsan_links_task")
    except Exception as e:
        logger.error(f"Error in save_batdongsan_links_task: {e}")
        raise

save_batdongsan_links = PythonOperator(
    task_id='save_batdongsan_links',
    python_callable=save_batdongsan_links_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=10)
)

# Task 2: Save Nhatot Links
def save_nhatot_links_task():
    try:
        scraper = NhatotScraper(
            output_file="/opt/airflow/data/crawled/nhatot_links.txt",
            error_file="/opt/airflow/data/crawled/nhatot_error_links.txt",
            save_interval=10,
            retry_attempts=5,
            retry_delay=3
        )
        result = scraper.run()
        if not result:
            raise Exception("Failed to scrape links")
        logger.info("Successfully completed save_nhatot_links_task")
    except Exception as e:
        logger.error(f"Error in save_nhatot_links_task: {e}")
        raise

save_nhatot_links = PythonOperator(
    task_id='save_nhatot_links',
    python_callable=save_nhatot_links_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=10)
)

# Task 3: Scrape BatDongSan Data
def scrape_batdongsan_data_task():
    try:
        result = batdongsan_scrape_data()
        if not result:
            raise Exception("BatDongSan scraping failed")
        return "BatDongSan scraping completed successfully"
    except Exception as e:
        raise Exception(f"Error in scrape_batdongsan_data_task: {str(e)}")

scrape_batdongsan = PythonOperator(
    task_id='scrape_batdongsan_data',
    python_callable=scrape_batdongsan_data_task,
    dag=dag,
)

# Task 4: Scrape Nhatot Data
def scrape_nhatot_data_task():
    try:
        result = nhatot_scrape_data()
        if not result:
            raise Exception("Nhatot scraping failed")
        return "Nhatot scraping completed successfully"
    except Exception as e:
        raise Exception(f"Error in scrape_nhatot_data_task: {str(e)}")

scrape_nhatot = PythonOperator(
    task_id='scrape_nhatot_data',
    python_callable=scrape_nhatot_data_task,
    dag=dag,
)

# Task 5: Process and Clean Data
def process_and_clean_data_task():
    try:
        result = clean_data()
        if not result:
            raise Exception("Processing and cleaning failed")
        return "Processing and cleaning completed successfully"
    except Exception as e:
        raise Exception(f"Error in process_and_clean_data_task: {str(e)}")

process_and_clean = PythonOperator(
    task_id='process_and_clean_data',
    python_callable=process_and_clean_data_task,
    dag=dag,
)

# Task 6: Save Data to Database
def save_to_database_task():
    try:
        result = save_to_database()
        if not result:
            raise Exception("Failed to save data to database")
        return "Data saved to database successfully"
    except Exception as e:
        raise Exception(f"Error in save_to_database_task: {str(e)}")

save_to_database_op = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5)
)

# Set task dependencies
save_batdongsan_links >> scrape_batdongsan
save_nhatot_links >> scrape_nhatot
[scrape_batdongsan, scrape_nhatot] >> process_and_clean >> save_to_database_op
