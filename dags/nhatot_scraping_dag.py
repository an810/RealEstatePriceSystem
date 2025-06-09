from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os
import logging

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from nhatot_save_link import NhatotScraper
from nhatot_scraper import scrape_data
from clean_data import clean_data

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
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'nhatot_scraping',
    default_args=default_args,
    description='DAG for scraping and processing Nhatot data',
    schedule_interval='0 21 * * *',  # Schedule every day at 9 pm
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['scraper', 'nhatot'],
)

# Task 1: Save Links
def save_links_task():
    try:
        # Use the refactored scraper with custom configuration
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
        logger.info("Successfully completed save_links_task")
    except Exception as e:
        logger.error(f"Error in save_links_task: {e}")
        raise

save_links_operator = PythonOperator(
    task_id='save_links',
    python_callable=save_links_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=10)
)

# Task 2: Scrape Data
def scrape_data_task():
    try:
        result = scrape_data()
        if not result:
            raise Exception("Scraping failed")
        return "Scraping completed successfully"
    except Exception as e:
        raise Exception(f"Error in scrape_data_task: {str(e)}")

scrape_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data_task,
    dag=dag,
)

# Task 3: Process and Clean Data
def process_and_clean_data_task():
    try:
        result = clean_data()
        if not result:
            raise Exception("Processing and cleaning failed")
        return "Processing and cleaning completed successfully"
    except Exception as e:
        raise Exception(f"Error in process_and_clean_data_task: {str(e)}")

process_and_clean_task = PythonOperator(
    task_id='process_and_clean_data',
    python_callable=process_and_clean_data_task,
    dag=dag,
)

# Set task dependencies
save_links_operator >> scrape_task >> process_and_clean_task 