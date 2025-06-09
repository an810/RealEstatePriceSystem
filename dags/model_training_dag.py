from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys
import os
import logging

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from model_training_module import train_model

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'model_training',
    default_args=default_args,
    description='Train ML models after data processing and saving is complete',
    schedule_interval='0 0 * * *',  # Schedule every day at 0 am
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent multiple runs at the same time
    tags=['model', 'training']
)

# Wait for real estate scraping and data saving to complete
wait_for_data = ExternalTaskSensor(
    task_id='wait_for_data_saving',
    external_dag_id='real_estate_scraping',
    external_task_id='save_to_database',
    mode='reschedule',
    timeout=64800,  # 18 hours timeout
    dag=dag
)

# Define training tasks
train_price_model = PythonOperator(
    task_id='train_price_model',
    python_callable=train_model,
    dag=dag
)

# Set task dependencies
wait_for_data >> train_price_model