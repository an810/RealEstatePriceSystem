from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add the scripts directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from check_availability import check_availability

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'check_real_estate_availability',
    default_args=default_args,
    description='Check availability of real estate listings',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['real_estate', 'availability'],
)

check_availability_task = PythonOperator(
    task_id='check_availability',
    python_callable=check_availability,
    dag=dag,
) 