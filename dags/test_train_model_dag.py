from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from train_price_model import train_price_model

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'test_train_price_model',
    default_args=default_args,
    description='DAG to train and evaluate the real estate price prediction model',
    schedule_interval='0 0 * * 0',  # Run weekly on Sunday at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['ml', 'training', 'real-estate'],
)

test_train_model_task = PythonOperator(
    task_id='test_train_price_model',
    python_callable=train_price_model,
    dag=dag,
)

# Task dependencies can be added here if we add more tasks in the future
test_train_model_task 