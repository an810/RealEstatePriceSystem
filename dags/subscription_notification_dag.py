from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the scripts directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from notify import process_subscriptions

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'subscription_notification',
    default_args=default_args,
    description='Process real estate subscriptions and send notifications',
    schedule_interval=timedelta(hours=24),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['real_estate', 'notifications'],
)

process_subscriptions_task = PythonOperator(
    task_id='process_subscriptions',
    python_callable=process_subscriptions,
    dag=dag,
) 

process_subscriptions_task