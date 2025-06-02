from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from model_training_module import train_model

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
    'train_stacking_model',
    default_args=default_args,
    description='DAG to train and evaluate the real estate price prediction model',
    schedule_interval='0 0 * * 0',  # Run weekly on Sunday at midnight
    start_date=days_ago(1),
    catchup=False,
    tags=['ml', 'training', 'real-estate'],
)

train_stacking_model_task = PythonOperator(
    task_id='train_stacking_model',
    python_callable=train_model,
    dag=dag,
)

# Task dependencies can be added here if we add more tasks in the future
train_stacking_model_task 