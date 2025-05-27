from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from sqlalchemy import create_engine, text
import mlflow
import pandas as pd
import sqlalchemy

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

def test_database_connection():
    """Test connection to the real estate database and run some queries"""
    try:
        # Log SQLAlchemy version
        logger.info(f"SQLAlchemy version: {sqlalchemy.__version__}")
        
        # Database connection parameters
        db_params = {
            'dbname': 'real_estate',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'real_estate_db',
            'port': '5432'
        }

        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        
        # Test connection and queries
        with engine.connect() as connection:
            # Test 1: Count records in real_estate table
            result = connection.execute(text("SELECT COUNT(*) FROM real_estate"))
            count = result.scalar()
            logger.info(f"Number of records in real_estate table: {count}")
            
            # Test 2: Get sample data
            result = connection.execute(text("""
                SELECT title, price, area, number_of_bedrooms 
                FROM real_estate 
                LIMIT 5
            """))
            sample_data = result.fetchall()
            logger.info("Sample data from real_estate table:")
            for row in sample_data:
                logger.info(f"Title: {row.title}, Price: {row.price}, Area: {row.area}, Bedrooms: {row.number_of_bedrooms}")
            
            # Test 3: Check table structure
            result = connection.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'real_estate'
            """))
            columns = result.fetchall()
            logger.info("Table structure:")
            for col in columns:
                logger.info(f"Column: {col.column_name}, Type: {col.data_type}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error testing database connection: {str(e)}")
        return False

def test_mlflow_connection():
    """Test connection to MLflow"""
    try:
        # Set MLflow tracking URI
        mlflow.set_tracking_uri("http://mlflow:5000")
        
        # Test 1: List all experiments
        experiments = mlflow.search_experiments()
        logger.info(f"Number of MLflow experiments: {len(experiments)}")
        
        # Test 2: Get latest run from price prediction model
        try:
            latest_run = mlflow.search_runs(
                experiment_ids=[exp.experiment_id for exp in experiments],
                filter_string="tags.mlflow.runName = 'price_prediction_model'",
                max_results=1
            )
            if not latest_run.empty:
                logger.info("Latest price prediction model run:")
                logger.info(f"Run ID: {latest_run['run_id'].iloc[0]}")
                logger.info(f"MAE: {latest_run['metrics.mae'].iloc[0]}")
                logger.info(f"R2: {latest_run['metrics.r2'].iloc[0]}")
            else:
                logger.info("No price prediction model runs found")
        except Exception as e:
            logger.warning(f"Could not fetch latest run: {str(e)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing MLflow connection: {str(e)}")
        return False

# Create DAG
dag = DAG(
    'test_connections',
    default_args=default_args,
    description='Test database and MLflow connections',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'connection']
)

# Create tasks
test_db = PythonOperator(
    task_id='test_database_connection',
    python_callable=test_database_connection,
    dag=dag
)

test_mlflow = PythonOperator(
    task_id='test_mlflow_connection',
    python_callable=test_mlflow_connection,
    dag=dag
)

# Set task dependencies
test_db >> test_mlflow 