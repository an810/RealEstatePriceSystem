import mlflow
import mlflow.xgboost
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error
import logging
from sqlalchemy import create_engine, text, MetaData, Table, select
import os
from mlflow.tracking import MlflowClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_mlflow():
    """Setup MLflow tracking and experiment"""
    try:
        # Set MLflow tracking URI
        mlflow.set_tracking_uri("http://mlflow:5000")
        
        # Set experiment name
        experiment_name = "real_estate_price_prediction"
        
        # Get or create experiment
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(experiment_name)
            logger.info(f"Created new experiment with ID: {experiment_id}")
        else:
            experiment_id = experiment.experiment_id
            logger.info(f"Using existing experiment with ID: {experiment_id}")
        
        # Set the experiment
        mlflow.set_experiment(experiment_name)
        
        return experiment_id
    except Exception as e:
        logger.error(f"Error setting up MLflow: {str(e)}")
        raise

def load_data_from_db():
    """Load data from PostgreSQL database"""
    try:
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
        
        # Load data from database
        logger.info("Loading data from database...")
        
        # Create metadata and table objects
        metadata = MetaData()
        real_estate_table = Table('real_estate', metadata, autoload_with=engine)
        
        # Create select statement
        query = select([
            real_estate_table.c.title,
            real_estate_table.c.url_id,
            real_estate_table.c.area,
            real_estate_table.c.price,
            real_estate_table.c.number_of_bedrooms,
            real_estate_table.c.number_of_toilets,
            real_estate_table.c.legal,
            real_estate_table.c.lat,
            real_estate_table.c.lon,
            real_estate_table.c.district_id,
            real_estate_table.c.district_name,
            real_estate_table.c.province
        ])
        
        # Execute query and load into DataFrame
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Successfully loaded {len(df)} records from database")
            return df
            
    except Exception as e:
        logger.error(f"Error loading data from database: {str(e)}")
        raise

def train_price_model():
    try:
        # Setup MLflow
        setup_mlflow()
        
        # Load data from database
        df = load_data_from_db()
        
        # Prepare features and target
        X = df.drop(['price', 'title', 'province', 'url_id', 'district_name'], axis=1)
        logger.info(f"X: {X.columns}")
        y = df['price']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=128)
        
        # Start MLflow run
        with mlflow.start_run(run_name="price_prediction_model"):
            # Create and train model
            model = XGBRegressor(random_state=42)
            model.fit(X_train, y_train)
            
            # Make predictions and evaluate
            y_pred = model.predict(X_test)
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            mape = mean_absolute_percentage_error(y_test, y_pred)
            
            # Log parameters
            mlflow.log_params({
                "random_state": 42,
                "test_size": 0.2
            })
            
            # Log metrics
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("r2", r2)
            mlflow.log_metric("mape", mape)
            
            # Log model
            mlflow.xgboost.log_model(model, "price_prediction_model")
            
            # Register model if it's better than the current production model
            try:
                # Try to load current production model
                current_model = mlflow.xgboost.load_model("models:/price_prediction_model/Production")
                current_mae = mean_absolute_error(y_test, current_model.predict(X_test))
                
                if mae < current_mae:
                    # Register the new model version
                    model_details = mlflow.register_model(
                        model_uri=f"runs:/{mlflow.active_run().info.run_id}/price_prediction_model",
                        name="price_prediction_model"
                    )
                    
                    # Transition the new model to Production
                    client = MlflowClient()
                    client.transition_model_version_stage(
                        name="price_prediction_model",
                        version=model_details.version,
                        stage="Production"
                    )
                    
                    # Archive the old production model
                    client.transition_model_version_stage(
                        name="price_prediction_model",
                        version=current_model.version,
                        stage="Archived"
                    )
                    
                    logger.info(f"New model version {model_details.version} promoted to Production")
                else:
                    logger.info("Current production model performs better, keeping it")
            except Exception as e:
                # If no production model exists, register this one as production
                model_details = mlflow.register_model(
                    model_uri=f"runs:/{mlflow.active_run().info.run_id}/price_prediction_model",
                    name="price_prediction_model"
                )
                
                # Set the first model as Production
                client = MlflowClient()
                client.transition_model_version_stage(
                    name="price_prediction_model",
                    version=model_details.version,
                    stage="Production"
                )
                
                logger.info(f"First model version {model_details.version} set as Production")
            
            return True
            
    except Exception as e:
        logger.error(f"Error in train_price_model: {str(e)}")
        return False

if __name__ == "__main__":
    train_price_model() 