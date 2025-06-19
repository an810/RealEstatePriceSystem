import logging
from typing import Dict, Any
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import sqlalchemy

logger = logging.getLogger(__name__)

def train_model(**context) -> Dict[str, Any]:
    """
    Main function to train the model. This function will be called by the Airflow task.
    All heavy imports are done inside this function to avoid DAG import timeouts.
    """
    try:
        # Import heavy ML libraries inside the function
        import mlflow
        import mlflow.xgboost
        import mlflow.lightgbm
        import mlflow.catboost
        import mlflow.sklearn
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error
        from sqlalchemy import create_engine, text, MetaData, Table, select
        from mlflow.tracking import MlflowClient
        from sklearn.preprocessing import StandardScaler, OneHotEncoder
        from xgboost import XGBRegressor
        import lightgbm as lgb
        from catboost import CatBoostRegressor
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.linear_model import LinearRegression
        import sys
        import os

        # Add the models directory to Python path
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from sklearn.ensemble import StackingRegressor

        def setup_mlflow():
            """Setup MLflow tracking and experiment"""
            try:
                mlflow.set_tracking_uri("http://mlflow:5000")
                experiment_name = "real_estate_price_prediction"
                
                # Enable autologging for all supported frameworks
                mlflow.autolog(
                    log_input_examples=True,
                    log_model_signatures=True,
                    log_models=True,
                    disable=False,
                    exclusive=False,
                    disable_for_unsupported_versions=False,
                    silent=False
                )
                
                experiment = mlflow.get_experiment_by_name(experiment_name)
                if experiment is None:
                    experiment_id = mlflow.create_experiment(experiment_name)
                    logger.info(f"Created new experiment with ID: {experiment_id}")
                else:
                    experiment_id = experiment.experiment_id
                    logger.info(f"Using existing experiment with ID: {experiment_id}")
                
                mlflow.set_experiment(experiment_name)
                return experiment_id
            except Exception as e:
                logger.error(f"Error setting up MLflow: {str(e)}")
                raise

        def load_data_from_db():
            """Load data from database"""
            print(sqlalchemy.__version__)
            try:
                logger.info("Loading data from database...")
                db_url = 'postgresql+psycopg2://postgres:postgres@real_estate_db:5432/real_estate'
                logger.info(f"Database URL: {db_url}")
                
                # Create database connection
                engine = create_engine(db_url)
                
                # Query to get the data
                query = """
                    SELECT 
                        area,
                        price,
                        number_of_bedrooms,
                        number_of_toilets,
                        legal,
                        lat,
                        lon,
                        district_id,
                        property_type_id
                    FROM real_estate
                """
                
                # Load data from database
                df = pd.read_sql(query, engine)
                
                # Validate data
                logger.info(f"Data shape: {df.shape}")
                logger.info(f"Columns: {df.columns.tolist()}")
                logger.info(f"Missing values:\n{df.isnull().sum()}")
                
                # Handle missing values
                numeric_columns = ['area', 'price', 'number_of_bedrooms', 'number_of_toilets', 'legal', 'lat', 'lon', 'district_id', 'property_type_id']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].fillna(-1)
                
                logger.info(f"Successfully loaded {len(df)} records from database")
                return df
                    
            except Exception as e:
                logger.error(f"Error loading data from database: {str(e)}")
                raise

        # Setup MLflow
        setup_mlflow()
        
        # Load data from database
        df = load_data_from_db()
        
        # Prepare features and target
        feature_columns = ['area', 'number_of_bedrooms', 'number_of_toilets', 'legal', 'lat', 'lon', 'district_id', 'property_type_id']
        X = df[feature_columns].copy()
        y = df['price'].copy()
        
        # Log data statistics
        logger.info(f"Feature statistics:\n{X.describe()}")
        logger.info(f"Target statistics:\n{y.describe()}")
        
        logger.info("reset index")
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        X_train = pd.DataFrame(X_train).reset_index(drop=True)
        X_test = pd.DataFrame(X_test).reset_index(drop=True)
        y_train = pd.Series(y_train).reset_index(drop=True)
        y_test = pd.Series(y_test).reset_index(drop=True)
        
        logger.info(f"X_train shape:\n{X_train.shape}")
        logger.info(f"y_train shape:\n{y_train.shape}")

        numeric_features = ['area', 'number_of_bedrooms', 'number_of_toilets', 'lat', 'lon']
        categorical_features = ['legal', 'district', 'property_type_id']

        preprocessor = ColumnTransformer(transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features)
        ])

        # scaler = StandardScaler()
        # X_train_scaled_numeric = scaler.fit_transform(X_train[numeric_features])
        # X_test_scaled_numeric = scaler.transform(X_test[numeric_features])

        # encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        # X_train_encoded = encoder.fit_transform(X_train[categorical_features])
        # X_test_encoded = encoder.transform(X_test[categorical_features])

        # # Combine back
        # X_train_scaled = np.hstack([X_train_scaled_numeric, X_train_encoded])
        # X_test_scaled = np.hstack([X_test_scaled_numeric, X_test_encoded])


        # Scale the features
        # scaler = StandardScaler()
        # X_train_scaled = scaler.fit_transform(X_train)
        # X_test_scaled = scaler.transform(X_test)
        
        # logger.info(f"X_train_scaled shape:\n{X_train_scaled.shape}")
        # logger.info(f"X_test_scaled shape:\n{X_test_scaled.shape}")
        
        # Start MLflow run
        with mlflow.start_run(run_name="price_prediction_stacking_model"):
            # Define base models
            base_models = [
                ('xgb', XGBRegressor(n_estimators=200, random_state=42)),
                ('lgb', lgb.LGBMRegressor(n_estimators=200, random_state=42)),
                ('cat', CatBoostRegressor(iterations=200, verbose=0, random_seed=42)),
                ('rf', RandomForestRegressor(n_estimators=100, random_state=42))
            ]

            
            # Meta model
            meta_model = LinearRegression()
            
            # Initialize stacking model
            # model = StackingRegressor(base_models=base_models, meta_model=meta_model, n_folds=5)
            model = StackingRegressor(
                estimators=base_models,
                final_estimator=meta_model,
                cv=5,                # số fold (cv thay cho n_folds)
                passthrough=False,   # nếu muốn stack cả input features
                n_jobs=-1            # chạy song song, optional
            )
            
            pipeline = Pipeline([
                ('preprocessor', preprocessor),
                ('regressor', model)
            ])


            # Train model - autologging will automatically log parameters, metrics, and artifacts
            pipeline.fit(X_train, y_train)
            
            # Make predictions and evaluate
            y_pred = pipeline.predict(X_test)
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            mape = mean_absolute_percentage_error(y_test, y_pred)
            
            # Log additional custom metrics
            mlflow.log_metrics({
                "mae": mae,
                "r2": r2,
                "mape": mape
            })
            
            # Save model and scaler
            mlflow.sklearn.log_model(pipeline, "price_prediction_stacking_model")
            # mlflow.sklearn.log_model(scaler, "stacking_scaler")
            
            # Register model if it's better than the current production model
            try:
                current_model = mlflow.pyfunc.load_model("models:/price_prediction_stacking_model/Production")
                current_mae = mean_absolute_error(y_test, current_model.predict(X_test))
                
                if mae < current_mae:
                    model_details = mlflow.register_model(
                        model_uri=f"runs:/{mlflow.active_run().info.run_id}/price_prediction_stacking_model",
                        name="price_prediction_stacking_model"
                    )
                    
                    client = MlflowClient()
                    client.transition_model_version_stage(
                        name="price_prediction_stacking_model",
                        version=model_details.version,
                        stage="Production"
                    )
                    
                    client.transition_model_version_stage(
                        name="price_prediction_stacking_model",
                        version=current_model.version,
                        stage="Archived"
                    )
                    
                    logger.info(f"New model version {model_details.version} promoted to Production")
                else:
                    logger.info("Current production model performs better, keeping it")
            except Exception as e:
                model_details = mlflow.register_model(
                    model_uri=f"runs:/{mlflow.active_run().info.run_id}/price_prediction_stacking_model",
                    name="price_prediction_stacking_model"
                )
                
                client = MlflowClient()
                client.transition_model_version_stage(
                    name="price_prediction_stacking_model",
                    version=model_details.version,
                    stage="Production"
                )
                
                logger.info(f"First model version {model_details.version} set as Production")
            
            return {"status": "success", "metrics": {"mae": mae, "r2": r2, "mape": mape}}
            
    except Exception as e:
        logger.error(f"Error in train_model: {str(e)}")
        return {"status": "error", "error": str(e)} 