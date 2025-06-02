import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, mean_absolute_percentage_error
from sklearn.base import BaseEstimator, RegressorMixin, clone
from sklearn.preprocessing import StandardScaler
import joblib

# Import libraries for base learners
from xgboost import XGBRegressor
import lightgbm as lgb
from catboost import CatBoostRegressor

# Simple Stacking Regressor
class StackingRegressor(BaseEstimator, RegressorMixin):
    def __init__(self, base_models, meta_model, n_folds=5):
        self.base_models = base_models
        self.meta_model = meta_model
        self.n_folds = n_folds

    def fit(self, X, y):
        from sklearn.model_selection import KFold
        self.base_models_ = [list() for _ in self.base_models]
        self.meta_features_ = np.zeros((X.shape[0], len(self.base_models)))
        kf = KFold(n_splits=self.n_folds, shuffle=True, random_state=42)

        for i, model in enumerate(self.base_models):
            for train_idx, holdout_idx in kf.split(X, y):
                instance = clone(model)
                instance.fit(X[train_idx], y[train_idx])
                self.base_models_[i].append(instance)
                self.meta_features_[holdout_idx, i] = instance.predict(X[holdout_idx])
        
        self.meta_model.fit(self.meta_features_, y)
        return self

    def predict(self, X):
        meta_features = np.column_stack([
            np.mean([model.predict(X) for model in base_models], axis=0)
            for base_models in self.base_models_
        ])
        return self.meta_model.predict(meta_features)


# === Usage Example ===

# Load your data (replace with your actual data)
data = pd.read_csv('data/cleaned/processed_data.tsv', sep='\t')

X = data.drop(columns=["price", "title", "url_id", "province"]).values
y = data["price"].values

# Split train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale the features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(f"X_train_scaled shape: {X_train_scaled.shape}")
print(f"X_test_scaled shape: {X_test_scaled.shape}")
print(f"y_train shape: {y_train.shape}")
print(f"y_test shape: {y_test.shape}")

# Define base models
base_models = [
    XGBRegressor(n_estimators=200, random_state=42),
    lgb.LGBMRegressor(n_estimators=200, random_state=42),
    CatBoostRegressor(iterations=200, verbose=0, random_seed=42),
    RandomForestRegressor(n_estimators=100, random_state=42)
]

# Meta model
meta_model = LinearRegression()

# Initialize stacking model
stacking_model = StackingRegressor(base_models=base_models, meta_model=meta_model, n_folds=5)

# Train
stacking_model.fit(X_train_scaled, y_train)

# Predict
y_pred = stacking_model.predict(X_test_scaled)

# Evaluate
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print(f"Stacking Ensemble RMSE: {rmse:.4f}")

mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)
print(f'Mean Absolute Error: {mae}')
print(f'R-squared: {r2}')
print(f'Mean Absolute Percentage Error (MAPE): {mape}')

joblib.dump(stacking_model, 'models/saved/stacking_model.joblib')
joblib.dump(scaler, 'models/saved/stacking_scaler.joblib')

print("Model and scaler saved successfully")