import pandas as pd
from sklearn.model_selection import train_test_split
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error, mean_squared_error
import joblib
import numpy as np
from sklearn.preprocessing import StandardScaler


df = pd.read_csv('data/cleaned/processed_data.tsv', sep='\t')

X = df.drop(['price', 'title', 'province', 'url_id'], axis=1)
y = df['price']

# Identify categorical features
categorical_features = ["district", "legal"]

# Get numerical features
numerical_features = [col for col in X.columns if col not in categorical_features]

# Scale numerical features
scaler = StandardScaler()
X[numerical_features] = scaler.fit_transform(X[numerical_features])

# Convert categorical columns to category dtype
for cat_col in categorical_features:
    X[cat_col] = X[cat_col].astype('category')

# Train/test split
X_train, X_valid, y_train, y_valid = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Create LightGBM datasets
train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=categorical_features)
valid_data = lgb.Dataset(X_valid, label=y_valid, categorical_feature=categorical_features)

# Parameters
params = {
    'objective': 'regression',
    'metric': 'rmse',
    'boosting_type': 'gbdt',
    'learning_rate': 0.05,
    'num_leaves': 31,
    'verbose': -1,
    'seed': 42
}

# Train model
model = lgb.train(
    params,
    train_data,
    num_boost_round=1000,
    valid_sets=[train_data, valid_data],
    callbacks=[
        lgb.early_stopping(stopping_rounds=50),
        lgb.log_evaluation(period=100)
    ]
)

# Predict
preds = model.predict(X_valid, num_iteration=model.best_iteration)

rmse = np.sqrt(mean_squared_error(y_valid, preds))
print(f"LightGBM RMSE: {rmse:.4f}")

mae = mean_absolute_error(y_valid, preds)
r2 = r2_score(y_valid, preds)
mape = mean_absolute_percentage_error(y_valid, preds)
print(f'Mean Absolute Error: {mae}')
print(f'R-squared: {r2}')
print(f'Mean Absolute Percentage Error (MAPE): {mape}')

# Save both the model and scaler
joblib.dump(model, 'models/saved/lightgbm_model.joblib')
joblib.dump(scaler, 'models/saved/lightgbm_scaler.joblib')

print("Model and scaler saved successfully")

