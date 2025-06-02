import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import mlflow.sklearn
import mlflow.xgboost
import mlflow.xgboost
import pandas as pd
from fastapi import APIRouter, HTTPException
from mlflow.tracking import MlflowClient
import logging
import os


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI router
router = APIRouter()

# Define required features for each model in the exact order expected by the model
MODEL_FEATURES = [
    'area', 'number_of_bedrooms', 'number_of_toilets', 
    'legal', 'lat', 'lon', 'district', 'property_type_id'
]

# Define required user input features
USER_INPUT_FEATURES = [
    'area', 'number_of_bedrooms', 'number_of_toilets', 
    'legal', 'district', 'property_type'
]

# Legal status mapping
LEGAL_STATUS_MAPPING = {
    'Chưa có sổ': 0,
    'Hợp đồng': 1,
    'Sổ đỏ': 2
}

# Property type mapping
PROPERTY_TYPE_MAPPING = {
    'Chung cư': 1,
    'Biệt thự': 2,
    'Nhà riêng': 3,
    'Đất': 4,
    'Khác': 0
}

# District coordinates mapping
DISTRICT_COORDINATES = {
    'Ba Đình': {'lat': 21.0357, 'lon': 105.8342},
    'Ba Vì': {'lat': 21.1992, 'lon': 105.4233},
    'Cầu Giấy': {'lat': 21.0367, 'lon': 105.8014},
    'Chương Mỹ': {'lat': 20.9233, 'lon': 105.7117},
    'Đan Phượng': {'lat': 21.0833, 'lon': 105.6667},
    'Đông Anh': {'lat': 21.1333, 'lon': 105.8500},
    'Đống Đa': {'lat': 21.0167, 'lon': 105.8333},
    'Gia Lâm': {'lat': 21.0167, 'lon': 105.9333},
    'Hà Đông': {'lat': 20.9714, 'lon': 105.7788},
    'Hai Bà Trưng': {'lat': 21.0167, 'lon': 105.8500},
    'Hoài Đức': {'lat': 21.0333, 'lon': 105.7000},
    'Hoàn Kiếm': {'lat': 21.0285, 'lon': 105.8542},
    'Hoàng Mai': {'lat': 20.9833, 'lon': 105.8500},
    'Long Biên': {'lat': 21.0333, 'lon': 105.9000},
    'Mê Linh': {'lat': 21.1833, 'lon': 105.7167},
    'Mỹ Đức': {'lat': 20.6500, 'lon': 105.7167},
    'Phú Xuyên': {'lat': 20.7333, 'lon': 105.9000},
    'Phúc Thọ': {'lat': 21.1000, 'lon': 105.5667},
    'Quốc Oai': {'lat': 20.9833, 'lon': 105.6167},
    'Sóc Sơn': {'lat': 21.2500, 'lon': 105.8500},
    'Sơn Tây': {'lat': 21.1333, 'lon': 105.5000},
    'Tây Hồ': {'lat': 21.0667, 'lon': 105.8167},
    'Thạch Thất': {'lat': 21.0333, 'lon': 105.5500},
    'Thanh Oai': {'lat': 20.8667, 'lon': 105.7667},
    'Thanh Trì': {'lat': 20.9333, 'lon': 105.8500},
    'Thanh Xuân': {'lat': 21.0000, 'lon': 105.8000},
    'Thường Tín': {'lat': 20.8667, 'lon': 105.8667},
    'Từ Liêm': {'lat': 21.0333, 'lon': 105.7500},
    'Ứng Hòa': {'lat': 20.7167, 'lon': 105.8333}
}

# Load district mapping
DISTRICT_MAPPING = {}
try:
    with open('/Users/ducan/Documents/Graduation-Thesis/RealEstatePriceSystem/data/cleaned/district_mapping.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if ':' in line:
                district_id, district_name = line.strip().split(': ')
                DISTRICT_MAPPING[district_name.strip()] = int(district_id)
except Exception as e:
    logger.error(f"Error loading district mapping: {str(e)}")
    raise

# Load URL mappings
BATDONGSAN_URLS = {}
NHATOT_URLS = {}

def load_url_mappings():
    """Load URL mappings from files."""
    global BATDONGSAN_URLS, NHATOT_URLS
    
    try:
        # Load batdongsan URLs
        if os.path.exists('data/output/batdongsan_url.tsv'):
            with open('data/cleaned/batdongsan_url.tsv', 'r', encoding='utf-8') as f:
                for line in f:
                    url_id, url = line.strip().split('\t')
                    BATDONGSAN_URLS[url_id.strip()] = url.strip()
        
        # Load nhatot URLs
        if os.path.exists('data/output/nhatot_url.tsv'):
            with open('data/output/nhatot_url.tsv', 'r', encoding='utf-8') as f:
                for line in f:
                    url_id, url = line.strip().split('\t')
                    NHATOT_URLS[url_id.strip()] = url.strip()
                        
    except Exception as e:
        logger.error(f"Error loading URL mappings: {str(e)}")
        raise

def get_property_url(url_id):
    """Get URL for a property based on its ID format."""
    # Check if it's a batdongsan ID (numeric)
    if url_id.isdigit():
        return {
            'source': 'batdongsan',
            'url': BATDONGSAN_URLS.get(url_id)
        }
    # Check if it's a nhatot ID (starts with 'pr')
    elif url_id.startswith('pr'):
        return {
            'source': 'nhatot',
            'url': NHATOT_URLS.get(url_id)
        }
    return None

def validate_input_data(data, required_features):
    """Validate input data has all required features."""
    missing_features = [f for f in required_features if f not in data]
    if missing_features:
        raise ValueError(f"Missing required features: {missing_features}")
    return True

def validate_districts(district_names):
    """Validate district names and convert to IDs."""
    if not isinstance(district_names, list):
        raise ValueError("Districts must be a list of district names")
    if not district_names:
        raise ValueError("At least one district must be specified")
    
    district_ids = []
    invalid_districts = []
    
    for district in district_names:
        if district not in DISTRICT_MAPPING:
            invalid_districts.append(district)
        else:
            district_ids.append(DISTRICT_MAPPING[district])
    
    if invalid_districts:
        raise ValueError(f"Invalid district names: {invalid_districts}")
    
    return district_ids

def get_district_id(district_name):
    """Get district ID from district name."""
    if district_name not in DISTRICT_MAPPING:
        raise ValueError(f"Invalid district name: {district_name}")
    return DISTRICT_MAPPING[district_name]

def load_models():
    """Load the production versions of both models."""
    global price_prediction_model
    
    try:
        # Initialize MLflow client
        client = MlflowClient()
        # model_name = "price_prediction_model"
        model_name = "price_prediction_stacking_model"
        
        # Get the production model version
        latest_production = client.get_latest_versions(model_name, stages=["Production"])
        
        if not latest_production:
            raise ValueError("No production model found for price prediction")
            
        # Load price prediction model from production
        price_prediction_model = mlflow.pyfunc.load_model(f"models:/{model_name}/Production")
        logger.info(f"Loaded price prediction model version {latest_production[0].version} from production")
        
    except Exception as e:
        logger.error(f"Error loading models: {str(e)}")
        raise

# Set MLflow tracking URI
mlflow.set_tracking_uri("http://localhost:5001")

# Load the models and URL mappings
try:
    load_models()
    load_url_mappings()
except Exception as e:
    logger.error(f"Error loading models or URL mappings: {str(e)}")
    raise

@router.post('/predict-price')
async def predict_price(data: dict):
    try:
        if price_prediction_model is None:
            raise HTTPException(status_code=500, detail="Price prediction model not loaded")
            
        # Validate input data - only check user-provided features
        validate_input_data(data, USER_INPUT_FEATURES)
        
        # Convert district name to ID
        district_id = get_district_id(data['district'])
        
        # Get district coordinates
        if data['district'] not in DISTRICT_COORDINATES:
            raise ValueError(f"Invalid district: {data['district']}")
        district_coords = DISTRICT_COORDINATES[data['district']]
        
        # Convert legal status text to number
        if data['legal'] not in LEGAL_STATUS_MAPPING:
            raise ValueError(f"Invalid legal status: {data['legal']}")
        legal_status = LEGAL_STATUS_MAPPING[data['legal']]

        # Convert property type text to number
        if data['property_type'] not in PROPERTY_TYPE_MAPPING:
            raise ValueError(f"Invalid property type: {data['property_type']}")
        property_type_id = PROPERTY_TYPE_MAPPING[data['property_type']]
        
        # Create DataFrame with features in the exact order expected by the model
        features = pd.DataFrame([{
            'area': float(data['area']),
            'number_of_bedrooms': int(data['number_of_bedrooms']),
            'number_of_toilets': int(data['number_of_toilets']),
            'legal': legal_status,
            'lat': district_coords['lat'],
            'lon': district_coords['lon'],
            'district': district_id,
            'property_type_id': property_type_id
        }])
        
        logger.info(f"data: {data}")
        logger.info(f"features: {features}")

        # Ensure columns are in the correct order
        features = features[MODEL_FEATURES]
        
        # Make prediction
        prediction = price_prediction_model.predict(features)
        logger.info(f"predicted price: {prediction}")
        
        return {
            'predicted_price': float(prediction[0]),
            'input_features': {
                'area': data['area'],
                'number_of_bedrooms': data['number_of_bedrooms'],
                'number_of_toilets': data['number_of_toilets'],
                'legal': data['legal'],
                'district': district_id,
                'property_type': data['property_type']
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in predict_price: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post('/reload-models')
async def reload_models():
    """Endpoint to reload models with the latest production versions."""
    try:
        load_models()
        load_url_mappings()  # Also reload URL mappings
        return {'message': 'Models and URL mappings reloaded successfully'}
    except Exception as e:
        logger.error(f"Error reloading models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001) 