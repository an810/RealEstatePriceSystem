from fastapi import FastAPI, APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
from sqlalchemy import text, create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import numpy as np
from contextlib import asynccontextmanager

app = FastAPI()
router = APIRouter()

# Database config (adjust as needed)
DB_CONFIG = {
    'dbname': 'real_estate',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5433'
}

def get_db_engine():
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

def load_property_data():
    """Load property data from database"""
    engine = get_db_engine()
    with engine.connect() as connection:
        query = """
        SELECT 
            url_id, title, price, area, number_of_bedrooms, number_of_toilets,
            legal, district_id, property_type_id, source, url, is_available
        FROM real_estate
        WHERE is_available = TRUE
        """
        result = connection.execute(text(query))
        # Convert result to list of namedtuple-like rows
        return list(result)

# Property type mapping
PROPERTY_TYPE_MAPPING = {
    'Chung cư': 1,
    'Biệt thự': 2,
    'Nhà riêng': 3,
    'Đất': 4
}

def convert_property_type_to_int(value: str) -> int:
    return PROPERTY_TYPE_MAPPING.get(value, -1)

def convert_phaply_to_int(value: str) -> int:
    if any(substring in str(value) for substring in ['chưa', 'Chưa', 'đang', 'Đang', 'chờ', 'Chờ', 'làm sổ']):
        return 0
    elif any(substring in str(value) for substring in ['Hợp đồng', 'hợp đồng', 'HĐMB', 'HDMB']):
        return 1
    elif any(substring in str(value) for substring in ['sổ đỏ', 'Sổ đỏ', 'SỔ ĐỎ', 'Có sổ', 'Sổ hồng', 'sổ hồng', 'SỔ HỒNG', 'Đã có', 'đã có', 'sẵn sổ', 'Sẵn sổ', 'sổ đẹp', 'Sổ đẹp', 'đầy đủ', 'Đầy đủ', 'rõ ràng', 'Rõ ràng', 'chính chủ', 'Chính chủ', 'sẵn sàng', 'Sẵn sàng']):
        return 2
    else:
        return -1
    
def load_district_mapping():
    """Load district mapping from file"""
    mapping = {}
    with open('/Users/ducan/Documents/Graduation-Thesis/RealEstatePriceSystem/data/cleaned/district_mapping.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if ':' in line:
                id_str, name = line.strip().split(':')
                mapping[name.strip()] = int(id_str)
    return mapping

def get_district_ids(districts: List[str]) -> List[int]:
    district_mapping = load_district_mapping()
    district_ids = []
    invalid_districts = []
    for district in districts:
        if district in district_mapping:
            district_ids.append(district_mapping[district])
        else:
            invalid_districts.append(district)
    if invalid_districts:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid district names: {', '.join(invalid_districts)}"
        )
    return district_ids


class PriceRange(BaseModel):
    min_price: float = Field(..., description="Minimum price in VND")
    max_price: float = Field(..., description="Maximum price in VND")

class AreaRange(BaseModel):
    min_area: float = Field(..., description="Minimum area in m2")
    max_area: float = Field(..., description="Maximum area in m2")

class SearchRequest(BaseModel):
    price_range: PriceRange
    area_range: AreaRange
    num_bedrooms: int
    num_toilets: int
    districts: List[str]   # Now using district_ids for simplicity!
    legal_status: str
    property_type: str

def validate_ranges(price_range: PriceRange, area_range: AreaRange):
    if price_range.min_price > price_range.max_price:
        raise HTTPException(status_code=400, detail="Minimum price cannot be greater than maximum price")
    if area_range.min_area > area_range.max_area:
        raise HTTPException(status_code=400, detail="Minimum area cannot be greater than maximum area")

def validate_rooms(num_bedrooms: int, num_toilets: int):
    if num_bedrooms < 0 or num_toilets < 0:
        raise HTTPException(status_code=400, detail="Number of bedrooms and toilets must be non-negative")

# --------------------
# In-memory data cache
# --------------------
class PropertyCache:
    def __init__(self):
        self.features = None  # np.ndarray [N, D]
        self.info = None      # list of DB rows
        self.scaler = None
        self.knn_model = None

    def load_and_fit(self, data):
        features = []
        info = []
        for row in data:
            features.append([
                row.price,
                row.area,
                row.number_of_bedrooms,
                row.number_of_toilets,
                row.legal,
                row.district_id,
                row.property_type_id
            ])
            info.append(row)
        features = np.array(features)
        self.features = features
        self.info = info
        self.scaler = StandardScaler().fit(features)
        scaled_features = self.scaler.transform(features)
        n_neighbors = min(10, len(features))
        self.knn_model = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
        self.knn_model.fit(scaled_features)

    def is_loaded(self):
        return self.features is not None and self.knn_model is not None

property_cache = PropertyCache()


@router.post("/reload-data")
def reload_data():
    try:
        data = load_property_data()
        property_cache.load_and_fit(data)
        return {"message": "Property data and KNN model reloaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload data: {str(e)}")

# ----------------
# Search endpoint
# ----------------
@router.post("/search")
async def search_properties(request: SearchRequest):
    if not property_cache.is_loaded():
        raise HTTPException(status_code=503, detail="Data is not loaded yet, please try again later.")
    try:
        validate_ranges(request.price_range, request.area_range)
        validate_rooms(request.num_bedrooms, request.num_toilets)
        legal_status_id = convert_phaply_to_int(request.legal_status)
        if legal_status_id == -1:
            raise HTTPException(status_code=400, detail="Invalid legal status")
        property_type_id = convert_property_type_to_int(request.property_type)
        if property_type_id == -1:
            raise HTTPException(status_code=400, detail="Invalid property type")
        if not request.districts:
            raise HTTPException(status_code=400, detail="At least one district_id is required")

        results_by_district = {}
        district_ids = get_district_ids(request.districts)
        for district_id in district_ids:
            # Filter in-memory
            idxs = [i for i, row in enumerate(property_cache.info) if row.district_id == district_id]
            if not idxs:
                results_by_district[str(district_id)] = []
                continue
            sub_features = property_cache.features[idxs]
            sub_info = [property_cache.info[i] for i in idxs]
            sub_scaled = property_cache.scaler.transform(sub_features)

            # Build KNN for this district
            n_neighbors = min(3, len(sub_scaled))
            knn = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
            knn.fit(sub_scaled)
            query_point = [
                (request.price_range.min_price + request.price_range.max_price) / 2,
                (request.area_range.min_area + request.area_range.max_area) / 2,
                request.num_bedrooms,
                request.num_toilets,
                legal_status_id,
                district_id,
                property_type_id
            ]
            scaled_query = property_cache.scaler.transform([query_point])
            distances, indices = knn.kneighbors(scaled_query)
            district_results = []
            for idx, distance in zip(indices[0], distances[0]):
                property_info = sub_info[idx]
                district_results.append({
                    'url_id': property_info.url_id,
                    'title': property_info.title,
                    'price': property_info.price,
                    'area': property_info.area,
                    'number_of_bedrooms': property_info.number_of_bedrooms,
                    'number_of_toilets': property_info.number_of_toilets,
                    'legal': property_info.legal,
                    'district_id': property_info.district_id,
                    'property_type_id': property_info.property_type_id,
                    'url': property_info.url,
                    'source': property_info.source,
                    'similarity_score': 1 - distance
                })
            results_by_district[str(district_id)] = district_results

        return {'message': 'Search completed successfully', 'results_by_district': results_by_district}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
