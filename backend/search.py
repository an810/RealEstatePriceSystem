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
            legal_id, district_id, property_type_id, source, url, is_available
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
    """Load district mapping from database"""
    mapping = {}
    reverse_mapping = {}
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = """
                SELECT district_id, district 
                FROM district_mapping 
                ORDER BY district_id
            """
            result = conn.execute(text(query))
            for row in result:
                mapping[row.district] = row.district_id
                reverse_mapping[row.district_id] = row.district
        return mapping, reverse_mapping
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load district mapping from database: {str(e)}"
        )

def get_district_ids(districts: List[str]) -> List[int]:
    district_mapping, _ = load_district_mapping()
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
    districts: List[str]
    legal_statuses: List[str]
    property_types: List[str]

def validate_ranges(price_range: PriceRange, area_range: AreaRange):
    if price_range.min_price > price_range.max_price:
        raise HTTPException(status_code=400, detail="Minimum price cannot be greater than maximum price")
    if area_range.min_area > area_range.max_area:
        raise HTTPException(status_code=400, detail="Minimum area cannot be greater than maximum area")

def validate_rooms(num_bedrooms: int, num_toilets: int):
    if num_bedrooms < 0 or num_toilets < 0:
        raise HTTPException(status_code=400, detail="Number of bedrooms and toilets must be non-negative")

def validate_legal_statuses(legal_statuses: List[str]) -> List[int]:
    legal_status_ids = []
    invalid_statuses = []
    for status in legal_statuses:
        status_id = convert_phaply_to_int(status)
        if status_id == -1:
            invalid_statuses.append(status)
        else:
            legal_status_ids.append(status_id)
    if invalid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid legal statuses: {', '.join(invalid_statuses)}"
        )
    return legal_status_ids

def validate_property_types(property_types: List[str]) -> List[int]:
    property_type_ids = []
    invalid_types = []
    for prop_type in property_types:
        type_id = convert_property_type_to_int(prop_type)
        if type_id == -1:
            invalid_types.append(prop_type)
        else:
            property_type_ids.append(type_id)
    if invalid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid property types: {', '.join(invalid_types)}"
        )
    return property_type_ids

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
                row.legal_id,
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
        print(f"Loaded {len(data)} properties")
        print(data[:2])  # Preview the first 2 rows

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
        
        legal_status_ids = validate_legal_statuses(request.legal_statuses)
        property_type_ids = validate_property_types(request.property_types)
        
        if not request.districts:
            raise HTTPException(status_code=400, detail="At least one district is required")

        results_by_district = {}
        _, reverse_district_mapping = load_district_mapping()
        district_ids = get_district_ids(request.districts)
        
        for district_id in district_ids:
            district_results = []
            district_name = reverse_district_mapping[district_id]
            
            # Loop through each combination of legal status and property type
            for legal_status_id in legal_status_ids:
                for property_type_id in property_type_ids:
                    # Filter properties for this specific combination
                    idxs = [i for i, row in enumerate(property_cache.info) 
                           if row.district_id == district_id 
                           and row.legal_id == legal_status_id
                           and row.property_type_id == property_type_id]
                    
                    if not idxs:
                        continue
                        
                    sub_features = property_cache.features[idxs]
                    sub_info = [property_cache.info[i] for i in idxs]
                    sub_scaled = property_cache.scaler.transform(sub_features)

                    # Build KNN for this specific combination
                    n_neighbors = min(3, len(sub_scaled))
                    knn = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
                    knn.fit(sub_scaled)
                    
                    # Create query point for this specific combination
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
                    
                    # Add results for this combination
                    for idx, distance in zip(indices[0], distances[0]):
                        property_info = sub_info[idx]
                        result = {
                            'url_id': property_info.url_id,
                            'title': property_info.title,
                            'price': property_info.price,
                            'area': property_info.area,
                            'number_of_bedrooms': property_info.number_of_bedrooms,
                            'number_of_toilets': property_info.number_of_toilets,
                            'legal': property_info.legal_id,
                            'district': district_name,
                            'property_type_id': property_info.property_type_id,
                            'url': property_info.url,
                            'source': property_info.source,
                            'similarity_score': 1 - distance,
                            'legal_status_id': legal_status_id,
                            'property_type_id': property_type_id
                        }
                        district_results.append(result)
            
            # Sort results by similarity score and take top 3 for this district
            district_results.sort(key=lambda x: x['similarity_score'], reverse=True)
            results_by_district[district_name] = district_results[:3]

        return {
            'message': 'Search completed successfully', 
            'results_by_district': results_by_district
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
