from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from sqlalchemy import create_engine
from sklearn.neighbors import NearestNeighbors

router = APIRouter()


# Database configuration
DB_CONFIG = {
    'dbname': 'real_estate',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5433'
}

def get_db_engine():
    """Get database engine instance"""
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

def load_district_mapping():
    """Load district mapping from file"""
    mapping = {}
    with open('/Users/ducan/Documents/Graduation-Thesis/RealEstatePriceSystem/data/cleaned/district_mapping.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if ':' in line:
                id_str, name = line.strip().split(':')
                mapping[name.strip()] = int(id_str)
    return mapping

def convert_phaply_to_int(value: str) -> int:
    """Convert legal status string to integer code"""
    if any(substring in str(value) for substring in ['chưa', 'Chưa', 'đang', 'Đang', 'chờ', 'Chờ', 'làm sổ']):
        return 0
    elif any(substring in str(value) for substring in ['Hợp đồng', 'hợp đồng', 'HĐMB', 'HDMB']):
        return 1
    elif any(substring in str(value) for substring in ['sổ đỏ', 'Sổ đỏ', 'SỔ ĐỎ', 'Có sổ', 'Sổ hồng', 'sổ hồng', 'SỔ HỒNG', 'Đã có', 'đã có', 'sẵn sổ', 'Sẵn sổ', 'sổ đẹp', 'Sổ đẹp', 'đầy đủ', 'Đầy đủ', 'rõ ràng', 'Rõ ràng', 'chính chủ', 'Chính chủ', 'sẵn sàng', 'Sẵn sàng']):
        return 2
    else:
        return -1

class PriceRange(BaseModel):
    min_price: float = Field(..., description="Minimum price in VND")
    max_price: float = Field(..., description="Maximum price in VND")

class AreaRange(BaseModel):
    min_area: float = Field(..., description="Minimum area in m2")
    max_area: float = Field(..., description="Maximum area in m2")

class SearchRequest(BaseModel):
    price_range: PriceRange
    area_range: AreaRange
    num_bedrooms: int = Field(..., description="Number of bedrooms")
    num_toilets: int = Field(..., description="Number of toilets")
    districts: List[str] = Field(..., description="List of district names in Hanoi")
    legal_status: str = Field(..., description="Legal status of the property")
    property_type: str = Field(..., description="Type of property")

# Property type mapping
PROPERTY_TYPE_MAPPING = {
    'Chung cư': 1,
    'Biệt thự': 2,
    'Nhà riêng': 3,
    'Đất': 4
}

def convert_property_type_to_int(value: str) -> int:
    """Convert property type string to integer code"""
    return PROPERTY_TYPE_MAPPING.get(value, -1)

def convert_int_to_property_type(value: int) -> str:
    """Convert property type integer code to text format"""
    for prop_type, prop_id in PROPERTY_TYPE_MAPPING.items():
        if prop_id == value:
            return prop_type
    return "Không xác định"

def validate_ranges(price_range: PriceRange, area_range: AreaRange):
    """Validate price and area ranges"""
    if price_range.min_price > price_range.max_price:
        raise HTTPException(status_code=400, detail="Minimum price cannot be greater than maximum price")
    
    if area_range.min_area > area_range.max_area:
        raise HTTPException(status_code=400, detail="Minimum area cannot be greater than maximum area")

def validate_rooms(num_bedrooms: int, num_toilets: int):
    """Validate number of bedrooms and toilets"""
    if num_bedrooms < 0 or num_toilets < 0:
        raise HTTPException(status_code=400, detail="Number of bedrooms and toilets must be non-negative")

def get_district_ids(districts: List[str]) -> List[int]:
    """Convert district names to IDs"""
    district_mapping = load_district_mapping()
    district_ids = []
    invalid_districts = []
    
    for district in districts:
        if district == 'Từ Liêm':
            # Handle special case for Từ Liêm
            if 'Bắc Từ Liêm' in district_mapping:
                district_ids.append(district_mapping['Bắc Từ Liêm'])
            if 'Nam Từ Liêm' in district_mapping:
                district_ids.append(district_mapping['Nam Từ Liêm'])
        elif district in district_mapping:
            district_ids.append(district_mapping[district])
        else:
            invalid_districts.append(district)
    
    if invalid_districts:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid district names: {', '.join(invalid_districts)}"
        )
    
    return district_ids

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
        return result.fetchall()

def prepare_features(data, search_params, district_id=None):
    """Prepare features for KNN model"""
    # Filter data by district if specified
    if district_id is not None:
        data = [row for row in data if row.district_id == district_id]
    
    if not data:
        return None, None, None
    
    # Extract features from database
    features = []
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
    
    # Add search parameters as a query point
    query_point = [
        (search_params['price_range'].min_price + search_params['price_range'].max_price) / 2,
        (search_params['area_range'].min_area + search_params['area_range'].max_area) / 2,
        search_params['num_bedrooms'],
        search_params['num_toilets'],
        search_params['legal_status_id'],
        district_id if district_id is not None else search_params['district_ids'][0],
        search_params['property_type_id']
    ]
    
    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    scaled_query = scaler.transform([query_point])
    
    return scaled_features, scaled_query, data

def get_district_name(district_id: int) -> str:
    """Convert district ID to name"""
    district_mapping = load_district_mapping()
    for name, id in district_mapping.items():
        if id == district_id:
            return name
    return f"District {district_id}"

def convert_int_to_phaply(value: int) -> str:
    """Convert legal status integer code to text format"""
    if value == 0:
        return "Chưa có sổ"
    elif value == 1:
        return "Hợp đồng mua bán"
    elif value == 2:
        return "Đã có sổ"
    else:
        return "Không xác định"

@router.post("/search")
async def search_properties(request: SearchRequest):
    try:
        # Validate input data
        validate_ranges(request.price_range, request.area_range)
        validate_rooms(request.num_bedrooms, request.num_toilets)
        
        # Convert district names to IDs
        district_ids = get_district_ids(request.districts)
        
        # Convert legal status
        legal_status_id = convert_phaply_to_int(request.legal_status)
        if legal_status_id == -1:
            raise HTTPException(status_code=400, detail="Invalid legal status")

        # Convert property type
        property_type_id = convert_property_type_to_int(request.property_type)
        if property_type_id == -1:
            raise HTTPException(status_code=400, detail="Invalid property type")
        
        # Prepare search parameters
        search_params = {
            'price_range': request.price_range,
            'area_range': request.area_range,
            'num_bedrooms': request.num_bedrooms,
            'num_toilets': request.num_toilets,
            'district_ids': district_ids,
            'legal_status_id': legal_status_id,
            'property_type_id': property_type_id
        }
        
        # Load property data
        property_data = load_property_data()
        
        # Search for each district
        results_by_district = {}
        for district_id in district_ids:
            # Prepare features for this district
            scaled_features, scaled_query, district_data = prepare_features(property_data, search_params, district_id)
            
            if scaled_features is None:
                results_by_district[get_district_name(district_id)] = []
                continue
            
            # Initialize and fit KNN model
            n_neighbors = min(3, len(scaled_features))  # Get top 3 matches per district
            knn = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
            knn.fit(scaled_features)
            
            # Find nearest neighbors
            distances, indices = knn.kneighbors(scaled_query)
            
            # Prepare results for this district
            district_results = []
            for idx, distance in zip(indices[0], distances[0]):
                property_info = district_data[idx]
                district_results.append({
                    'url_id': property_info.url_id,
                    'title': property_info.title,
                    'url': property_info.url,
                    'source': property_info.source,
                    'price': property_info.price,
                    'area': property_info.area,
                    'number_of_bedrooms': property_info.number_of_bedrooms,
                    'number_of_toilets': property_info.number_of_toilets,
                    'legal_status': convert_int_to_phaply(property_info.legal),
                    'property_type_id': property_info.property_type_id,
                    'district_id': property_info.district_id,
                    'similarity_score': 1 - distance  # Convert distance to similarity score
                })
            
            results_by_district[get_district_name(district_id)] = district_results
        
        return {
            'message': 'Search completed successfully',
            'results_by_district': results_by_district
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
