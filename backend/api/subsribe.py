from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime
from sqlalchemy import text, create_engine

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

class SubscribeRequest(BaseModel):
    user_name: str = Field(..., description="User name")
    price_range: PriceRange
    area_range: AreaRange
    num_bedrooms: int = Field(..., description="Number of bedrooms")
    num_toilets: int = Field(..., description="Number of toilets")
    districts: List[str] = Field(..., description="List of district names in Hanoi")
    legal_status: str = Field(..., description="Legal status of the property")
    user_id: str = Field(..., description="User identifier (email or telegram ID)")
    user_type: str = Field(..., description="Type of user (email or telegram)")

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

def create_subscription_table(connection):
    """Create subscription table if it doesn't exist"""
    connection.execute(text("""
        CREATE TABLE IF NOT EXISTS subscription (
            id SERIAL PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            min_price FLOAT NOT NULL,
            max_price FLOAT NOT NULL,
            min_area FLOAT NOT NULL,
            max_area FLOAT NOT NULL,
            num_bedrooms INTEGER NOT NULL,
            num_toilets INTEGER NOT NULL,
            district_ids VARCHAR(255) NOT NULL,
            legal_status_id INTEGER NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            user_type VARCHAR(10) NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """))

def save_subscription(connection, subscription_data: dict):
    """Save subscription data to database"""
    connection.execute(text("""
        INSERT INTO subscription (
            user_name, min_price, max_price, min_area, max_area,
            num_bedrooms, num_toilets, district_ids,
            legal_status_id, user_id, user_type, created_at
        ) VALUES (
            :user_name, :min_price, :max_price, :min_area, :max_area,
            :num_bedrooms, :num_toilets, :district_ids,
            :legal_status_id, :user_id, :user_type, :created_at
        )
    """), subscription_data)

@router.post("/subscribe")
async def subscribe(request: SubscribeRequest):
    try:
        # Validate input data
        validate_ranges(request.price_range, request.area_range)
        validate_rooms(request.num_bedrooms, request.num_toilets)
        
        # Validate user_type
        if request.user_type not in ['email', 'telegram']:
            raise HTTPException(status_code=400, detail="Invalid user_type. Must be either 'email' or 'telegram'")
        
        # Convert district names to IDs
        district_ids = get_district_ids(request.districts)
        
        # Convert legal status
        legal_status_id = convert_phaply_to_int(request.legal_status)
        if legal_status_id == -1:
            raise HTTPException(status_code=400, detail="Invalid legal status")
        
        # Prepare subscription data
        subscription_data = {
            "user_name": request.user_name,
            "min_price": request.price_range.min_price,
            "max_price": request.price_range.max_price,
            "min_area": request.area_range.min_area,
            "max_area": request.area_range.max_area,
            "num_bedrooms": request.num_bedrooms,
            "num_toilets": request.num_toilets,
            "district_ids": ','.join(map(str, district_ids)),  # Convert array to comma-separated string
            "legal_status_id": legal_status_id,
            "user_id": request.user_id,
            "user_type": request.user_type,
            "created_at": datetime.now()
        }
        
        # Save to database
        engine = get_db_engine()
        with engine.begin() as connection:
            create_subscription_table(connection)
            save_subscription(connection, subscription_data)
        
        return {
            "message": "Subscription created successfully",
            "data": {
                "user_name": request.user_name,
                "price_range": {
                    "min": request.price_range.min_price,
                    "max": request.price_range.max_price
                },
                "area_range": {
                    "min": request.area_range.min_area,
                    "max": request.area_range.max_area
                },
                "num_bedrooms": request.num_bedrooms,
                "num_toilets": request.num_toilets,
                "district_ids": district_ids,
                "legal_status_id": legal_status_id,
                "user_id": request.user_id
            }
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/unsubscribe/{user_id}")
async def unsubscribe(user_id: str):
    try:
        engine = get_db_engine()
        with engine.begin() as connection:
            # Delete all subscriptions for the given user_id
            result = connection.execute(
                text("DELETE FROM subscription WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            
            if result.rowcount == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No active subscription found for user {user_id}"
                )
            
            return {
                "message": "Successfully unsubscribed",
                "user_id": user_id
            }
            
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
