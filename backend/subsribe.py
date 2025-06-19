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
    """Load district mapping from database"""
    mapping = {}
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
        return mapping
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load district mapping from database: {str(e)}"
        )

def load_legal_status_mapping():
    """Load legal status mapping from database"""
    mapping = {}
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = """
                SELECT legal_id, legal 
                FROM legal_mapping 
                ORDER BY legal_id
            """
            result = conn.execute(text(query))
            for row in result:
                mapping[row.legal] = row.legal_id
        return mapping
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load legal status mapping from database: {str(e)}"
        )

def load_property_type_mapping():
    """Load property type mapping from database"""
    mapping = {}
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = """
                SELECT property_type_id, property_type 
                FROM property_type_mapping 
                ORDER BY property_type_id
            """
            result = conn.execute(text(query))
            for row in result:
                mapping[row.property_type] = row.property_type_id
        return mapping
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load property type mapping from database: {str(e)}"
        )

def convert_legal_status_to_int(value: str) -> int:
    """Convert legal status string to integer code"""
    legal_mapping = load_legal_status_mapping()
    return legal_mapping.get(value, -1)

def convert_property_type_to_int(value: str) -> int:
    """Convert property type string to integer code"""
    property_mapping = load_property_type_mapping()
    return property_mapping.get(value, -1)

def convert_int_to_property_type(value: int) -> str:
    """Convert property type integer code to text format"""
    property_mapping = load_property_type_mapping()
    reverse_mapping = {v: k for k, v in property_mapping.items()}
    return reverse_mapping.get(value, "Không xác định")

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
    legal_statuses: List[str] = Field(..., description="List of legal statuses of the property")
    property_types: List[str] = Field(..., description="List of property types")
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

def save_subscription_with_relations(connection, subscription_data: dict, district_ids: List[int], legal_status_ids: List[int], property_type_ids: List[int]):
    """Save subscription data to database with intermediate tables"""
    # Insert main subscription record
    result = connection.execute(text("""
        INSERT INTO subscription (
            user_id, user_name, user_type, min_price, max_price, min_area, max_area,
            num_bedrooms, num_toilets, created_at, updated_at
        ) VALUES (
            :user_id, :user_name, :user_type, :min_price, :max_price, :min_area, :max_area,
            :num_bedrooms, :num_toilets, :created_at, :updated_at
        ) RETURNING id
    """), subscription_data)
    
    subscription_id = result.fetchone()[0]
    
    # Insert district subscriptions
    for district_id in district_ids:
        connection.execute(text("""
            INSERT INTO district_subscription (user_id, district_id, created_at)
            VALUES (:user_id, :district_id, :created_at)
        """), {
            "user_id": subscription_data["user_id"],
            "district_id": district_id,
            "created_at": subscription_data["created_at"],
            "updated_at": subscription_data["updated_at"]
        })
    
    # Insert legal status subscriptions
    for legal_status_id in legal_status_ids:
        connection.execute(text("""
            INSERT INTO legal_subscription (user_id, legal_id, created_at)
            VALUES (:user_id, :legal_id, :created_at)
        """), {
            "user_id": subscription_data["user_id"],
            "legal_id": legal_status_id,
            "created_at": subscription_data["created_at"],
            "updated_at": subscription_data["updated_at"]
        })
    
    # Insert property type subscriptions
    for property_type_id in property_type_ids:
        connection.execute(text("""
            INSERT INTO property_type_subscription (user_id, property_type_id, created_at)
            VALUES (:user_id, :property_type_id, :created_at)
        """), {
            "user_id": subscription_data["user_id"],
            "property_type_id": property_type_id,
            "created_at": subscription_data["created_at"],
            "updated_at": subscription_data["updated_at"]
        })
    
    return subscription_id

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
        
        # Convert legal statuses to IDs
        legal_status_ids = []
        invalid_legal_statuses = []
        for legal_status in request.legal_statuses:
            legal_status_id = convert_legal_status_to_int(legal_status)
            if legal_status_id == -1:
                invalid_legal_statuses.append(legal_status)
            else:
                legal_status_ids.append(legal_status_id)
        
        if invalid_legal_statuses:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid legal statuses: {', '.join(invalid_legal_statuses)}"
            )

        # Convert property types to IDs
        property_type_ids = []
        invalid_property_types = []
        for property_type in request.property_types:
            property_type_id = convert_property_type_to_int(property_type)
            if property_type_id == -1:
                invalid_property_types.append(property_type)
            else:
                property_type_ids.append(property_type_id)
        
        if invalid_property_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid property types: {', '.join(invalid_property_types)}"
            )
        
        # Prepare subscription data
        subscription_data = {
            "user_name": request.user_name,
            "min_price": request.price_range.min_price,
            "max_price": request.price_range.max_price,
            "min_area": request.area_range.min_area,
            "max_area": request.area_range.max_area,
            "num_bedrooms": request.num_bedrooms,
            "num_toilets": request.num_toilets,
            "user_id": request.user_id,
            "user_type": request.user_type,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        # Save to database
        engine = get_db_engine()
        with engine.begin() as connection:
            subscription_id = save_subscription_with_relations(
                connection, subscription_data, district_ids, legal_status_ids, property_type_ids
            )
        
        return {
            "message": "Subscription created successfully",
            "data": {
                "subscription_id": subscription_id,
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
                "legal_status_ids": legal_status_ids,
                "property_type_ids": property_type_ids,
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
            # Delete from intermediate tables first
            connection.execute(
                text("DELETE FROM district_subscription WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            connection.execute(
                text("DELETE FROM legal_subscription WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            connection.execute(
                text("DELETE FROM property_type_subscription WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            
            # Delete from main subscription table
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
