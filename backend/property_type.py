from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
from sqlalchemy import text, create_engine

router = APIRouter(
    prefix="/api/property-type",
    tags=["property_type"]
)

# Database config (same as search.py)
DB_CONFIG = {
    'dbname': 'real_estate',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'real_estate_db',
    'port': '5432'
}

def get_db_engine():
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

class PropertyTypeResponse(BaseModel):
    property_type_id: int
    property_type: str

@router.get("/", response_model=List[PropertyTypeResponse])
async def get_property_type():
    """
    Get all distinct property types that have real estate listings
    """
    query = """
        SELECT DISTINCT 
            pm.property_type_id,
            pm.property_type
        FROM property_type_mapping pm
        INNER JOIN real_estate re ON re.property_type_id = pm.property_type_id
        ORDER BY pm.property_type_id;
    """
    
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return [PropertyTypeResponse(property_type_id=row.property_type_id, property_type=row.property_type) for row in result]

