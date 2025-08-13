from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
from sqlalchemy import text, create_engine

router = APIRouter(
    prefix="/api/districts",
    tags=["districts"]
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

class DistrictResponse(BaseModel):
    district_id: int
    district: str

@router.get("/", response_model=List[DistrictResponse])
async def get_districts():
    """
    Get all distinct districts that have real estate listings
    """
    query = """
        SELECT DISTINCT 
            dm.district_id,
            dm.district
        FROM district_mapping dm
        INNER JOIN real_estate re ON re.district_id = dm.district_id
        ORDER BY dm.district_id;
    """
    
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return [DistrictResponse(district_id=row.district_id, district=row.district) for row in result]

