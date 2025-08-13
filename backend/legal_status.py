from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
from sqlalchemy import text, create_engine

router = APIRouter(
    prefix="/api/legal-status",
    tags=["legal_status"]
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

class LegalStatusResponse(BaseModel):
    legal_id: int
    legal: str

@router.get("/", response_model=List[LegalStatusResponse])
async def get_legal_status():
    """
    Get all distinct legal statuses that have real estate listings
    """
    query = """
        SELECT DISTINCT 
            lm.legal_id,
            lm.legal
        FROM legal_mapping lm
        INNER JOIN real_estate re ON re.legal_id = lm.legal_id
        ORDER BY lm.legal_id;
    """
    
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return [LegalStatusResponse(legal_id=row.legal_id, legal=row.legal) for row in result]

