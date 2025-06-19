import logging
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_engine():
    """Get database engine instance"""
    db_params = {
        'dbname': 'real_estate',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'real_estate_db',
        'port': '5432'
    }
    return create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

def update_district_mapping():
    """Update district mapping in database"""
    try:
        engine = get_db_engine()
        
        with engine.begin() as conn:
            # Ensure Từ Liêm exists with correct ID
            logger.info("Ensuring Từ Liêm exists with correct ID...")
            conn.execute(text("""
                INSERT INTO district_mapping (district_id, district) VALUES (28, 'Từ Liêm')
                ON CONFLICT (district_id) DO UPDATE SET 
                    district = EXCLUDED.district,
                    updated_at = CURRENT_TIMESTAMP
            """))
            
            # Remove any separate Bắc Từ Liêm or Nam Từ Liêm entries if they exist
            logger.info("Removing separate Bắc Từ Liêm and Nam Từ Liêm entries...")
            conn.execute(text("DELETE FROM district_mapping WHERE district IN ('Bắc Từ Liêm', 'Nam Từ Liêm')"))
            
            # Verify the changes
            logger.info("Verifying changes...")
            result = conn.execute(text("SELECT district_id, district FROM district_mapping ORDER BY district_id"))
            districts = [(row.district_id, row.district) for row in result]
            
            logger.info("Updated district mapping:")
            for district_id, district in districts:
                logger.info(f"  {district_id}: {district}")
                
        logger.info("District mapping updated successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error updating district mapping: {str(e)}")
        return False

if __name__ == "__main__":
    update_district_mapping() 