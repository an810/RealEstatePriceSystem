import pandas as pd
import logging
import os
from batdongsan_processor import BatDongSanProcessor
from nhatot_processor import NhatotProcessor
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

def load_district_mapping():
    """Load district mapping from database"""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = """
                SELECT district_id, district 
                FROM district_mapping 
                ORDER BY district_id
            """
            result = conn.execute(text(query))
            district_mapping = {row.district: row.district_id for row in result}
            logger.info(f"Loaded {len(district_mapping)} district mappings from database")
            return district_mapping
    except Exception as e:
        logger.error(f"Error loading district mapping from database: {str(e)}")
        raise

def infer_property_type(title):
    if pd.isna(title):
        return "Khác", 0
    title = str(title).lower()
    if "chung cư" in title or "căn hộ" in title:
        return "Chung cư", 1
    if "biệt thự" in title or "liền kề" in title:
        return "Biệt thự", 2
    if "nhà riêng" in title or "nhà mặt phố" in title or "nhà" in title:
        return "Nhà riêng", 3
    if "đất" in title:
        return "Đất", 4
    return "Khác", 0

def process_and_clean_data():
    """Process data from both sources and then clean it"""
    try:
        # Process BatDongSan data
        logger.info("Processing BatDongSan data...")
        batdongsan_processor = BatDongSanProcessor()
        if not batdongsan_processor.process_data():
            raise Exception("Failed to process BatDongSan data")

        # Process Nhatot data
        logger.info("Processing Nhatot data...")
        nhatot_processor = NhatotProcessor()
        if not nhatot_processor.process_data():
            raise Exception("Failed to process Nhatot data")

        # Read processed data from both sources
        logger.info("Reading processed data from both sources...")
        df1 = pd.read_csv('/opt/airflow/data/output/processed_batdongsan.tsv', sep='\t')
        df2 = pd.read_csv('/opt/airflow/data/output/processed_nhatot.tsv', sep='\t')

        # Merge data
        logger.info("Merging data from both sources...")
        result_df = pd.concat([df1, df2], ignore_index=True)

        # Save merged data
        logger.info("Saving merged data...")
        result_df.to_csv('/opt/airflow/data/output/merged_file.tsv', sep='\t', index=False)

        # Read merged data for filtering
        logger.info("Reading merged data for filtering...")
        df = pd.read_csv('/opt/airflow/data/output/merged_file.tsv', sep='\t')

        # Remove unnecessary columns
        logger.info("Removing unnecessary columns...")
        df.drop(['direction', 'furniture', 'width', 'address'], axis=1, inplace=True)

        # Filter out invalid data
        logger.info("Filtering out invalid data...")
        df = df[(df['number_of_bedrooms'] != 0) & (df['number_of_toilets'] != 0)]
        df.dropna(subset=['lat', 'lon'], how='any', inplace=True)
        df = df[(df['price'] != 0) & (df['price'].notna()) & (df['area'] != 0)]

        # Remove outliers using percentile method
        logger.info("Removing outliers...")
        upper_limit = df['area'].quantile(0.95)
        df = df[df['area'] <= upper_limit]

        upper_limit = df['price'].quantile(0.95)
        df = df[df['price'] <= upper_limit]

        upper_limit = df['number_of_bedrooms'].quantile(0.95)
        df = df[df['number_of_bedrooms'] <= upper_limit]

        upper_limit = df['number_of_toilets'].quantile(0.95)
        df = df[df['number_of_toilets'] <= upper_limit]

        # Load district mapping from database
        logger.info("Loading district mapping from database...")
        district_mapping = load_district_mapping()

        # Add property_type column using the infer_property_type function
        logger.info("Inferring property type...")
        df[['property_type', 'property_type_id']] = df['title'].apply(infer_property_type).apply(pd.Series)

        # Map district names to IDs from database
        logger.info("Mapping district names to IDs from database...")
        df['district_id'] = df['district'].map(district_mapping)
        
        # Handle special case: map both "Nam Từ Liêm" and "Bắc Từ Liêm" to "Từ Liêm"
        logger.info("Handling special district mappings...")
        tu_liem_id = district_mapping.get('Từ Liêm')
        if tu_liem_id is not None:
            df.loc[df['district'].isin(['Nam Từ Liêm', 'Bắc Từ Liêm']), 'district_id'] = tu_liem_id
            logger.info(f"Mapped 'Nam Từ Liêm' and 'Bắc Từ Liêm' to 'Từ Liêm' (ID: {tu_liem_id})")
        
        # Log unmapped districts for debugging
        unmapped_districts = df[df['district_id'].isna()]['district'].unique()
        if len(unmapped_districts) > 0:
            logger.warning(f"Found {len(unmapped_districts)} unmapped districts: {unmapped_districts}")
        
        # Keep original district name and drop unmapped rows
        df = df.dropna(subset=['district_id'])
        
        # Convert district_id to integer type
        logger.info("Converting district_id to integer type...")
        df['district_id'] = df['district_id'].astype(int)
        
        # Save processed data for visualization
        logger.info("Saving visualization data...")
        df.to_csv('/opt/airflow/data/cleaned/visualization_data.tsv', sep='\t', index=False)
        
        # Save processed data
        logger.info("Saving processed data...")
        df.to_csv('/opt/airflow/data/cleaned/processed_data.tsv', sep='\t', index=False)
        
        logger.info("Data processing and cleaning completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in process_and_clean_data: {str(e)}")
        return False

def clean_data():
    """Function to be called from the DAG"""
    return process_and_clean_data()

if __name__ == "__main__":
    clean_data()
