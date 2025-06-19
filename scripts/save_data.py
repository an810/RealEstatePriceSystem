import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table
import logging
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def strip_url_id(url_id):
    if isinstance(url_id, float) and url_id.is_integer():
        return str(int(url_id))
    return str(url_id).strip()

def load_url_mappings():
    """Load URL mappings from both sources"""
    logger.info("Loading URL mappings...")
    try:
        # Load batdongsan URLs
        batdongsan_df = pd.read_csv('/opt/airflow/data/crawled/batdongsan_url.tsv', sep='\t')
        batdongsan_df['source'] = 'batdongsan'
        # Rename columns to match database schema
        batdongsan_df = batdongsan_df.rename(columns={
            'id': 'url_id',
            'updated_date': 'crawled_date'
        })
        
        # Load nhatot URLs
        nhatot_df = pd.read_csv('/opt/airflow/data/crawled/nhatot_url.tsv', sep='\t')
        nhatot_df['source'] = 'nhatot'
        nhatot_df['id'] = nhatot_df['id'].apply(strip_url_id)
        # Rename columns to match database schema
        nhatot_df = nhatot_df.rename(columns={
            'id': 'url_id',
            'updated_date': 'crawled_date'
        })

        logger.info(f"Batdongsan URL shape: {batdongsan_df.shape}")
        logger.info(f"Nhatot URL shape: {nhatot_df.shape}")

        # Remove duplicate rows and keep the last one
        batdongsan_df = batdongsan_df.drop_duplicates(subset=['url_id'], keep='last')
        nhatot_df = nhatot_df.drop_duplicates(subset=['url_id'], keep='last')
        
        logger.info(f"Batdongsan URL shape after deduplication: {batdongsan_df.shape}")
        logger.info(f"Nhatot URL shape after deduplication: {nhatot_df.shape}")

        # Select only the columns we need for the database
        columns_to_keep = ['url_id', 'url', 'source']
        batdongsan_df = batdongsan_df[columns_to_keep]
        nhatot_df = nhatot_df[columns_to_keep]
        
        # Combine both dataframes
        url_df = pd.concat([batdongsan_df, nhatot_df], ignore_index=True)
        
        return url_df
    except Exception as e:
        logger.error(f"Error loading URL mappings: {str(e)}")
        raise

def create_tables(engine):
    """Create tables if they don't exist"""
    try:
        with engine.begin() as conn:
            # Create real_estate table with updated schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS real_estate (
                    url_id VARCHAR(30),
                    title VARCHAR(255),
                    area FLOAT,
                    price FLOAT,
                    number_of_bedrooms INTEGER,
                    number_of_toilets INTEGER,
                    legal_id INTEGER,
                    property_type_id INTEGER,
                    district_id INTEGER,
                    province VARCHAR(50),
                    is_available BOOLEAN DEFAULT TRUE,
                    lat FLOAT,
                    lon FLOAT,
                    source VARCHAR(50),
                    url VARCHAR(512),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT real_estate_pkey PRIMARY KEY (url_id)
                )
            """))
            
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def create_indexes(engine):
    """Create indexes for better query performance"""
    try:
        with engine.begin() as conn:
            # Indexes for real_estate table
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_price ON real_estate(price);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_district_id ON real_estate(district_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_area ON real_estate(area);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_source ON real_estate(source);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_is_available ON real_estate(is_available);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_property_type_id ON real_estate(property_type_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_legal_id ON real_estate(legal_id);"))
            
        logger.info("Indexes created successfully")
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")
        raise

def save_to_database():
    """Load cleaned data and save to PostgreSQL database"""
    try:
        # Database connection parameters
        db_params = {
            'dbname': 'real_estate',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'real_estate_db',
            'port': '5432'
        }

        # Create SQLAlchemy engine
        engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
        logger.info(f"SQLAlchemy version: {sqlalchemy.__version__}")
        
        # Create tables if they don't exist
        logger.info("Creating tables if they don't exist...")
        create_tables(engine)

        # Read cleaned data
        logger.info("Reading cleaned data...")
        df = pd.read_csv('/opt/airflow/data/cleaned/visualization_data.tsv', sep='\t')

        logger.info(f"Dataframe shape before deduplication: {df.shape}")

        # Rename legal column to legal_id to match new schema
        if 'legal' in df.columns:
            df = df.rename(columns={'legal': 'legal_id'})
        
        # Ensure all numeric columns are properly typed
        numeric_columns = ['area', 'price', 'number_of_bedrooms', 'number_of_toilets', 'legal_id', 'lat', 'lon', 'property_type_id', 'district_id']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)
                if col in ['number_of_bedrooms', 'number_of_toilets', 'legal_id', 'property_type_id', 'district_id']:
                    df[col] = df[col].astype(int)

        # Load URL data and combine with real estate data
        logger.info("Loading URL data...")
        url_df = load_url_mappings()
        
        # Merge real estate data with URL data
        df['url_id'] = df['url_id'].astype(str).str.strip()
        url_df['url_id'] = url_df['url_id'].astype(str).str.strip()
        
        not_in_url_df = ~df['url_id'].isin(url_df['url_id'])
        print(df[not_in_url_df][['url_id', 'title']].head())
        print(f"Rows in main data missing from url_df: {not_in_url_df.sum()}")

        df = pd.merge(df, url_df[['url_id', 'source', 'url']], on='url_id', how='left')

        # drop rows where url and source is null
        df = df.dropna(subset=['url', 'source'])

        # Add is_available column (default to True)
        df['is_available'] = True
        
        # Add created_at, updated_at column
        df['created_at'] = pd.Timestamp.now()
        df['updated_at'] = pd.Timestamp.now()

        # Remove duplicates based on url_id (primary key)
        logger.info(f"Dataframe shape before deduplication: {df.shape}")
        df = df.drop_duplicates(subset=['url_id'], keep='last')
        logger.info(f"Dataframe shape after deduplication: {df.shape}")

        # Define the columns that match the database schema
        db_columns = [
            'url_id', 'title', 'area', 'price', 'number_of_bedrooms', 'number_of_toilets',
            'legal_id', 'property_type_id', 'district_id', 'province', 'is_available',
            'lat', 'lon', 'source', 'url', 'created_at', 'updated_at'
        ]
        
        # Filter DataFrame to only include columns that exist in the database schema
        available_columns = [col for col in db_columns if col in df.columns]
        missing_columns = [col for col in db_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns in DataFrame: {missing_columns}")
        
        # Select only the columns that match the database schema
        df = df[available_columns]
        logger.info(f"Final DataFrame columns: {list(df.columns)}")
        logger.info(f"Final DataFrame shape: {df.shape}")

        # Save real estate data to database
        logger.info("Saving real estate data to database ...")
        try:
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Create metadata and table objects
            metadata = MetaData()
            real_estate_table = Table('real_estate', metadata, autoload_with=engine)
            
            # Insert data in chunks with upsert
            chunk_size = 1000
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                with engine.begin() as conn:
                    # Create upsert statement
                    stmt = insert(real_estate_table).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['url_id'],
                        set_={
                            'title': stmt.excluded.title,
                            'area': stmt.excluded.area,
                            'price': stmt.excluded.price,
                            'number_of_bedrooms': stmt.excluded.number_of_bedrooms,
                            'number_of_toilets': stmt.excluded.number_of_toilets,
                            'legal_id': stmt.excluded.legal_id,
                            'lat': stmt.excluded.lat,
                            'lon': stmt.excluded.lon,
                            'district_id': stmt.excluded.district_id,
                            'province': stmt.excluded.province,
                            'property_type_id': stmt.excluded.property_type_id,
                            'source': stmt.excluded.source,
                            'is_available': stmt.excluded.is_available,
                            'created_at': stmt.excluded.created_at,
                            'updated_at': stmt.excluded.updated_at
                        }
                    )
                    conn.execute(stmt)
                logger.info(f"Processed {i + len(chunk)} records")
                
        except Exception as e:
            logger.error(f"Error saving real estate data to database: {str(e)}")
            raise

        # Create indexes
        logger.info("Creating indexes...")
        create_indexes(engine)

        logger.info("Data successfully saved to database!")
        return True

    except Exception as e:
        logger.error(f"Error saving data to database: {str(e)}")
        return False

if __name__ == "__main__":
    save_to_database()
