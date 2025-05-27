import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table
import logging
import sqlalchemy
from sqlalchemy.dialects.postgresql import insert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_district_mapping():
    """Load district mapping from file"""
    district_mapping = {}
    try:
        with open('/opt/airflow/data/cleaned/district_mapping.txt', 'r', encoding='utf-8') as f:
            for line in f:
                if ':' in line:
                    district_id, district_name = line.strip().split(': ')
                    district_mapping[district_name.strip()] = int(district_id)
        return district_mapping
    except Exception as e:
        logger.error(f"Error loading district mapping: {str(e)}")
        raise

def load_url_mappings():
    """Load URL mappings from both sources"""
    try:
        # Load batdongsan URLs
        batdongsan_df = pd.read_csv('/opt/airflow/data/output/batdongsan_url.tsv', sep='\t')
        batdongsan_df['source'] = 'batdongsan'
        # Rename columns to match database schema
        batdongsan_df = batdongsan_df.rename(columns={
            'id': 'url_id',
            'updated_date': 'crawled_date'
        })
        
        # Load nhatot URLs
        nhatot_df = pd.read_csv('/opt/airflow/data/output/nhatot_url.tsv', sep='\t')
        nhatot_df['source'] = 'nhatot'
        # Rename columns to match database schema
        nhatot_df = nhatot_df.rename(columns={
            'id': 'url_id',
            'updated_date': 'crawled_date'
        })
        
        # Select only the columns we need for the database
        columns_to_keep = ['url_id', 'url', 'source', 'crawled_date']
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
            # Create real_estate table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS real_estate (
                    id SERIAL,
                    url_id VARCHAR(30) PRIMARY KEY,
                    title VARCHAR(255),
                    area FLOAT,
                    price FLOAT,
                    number_of_bedrooms INTEGER,
                    number_of_toilets INTEGER,
                    legal INTEGER,
                    lat FLOAT,
                    lon FLOAT,
                    district_id INTEGER,
                    district_name VARCHAR(50),
                    province VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create url table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS url (
                    id SERIAL ,
                    url_id VARCHAR(30) PRIMARY KEY,
                    url VARCHAR(512),
                    source VARCHAR(50),
                    crawled_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_district_name ON real_estate(district_name);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_area ON real_estate(area);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_url_id ON real_estate(url_id);"))
            
            # Indexes for url table
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_url_id ON url(url_id);"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_url_source ON url(source);"))
            
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

        # Load district mapping
        logger.info("Loading district mapping...")
        district_mapping = load_district_mapping()

        # Convert district names to IDs while keeping the original district name
        logger.info("Converting district names to IDs...")
        df['district_id'] = df['district'].map(district_mapping)
        
        # Handle NaN values in district_id
        df['district_id'] = df['district_id'].fillna(-1).astype(int)
        
        # Rename the original district column to district_name
        df = df.rename(columns={'district': 'district_name'})
        
        # Ensure all numeric columns are properly typed
        numeric_columns = ['area', 'price', 'number_of_bedrooms', 'number_of_toilets', 'legal', 'lat', 'lon']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)
            if col in ['number_of_bedrooms', 'number_of_toilets', 'legal']:
                df[col] = df[col].astype(int)

        # Remove duplicates based on url_id (primary key)
        df = df.drop_duplicates(subset=['url_id'], keep='last')
        logger.info(f"Dataframe shape after deduplication: {df.shape}")

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
                            'legal': stmt.excluded.legal,
                            'lat': stmt.excluded.lat,
                            'lon': stmt.excluded.lon,
                            'district_id': stmt.excluded.district_id,
                            'district_name': stmt.excluded.district_name,
                            'province': stmt.excluded.province
                        }
                    )
                    conn.execute(stmt)
                logger.info(f"Processed {i + len(chunk)} records")
                
        except Exception as e:
            logger.error(f"Error saving real estate data to database: {str(e)}")
            raise

        # Load and save URL mappings
        logger.info("Loading URL mappings...")
        url_df = load_url_mappings()

        # Filter URL data to only include URLs that exist in real estate data
        logger.info(f"URL dataframe shape before filtering: {url_df.shape}")
        url_df = url_df[url_df['url_id'].isin(df['url_id'])]
        logger.info(f"URL dataframe shape after filtering: {url_df.shape}")

        # Remove duplicates from URL data
        url_df = url_df.drop_duplicates(subset=['url_id'], keep='last')
        logger.info(f"URL dataframe shape after deduplication: {url_df.shape}")

        # Save URL data to database
        logger.info("Saving URL data to database...")
        try:
            # Convert DataFrame to list of dictionaries
            url_records = url_df.to_dict('records')
            
            # Create metadata and table objects
            metadata = MetaData()
            url_table = Table('url', metadata, autoload_with=engine)
            
            # Insert data in chunks with upsert
            chunk_size = 1000
            for i in range(0, len(url_records), chunk_size):
                chunk = url_records[i:i + chunk_size]
                with engine.begin() as conn:
                    # Create upsert statement
                    stmt = insert(url_table).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['url_id'],
                        set_={
                            'url': stmt.excluded.url,
                            'source': stmt.excluded.source,
                            'crawled_date': stmt.excluded.crawled_date
                        }
                    )
                    conn.execute(stmt)
                logger.info(f"Processed {i + len(chunk)} URL records")
                
        except Exception as e:
            logger.error(f"Error saving URL data to database: {str(e)}")
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
