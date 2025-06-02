import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'data_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Local paths configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.path.join(DATA_DIR, 'test_cleaned')

def strip_url_id(url_id):
    if isinstance(url_id, float) and url_id.is_integer():
        return str(int(url_id))
    return str(url_id).strip()

def load_district_mapping():
    """Load district mapping from file"""
    district_mapping = {}
    try:
        mapping_path = os.path.join(DATA_DIR, 'cleaned', 'district_mapping.txt')
        with open(mapping_path, 'r', encoding='utf-8') as f:
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
        batdongsan_path = os.path.join(DATA_DIR, 'output', 'batdongsan_url.tsv')
        batdongsan_df = pd.read_csv(batdongsan_path, sep='\t')
        batdongsan_df['source'] = 'batdongsan'
        # Rename columns to match schema
        batdongsan_df = batdongsan_df.rename(columns={
            'id': 'url_id',
            'updated_date': 'crawled_date'
        })
        
        # Load nhatot URLs
        nhatot_path = os.path.join(DATA_DIR, 'output', 'nhatot_url.tsv')
        nhatot_df = pd.read_csv(nhatot_path, sep='\t')
        nhatot_df['source'] = 'nhatot'
        nhatot_df['id'] = nhatot_df['id'].apply(strip_url_id)

        # Rename columns to match schema
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

        # Select only the columns we need
        columns_to_keep = ['url_id', 'url', 'source']
        batdongsan_df = batdongsan_df[columns_to_keep]
        nhatot_df = nhatot_df[columns_to_keep]
      
        logger.info(f"nhatot_df: {nhatot_df.head()}")

        # Combine both dataframes
        url_df = pd.concat([batdongsan_df, nhatot_df], ignore_index=True)
        
        url_df['url_id'] = url_df['url_id'].astype(str).str.strip().fillna('')
        url_df['url'] = url_df['url'].astype(str).str.strip().fillna('')

        # DEBUG - search rows where url_id start with 1 and not start with pr in url_df
        logger.info(f"Rows where url_id start with 1 and not start with pr in url_df: {url_df[(url_df['url_id'].str.startswith('1')) & (~url_df['url_id'].str.startswith('pr'))].shape}")
        # DEBUG - search rows where url_id start with 1 and not start with pr in url_df and url is not null
        logger.info(f"Rows where url_id start with 1 and not start with pr in url_df and url is not null: {url_df[(url_df['url_id'].str.startswith('1')) & (url_df['url'].notna()) & (~url_df['url_id'].str.startswith('pr'))].shape}")
        # DEBUG - search rows where url_id start with 1 and not start with pr in url_df and url is null
        logger.info(f"Rows where url_id start with 1 and not start with pr in url_df and url is null: {url_df[(url_df['url_id'].str.startswith('1')) & (url_df['url'].isna()) & (~url_df['url_id'].str.startswith('pr'))].shape}")
        


        return url_df
    except Exception as e:
        logger.error(f"Error loading URL mappings: {str(e)}")
        raise

def process_data():
    """Process data and save to local files"""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Read cleaned data
        logger.info("Reading cleaned data...")
        data_path = os.path.join(DATA_DIR, 'cleaned', 'visualization_data.tsv')
        df = pd.read_csv(data_path, sep='\t')

        # DEBUG - search rows where url_id start with 1%
        logger.info(f"Rows where url_id start with pr: {df[df['url_id'].str.startswith('pr')].shape}")
        logger.info(f"Rows where url_id start with 1: {df[df['url_id'].str.startswith('1')].shape}")

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
        numeric_columns = ['area', 'price', 'number_of_bedrooms', 'number_of_toilets', 'legal', 'lat', 'lon', 'property_type_id']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1)
            if col in ['number_of_bedrooms', 'number_of_toilets', 'legal', 'property_type_id']:
                df[col] = df[col].astype(int)

        # DEBUG - search rows where url_id start with 1 and print some rows
        logger.info(f"Rows where url_id start with 1: {df[df['url_id'].str.startswith('1')].shape}")
        logger.info(f"Rows where url_id start with 1: {df[df['url_id'].str.startswith('1')].head()}")

        # Load URL data and combine with real estate data
        logger.info("Loading URL data...")
        url_df = load_url_mappings()

        # Merge real estate data with URL data
        df['url_id'] = df['url_id'].astype(str).str.strip()
        url_df['url_id'] = url_df['url_id'].astype(str).str.strip()
        
        # DEBUG - search rows where url_id start with 1 in url_df and url is not null
        logger.info(f"Rows where url_id start with 1 in url_df: {url_df[url_df['url_id'].str.startswith('1')].shape}")
        logger.info(f"Rows where url_id start with 1 in url_df and url is not null: {url_df[(url_df['url_id'].str.startswith('1')) & (url_df['url'].notna())].shape}")


        not_in_url_df = ~df['url_id'].isin(url_df['url_id'])
        print(df[not_in_url_df][['url_id', 'title']].head())
        print(f"Rows in main data missing from url_df: {not_in_url_df.sum()}")

        # DEBUG - search rows where url_id start with 1
        logger.info(f"Rows where url_id start with 1 - before merge: {df[df['url_id'].str.startswith('1')].shape}")


        df = pd.merge(df, url_df[['url_id', 'source', 'url']], on='url_id', how='left')
        
        # DEBUG - search rows where url_id start with 1%
        logger.info(f"Rows where url_id start with 1 - after merge: {df[df['url_id'].str.startswith('1')].shape}")
        logger.info(f"Rows where url_id start with 1 and url is not null - after merge: {df[(df['url_id'].str.startswith('1')) & (df['url'].notna())].shape}")  
        
        # drop rows where url and source is null
        df = df.dropna(subset=['url', 'source'])

        # DEBUG - search rows where url_id start with 1
        logger.info(f"Rows where url_id start with 1 - after dropna: {df[df['url_id'].str.startswith('1')].shape}")

        # Add is_available column (default to True)
        df['is_available'] = True
        
        # Add created_at, updated_at column
        df['created_at'] = pd.Timestamp.now()
        df['updated_at'] = pd.Timestamp.now()

        

        # Remove duplicates based on url_id (primary key)
        logger.info(f"Dataframe shape before deduplication: {df.shape}")
        df = df.drop_duplicates(subset=['url_id'], keep='last')
        logger.info(f"Dataframe shape after deduplication: {df.shape}")

        # DEBUG - search rows where url_id start with 1%
        logger.info(f"Rows where url_id start with 1: {df[df['url_id'].str.startswith('1')].shape}")

        # Save processed data to files
        logger.info("Saving processed data to files...")
        
        # Save full dataset
        output_path = os.path.join(OUTPUT_DIR, 'processed_data.tsv')
        df.to_csv(output_path, sep='\t', index=False)
        logger.info(f"Full dataset saved to {output_path}")

        # Save summary statistics
        summary_stats = {
            'total_records': len(df),
            'unique_districts': df['district_name'].nunique(),
            'unique_property_types': df['property_type'].nunique(),
            'avg_price': df['price'].mean(),
            'avg_area': df['area'].mean(),
            'source_distribution': df['source'].value_counts().to_dict()
        }
        
        # Save summary to JSON
        import json
        summary_path = os.path.join(OUTPUT_DIR, 'summary_stats.json')
        with open(summary_path, 'w') as f:
            json.dump(summary_stats, f, indent=2)
        logger.info(f"Summary statistics saved to {summary_path}")

        logger.info("Data processing completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return False

if __name__ == "__main__":
    process_data() 