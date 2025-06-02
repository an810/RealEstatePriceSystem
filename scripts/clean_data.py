import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def clean_data():
    """Merge and filter data from both sources before model training"""
    try:
        # Read data from both sources
        logger.info("Reading data from BatDongSan and Nhatot...")
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
        upper_limit = df['price'].quantile(0.90)
        df = df[df['price'] <= upper_limit]

        upper_limit = df['number_of_bedrooms'].quantile(0.95)
        df = df[df['number_of_bedrooms'] <= upper_limit]

        upper_limit = df['number_of_toilets'].quantile(0.95)
        df = df[df['number_of_toilets'] <= upper_limit]

        # Create and save district mapping
        logger.info("Creating district mapping...")
        district_mapping = {district: i for i, district in enumerate(df['district'].unique())}
        
        # Ensure output directory exists
        os.makedirs('/opt/airflow/data/cleaned', exist_ok=True)
        
        with open('/opt/airflow/data/cleaned/district_mapping.txt', 'w', encoding='utf-8') as f:
            for district, index in district_mapping.items():
                f.write(f"{index}: {district}\n")

        # Add property_type column using the infer_property_type function
        logger.info("Inferring property type...")
        df[['property_type', 'property_type_id']] = df['title'].apply(infer_property_type).apply(pd.Series)

        # Save processed data for visualization (with property_type)
        logger.info("Saving visualization data...")
        df.to_csv('/opt/airflow/data/cleaned/visualization_data.tsv', sep='\t', index=False)
        
        # Map district names to indices
        logger.info("Mapping district names to indices...")
        df['district'] = df['district'].map(district_mapping)

        # Save processed data (with property_type)
        logger.info("Saving processed data...")
        df.to_csv('/opt/airflow/data/cleaned/processed_data.tsv', sep='\t', index=False)
        
        logger.info("Data preparation completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in prepare_data: {str(e)}")
        return False

if __name__ == "__main__":
    clean_data()
