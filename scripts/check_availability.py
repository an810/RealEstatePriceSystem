import pandas as pd
import requests
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import time
from urllib.parse import urlparse
import concurrent.futures
from typing import List, Dict, Tuple

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

def get_headers(source: str) -> Dict[str, str]:
    """Get appropriate headers for each source"""
    common_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    if source == 'batdongsan':
        return {
            **common_headers,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
    elif source == 'nhatot':
        return {
            **common_headers,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }
    return common_headers

def check_url_availability(url: str, source: str) -> Tuple[bool, str]:
    """Check if a URL is still available"""
    try:
        headers = get_headers(source)
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            # Check for specific indicators of unavailability based on source
            if source == 'batdongsan':
                # Check for the specific span class indicating expired listing
                if '<span class="pr-expired__title">' in response.text:
                    logger.info(f"URL {url} response: {response.text}")
                    return False, 'expired'
                # Also check for other common indicators
                if 'tin đã hết hạn' in response.text.lower() or 'tin không tồn tại' in response.text.lower():
                    logger.info(f"URL {url} response: {response.text}")
                    return False, 'expired'
            elif source == 'nhatot':
                if 'tin đã hết hạn' in response.text.lower() or 'tin không tồn tại' in response.text.lower():
                    logger.info(f"URL {url} response: {response.text}")
                    return False, 'expired'
                if '<div class="NotFound_content_KtIbC">' in response.text:
                    logger.info(f"URL {url} response: {response.text}")
                    return False, 'not_found'
            return True, 'available'
        elif response.status_code == 404:
            logger.info(f"URL {url} response: {response.text}")
            return False, 'not_found'
        else:
            return False, f'error_{response.status_code}'
    except requests.RequestException as e:
        return False, f'error_{str(e)}'

def process_batch(urls: List[Dict]) -> List[Dict]:
    """Process a batch of URLs and return their availability status"""
    results = []
    for item in urls:
        url = item['url']
        source = item['source']
        url_id = item['url_id']
        
        is_available, status = check_url_availability(url, source)
        
        results.append({
            'url_id': url_id,
            'is_available': is_available,
            'status': status,
            'updated_at': datetime.now()
        })
        
        # Add a small delay between requests to avoid overwhelming the servers
        time.sleep(1)
    
    return results

def update_database(engine, results: List[Dict]):
    """Update the database with availability results"""
    try:
        with engine.begin() as conn:
            for result in results:
                conn.execute(
                    text("""
                        UPDATE real_estate 
                        SET is_available = :is_available,
                            updated_at = :updated_at
                        WHERE url_id = :url_id
                    """),
                    {
                        'url_id': result['url_id'],
                        'is_available': result['is_available'],
                        'updated_at': result['updated_at']
                    }
                )
    except Exception as e:
        logger.error(f"Error updating database: {str(e)}")
        raise

def check_availability():
    """Main function to check availability of all real estate listings"""
    try:
        engine = get_db_engine()
        
        # Get all URLs from database
        with engine.connect() as conn:
            query = """
                SELECT url_id, url, source 
                FROM real_estate 
                WHERE is_available = TRUE
            """
            result = conn.execute(text(query))
            urls = [dict(row) for row in result]
        
        logger.info(f"Found {len(urls)} active listings to check")
        
        # Process URLs in batches
        batch_size = 100
        all_results = []
        
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            batch_results = process_batch(batch)
            all_results.extend(batch_results)
            
            # Update database after each batch
            update_database(engine, batch_results)
            
            logger.info(f"Processed batch {i//batch_size + 1}/{(len(urls) + batch_size - 1)//batch_size}")
        
        # Log summary
        available_count = sum(1 for r in all_results if r['is_available'])
        unavailable_count = len(all_results) - available_count
        
        logger.info(f"Scan completed. Results:")
        logger.info(f"Total checked: {len(all_results)}")
        logger.info(f"Still available: {available_count}")
        logger.info(f"No longer available: {unavailable_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking availability: {str(e)}")
        return False

if __name__ == "__main__":
    check_availability() 