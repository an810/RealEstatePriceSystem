import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from sqlalchemy import create_engine, text
from typing import List, Dict
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import sys

# Database connection
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("Error: DATABASE_URL environment variable is not set")
    sys.exit(1)

try:
    engine = create_engine(DB_URL)
except Exception as e:
    print(f"Error creating database engine: {str(e)}")
    sys.exit(1)

# Email configuration
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")

# Telegram configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

def get_subscriptions():
    """Get all active subscriptions from the database"""
    with engine.connect() as connection:
        query = """
        SELECT * FROM subscription
        """
        result = connection.execute(text(query))
        return result.fetchall()

def get_properties_by_district(district_id: int):
    """Get properties for a specific district with their URLs"""
    with engine.connect() as connection:
        query = """
        SELECT 
            r.id, r.price, r.area, r.number_of_bedrooms, r.number_of_toilets,
            r.legal, r.district_id, r.title, r.url_id,
            u.url
        FROM real_estate r
        LEFT JOIN url u ON r.url_id = u.url_id
        WHERE r.district_id = :district_id
        """
        result = connection.execute(text(query), {"district_id": district_id})
        return result.fetchall()

def prepare_features(properties, search_params):
    """Prepare features for KNN model"""
    features = []
    for prop in properties:
        features.append([
            prop.price,
            prop.area,
            prop.number_of_bedrooms,
            prop.number_of_toilets,
            prop.legal,
            prop.district_id
        ])
    
    # Add search parameters as a query point
    query_point = [
        search_params['price_range'],
        search_params['area_range'],
        search_params['num_bedrooms'],
        search_params['num_toilets'],
        search_params['legal_status_id'],
        search_params['district_id']  # Use the current district
    ]
    
    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    scaled_query = scaler.transform([query_point])
    
    return scaled_features, scaled_query, properties

def find_matching_properties_for_district(district_id: int, search_params: Dict):
    """Find matching properties for a specific district"""
    # Get properties for the district
    properties = get_properties_by_district(district_id)
    
    if not properties:
        return []
    
    # Update search params with current district
    search_params['district_id'] = district_id
    
    # Prepare features and find matches
    scaled_features, scaled_query, properties = prepare_features(properties, search_params)
    
    # Initialize and fit KNN model
    n_neighbors = min(3, len(scaled_features))
    knn = NearestNeighbors(n_neighbors=n_neighbors, metric='cosine')
    knn.fit(scaled_features)
    
    # Find nearest neighbors
    distances, indices = knn.kneighbors(scaled_query)
    
    # Prepare results
    matches = []
    for idx, distance in zip(indices[0], distances[0]):
        prop = properties[idx]
        matches.append({
            'id': prop.id,
            'title': prop.title,
            'url': prop.url,
            'price': prop.price,
            'area': prop.area,
            'number_of_bedrooms': prop.number_of_bedrooms,
            'number_of_toilets': prop.number_of_toilets,
            'legal_status': prop.legal,
            'district_id': prop.district_id,
            'similarity_score': 1 - distance
        })
    
    return matches

def get_district_name(district_id: int) -> str:
    """Get district name from district ID"""
    with engine.connect() as connection:
        query = """
        SELECT name FROM district WHERE id = :district_id
        """
        result = connection.execute(text(query), {"district_id": district_id})
        row = result.fetchone()
        return row.name if row else f"District {district_id}"

def batch_send_email_notifications(email_properties: Dict[str, Dict[int, List[Dict]]]):
    """Send email notifications in batch"""
    if not email_properties:
        return
    
    # Create SMTP connection once
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        
        for email, district_matches in email_properties.items():
            msg = MIMEMultipart()
            msg['From'] = EMAIL_FROM
            msg['To'] = email
            msg['Subject'] = "New Real Estate Matches Found"

            body = "We found some properties that match your subscription criteria:\n\n"
            
            for district_id, properties in district_matches.items():
                district_name = get_district_name(district_id)
                body += f"\n=== {district_name} ===\n\n"
                
                for prop in properties:
                    body += f"Title: {prop['title']}\n"
                    body += f"Price: {prop['price']:,.0f} VND\n"
                    body += f"Area: {prop['area']} m²\n"
                    body += f"Bedrooms: {prop['number_of_bedrooms']}\n"
                    body += f"Toilets: {prop['number_of_toilets']}\n"
                    body += f"Legal Status: {prop['legal_status']}\n"
                    body += f"URL: {prop['url']}\n\n"

            msg.attach(MIMEText(body, 'plain'))
            server.send_message(msg)

def batch_send_telegram_notifications(telegram_properties: Dict[str, Dict[int, List[Dict]]]):
    """Send Telegram notifications in batch"""
    if not telegram_properties:
        return
    
    for chat_id, district_matches in telegram_properties.items():
        message = "We found some properties that match your subscription criteria:\n\n"
        
        for district_id, properties in district_matches.items():
            district_name = get_district_name(district_id)
            message += f"\n=== {district_name} ===\n\n"
            
            for prop in properties:
                message += f"*{prop['title']}*\n"
                message += f"Price: {prop['price']:,.0f} VND\n"
                message += f"Area: {prop['area']} m²\n"
                message += f"Bedrooms: {prop['number_of_bedrooms']}\n"
                message += f"Toilets: {prop['number_of_toilets']}\n"
                message += f"Legal Status: {prop['legal_status']}\n"
                message += f"[View Property]({prop['url']})\n\n"

        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        
        requests.post(TELEGRAM_API_URL, json=payload)

def process_subscriptions():
    """Main function to process all subscriptions and send notifications"""
    subscriptions = get_subscriptions()
    
    # Group properties by notification type and district
    email_properties = {}  # Dict[email, Dict[district_id, List[properties]]]
    telegram_properties = {}  # Dict[chat_id, Dict[district_id, List[properties]]]
    
    for sub in subscriptions:
        # Parse district IDs from comma-separated string
        district_ids = [int(did.strip()) for did in sub.district_ids.split(',')]
        
        # Prepare search parameters
        search_params = {
            'price_range': (sub.min_price + sub.max_price) / 2,
            'area_range': (sub.min_area + sub.max_area) / 2,
            'num_bedrooms': sub.num_bedrooms,
            'num_toilets': sub.num_toilets,
            'legal_status_id': sub.legal_status_id
        }
        
        # Find matches for each district
        district_matches = {}
        for district_id in district_ids:
            matches = find_matching_properties_for_district(district_id, search_params)
            if matches:
                district_matches[district_id] = matches
        
        if not district_matches:
            continue
        
        # Group by notification type
        if sub.user_type == 'email':
            email_properties[sub.user_id] = district_matches
        elif sub.user_type == 'telegram':
            telegram_properties[sub.user_id] = district_matches
    
    # Send notifications in batch
    batch_send_email_notifications(email_properties)
    batch_send_telegram_notifications(telegram_properties)

if __name__ == "__main__":
    process_subscriptions()
