import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

# Database connection
DB_URL = "postgresql://postgres:postgres@real_estate_db:5432/real_estate"
if not DB_URL:
    print("Error: DATABASE_URL environment variable is not set")
    sys.exit(1)

try:
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
    engine = create_engine(DB_URL)
except Exception as e:
    print(f"Error creating database engine: {str(e)}")
    sys.exit(1)

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "anmd.clone@gmail.com"
SMTP_PASSWORD = "plhl bcdx ytle wiec"
EMAIL_FROM = "anmd.clone@gmail.com"

# Telegram configuration
TELEGRAM_BOT_TOKEN = "8135521232:AAH6i6cIc0LzGLtp_tgXfMmnDH5HV-MDTUc"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# Property type mapping
PROPERTY_TYPE_MAPPING = {
    'Chung cư': 1,
    'Biệt thự': 2,
    'Nhà riêng': 3,
    'Đất': 4
}

def convert_property_type_to_int(value: str) -> int:
    """Convert property type string to integer code"""
    return PROPERTY_TYPE_MAPPING.get(value, -1)

def convert_int_to_property_type(value: int) -> str:
    """Convert property type integer code to text format"""
    for prop_type, prop_id in PROPERTY_TYPE_MAPPING.items():
        if prop_id == value:
            return prop_type
    return "Không xác định"

def convert_phaply_to_int(value: str) -> int:
    """Convert legal status string to integer code"""
    if any(substring in str(value) for substring in ['chưa', 'Chưa', 'đang', 'Đang', 'chờ', 'Chờ', 'làm sổ']):
        return 0
    elif any(substring in str(value) for substring in ['Hợp đồng', 'hợp đồng', 'HĐMB', 'HDMB']):
        return 1
    elif any(substring in str(value) for substring in ['sổ đỏ', 'Sổ đỏ', 'SỔ ĐỎ', 'Có sổ', 'Sổ hồng', 'sổ hồng', 'SỔ HỒNG', 'Đã có', 'đã có', 'sẵn sổ', 'Sẵn sổ', 'sổ đẹp', 'Sổ đẹp', 'đầy đủ', 'Đầy đủ', 'rõ ràng', 'Rõ ràng', 'chính chủ', 'Chính chủ', 'sẵn sàng', 'Sẵn sàng']):
        return 2
    else:
        return -1

def convert_int_to_phaply(value: int) -> str:
    """Convert legal status integer code to text format"""
    if value == 0:
        return "Chưa có sổ"
    elif value == 1:
        return "Hợp đồng mua bán"
    elif value == 2:
        return "Đã có sổ"
    else:
        return "Không xác định"

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
            r.url_id, r.price, r.area, r.number_of_bedrooms, r.number_of_toilets,
            r.legal, r.district_id, r.title, r.property_type_id,
            r.source, r.url
        FROM real_estate r
        WHERE r.district_id = :district_id AND r.is_available = TRUE
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
            prop.district_id,
            prop.property_type_id
        ])
    
    # Add search parameters as a query point
    query_point = [
        search_params['price_range'],
        search_params['area_range'],
        search_params['num_bedrooms'],
        search_params['num_toilets'],
        search_params['legal_status_id'],
        search_params['district_id'],
        search_params['property_type_id']
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
            'url_id': prop.url_id,
            'title': prop.title,
            'url': prop.url,
            'source': prop.source,
            'price': prop.price,
            'area': prop.area,
            'number_of_bedrooms': prop.number_of_bedrooms,
            'number_of_toilets': prop.number_of_toilets,
            'legal_status': convert_int_to_phaply(prop.legal),
            'property_type': convert_int_to_property_type(prop.property_type_id),
            'district_id': prop.district_id,
            'similarity_score': 1 - distance
        })
    
    return matches

def get_district_name(district_id: int) -> str:
    """Get district name from district ID using the mapping file"""
    mapping_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'cleaned', 'district_mapping.txt')
    try:
        with open(mapping_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    id_str, name = line.strip().split(': ')
                    if int(id_str) == district_id:
                        return name
        return f"District {district_id}"
    except Exception as e:
        print(f"Error reading district mapping file: {str(e)}")
        return f"District {district_id}"

def batch_send_email_notifications(email_properties: Dict[str, Dict[int, List[Dict]]], email_requirements: Dict[str, Dict]):
    """Send email notifications in batch"""
    if not email_properties:
        return
    
    # Create SMTP connection once
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        
        for email, district_matches in email_properties.items():
            search_params = email_requirements[email]
            msg = MIMEMultipart()
            msg['From'] = EMAIL_FROM
            msg['To'] = email
            msg['Subject'] = "New Real Estate Matches Found"

            # HTML email content
            html = f"""
            <html>
            <head>
            <style>
            body {{
                font-family: Arial, sans-serif;
                background: #f7f7f7;
                color: #333;
                margin: 0;
                padding: 0;
            }}
            .container {{
                background: #fff;
                margin: 20px auto;
                padding: 24px;
                max-width: 640px;
                border-radius: 10px;
                box-shadow: 0 2px 12px #00000020;
            }}
            .criteria-table, .prop-table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            .criteria-table th, .criteria-table td,
            .prop-table th, .prop-table td {{
                border: 1px solid #ddd;
                padding: 8px 10px;
                text-align: left;
            }}
            .criteria-table th {{
                background: #e9ecef;
            }}
            .district-title {{
                color: #007bff;
                font-weight: bold;
                margin-top: 20px;
            }}
            .footer {{
                margin-top: 40px;
                font-size: 12px;
                color: #888;
                text-align: center;
            }}
            a {{
                color: #007bff;
            }}
            </style>
            </head>
            <body>
            <div class="container">
                <h2>📰 New Real Estate Matches Found</h2>
                <p>
                    Hi, this is a daily notification from <b>Real Estate Bot</b>.<br>
                    Below are your subscription criteria:
                </p>
                <table class="criteria-table">
                <tr><th>Price Range</th><td>{search_params['price_range']} billion VND</td></tr>
                <tr><th>Area Range</th><td>{search_params['area_range']} m²</td></tr>
                <tr><th>Bedrooms</th><td>{search_params['num_bedrooms']}</td></tr>
                <tr><th>Toilets</th><td>{search_params['num_toilets']}</td></tr>
                <tr><th>Legal Status</th><td>{convert_int_to_phaply(search_params['legal_status_id'])}</td></tr>
                <tr><th>Property Type</th><td>{convert_int_to_property_type(search_params['property_type_id'])}</td></tr>
                </table>
            """

            if district_matches:
                html += "<p>We found some properties that match your subscription criteria:</p>"
                for district_id, properties in district_matches.items():
                    district_name = get_district_name(district_id)
                    html += f"""
                    <div class="district-title">District: {district_name}</div>
                    <table class="prop-table">
                    <tr>
                        <th>Title</th>
                        <th>Price</th>
                        <th>Area</th>
                        <th>Bedrooms</th>
                        <th>Toilets</th>
                        <th>Legal Status</th>
                        <th>Type</th>
                        <th>Source</th>
                        <th>Link</th>
                    </tr>
                    """
                    for prop in properties:
                        html += f"""
                        <tr>
                        <td>{prop['title']}</td>
                        <td>{prop['price']:,.0f} billion VND</td>
                        <td>{prop['area']} m²</td>
                        <td>{prop['number_of_bedrooms']}</td>
                        <td>{prop['number_of_toilets']}</td>
                        <td>{prop['legal_status']}</td>
                        <td>{prop['property_type']}</td>
                        <td>{prop['source']}</td>
                        <td><a href="{prop['url']}">View</a></td>
                        </tr>
                        """
                    html += "</table>"
            else:
                html += "<p>No properties matched your criteria today. Check back tomorrow!</p>"

            html += """
                <div class="footer">
                <p>Real Estate Bot &mdash; You are receiving this email because you subscribed for property notifications.<br>
                If you wish to unsubscribe, please contact us.</p>
                </div>
            </div>
            </body>
            </html>
            """

            # Attach HTML content
            msg.attach(MIMEText(html, 'html'))
            server.send_message(msg)
            logger.info(f"HTML Email sent to {email}")

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
                message += f"Property Type: {prop['property_type']}\n"
                message += f"Source: {prop['source']}\n"
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
    email_requirements = {}  # Dict[email, search_params]
    telegram_properties = {}  # Dict[chat_id, Dict[district_id, List[properties]]]
    telegram_requirements = {}  # Dict[chat_id, search_params]

    for sub in subscriptions:
        # Parse district IDs from comma-separated string
        district_ids = [int(did.strip()) for did in sub.district_ids.split(',')]
        
        # Prepare search parameters
        search_params = {
            'price_range': (sub.min_price + sub.max_price) / 2,
            'area_range': (sub.min_area + sub.max_area) / 2,
            'num_bedrooms': sub.num_bedrooms,
            'num_toilets': sub.num_toilets,
            'legal_status_id': sub.legal_status_id,
            'property_type_id': sub.property_type_id
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
            email_requirements[sub.user_id] = search_params
        elif sub.user_type == 'telegram':
            telegram_properties[sub.user_id] = district_matches
            telegram_requirements[sub.user_id] = search_params
    
    # Send notifications in batch
    batch_send_email_notifications(email_properties, email_requirements)
    # batch_send_telegram_notifications(telegram_properties)

if __name__ == "__main__":
    process_subscriptions()
