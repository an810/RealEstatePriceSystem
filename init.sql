-- Create tables

CREATE TABLE IF NOT EXISTS real_estate (
    url_id VARCHAR(30),
    title VARCHAR(255),
    area FLOAT,
    price FLOAT,
    number_of_bedrooms INTEGER,
    number_of_toilets INTEGER,
    legal INTEGER,
    property_type VARCHAR(50),
    property_type_id INTEGER,
    district_id INTEGER,
    district_name VARCHAR(50),
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

CREATE TABLE IF NOT EXISTS subscription (
    id SERIAL PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    min_price FLOAT NOT NULL,
    max_price FLOAT NOT NULL,
    min_area FLOAT NOT NULL,
    max_area FLOAT NOT NULL,
    num_bedrooms INTEGER NOT NULL,
    num_toilets INTEGER NOT NULL,
    district_ids VARCHAR(255) NOT NULL,
    legal_status_id INTEGER NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    user_type VARCHAR(10) NOT NULL,
    created_at TIMESTAMP NOT NULL
);