-- Create tables

CREATE TABLE IF NOT EXISTS url (
    id SERIAL PRIMARY KEY,
    url_id VARCHAR(30) UNIQUE,
    url VARCHAR(255),
    source VARCHAR(30),
    crawled_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS real_estate (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    url_id VARCHAR(255),
    area FLOAT,
    price FLOAT,
    number_of_bedrooms INTEGER,
    number_of_toilets INTEGER,
    legal INTEGER,
    lat FLOAT,
    lon FLOAT,
    district_id INTEGER,
    district_name VARCHAR(30),
    province VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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