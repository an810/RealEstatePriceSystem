-- 1. real_estate
CREATE TABLE IF NOT EXISTS real_estate (
    url_id              VARCHAR(30)     PRIMARY KEY,
    title               VARCHAR(255),
    area                FLOAT,
    price               FLOAT,
    number_of_bedrooms  INTEGER,
    number_of_toilets   INTEGER,
    legal_id            INTEGER,
    property_type_id    INTEGER,
    district_id         INTEGER,
    province            VARCHAR(50),
    is_available        BOOLEAN         DEFAULT TRUE,
    lat                 FLOAT,
    lon                 FLOAT,
    source              VARCHAR(50),
    url                 VARCHAR(512),
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- 2. district_mapping
CREATE TABLE IF NOT EXISTS district_mapping (
    district_id INTEGER     PRIMARY KEY,
    district    VARCHAR(50),
    created_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial district mappings
INSERT INTO district_mapping (district_id, district) VALUES
    (1, 'Ba Đình'),
    (2, 'Ba Vì'),
    (3, 'Cầu Giấy'),
    (4, 'Chương Mỹ'),
    (5, 'Đan Phượng'),
    (6, 'Đông Anh'),
    (7, 'Đống Đa'),
    (8, 'Gia Lâm'),
    (9, 'Hà Đông'),
    (10, 'Hai Bà Trưng'),
    (11, 'Hoài Đức'),
    (12, 'Hoàn Kiếm'),
    (13, 'Hoàng Mai'),
    (14, 'Long Biên'),
    (15, 'Mê Linh'),
    (16, 'Mỹ Đức'),
    (17, 'Phú Xuyên'),
    (18, 'Phúc Thọ'),
    (19, 'Quốc Oai'),
    (20, 'Sóc Sơn'),
    (21, 'Sơn Tây'),
    (22, 'Tây Hồ'),
    (23, 'Thạch Thất'),
    (24, 'Thanh Oai'),
    (25, 'Thanh Trì'),
    (26, 'Thanh Xuân'),
    (27, 'Thường Tín'),
    (28, 'Từ Liêm'),
    (29, 'Ứng Hòa')
ON CONFLICT (district_id) DO NOTHING;

-- 3. property_type_mapping
CREATE TABLE IF NOT EXISTS property_type_mapping (
    property_type_id INTEGER     PRIMARY KEY,
    property_type    VARCHAR(50),
    created_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial property type mappings
INSERT INTO property_type_mapping (property_type_id, property_type) VALUES
    (1, 'Chung cư'),
    (2, 'Biệt thự'),
    (3, 'Nhà riêng'),
    (4, 'Đất')
ON CONFLICT (property_type_id) DO NOTHING;

-- 4. legal_mapping
CREATE TABLE IF NOT EXISTS legal_mapping (
    legal_id    INTEGER     PRIMARY KEY,
    legal       VARCHAR(50),
    created_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO legal_mapping (legal_id, legal) VALUES
    (0, 'Chưa có sổ'),
    (1, 'Hợp đồng'),
    (2, 'Sổ đỏ')
ON CONFLICT (legal_id) DO NOTHING;

-- 5. subscription
CREATE TABLE IF NOT EXISTS subscription (
    id             SERIAL      PRIMARY KEY,
    user_id        VARCHAR(256) NOT NULL,
    user_name      VARCHAR(128) NOT NULL,
    user_type      VARCHAR(10)  NOT NULL,
    min_price      FLOAT        NOT NULL,
    max_price      FLOAT        NOT NULL,
    min_area       FLOAT        NOT NULL,
    max_area       FLOAT        NOT NULL,
    num_bedrooms   INTEGER      NOT NULL,
    num_toilets    INTEGER      NOT NULL,
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- 6. district_subscription
CREATE TABLE IF NOT EXISTS district_subscription (
    id          SERIAL      PRIMARY KEY,
    user_id     VARCHAR(256) NOT NULL,
    district_id INTEGER      NOT NULL REFERENCES district_mapping(district_id),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);  

-- 7. property_type_subscription
CREATE TABLE IF NOT EXISTS property_type_subscription (
    id                  SERIAL       PRIMARY KEY,
    user_id             VARCHAR(256) NOT NULL,
    property_type_id    INTEGER      NOT NULL REFERENCES property_type_mapping(property_type_id),
    created_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);  

-- 8. legal_subscription
CREATE TABLE IF NOT EXISTS legal_subscription (
    id          SERIAL       PRIMARY KEY,
    user_id     VARCHAR(256) NOT NULL,
    legal_id    INTEGER      NOT NULL REFERENCES legal_mapping(legal_id),
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);  
