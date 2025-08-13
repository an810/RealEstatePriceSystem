import pandas as pd
import json
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import os
from sqlalchemy import text, create_engine

router = APIRouter()

# Database configuration
DB_CONFIG = {
    'dbname': 'real_estate',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'real_estate_db',
    'port': '5432'
}

def get_db_engine():
    """Get database engine instance"""
    return create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

# Define color schemes
PROPERTY_TYPE_COLORS = {
    "Chung cư": "#1f77b4",  # Blue
    "Biệt thự": "#ff7f0e",  # Orange
    "Nhà riêng": "#2ca02c",  # Green
    "Đất": "#d62728",  # Red
    "Khác": "#9467bd"   # Purple
}

# Define property type mapping
PROPERTY_TYPE_MAPPING = {
    1: "Chung cư",
    2: "Biệt thự",
    3: "Nhà riêng",
    4: "Đất",
    0: "Khác"
}

def get_centroid(feature):
    geometry = feature["geometry"]
    coords = geometry["coordinates"]

    if geometry["type"] == "Polygon":
        ring = coords[0]
    elif geometry["type"] == "MultiPolygon":
        ring = coords[0][0]
    else:
        return None, None

    lon = [pt[0] for pt in ring]
    lat = [pt[1] for pt in ring]
    return sum(lon) / len(lon), sum(lat) / len(lat)

# List of Hà Nội districts
HANOI_DISTRICTS = [
    'Ba Đình', 'Ba Vì', 'Cầu Giấy', 'Chương Mỹ', 'Đan Phượng', 'Đông Anh', 'Đống Đa',
    'Gia Lâm', 'Hà Đông', 'Hai Bà Trưng', 'Hoài Đức', 'Hoàn Kiếm', 'Hoàng Mai',
    'Long Biên', 'Mê Linh', 'Mỹ Đức', 'Phú Xuyên', 'Phúc Thọ', 'Quốc Oai', 'Sóc Sơn',
    'Sơn Tây', 'Tây Hồ', 'Thạch Thất', 'Thanh Oai', 'Thanh Trì', 'Thanh Xuân',
    'Thường Tín', 'Từ Liêm', 'Ứng Hòa'
]

# Mapping GeoJSON English district names to Vietnamese
GEOJSON_NAME_MAP = {
    "Ba Dinh": "Ba Đình",
    "Ba Vi": "Ba Vì",
    "Cau Giay": "Cầu Giấy",
    "Chuong My": "Chương Mỹ",
    "Dan Phuong": "Đan Phượng",
    "Dong Anh": "Đông Anh",
    "Dong Da": "Đống Đa",
    "Gia Lam": "Gia Lâm",
    "Ha Dong": "Hà Đông",
    "Hai Ba Trung": "Hai Bà Trưng",
    "Hoai Duc": "Hoài Đức",
    "Hoan Kiem": "Hoàn Kiếm",
    "Hoang Mai": "Hoàng Mai",
    "Long Bien": "Long Biên",
    "Me Linh": "Mê Linh",
    "My Duc": "Mỹ Đức",
    "Phu Xuyen": "Phú Xuyên",
    "Phuc Tho": "Phúc Thọ",
    "Quoc Oai": "Quốc Oai",
    "Soc Son": "Sóc Sơn",
    "Son Tay": "Sơn Tây",
    "Tay Ho": "Tây Hồ",
    "Thach That": "Thạch Thất",
    "Thanh Oai": "Thanh Oai",
    "Thanh Tri": "Thanh Trì",
    "Thanh Xuan": "Thanh Xuân",
    "Thuong Tin": "Thường Tín",
    "Tu Liem": "Từ Liêm",
    "Ung Hoa": "Ứng Hòa"
}

def load_and_process_data() -> Dict[str, Any]:
    try:
        # Load data from database instead of TSV file
        engine = get_db_engine()
        
        query = """
        SELECT 
            re.url_id,
            re.title,
            re.area,
            re.price,
            re.number_of_bedrooms,
            re.number_of_toilets,
            re.legal_id,
            re.property_type_id,
            re.district_id,
            re.province,
            re.lat,
            re.lon,
            re.source,
            re.url,
            dm.district,
            ptm.property_type,
            lm.legal
        FROM real_estate re
        LEFT JOIN district_mapping dm ON re.district_id = dm.district_id
        LEFT JOIN property_type_mapping ptm ON re.property_type_id = ptm.property_type_id
        LEFT JOIN legal_mapping lm ON re.legal_id = lm.legal_id
        WHERE re.is_available = TRUE
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Clean data
        required_columns = ["area", "price", "lat", "lon", "property_type", "district", "property_type_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        df_clean = df.dropna(subset=required_columns).copy()
        
        if df_clean.empty:
            raise HTTPException(
                status_code=400,
                detail="No valid data after cleaning"
            )
        
        # Combine Bắc Từ Liêm and Nam Từ Liêm into Từ Liêm
        df_clean.loc[:, "district"] = df_clean["district"].replace({
            "Bắc Từ Liêm": "Từ Liêm",
            "Nam Từ Liêm": "Từ Liêm"
        })

        # 1. Price Distribution
        price_dist = {
            "type": "histogram",
            "x": df_clean["price"].tolist(),
            "nbins": 50,
            "title": "Distribution of Property Prices (in billion VND)",
            "xaxis_title": "Price (billion VND)",
            "yaxis_title": "Count",
            "color": "#1f77b4"  # Blue
        }

        # 2. Area Distribution
        area_dist = {
            "type": "histogram",
            "x": df_clean["area"].tolist(),
            "nbins": 50,
            "title": "Distribution of Property Areas (m²)",
            "xaxis_title": "Area (m²)",
            "yaxis_title": "Count",
            "color": "#2ca02c"  # Green
        }

        # 3. Property Type Pie Chart
        property_type_counts = df_clean["property_type"].value_counts()
        property_type_pie = {
            "type": "pie",
            "labels": property_type_counts.index.tolist(),
            "values": property_type_counts.values.tolist(),
            "title": "Property Type Distribution",
            "colors": [PROPERTY_TYPE_COLORS.get(pt, "#9467bd") for pt in property_type_counts.index]
        }

        # 4. Number of Listings per District
        district_counts = df_clean["district"].value_counts().reset_index()
        district_counts.columns = ["district", "count"]
        district_bar = {
            "type": "bar",
            "x": district_counts["district"].tolist(),
            "y": district_counts["count"].tolist(),
            "title": "Number of Listings per District",
            "xaxis_title": "District",
            "yaxis_title": "Number of Listings",
            "color": "#ff7f0e"  # Orange
        }

        # 5. Price vs Area Scatter Plot
        scatter_data = {
            "type": "scatter",
            "x": df_clean["area"].tolist(),
            "y": df_clean["price"].tolist(),
            "color": [PROPERTY_TYPE_COLORS.get(pt, "#9467bd") for pt in df_clean["property_type"]],
            "title": "Price vs Area by Property Type",
            "xaxis_title": "Area (m²)",
            "yaxis_title": "Price (billion VND)",
            "hover_data": {
                "title": df_clean["title"].tolist() if "title" in df_clean.columns else [""] * len(df_clean),
                "district": df_clean["district"].tolist(),
                "property_type": df_clean["property_type"].tolist()
            }
        }

        # scatter_data = {
        #     "type": "scatter",    
        #     "x": df_clean["area"].tolist(),
        #     "y": df_clean["price"].tolist(),
        #     "property_types": list(PROPERTY_TYPE_COLORS.keys()),
        #     "colors": PROPERTY_TYPE_COLORS,
        #     "title": "Price vs Area by Property Type",
        #     "xaxis_title": "Area (m²)",
        #     "yaxis_title": "Price (tỷ VND)",
        #     "hover_data": {
        #         "title": df_clean["title"].tolist() if "title" in df_clean.columns else [""] * len(df_clean),
        #         "district": df_clean["district"].tolist(),
        #         "property_type": df_clean["property_type"].tolist()
        #     }
        # }

        # 6. Map of Listings
        listings_map = {
            "type": "scattermapbox",
            "lat": df_clean["lat"].tolist(),
            "lon": df_clean["lon"].tolist(),
            "color": [PROPERTY_TYPE_COLORS.get(pt, "#9467bd") for pt in df_clean["property_type"]],
            # "property_types": list(PROPERTY_TYPE_COLORS.keys()),
            # "colors": PROPERTY_TYPE_COLORS,
            "size": df_clean["price"].tolist(),
            "hover_name": df_clean["title"].tolist() if "title" in df_clean.columns else [""] * len(df_clean),
            "hover_data": {
                "title": df_clean["title"].tolist() if "title" in df_clean.columns else [""] * len(df_clean),
                "district": df_clean["district"].tolist(),
                "price": df_clean["price"].tolist(),
                "area": df_clean["area"].tolist(),
                "property_type": df_clean["property_type"].tolist()
            },
            "zoom": 10,
            "title": "Property Listings Map"
        }

        # 7. Choropleth Map
        avg_price = df_clean[df_clean["district"].isin(HANOI_DISTRICTS)] \
            .groupby("district")["price"] \
            .mean().reset_index()
        avg_price.columns = ["district", "avg_price"]

        # 8. Legal Status Distribution
        legal_status_counts = df_clean["legal"].value_counts()
        legal_status_pie = {
            "type": "pie",
            "labels": legal_status_counts.index.tolist(),
            "values": legal_status_counts.values.tolist(),
            "title": "Legal Status Distribution",
            "colors": ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        }

        # 9. Price per Area by District and Property Type
        # Convert price from billion to million VND
        df_clean.loc[:, "price_per_area"] = (df_clean["price"] * 1000) / df_clean["area"]
        
        # 10. Price per Area Distribution
        price_per_area_dist = {
            "type": "histogram",
            "x": df_clean["price_per_area"].tolist(),
            "nbins": 50,
            "title": "Distribution of Price per Area",
            "xaxis_title": "Price per Area (million VND/m²)",
            "yaxis_title": "Count",
            "color": "#1f77b4"  # Blue
        }
        
        # Create pivot table for averages
        pivot_data = pd.pivot_table(
            df_clean,
            values='price_per_area',
            index='district',
            columns='property_type_id',
            aggfunc='mean',
            fill_value=0
        ).round(2)
        
        # Get unique districts and property types
        districts = sorted(pivot_data.index.tolist())
        property_types = sorted(pivot_data.columns.tolist())
        
        # Create data structure for grouped bar chart
        price_per_area_data = {}
        for pt in property_types:
            name = PROPERTY_TYPE_MAPPING.get(pt, "Khác")
            price_per_area_data[name] = pivot_data[pt].tolist()

        price_per_area_bar = {
            "type": "grouped_bar",
            "property_types": [PROPERTY_TYPE_MAPPING.get(pt, "Khác") for pt in property_types],
            "districts": districts,
            "data": price_per_area_data,
            "title": "Average Price per Area by District and Property Type",
            "xaxis_title": "District",
            "yaxis_title": "Price per Area (million VND/m²)",
            "colors": [PROPERTY_TYPE_COLORS.get(PROPERTY_TYPE_MAPPING.get(pt, "Khác"), "#9467bd") for pt in property_types]
        }

        # Load GeoJSON
        try:
            with open("/opt/airflow/data/diaphanhuyen.geojson", "r", encoding="utf-8") as f:
                geojson_data = json.load(f)
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail="GeoJSON file not found. Please ensure diaphanhuyen.geojson is in the data directory."
            )
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="Invalid GeoJSON file format"
            )

        # Filter to Hà Nội
        hanoi_features = [f for f in geojson_data["features"] 
                         if f["properties"].get("Ten_Tinh") == "Hà Nội"]
        hanoi_geojson = {
            "type": "FeatureCollection",
            "features": hanoi_features
        }

        # Rename GeoJSON key to match district names
        for f in hanoi_geojson["features"]:
            eng_name = f["properties"]["Ten_Huyen"].title()
            f["id"] = GEOJSON_NAME_MAP.get(eng_name)

        # Prepare centroid lists for district labels
        lat_list = []
        lon_list = []
        text_list = []

        for feature in hanoi_geojson["features"]:
            district_vn = feature["id"]
            centroid_lon, centroid_lat = get_centroid(feature)
            if centroid_lon is not None and centroid_lat is not None:
                lat_list.append(centroid_lat)
                lon_list.append(centroid_lon)
                text_list.append(district_vn)

        choropleth_map = {
            "type": "choroplethmapbox",
            "geojson": hanoi_geojson,
            "locations": avg_price["district"].tolist(),
            "z": avg_price["avg_price"].tolist(),
            "colorscale": [
                [0, "#f7fbff"],    # Light blue
                [0.2, "#deebf7"],
                [0.4, "#c6dbef"],
                [0.6, "#9ecae1"],
                [0.8, "#6baed6"],
                [1, "#2171b5"]     # Dark blue
            ],
            "marker": {
                "opacity": 0.8
            },
            "colorbar": {
                "title": "Avg Price (billion VND)"
            },
            "labels": {
                "avg_price": "Avg Price (billion VND)"
            },
            "center": {"lat": 21.0285, "lon": 105.8542},
            "zoom": 9,
            "title": "Average Real Estate Price by District (billion VND)",
            "district_labels": {
                "lat": lat_list,
                "lon": lon_list,
                "text": text_list
            }
        }

        return {
            "price_distribution": price_dist,
            "area_distribution": area_dist,
            "property_type_distribution": property_type_pie,
            "district_distribution": district_bar,
            "price_area_scatter": scatter_data,
            "listings_map": listings_map,
            "choropleth_map": choropleth_map,
            "legal_status_distribution": legal_status_pie,
            "price_per_area_stats": price_per_area_bar,
            "price_per_area_distribution": price_per_area_dist
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error loading data from database: {str(e)}"
        )

@router.get("/api/visualizations")
async def get_visualizations():
    """
    Endpoint to get all visualization data
    """
    return load_and_process_data() 