import pandas as pd
import json
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import os

router = APIRouter()

# Define color schemes
PROPERTY_TYPE_COLORS = {
    "Chung cư": "#1f77b4",  # Blue
    "Biệt thự": "#ff7f0e",  # Orange
    "Nhà riêng": "#2ca02c",  # Green
    "Đất": "#d62728",  # Red
    "Khác": "#9467bd"   # Purple
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

def infer_property_type(title):
    if pd.isna(title):
        return "Khác"
    title = str(title).lower()
    if "chung cư" in title or "căn hộ" in title:
        return "Chung cư"
    if "biệt thự" in title or "liền kề" in title:
        return "Biệt thự"
    if "nhà riêng" in title or "nhà mặt phố" in title or "nhà" in title:
        return "Nhà riêng"
    if "đất" in title:
        return "Đất"
    return "Khác"

def load_and_process_data() -> Dict[str, Any]:
    try:
        # Load the real estate data
        df = pd.read_csv("/Users/ducan/Documents/Graduation-Thesis/RealEstatePriceSystem/data/cleaned/visualization_data.tsv", sep="\t")
        
        # Add property_type if it doesn't exist
        if 'property_type' not in df.columns:
            df['property_type'] = df['title'].apply(infer_property_type)
        
        # Clean data
        required_columns = ["area", "price", "lat", "lon", "property_type", "district"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )
        
        df_clean = df.dropna(subset=required_columns)
        
        if df_clean.empty:
            raise HTTPException(
                status_code=400,
                detail="No valid data after cleaning"
            )
        
        # Combine Bắc Từ Liêm and Nam Từ Liêm into Từ Liêm
        df_clean["district"] = df_clean["district"].replace({
            "Bắc Từ Liêm": "Từ Liêm",
            "Nam Từ Liêm": "Từ Liêm"
        })

        # 1. Price Distribution
        price_dist = {
            "type": "histogram",
            "x": df_clean["price"].tolist(),
            "nbins": 50,
            "title": "Distribution of Property Prices (in billion VND)",
            "xaxis_title": "Price (tỷ VND)",
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
            "yaxis_title": "Price (tỷ VND)",
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

        # Load GeoJSON
        try:
            with open("/Users/ducan/Documents/Graduation-Thesis/RealEstatePriceSystem/data/diaphanhuyen.geojson", "r", encoding="utf-8") as f:
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
                "title": "Avg Price (tỷ VND)"
            },
            "labels": {
                "avg_price": "Avg Price (tỷ VND)"
            },
            "center": {"lat": 21.0285, "lon": 105.8542},
            "zoom": 9,
            "title": "Average Real Estate Price by District (tỷ VND)",
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
            "choropleth_map": choropleth_map
        }
    except pd.errors.EmptyDataError:
        raise HTTPException(
            status_code=400,
            detail="The input file is empty"
        )
    except pd.errors.ParserError:
        raise HTTPException(
            status_code=400,
            detail="Error parsing the input file. Please check the file format."
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.get("/api/visualizations")
async def get_visualizations():
    """
    Endpoint to get all visualization data
    """
    return load_and_process_data() 