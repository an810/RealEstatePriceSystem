import json

def extract_hanoi_districts():
    # Read the GeoJSON file
    with open('data/diaphanhuyen.geojson', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract districts where Ten_tinh is 'Hà Nội'
    hanoi_districts = []
    for feature in data['features']:
        properties = feature['properties']
        if properties.get('Ten_Tinh') == 'Hà Nội':
            district_name = properties.get('Ten_Huyen')
            if district_name:
                hanoi_districts.append(district_name)
    
    # Sort districts alphabetically
    hanoi_districts.sort()
    
    # Save to text file with indices
    with open('data/hanoi_districts.txt', 'w', encoding='utf-8') as f:
        for idx, district in enumerate(hanoi_districts, 1):
            f.write(f"{idx}. {district}\n")
    
    print(f"Successfully extracted {len(hanoi_districts)} districts of Hanoi")
    print("Results saved to data/hanoi_districts.txt")

if __name__ == "__main__":
    extract_hanoi_districts()
