import json
import re
import os

def parse_price(price_str):
    if not price_str or not isinstance(price_str, str):
        return None, None
    
    # Remove currency symbol
    price_str = price_str.replace('₹', '').strip()
    
    parts = price_str.split('-')
    
    def normalize_single_price(p):
        p = p.strip().lower()
        multiplier = 1.0
        if 'cr' in p:
            multiplier = 100.0
            p = p.replace('cr', '').strip()
        elif 'l' in p:
            multiplier = 1.0
            p = p.replace('l', '').strip()
        
        try:
            return float(p) * multiplier
        except ValueError:
            return None

    if len(parts) == 1:
        val = normalize_single_price(parts[0])
        return val, val
    elif len(parts) == 2:
        return normalize_single_price(parts[0]), normalize_single_price(parts[1])
    
    return None, None

def classify_zone(item):
    text = (str(item.get('project_name') or '') + ' ' + 
            str(item.get('housing_url') or '') + ' ' + 
            str(item.get('source_url_99acres') or '') + ' ' +
            str(item.get('promoter_name') or '')).lower()
    
    if 'yamuna expressway' in text or 'yeida' in text:
        return "Yamuna Expressway"

    if 'noida extension' in text or 'greater noida west' in text:
        return "Noida Extension"

    if 'greater noida' in text:
        return "Greater Noida"

    if 'noida' in text:
        return "Noida"

    return "Unknown"

def calculate_price_per_sqft(min_price, max_price, bhk_areas_sqft):
    if not bhk_areas_sqft or not isinstance(bhk_areas_sqft, dict):
        return None
    
    areas = []
    for area in bhk_areas_sqft.values():
        try:
            # Handle ranges in area like "1200 - 1400"
            if '-' in str(area):
                parts = str(area).split('-')
                areas.append(float(parts[0].strip()))
                areas.append(float(parts[1].strip()))
            else:
                areas.append(float(str(area).strip()))
        except ValueError:
            continue
            
    if not areas:
        return None
        
    min_area = min(areas)
    max_area = max(areas)
    
    # Basic logic: min_price / min_area, max_price / max_area
    # Prices are in lakhs, convert to absolute for sqft calculation
    if min_price and min_area:
        price_sqft_min = (min_price * 100000) / min_area
    else:
        price_sqft_min = None
        
    if max_price and max_area:
        price_sqft_max = (max_price * 100000) / max_area
    else:
        price_sqft_max = None
        
    if price_sqft_min and price_sqft_max:
        return round((price_sqft_min + price_sqft_max) / 2)
    elif price_sqft_min:
        return round(price_sqft_min)
    elif price_sqft_max:
        return round(price_sqft_max)
        
    return None

def analyze():
    combined_path = 'data/housing_combined.json'
    enriched_path = 'data/antigravity/housing_enriched.json'
    input_path = combined_path if os.path.exists(combined_path) else enriched_path
    output_path = 'data/housing_analyzed.json'
    summary_path = 'data/summary_stats.json'

    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return
    print(f"Reading from {input_path}")

    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    analyzed_data = []
    
    for item in data:
        min_p, max_p = parse_price(item.get('price_range'))
        zone = classify_zone(item)
        price_sqft = calculate_price_per_sqft(min_p, max_p, item.get('bhk_areas_sqft'))
        
        item['min_price'] = min_p
        item['max_price'] = max_p
        item['zone'] = zone
        item['price_per_sqft'] = price_sqft
        
        analyzed_data.append(item)
        
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analyzed_data, f, indent=2)
        
    # Summary stats
    zones = ["Noida", "Greater Noida", "Noida Extension", "Yamuna Expressway"]
    stats = {
        "avg_price_by_zone": {},
        "top_developers": {},
        "bhk_distribution": {}
    }
    
    zone_prices = {z: [] for z in zones}
    developer_counts = {}
    bhk_counts = {}
    
    for item in analyzed_data:
        z = item.get('zone')
        if z in zone_prices:
            if item.get('min_price'):
                zone_prices[z].append(item['min_price'])
            if item.get('max_price'):
                zone_prices[z].append(item['max_price'])
                
        dev = item.get('developer_housing') or item.get('promoter_name')
        if dev:
            developer_counts[dev] = developer_counts.get(dev, 0) + 1
            
        for bhk in (item.get('bhk_types') or []):
            bhk_counts[bhk] = bhk_counts.get(bhk, 0) + 1
            
    for z in zones:
        if zone_prices[z]:
            stats["avg_price_by_zone"][z] = round(sum(zone_prices[z]) / len(zone_prices[z]), 2)
        else:
            stats["avg_price_by_zone"][z] = 0
            
    # Top 10 developers
    sorted_devs = sorted(developer_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    stats["top_developers"] = dict(sorted_devs)
    
    stats["bhk_distribution"] = bhk_counts
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)

if __name__ == "__main__":
    analyze()
