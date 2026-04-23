"""
Merge geocoded coordinates + price analysis into one final JSON.

Inputs:
  data/housing_geocoded.json   — from geocode.py  (lat, lng, geocode_method)
  data/housing_analyzed.json   — from analyze_housing.py (min_price, max_price, zone, price_per_sqft)

Output:
  data/projects_final.json     — all fields merged, ready for the frontend

Run after both geocode.py and analyze_housing.py have completed.
"""

import json
import os
import sys

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GEOCODED = os.path.join(BASE, "data", "housing_geocoded.json")
ANALYZED = os.path.join(BASE, "data", "housing_analyzed.json")
OUTPUT   = os.path.join(BASE, "data", "projects_final.json")

# Fields added by the geocoder
GEO_FIELDS = ("lat", "lng", "geocode_method")

# Fields added by the analyzer
ANALYSIS_FIELDS = ("min_price", "max_price", "zone", "price_per_sqft")


def classify_zone_by_coords(lat, lng):
    """
    Rectangular bounding boxes, checked in priority order (south→north).
    Boundary at lng≈77.42 separates Noida (west) from Noida Extension /
    Greater Noida (east of the Hindon).
    """
    if lat is None or lng is None:
        return None

    # Yamuna Expressway / YEIDA — southernmost belt toward Agra
    if lat < 28.40 and lng > 77.45:
        return "Yamuna Expressway"

    # Greater Noida proper — south-east, east of the Hindon
    if 28.40 <= lat <= 28.56 and lng > 77.42:
        return "Greater Noida"

    # Noida Extension / Greater Noida West — north-east, east of the Hindon
    if lat > 28.56 and lng > 77.41:
        return "Noida Extension"

    # Noida proper — sectors 1–168, west of the Hindon
    if 28.48 <= lat <= 28.65 and 77.28 <= lng <= 77.45:
        return "Noida"

    return None


def main():
    for path in (GEOCODED, ANALYZED):
        if not os.path.exists(path):
            print(f"ERROR: {path} not found. Run the relevant pipeline step first.")
            sys.exit(1)

    with open(GEOCODED, encoding="utf-8") as f:
        geocoded = json.load(f)

    with open(ANALYZED, encoding="utf-8") as f:
        analyzed = json.load(f)

    # Index analysis records by rera_id for O(1) lookup
    analysis_by_id = {r["rera_id"]: r for r in analyzed}

    merged = []
    no_coords = 0
    no_analysis = 0
    zone_counts = {}

    for row in geocoded:
        rid = row["rera_id"]

        # Start from geocoded record (has lat/lng on top of housing_enriched fields)
        out = dict(row)

        # Overlay analysis fields
        analysis = analysis_by_id.get(rid)
        if analysis:
            for field in ANALYSIS_FIELDS:
                out[field] = analysis.get(field)
        else:
            no_analysis += 1
            for field in ANALYSIS_FIELDS:
                out[field] = None

        # Prefer coordinate-based zone over the text-based one from the analyzer.
        coord_zone = classify_zone_by_coords(out.get("lat"), out.get("lng"))
        if coord_zone:
            out["zone"] = coord_zone
        elif not out.get("zone"):
            out["zone"] = "Unknown"

        if out.get("lat") is None:
            no_coords += 1

        zone_counts[out["zone"]] = zone_counts.get(out["zone"], 0) + 1
        merged.append(out)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)

    geocoded_count = len(merged) - no_coords
    print(f"Merged {len(merged)} records")
    print(f"  With coordinates : {geocoded_count}")
    print(f"  Missing coords   : {no_coords}  (will not render on map)")
    print(f"  Missing analysis : {no_analysis}")
    print(f"  Zone distribution:")
    for zone in sorted(zone_counts, key=lambda z: -zone_counts[z]):
        print(f"    {zone:<20} {zone_counts[zone]}")
    print(f"Output → {OUTPUT}")


if __name__ == "__main__":
    main()
