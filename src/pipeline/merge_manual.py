"""
Merge hand-entered projects (data/manual_additions.json) into the scraped
RERA dataset (data/antigravity/housing_enriched.json).

Produces:  data/housing_combined.json  - read by analyze_housing.py and geocode.py

If a manual entry ships with `manual_lat` / `manual_lng`, those coordinates are
seeded into data/geocoding_progress.json so the geocoder preserves them instead
of overwriting with a Nominatim lookup.

The scraped file is never modified.
"""

import json
import os
import sys

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENRICHED = os.path.join(BASE, "data", "antigravity", "housing_enriched.json")
MANUAL   = os.path.join(BASE, "data", "manual_additions.json")
OUTPUT   = os.path.join(BASE, "data", "housing_combined.json")
PROGRESS = os.path.join(BASE, "data", "geocoding_progress.json")


def load_json(path, default):
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def seed_geocoding_progress(manual_entries):
    """For manual entries with explicit lat/lng, mark them as already-geocoded
    so geocode.py skips them on the next run."""
    progress = load_json(PROGRESS, [])
    by_id = {r["rera_id"]: r for r in progress}

    for entry in manual_entries:
        lat = entry.get("manual_lat")
        lng = entry.get("manual_lng")
        if lat is None or lng is None:
            continue
        rid = entry["rera_id"]
        existing = by_id.get(rid, {})
        if existing.get("lat") is not None:
            continue  # already geocoded
        by_id[rid] = {
            "rera_id": rid,
            "lat": lat,
            "lng": lng,
            "method": "manual",
        }

    with open(PROGRESS, "w", encoding="utf-8") as f:
        json.dump(list(by_id.values()), f, indent=2, ensure_ascii=False)


def main():
    if not os.path.exists(ENRICHED):
        print(f"ERROR: {ENRICHED} not found.")
        sys.exit(1)

    enriched = load_json(ENRICHED, [])
    manual   = load_json(MANUAL, [])

    existing_ids = {p["rera_id"] for p in enriched}

    combined = list(enriched)
    added = 0
    skipped = 0
    for entry in manual:
        rid = entry.get("rera_id")
        if not rid:
            print(f"  skipping manual entry without rera_id: {entry.get('project_name')}")
            continue
        if rid in existing_ids:
            skipped += 1
            continue
        combined.append(entry)
        existing_ids.add(rid)
        added += 1

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    seed_geocoding_progress(manual)

    print(f"Scraped entries   : {len(enriched)}")
    print(f"Manual entries    : {len(manual)}")
    print(f"  added           : {added}")
    print(f"  skipped (dupes) : {skipped}")
    print(f"Combined total    : {len(combined)}")
    print(f"Output -> {OUTPUT}")


if __name__ == "__main__":
    main()
