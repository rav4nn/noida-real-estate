"""
Geocoder for Noida real estate projects
========================================
Strategy per record:
  1. If housing_url present: fetch page with curl_cffi (Chrome TLS),
     extract lat/lng from JSON-LD schema.org or <meta> tags.
  2. Fallback: Nominatim (OpenStreetMap) — 1 req/sec rate limit.

Inputs:  data/antigravity/housing_enriched.json
Output:  data/housing_geocoded.json   (all original fields + lat, lng)
Progress: data/geocoding_progress.json  (resume-safe)
"""

import json
import re
import sys
import os
import time
import urllib.request
import urllib.parse

try:
    from curl_cffi import requests as cf
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("WARNING: curl_cffi not installed — housing.com fetches disabled, Nominatim-only mode")

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COMBINED = os.path.join(BASE, "data", "housing_combined.json")
ENRICHED = os.path.join(BASE, "data", "antigravity", "housing_enriched.json")
INPUT    = COMBINED if os.path.exists(COMBINED) else ENRICHED
PROGRESS = os.path.join(BASE, "data", "geocoding_progress.json")
OUTPUT   = os.path.join(BASE, "data", "housing_geocoded.json")

_PROFILES = ["safari172_ios", "safari155", "safari180_ios", "safari170"]
HOUSING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 "
                  "(KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

NOMINATIM_UA = "noida-real-estate-geocoder/1.0 (research project)"

# ── Coordinate extraction from housing.com HTML ────────────────────────────────

def _extract_from_jsonld(html: str) -> tuple[float, float] | None:
    """
    Find all <script type="application/ld+json"> blocks and look for
    latitude / longitude fields. Returns (lat, lng) or None.
    """
    blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    for block in blocks:
        try:
            obj = json.loads(block)
        except (json.JSONDecodeError, ValueError):
            # Try to extract raw numeric values even from malformed JSON
            lat_m = re.search(r'"latitude"\s*:\s*([+-]?\d+\.?\d*)', block)
            lng_m = re.search(r'"longitude"\s*:\s*([+-]?\d+\.?\d*)', block)
            if lat_m and lng_m:
                lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
                if _plausible(lat, lng):
                    return lat, lng
            continue

        # Walk nested structures
        for candidate in _iter_jsonld(obj):
            lat = candidate.get("latitude") or (candidate.get("geo") or {}).get("latitude")
            lng = candidate.get("longitude") or (candidate.get("geo") or {}).get("longitude")
            if lat is None or lng is None:
                continue
            try:
                lat, lng = float(lat), float(lng)
            except (TypeError, ValueError):
                continue
            if _plausible(lat, lng):
                return lat, lng
    return None


def _iter_jsonld(obj):
    """Yield every dict in a JSON-LD tree."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_jsonld(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_jsonld(item)


def _extract_from_meta(html: str) -> tuple[float, float] | None:
    """
    Check Open Graph / place meta tags for lat/lng.
    """
    patterns = [
        (r'<meta[^>]+property=["\']og:latitude["\'][^>]+content=["\']([^"\']+)["\']',
         r'<meta[^>]+property=["\']og:longitude["\'][^>]+content=["\']([^"\']+)["\']'),
        (r'<meta[^>]+property=["\']place:location:latitude["\'][^>]+content=["\']([^"\']+)["\']',
         r'<meta[^>]+property=["\']place:location:longitude["\'][^>]+content=["\']([^"\']+)["\']'),
        # Also try reversed content/property order
        (r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:latitude["\']',
         r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:longitude["\']'),
    ]
    for lat_pat, lng_pat in patterns:
        lat_m = re.search(lat_pat, html, re.IGNORECASE)
        lng_m = re.search(lng_pat, html, re.IGNORECASE)
        if lat_m and lng_m:
            try:
                lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
                if _plausible(lat, lng):
                    return lat, lng
            except ValueError:
                continue
    return None


def _extract_from_js(html: str) -> tuple[float, float] | None:
    """
    Last-resort: look for lat/lng embedded in JS objects or window.__state.
    Handles patterns like: "lat":28.5355 or latitude:28.5355
    """
    patterns = [
        r'"lat"\s*:\s*([+-]?\d{2}\.\d{3,})',
        r'"lng"\s*:\s*([+-]?\d{2}\.\d{3,})',
        r'"latitude"\s*:\s*([+-]?\d{2}\.\d{3,})',
        r'"longitude"\s*:\s*([+-]?\d{2,3}\.\d{3,})',
    ]
    lat_m = re.search(patterns[0], html) or re.search(patterns[2], html)
    lng_m = re.search(patterns[1], html) or re.search(patterns[3], html)
    if lat_m and lng_m:
        try:
            lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
            if _plausible(lat, lng):
                return lat, lng
        except ValueError:
            pass
    return None


def _plausible(lat: float, lng: float) -> bool:
    """Sanity-check: coordinates must be in the NCR/UP region."""
    return 27.0 <= lat <= 30.0 and 76.5 <= lng <= 79.0


def extract_coords(html: str) -> tuple[float, float] | None:
    return (
        _extract_from_jsonld(html)
        or _extract_from_meta(html)
        or _extract_from_js(html)
    )


# ── Housing.com page fetch ──────────────────────────────────────────────────────

def fetch_housing_page(url: str) -> str | None:
    if not HAS_CURL_CFFI:
        return None
    for attempt in range(3):
        try:
            resp = cf.get(url, headers=HOUSING_HEADERS, impersonate=_PROFILES[0], timeout=25)
            if resp.status_code == 406:
                for p in _PROFILES[1:]:
                    resp = cf.get(url, headers=HOUSING_HEADERS, impersonate=p, timeout=25)
                    if resp.status_code != 406:
                        break
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (429, 503):
                time.sleep(30)
        except Exception as e:
            print(f"    fetch error (attempt {attempt + 1}): {e}")
            time.sleep(10)
    return None


# ── Nominatim fallback ──────────────────────────────────────────────────────────

_last_nominatim_call = 0.0

def nominatim_geocode(project_name: str, district: str = "", housing_url: str = "") -> tuple[float, float] | None:
    """Rate-limited (1 req/sec) Nominatim lookup."""
    global _last_nominatim_call

    # Extract sector from housing_url slug if present (e.g. "in-sector-107")
    sector_hint = ""
    if housing_url:
        m = re.search(r'in-sector-(\w+)$', housing_url.rstrip("/"))
        if m:
            sector_hint = f"Sector {m.group(1)}"

    city = "Greater Noida" if "buddha" in district.lower() or "greater" in district.lower() else "Noida"
    if "extension" in housing_url.lower() or "noida-extension" in housing_url.lower():
        city = "Noida Extension"
    location = f"{sector_hint} {city}".strip() if sector_hint else city
    query = f"{project_name} {location} Uttar Pradesh India"

    elapsed = time.time() - _last_nominatim_call
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)

    params = urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "addressdetails": 0,
        "countrycodes": "in",
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": NOMINATIM_UA})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            _last_nominatim_call = time.time()
            results = json.loads(resp.read().decode())
        if results:
            lat, lng = float(results[0]["lat"]), float(results[0]["lon"])
            if _plausible(lat, lng):
                return lat, lng
    except Exception as e:
        print(f"    Nominatim error: {e}")
        _last_nominatim_call = time.time()

    return None


# ── Progress helpers ────────────────────────────────────────────────────────────

def load_progress() -> dict:
    if os.path.exists(PROGRESS):
        with open(PROGRESS, encoding="utf-8") as f:
            records = json.load(f)
        return {r["rera_id"]: r for r in records}
    return {}


def save_progress(progress: dict):
    with open(PROGRESS, "w", encoding="utf-8") as f:
        json.dump(list(progress.values()), f, indent=2, ensure_ascii=False)


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    with open(INPUT, encoding="utf-8") as f:
        projects = json.load(f)
    print(f"Loaded {len(projects)} projects")

    progress = load_progress()
    done = sum(1 for r in progress.values() if r.get("lat") is not None)
    print(f"Progress: {done} already geocoded, "
          f"{len(projects) - len(progress)} not yet attempted\n")

    for i, p in enumerate(projects, 1):
        rid  = p["rera_id"]
        name = p["project_name"]
        dist = p.get("district", "")
        url  = p.get("housing_url")

        if rid in progress and progress[rid].get("lat") is not None:
            continue  # already done

        print(f"[{i}/{len(projects)}] {name[:55]}")
        lat = lng = None
        method = None

        # ── Strategy 1: housing.com page ──────────────────────────────────
        if url and HAS_CURL_CFFI:
            html = fetch_housing_page(url)
            if html:
                coords = extract_coords(html)
                if coords:
                    lat, lng = coords
                    method = "housing.com"
                    print(f"  housing.com  → {lat:.5f}, {lng:.5f}")
            time.sleep(2)

        # ── Strategy 2: Nominatim ─────────────────────────────────────────
        if lat is None:
            coords = nominatim_geocode(name, dist, url or "")
            if coords:
                lat, lng = coords
                method = "nominatim"
                print(f"  nominatim    → {lat:.5f}, {lng:.5f}")
            else:
                print(f"  not found")

        progress[rid] = {"rera_id": rid, "lat": lat, "lng": lng, "method": method}
        save_progress(progress)

    # ── Merge & write output ───────────────────────────────────────────────
    coords_by_id = {r["rera_id"]: r for r in progress.values()}
    result = []
    for p in projects:
        row = dict(p)
        geo = coords_by_id.get(p["rera_id"], {})
        row["lat"]          = geo.get("lat")
        row["lng"]          = geo.get("lng")
        row["geocode_method"] = geo.get("method")
        result.append(row)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    geocoded = sum(1 for r in result if r.get("lat") is not None)
    print(f"\nDone. {geocoded}/{len(result)} geocoded → {OUTPUT}")


if __name__ == "__main__":
    main()
