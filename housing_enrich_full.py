"""
Housing.com full enrichment — 754 RERA projects
================================================
Strategy:
  1. DuckDuckGo HTML endpoint  → find Housing.com project URL (no site: filter needed)
  2. curl_cffi (Chrome TLS)    → fetch project page (bypasses Akamai 406)
  3. Regex + JSON-LD           → extract buildings, units, BHK areas, price, RERA
  4. Incremental save          → resume-safe, saves every project
  5. Second pass               → retries all URL-not-found after 5-min cooldown

Output files:
  data/antigravity/housing_progress.json   ← live progress (one record per project)
  data/antigravity/housing_enriched.json   ← final merged dataset
"""

import re, json, time, sys, os
from collections import Counter
from curl_cffi import requests as cf

# Force UTF-8 stdout so ₹ and other non-ASCII chars don't crash on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE   = os.path.dirname(os.path.abspath(__file__))
INPUT  = os.path.join(BASE, "data", "antigravity", "projects_raw.json")
PROG   = os.path.join(BASE, "data", "antigravity", "housing_progress.json")
OUTPUT = os.path.join(BASE, "data", "antigravity", "housing_enriched.json")

# ── HTTP headers ───────────────────────────────────────────────────────────────
HOUSING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
DDG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,*/*",
    "Content-Type": "application/x-www-form-urlencoded",
}


# ── URL discovery ──────────────────────────────────────────────────────────────
def clean_project_name(name: str) -> str:
    """Remove boilerplate phase/tower suffixes for a cleaner search query."""
    # Special handling for authority schemes (BHS, RHS, etc.)
    if re.search(r'\bBHS[-\s]\d+', name, re.I):
        name = "GNIDA Authority " + name
    
    name = re.sub(r'\(.*?\)', '', name)            # remove parentheses
    name = re.sub(r'\bphase[-\s]?\w+\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\btower\s+[A-Z\s&,]+$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s{2,}', ' ', name)
    return name.strip()


def _ddg_first_url(queries: list[str]) -> str | None:
    """Run a list of DDG queries in order; return the first Housing.com project URL found."""
    for attempt, query in enumerate(queries):
        try:
            resp = cf.post(
                "https://html.duckduckgo.com/html/",
                data={"q": query},
                headers=DDG_HEADERS,
                impersonate="chrome124",
                timeout=20,
            )
            if resp.status_code != 200:
                print(f"    DDG HTTP {resp.status_code}")
                time.sleep(10)
                continue

            urls = re.findall(
                r'href="(https?://housing\.com/in/buy/projects/page/[^"?#]+)"',
                resp.text
            )
            if urls:
                url = urls[0]
                url = re.sub(r'/(reviews|photos|floor-plan|amenities|overview|gallery).*$', '', url)
                return url

        except Exception as e:
            print(f"    DDG error (attempt {attempt+1}): {e}")
            time.sleep(15)

        if attempt < len(queries) - 1:
            time.sleep(4)

    return None


def find_housing_url(project_name: str, district: str = "") -> str | None:
    """
    Search DuckDuckGo HTML endpoint for a Housing.com project page.
    Returns the first valid /in/buy/projects/page/ URL found, or None.
    """
    city_hint = "greater noida" if "buddha" in district.lower() or "greater" in district.lower() else "noida"
    clean = clean_project_name(project_name)

    queries = [
        f"{clean} {city_hint} housing.com",
        f"{project_name} {city_hint} housing.com project",
        f"{clean} noida housing.com",
    ]
    return _ddg_first_url(queries)


def find_housing_url_by_rera(rera_id: str, project_name: str) -> str | None:
    """
    Fallback: search DDG using the RERA ID directly to find the correct project page.
    Used when a name-based search returned a page that doesn't contain the expected RERA ID.
    """
    queries = [
        f"{rera_id} housing.com",
        f"{rera_id} {project_name} housing.com",
    ]
    return _ddg_first_url(queries)


# ── Page fetch ────────────────────────────────────────────────────────────────
def fetch_page(url: str, retries: int = 2) -> str | None:
    for attempt in range(retries + 1):
        try:
            resp = cf.get(url, headers=HOUSING_HEADERS, impersonate="chrome124", timeout=25)
            if resp.status_code == 200:
                return resp.text
            print(f"    Housing HTTP {resp.status_code}")
            if resp.status_code in (429, 503):
                time.sleep(30)
        except Exception as e:
            print(f"    Fetch error (attempt {attempt+1}): {e}")
            time.sleep(10)
    return None


# ── Data extraction ────────────────────────────────────────────────────────────
def normalize_bhk(s: str) -> str:
    m = re.match(r'(\d[\d\.]*)\s*BHK', s.strip(), re.IGNORECASE)
    return f"{m.group(1)} BHK" if m else s.strip().upper()


def extract_best_bhk_areas(text: str) -> dict:
    matches = re.findall(
        r'(\d[\d\.]*\s*BHK)[^\d]{0,30}?(\d[\d,]+)\s*(?:sq\.?\s*ft|sqft)',
        text, re.IGNORECASE
    )
    buckets: dict[str, Counter] = {}
    for bhk, area in matches:
        key = normalize_bhk(bhk)
        area_int = int(area.replace(",", ""))
        if 400 <= area_int <= 8000:
            buckets.setdefault(key, Counter())[str(area_int)] += 1
    result = {}
    for k, v in sorted(buckets.items()):
        top_area, top_count = v.most_common(1)[0]
        # "4000" with only one mention is a Housing.com boilerplate placeholder
        if top_area == "4000" and top_count == 1:
            continue
        result[k] = top_area
    return result


def extract_buildings(html: str) -> str | None:
    # Housing.com summary template: "5 Buildings - 1059 units"
    template = re.findall(r'(\d+)\s*Buildings?\s*-\s*\d+\s*units?', html, re.IGNORECASE)
    if template:
        return Counter(template).most_common(1)[0][0]
    # Contextual phrases
    ctx = re.findall(
        r'(?:consists?\s+of|has|with|comprising|total\s+of|spread\s+across)\s+(\d+)\s*(?:building|tower)s?',
        html, re.IGNORECASE
    )
    if ctx:
        return Counter(ctx).most_common(1)[0][0]
    # Last resort: repeated mention
    all_m = re.findall(r'(\d+)\s*(?:building|tower)s?', html, re.IGNORECASE)
    if all_m:
        c = Counter(all_m)
        top, count = c.most_common(1)[0]
        return top if count >= 2 else None
    return None


def fmt_price(v: str) -> str:
    n = int(v)
    cr = n / 1_00_00_000
    return f"{cr:.2f} Cr" if cr >= 1 else f"{n / 1_00_000:.2f} L"


def extract(html: str, rera_id: str) -> dict:
    result: dict = {}

    # RERA IDs on page
    found = list(set(re.findall(r'UPRERAPRJ\d+', html)))
    result["rera_ids_found"] = found
    result["rera_match"] = rera_id in found

    # Buildings
    result["buildings"] = extract_buildings(html)

    # Total units — prefer Housing.com summary template first
    units_tmpl = re.findall(r'\d+\s*Buildings?\s*-\s*(\d[\d,]*)\s*units?', html, re.IGNORECASE)
    if units_tmpl:
        result["total_units"] = units_tmpl[0].replace(",", "")
    else:
        units = re.findall(r'(\d[\d,]+)\s*(?:residential\s+)?units?', html, re.IGNORECASE)
        result["total_units"] = units[0].replace(",", "") if units else None

    # Land area
    land = re.findall(r'([\d\.]+)\s*acres?', html, re.IGNORECASE)
    result["land_area_acres"] = land[0] if land else None

    # BHK types (normalised)
    raw_bhk = re.findall(r'\d[\d\.]*\s*BHK', html, re.IGNORECASE)
    bhk_types = sorted(set(normalize_bhk(b) for b in raw_bhk))
    result["bhk_types"] = bhk_types or None

    # BHK-wise carpet areas
    bhk_areas = extract_best_bhk_areas(html)
    result["bhk_areas_sqft"] = bhk_areas if bhk_areas else None

    # Possession date
    poss = re.findall(r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[,\s]+20\d\d', html)
    result["possession_date"] = poss[0] if poss else None

    # Price range
    price_m = re.findall(
        r'\u20b9\s*([\d\.]+\s*(?:Cr|L))\s*[-\u2013]\s*\u20b9?\s*([\d\.]+\s*(?:Cr|L))',
        html
    )
    if price_m:
        result["price_range"] = f"\u20b9{price_m[0][0].strip()} - \u20b9{price_m[0][1].strip()}"
    else:
        low  = re.search(r'"lowPrice"\s*:\s*(\d+)', html)
        high = re.search(r'"highPrice"\s*:\s*(\d+)', html)
        if low and high:
            result["price_range"] = f"{fmt_price(low.group(1))} - {fmt_price(high.group(1))}"
        else:
            result["price_range"] = None

    # Developer
    dev = re.findall(r'(?:developed|built)\s+by\s+([A-Z][A-Za-z0-9 &\(\)\.]+?)[\.<\n]', html)
    result["developer"] = dev[0].strip() if dev else None

    return result


# ── Progress helpers ──────────────────────────────────────────────────────────
def load_progress() -> dict:
    if os.path.exists(PROG):
        with open(PROG, encoding="utf-8") as f:
            records = json.load(f)
        return {r["rera_id"]: r for r in records}
    return {}


def save_progress(progress: dict):
    with open(PROG, "w", encoding="utf-8") as f:
        json.dump(list(progress.values()), f, indent=2, ensure_ascii=False)


# ── Main enrichment loop ──────────────────────────────────────────────────────
def enrich_batch(projects: list, progress: dict, label: str = "Pass 1"):
    total = len(projects)
    for i, p in enumerate(projects, 1):
        rid   = p["rera_id"]
        name  = p["project_name"]
        dist  = p.get("district", "")

        print(f"\n[{label}] {i}/{total}  {name[:55]}")

        # ── URL discovery ──────────────────────────────────────────────────
        url = find_housing_url(name, dist)
        if not url:
            print(f"  URL not found — marking for retry")
            progress[rid] = {
                "rera_id": rid, "project_name": name,
                "housing_url": None, "error": "URL not found"
            }
            save_progress(progress)
            time.sleep(5)   # short pause before next search
            continue

        print(f"  URL: {url}")
        time.sleep(2)   # pause between DDG search and Housing fetch

        # ── Page fetch ─────────────────────────────────────────────────────
        html = fetch_page(url)
        if not html:
            print(f"  Page fetch failed")
            progress[rid] = {
                "rera_id": rid, "project_name": name,
                "housing_url": url, "error": "fetch failed"
            }
            save_progress(progress)
            time.sleep(5)
            continue

        # ── Extract ────────────────────────────────────────────────────────
        data = extract(html, rid)
        data.update({"rera_id": rid, "project_name": name, "housing_url": url})
        progress[rid] = data
        save_progress(progress)

        print(f"  RERA match={data['rera_match']}  "
              f"buildings={data['buildings']}  units={data['total_units']}  "
              f"BHK={data['bhk_types']}  price={data.get('price_range')}")

        # ── RERA-ID retry if page doesn't match ───────────────────────────
        if not data["rera_match"]:
            print(f"  RERA mismatch — retrying with RERA ID query")
            time.sleep(4)
            rera_url = find_housing_url_by_rera(rid, name)
            if rera_url and rera_url != url:
                print(f"  RERA URL: {rera_url}")
                time.sleep(2)
                rera_html = fetch_page(rera_url)
                if rera_html:
                    rera_data = extract(rera_html, rid)
                    if rera_data["rera_match"]:
                        rera_data.update({"rera_id": rid, "project_name": name, "housing_url": rera_url})
                        progress[rid] = rera_data
                        save_progress(progress)
                        print(f"  RERA retry matched!")
                        time.sleep(4)
                        continue
            print(f"  RERA retry did not resolve — keeping original page")

        time.sleep(4)   # polite gap between projects


# ── Merge with original dataset ───────────────────────────────────────────────
def merge_and_save(projects_raw: list, progress: dict):
    progress_by_id = {r["rera_id"]: r for r in progress.values()}
    merged = []
    for p in projects_raw:
        rid = p["rera_id"]
        row = dict(p)   # start with original RERA fields
        if rid in progress_by_id:
            enrich = progress_by_id[rid]
            row["housing_url"]     = enrich.get("housing_url")
            row["housing_rera_match"] = enrich.get("rera_match")
            row["buildings"]       = enrich.get("buildings")
            row["total_units"]     = enrich.get("total_units")
            row["land_area_acres"] = enrich.get("land_area_acres")
            row["bhk_types"]       = enrich.get("bhk_types")
            row["bhk_areas_sqft"]  = enrich.get("bhk_areas_sqft")
            row["possession_date"] = enrich.get("possession_date")
            row["price_range"]     = enrich.get("price_range")
            row["developer_housing"] = enrich.get("developer")
            row["enrich_error"]    = enrich.get("error")
        merged.append(row)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"\nMerged dataset saved -> {OUTPUT}")
    return merged


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    # Load raw projects
    with open(INPUT, encoding="utf-8") as f:
        projects_raw = json.load(f)
    print(f"Loaded {len(projects_raw)} projects from {INPUT}")

    # Load existing progress (resume support)
    progress = load_progress()
    done = {rid for rid, r in progress.items() if not r.get("error")}
    url_missing = {rid for rid, r in progress.items() if r.get("error") == "URL not found"}
    print(f"Progress: {len(done)} done, {len(url_missing)} URL-missing, "
          f"{len(projects_raw) - len(progress)} not yet attempted")

    # ── Pass 1: projects not yet attempted ───────────────────────────────
    todo_pass1 = [p for p in projects_raw if p["rera_id"] not in progress]
    if todo_pass1:
        print(f"\n=== Pass 1: {len(todo_pass1)} projects ===")
        enrich_batch(todo_pass1, progress, label="Pass 1")
    else:
        print("\nPass 1 already complete.")

    # ── Pass 2: retry URL-not-found after a cooldown ─────────────────────
    url_missing_after_p1 = [
        p for p in projects_raw
        if progress.get(p["rera_id"], {}).get("error") == "URL not found"
    ]
    if url_missing_after_p1:
        print(f"\n=== Pass 2: {len(url_missing_after_p1)} URL-not-found retries ===")
        print("Waiting 3 minutes before retrying to let DDG cooldown...")
        time.sleep(180)
        enrich_batch(url_missing_after_p1, progress, label="Pass 2")

    # ── Pass 3: RERA-ID retry for existing mismatches ────────────────────
    rera_mismatches = [
        p for p in projects_raw
        if (progress.get(p["rera_id"], {}).get("housing_url") and progress.get(p["rera_id"], {}).get("rera_match") is False)
        or (progress.get(p["rera_id"], {}).get("error") == "URL not found")
    ]
    if rera_mismatches:
        print(f"\n=== Pass 3: {len(rera_mismatches)} RERA-mismatch retries (RERA-ID search) ===")
        for i, p in enumerate(rera_mismatches, 1):
            rid  = p["rera_id"]
            name = p["project_name"]
            print(f"\n[Pass 3] {i}/{len(rera_mismatches)}  {name[:55]}")
            url = find_housing_url_by_rera(rid, name)
            if not url:
                print(f"  No URL found via RERA ID")
                time.sleep(5)
                continue
            existing_url = progress[rid].get("housing_url")
            if url == existing_url:
                print(f"  Same URL as before — skipping")
                time.sleep(4)
                continue
            print(f"  RERA URL: {url}")
            time.sleep(2)
            html = fetch_page(url)
            if not html:
                print(f"  Fetch failed")
                time.sleep(5)
                continue
            data = extract(html, rid)
            data.update({"rera_id": rid, "project_name": name, "housing_url": url})
            progress[rid] = data
            save_progress(progress)
            print(f"  match={data['rera_match']}  BHK={data['bhk_types']}  "
                  f"price={data.get('price_range')}")
            time.sleep(4)

    # ── Final stats ───────────────────────────────────────────────────────
    enriched_count  = sum(1 for r in progress.values() if not r.get("error"))
    url_found_count = sum(1 for r in progress.values() if r.get("housing_url"))
    rera_match_count = sum(1 for r in progress.values() if r.get("rera_match"))
    buildings_count  = sum(1 for r in progress.values() if r.get("buildings"))

    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"  Total projects       : {len(projects_raw)}")
    print(f"  URLs found           : {url_found_count}")
    print(f"  Pages fetched        : {enriched_count}")
    print(f"  RERA ID matched      : {rera_match_count}")
    print(f"  Buildings extracted  : {buildings_count}")
    print(f"{'='*60}")

    # ── Merge and save final output ───────────────────────────────────────
    merge_and_save(projects_raw, progress)
    print("\nDone.")


if __name__ == "__main__":
    main()
