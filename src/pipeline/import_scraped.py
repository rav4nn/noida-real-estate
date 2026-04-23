"""
Diff the housing.com scraper's output against our current dataset and surface
candidate projects to add to data/manual_additions.json.

Inputs:
  C:/Users/harde/Projects/housing.com-scraper/output/results.json   (discover-mode run)
  data/projects_final.json                                          (current dataset)

Output:
  data/missing_projects_report.json   — candidates grouped by confidence

Matching strategy:
  1. Exact URL match on housing_url (most reliable)
  2. Fuzzy match on normalized project name + developer
  3. Unmatched entries are written to the report as "new"

The report is NOT auto-merged into manual_additions.json. Review it first,
copy the entries you trust, and paste them into data/manual_additions.json.
"""

import json
import os
import re
import sys

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE            = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRAPER_OUTPUT  = r"C:\Users\harde\Projects\housing.com-scraper\output\results.json"
FINAL_DATASET   = os.path.join(BASE, "data", "projects_final.json")
REPORT_OUTPUT   = os.path.join(BASE, "data", "missing_projects_report.json")


def normalize_name(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace, trim common suffixes."""
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\(.*?\)", " ", s)                    # drop parenthetical
    s = re.sub(r"\b(phase|ph|tower|towers)\s*[-\d]*\b", " ", s)  # phase/tower markers
    s = re.sub(r"[^a-z0-9]+", " ", s)                 # punctuation → space
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    scraped = load_json(SCRAPER_OUTPUT)
    if scraped is None:
        print(f"ERROR: scraper output not found at {SCRAPER_OUTPUT}")
        print("Run the housing.com scraper in discover mode first.")
        sys.exit(1)

    existing = load_json(FINAL_DATASET) or []

    # Indexes for fast lookup
    existing_urls = {p["housing_url"] for p in existing if p.get("housing_url")}
    existing_names = {
        normalize_name(p.get("project_name", "")): p for p in existing
        if p.get("project_name")
    }

    matched_by_url = 0
    matched_by_name = 0
    candidates = []

    for s in scraped:
        s_url = s.get("housing_url")
        s_name = s.get("page_project_name") or s.get("input_name") or ""
        s_norm = normalize_name(s_name)

        if s_url and s_url in existing_urls:
            matched_by_url += 1
            continue

        if s_norm and s_norm in existing_names:
            matched_by_name += 1
            continue

        # Fuzzy: check if normalized scraped name is a substring/superstring of any existing
        fuzzy_hit = None
        for norm_existing in existing_names:
            if not norm_existing or not s_norm:
                continue
            if (s_norm in norm_existing) or (norm_existing in s_norm):
                # Require a minimum overlap length so tiny tokens don't match
                if min(len(s_norm), len(norm_existing)) >= 8:
                    fuzzy_hit = existing_names[norm_existing].get("project_name")
                    break
        if fuzzy_hit:
            matched_by_name += 1
            continue

        # It's a candidate new project
        candidate = {
            "suggested_rera_id": f"MANUAL_{re.sub(r'[^A-Z0-9]+', '_', s_name.upper()).strip('_')}",
            "project_name": s_name,
            "promoter_name": s.get("developer"),
            "project_type": "Residential",
            "district": "Gautam Buddha Nagar",
            "housing_url": s_url.rstrip("\\") if s_url else s_url,
            "buildings": s.get("buildings"),
            "total_units": s.get("total_units"),
            "land_area_acres": s.get("land_area_acres"),
            "bhk_types": s.get("bhk_types"),
            "bhk_areas_sqft": s.get("bhk_areas_sqft"),
            "possession_date": s.get("possession_date"),
            "price_range": s.get("price_range"),
            "rera_ids_on_page": s.get("rera_ids_on_page"),
            "manual_entry": True,
            "notes": "Discovered via housing.com scraper, not in UP RERA dataset",
        }
        candidates.append(candidate)

    # Sort candidates: entries with RERA IDs on their housing.com page first
    # (those might actually be in the RERA dataset under a name mismatch and
    # deserve a closer look before dropping straight into manual_additions).
    candidates.sort(key=lambda c: (
        0 if c.get("rera_ids_on_page") else 1,
        (c.get("project_name") or "").lower(),
    ))

    report = {
        "summary": {
            "scraped_total": len(scraped),
            "matched_by_url": matched_by_url,
            "matched_by_name": matched_by_name,
            "candidates_new": len(candidates),
        },
        "candidates": candidates,
    }

    with open(REPORT_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Scraper output      : {len(scraped)} projects")
    print(f"Already in dataset  : {matched_by_url + matched_by_name}")
    print(f"  by URL            : {matched_by_url}")
    print(f"  by name           : {matched_by_name}")
    print(f"Candidate additions : {len(candidates)}")
    print(f"Report -> {REPORT_OUTPUT}")


if __name__ == "__main__":
    main()
