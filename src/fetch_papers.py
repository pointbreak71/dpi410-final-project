"""Multi-journal fetch for recent years (testing pipeline).

Run: python -m src.fetch_papers

Fetches from OpenAlex using display_name filter for three journals.
Saves to data/raw/openalex/{journal_key}/{year}.jsonl
"""
import json
import time
from pathlib import Path
from typing import Optional

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "openalex"


def http_get_retry(session: requests.Session, url: str, params: dict, retries: int = 3) -> Optional[requests.Response]:
    """Simple retry wrapper."""
    delay = 1
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries:
                print(f"    Failed after {retries} retries: {e}")
                return None
            time.sleep(delay)
            delay *= 2
    return None


def fetch_year(session: requests.Session, journal_name: str, journal_key: str, year: int) -> int:
    """Fetch all works for a journal-year from OpenAlex."""
    works = []
    cursor = "*"
    count = 0
    
    while cursor:
        params = {
            "filter": f"display_name:\"{journal_name}\",publication_year:{year}",
            "per-page": 100,
            "cursor": cursor,
            "select": "id,doi,title,authors,publication_year,abstract_inverted_index,primary_location,landing_page_url",
        }
        
        resp = http_get_retry(session, "https://api.openalex.org/works", params)
        if not resp:
            print(f"  {journal_key} {year}: request failed")
            break
        
        data = resp.json()
        results = data.get("results", [])
        works.extend(results)
        cursor = data.get("meta", {}).get("next_cursor")
        
        if results:
            count += len(results)
            print(f"  {journal_key} {year}: fetched {len(results)} (total: {count})")
        
        time.sleep(0.5)
    
    # Save to JSONL
    if works:
        out_dir = RAW_DIR / journal_key
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{year}.jsonl"
        
        with open(out_file, "w") as f:
            for w in works:
                f.write(json.dumps(w) + "\n")
        
        print(f"  → Wrote {len(works)} to {out_file.name}")
    
    return len(works)


def main():
    """Fetch from three journals: AER, QJE, AEJ Micro (recent years for testing)."""
    session = requests.Session()
    session.headers.update({"User-Agent": "AJ-Study/1.0"})
    
    # Recent years for testing, then expand if needed
    journals = [
        {"key": "aer", "name": "American Economic Review", "years": range(2015, 2026)},
        {"key": "qje", "name": "Quarterly Journal of Economics", "years": range(2015, 2026)},
        {"key": "aej_micro", "name": "American Economic Journal: Microeconomics", "years": range(2015, 2026)},
    ]
    
    print("Fetching papers from 3 journals (2015–2025)...\n")
    
    for journal in journals:
        print(f"Fetching {journal['key']}...")
        for year in journal["years"]:
            fetch_year(session, journal["name"], journal["key"], year)
    
    print("\nDone!")


if __name__ == "__main__":
    main()
