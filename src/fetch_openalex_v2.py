"""Fetch papers from OpenAlex with API key support and Crossref fallback.

Run: python -m src.fetch_openalex_v2

Reads OPENALEX_API_KEY from environment or .env file.
Logs errors to logs/openalex_errors.log.
Falls back to Crossref ISSN enumeration if OpenAlex fails.
"""
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import requests
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config_multi.yaml"
RAW_DIR = ROOT / "data" / "raw" / "openalex"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "openalex_errors.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv(ROOT / ".env")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")


def http_get(session: requests.Session, url: str, params: dict, retries: int = 3) -> Optional[requests.Response]:
    """GET with exponential backoff."""
    delay = 1
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries:
                logger.error(f"Failed {retries} times: {e}")
                return None
            time.sleep(delay)
            delay *= 2
    return None


def fetch_openalex_year(session: requests.Session, journal_key: str, journal_name: str, year: int) -> list:
    """Fetch from OpenAlex using display_name filter."""
    works = []
    cursor = "*"
    params_base = {
        "filter": f'display_name:"{journal_name}",publication_year:{year}',
        "per-page": 100,
        "select": "id,doi,title,authors,publication_year,abstract_inverted_index,primary_location,landing_page_url",
    }
    if OPENALEX_API_KEY:
        params_base["api_key"] = OPENALEX_API_KEY
    
    while cursor:
        params = {**params_base, "cursor": cursor}
        
        resp = http_get(session, "https://api.openalex.org/works", params)
        if not resp:
            logger.warning(f"{journal_key} {year}: OpenAlex request failed")
            return []
        
        if resp.status_code != 200:
            logger.error(
                f"{journal_key} {year}: OpenAlex returned {resp.status_code}\n"
                f"URL: {resp.url}\n"
                f"Response: {resp.text[:500]}"
            )
            return []
        
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"{journal_key} {year}: JSON decode failed: {e}")
            return []
        
        results = data.get("results", [])
        works.extend(results)
        cursor = data.get("meta", {}).get("next_cursor")
        
        if results:
            print(f"  {journal_key} {year}: fetched {len(results)} (total: {len(works)})")
        
        time.sleep(0.5)
    
    return works


def fetch_crossref_by_issn(session: requests.Session, issn: str, year: int, journal_key: str) -> list:
    """Fetch from Crossref by ISSN and year."""
    works = []
    cursor = "*"
    
    while cursor:
        params = {
            "filter": f"from-pub-date:{year}-01-01,until-pub-date:{year}-12-31,type:journal-article",
            "rows": 100,
            "cursor": cursor,
        }
        if cursor != "*":
            params["cursor"] = cursor
        
        resp = http_get(session, f"https://api.crossref.org/journals/{issn}/works", params)
        if not resp:
            logger.warning(f"{journal_key} (ISSN {issn}) {year}: Crossref request failed")
            break
        
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"{journal_key} (ISSN {issn}) {year}: JSON decode failed: {e}")
            break
        
        items = data.get("message", {}).get("items", [])
        for item in items:
            work = {
                "id": f"crossref_{item.get('DOI')}",
                "doi": item.get("DOI"),
                "title": " ".join(item.get("title", [])) if isinstance(item.get("title"), list) else item.get("title"),
                "authors": [{"author": {"display_name": a.get("literal", a.get("name", ""))}} for a in item.get("author", [])],
                "publication_year": item.get("published-print", {}).get("date-parts", [[year]])[0][0],
                "abstract_inverted_index": None,
                "primary_location": None,
                "landing_page_url": item.get("URL"),
                "source_enumeration": "crossref",
            }
            works.append(work)
        
        cursor = data.get("message", {}).get("next-cursor")
        
        if items:
            print(f"  {journal_key} (ISSN {issn}) {year}: fetched {len(items)} (total: {len(works)})")
        else:
            break
        
        time.sleep(0.5)
    
    return works


def load_config():
    """Load journals config."""
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    return cfg.get("journals", [])


def main():
    """Fetch papers for all journals."""
    session = requests.Session()
    session.headers.update({"User-Agent": "AJ-Multi-Journal-Study/1.0"})
    
    journals = load_config()
    
    # Journal ISSNs for Crossref fallback
    journal_issns = {
        "aer": ["0002-8282"],  # American Economic Review
        "qje": ["0033-5533", "1531-4650"],  # Quarterly Journal of Economics
        "aej_micro": ["1945-7669", "1945-7685"],  # AEJ: Microeconomics
    }
    
    print("Fetching papers with OpenAlex (fallback: Crossref)...\n")
    
    for journal in journals:
        key = journal["key"]
        name = journal["name"]
        start_year = journal["start_year"]
        end_year = journal["end_year"]
        
        print(f"\n{key} ({name}, {start_year}–{end_year})")
        
        for year in range(start_year, end_year + 1):
            # Try OpenAlex first
            works = fetch_openalex_year(session, key, name, year)
            
            # If OpenAlex fails, try Crossref
            if not works:
                logger.info(f"{key} {year}: Fallback to Crossref")
                for issn in journal_issns.get(key, []):
                    works.extend(fetch_crossref_by_issn(session, issn, year, key))
            
            # Save to JSONL
            if works:
                out_dir = RAW_DIR / key
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"{year}.jsonl"
                
                with open(out_file, "w") as f:
                    for w in works:
                        f.write(json.dumps(w) + "\n")
                
                print(f"  → Wrote {len(works)} to {out_file.name}")
    
    print("\nDone!")


if __name__ == "__main__":
    main()
