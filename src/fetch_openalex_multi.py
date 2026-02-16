"""Fetch papers from OpenAlex for multiple journals and year ranges.

Run: python -m src.fetch_openalex_multi

Reads config_multi.yaml and fetches from OpenAlex, saving to:
  data/raw/openalex/{journal_key}/{year}.jsonl
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config_multi.yaml"
RAW_DIR = ROOT / "data" / "raw" / "openalex"


def http_get(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15, retries: int = 3) -> Optional[requests.Response]:
    """HTTP GET with exponential backoff retry."""
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries:
                print(f"  Failed after {retries} attempts: {e}")
                return None
            time.sleep(delay)
            delay *= 2
    return None


def fetch_journal_year(session: requests.Session, journal_key: str, journal_name: str, year: int) -> list[dict]:
    """Fetch all works for a journal-year from OpenAlex with cursor pagination."""
    works = []
    cursor = "*"
    per_page = 100
    url = "https://api.openalex.org/works"
    
    while cursor:
        params = {
            "filter": f"display_name:\"{journal_name}\",publication_year:{year}",
            "per-page": per_page,
            "cursor": cursor,
            "select": "id,doi,title,authors,publication_year,abstract_inverted_index,primary_location,landing_page_url",
        }
        
        resp = http_get(session, url, params=params, timeout=15, retries=3)
        if not resp:
            print(f"  {journal_key} {year}: request failed, stopping")
            break
        
        try:
            data = resp.json()
        except Exception as e:
            print(f"  {journal_key} {year}: JSON decode failed ({e})")
            break
        
        results = data.get("results", [])
        works.extend(results)
        cursor = data.get("meta", {}).get("next_cursor")
        print(f"  {journal_key} {year}: fetched {len(results)} works (cursor: {cursor is not None})")
        time.sleep(0.5)  # rate limit
    
    return works


def load_config() -> list[dict]:
    """Load journals config from YAML."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    return cfg.get("journals", [])


def main():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (AEJ-Micro Study)"})
    
    journals = load_config()
    print(f"Loaded {len(journals)} journals from config")
    
    for journal in journals:
        key = journal["key"]
        name = journal["name"]
        start_year = journal["start_year"]
        end_year = journal["end_year"]
        
        print(f"\nFetching {key} ({start_year}–{end_year})...")
        
        for year in range(start_year, end_year + 1):
            works = fetch_journal_year(session, key, name, year)
            
            if works:
                out_dir = RAW_DIR / key
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"{year}.jsonl"
                
                with open(out_file, "w", encoding="utf-8") as fh:
                    for work in works:
                        fh.write(json.dumps(work, ensure_ascii=False) + "\n")
                
                print(f"  Wrote {len(works)} works to {out_file}")
            else:
                print(f"  {key} {year}: no works fetched")
    
    print("\nDone.")


if __name__ == "__main__":
    main()
