"""Enrich raw OpenAlex papers with JEL codes from multiple sources.

Run: python -m src.enrich_jel_multi

Reads raw JSONL from data/raw/openalex/{journal_key}/{year}.jsonl and attempts to populate JEL codes
using fallback strategy: AEA landing page → AEA search by DOI → Crossref → OpenAlex concepts → missing.
Writes enriched JSONL to same directory with _enriched.jsonl suffix.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import yaml

from src.utils_minimal import (
    extract_jel_from_aea_html,
    extract_jel_from_text,
    fetch_crossref_by_doi,
    http_get,
    normalize_doi,
    search_aea_by_doi,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config_multi.yaml"
RAW_DIR = ROOT / "data" / "raw" / "openalex"
HTML_CACHE_DIR = ROOT / "data" / "raw" / "html"


def safe_cache_filename(url: str) -> str:
    """Generate safe cache filename from URL."""
    import hashlib
    return hashlib.sha1(url.encode()).hexdigest()[:16]


def parse_concepts(work: dict) -> list[str]:
    """Extract concept display names from OpenAlex work object."""
    concepts = []
    for c in work.get("concepts", []):
        name = c.get("display_name")
        if name:
            concepts.append(name)
    return concepts


def extract_jel_for_paper(session: requests.Session, work: dict, journal_key: str) -> dict:
    """Attempt to extract JEL codes with priority fallback strategy."""
    result = {
        "jel_codes": [],
        "jel_raw": "",
        "jel_source": "missing",
    }
    
    landing_url = work.get("landing_page_url") or (work.get("primary_location") or {}).get("url")
    doi = normalize_doi(work.get("doi"))
    
    # Strategy A: If landing_page_url is on aeaweb.org, fetch directly
    if landing_url and "aeaweb.org" in landing_url.lower():
        try:
            cache_file = HTML_CACHE_DIR / journal_key / safe_cache_filename(landing_url)
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            
            if cache_file.exists():
                with open(cache_file, "r", encoding="utf-8") as fh:
                    html = fh.read()
            else:
                resp = http_get(session, landing_url, timeout=10, retries=2)
                if resp and resp.status_code == 200:
                    html = resp.text
                    with open(cache_file, "w", encoding="utf-8") as fh:
                        fh.write(html)
                else:
                    html = None
            
            if html:
                jel_dict = extract_jel_from_aea_html(html)
                if jel_dict:
                    result["jel_codes"] = jel_dict.get("jel_codes", [])
                    result["jel_raw"] = jel_dict.get("jel_raw", "")[:200]
                    result["jel_source"] = "aea_page"
                    return result
        except Exception as e:
            # Silently continue to next strategy
            pass
    
    # Strategy B: If landing_url missing or not AEA, try to search AEA by DOI
    if doi:
        try:
            aea_url = search_aea_by_doi(session, doi)
            if aea_url:
                cache_file = HTML_CACHE_DIR / journal_key / safe_cache_filename(aea_url)
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                
                if cache_file.exists():
                    with open(cache_file, "r", encoding="utf-8") as fh:
                        html = fh.read()
                else:
                    resp = http_get(session, aea_url, timeout=10, retries=2)
                    if resp and resp.status_code == 200:
                        html = resp.text
                        with open(cache_file, "w", encoding="utf-8") as fh:
                            fh.write(html)
                    else:
                        html = None
                
                if html:
                    jel_dict = extract_jel_from_aea_html(html)
                    if jel_dict:
                        result["jel_codes"] = jel_dict.get("jel_codes", [])
                        result["jel_raw"] = jel_dict.get("jel_raw", "")[:200]
                        result["jel_source"] = "aea_search"
                        return result
        except Exception:
            pass
    
    # Strategy C: Crossref by DOI
    if doi:
        try:
            cr_result = fetch_crossref_by_doi(session, doi)
            if cr_result and cr_result.get("jel_codes"):
                result["jel_codes"] = cr_result["jel_codes"]
                result["jel_raw"] = cr_result.get("jel_raw", "")[:200]
                result["jel_source"] = "crossref"
                return result
        except Exception:
            pass
    
    # Strategy D: OpenAlex concepts (not JEL but helpful)
    concepts = parse_concepts(work)
    if concepts:
        result["jel_codes"] = []  # We don't have JEL from OpenAlex
        result["jel_raw"] = ", ".join(concepts[:5])
        result["jel_source"] = "openalex_concepts"
        # Don't return here; let it fall through to "missing" if no JEL found
    
    return result


def enrich_journal_year(session: requests.Session, journal_key: str, year: int) -> int:
    """Read raw JSONL, enrich with JEL, write enriched JSONL. Return count of enriched papers."""
    raw_file = RAW_DIR / journal_key / f"{year}.jsonl"
    if not raw_file.exists():
        return 0
    
    enriched_file = RAW_DIR / journal_key / f"{year}_enriched.jsonl"
    enriched_papers = []
    
    with open(raw_file, "r", encoding="utf-8") as fh:
        for line_num, line in enumerate(fh):
            try:
                work = json.loads(line)
            except Exception:
                continue
            
            # Enrich with JEL
            jel_info = extract_jel_for_paper(session, work, journal_key)
            work["jel_codes"] = jel_info["jel_codes"]
            work["jel_raw"] = jel_info["jel_raw"]
            work["jel_source"] = jel_info["jel_source"]
            
            enriched_papers.append(work)
            time.sleep(0.1)  # rate limit
    
    # Write enriched JSONL
    with open(enriched_file, "w", encoding="utf-8") as fh:
        for paper in enriched_papers:
            fh.write(json.dumps(paper, ensure_ascii=False) + "\n")
    
    return len(enriched_papers)


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
    print(f"Enriching {len(journals)} journals...")
    
    for journal in journals:
        key = journal["key"]
        start_year = journal["start_year"]
        end_year = journal["end_year"]
        
        print(f"\nEnriching {key} ({start_year}–{end_year})...")
        
        for year in range(start_year, end_year + 1):
            count = enrich_journal_year(session, key, year)
            if count > 0:
                print(f"  {key} {year}: enriched {count} papers")
    
    print("\nDone.")


if __name__ == "__main__":
    main()
