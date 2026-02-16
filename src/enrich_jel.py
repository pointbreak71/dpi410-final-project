"""Enrich AEJ raw OpenAlex JSONL with JEL codes using multiple fallbacks.

Run: python -m src.enrich_jel
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from .utils_minimal import http_get, normalize_doi, extract_jel_from_text, safe_cache_path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
CACHE_DIR = RAW_DIR / "cache"


def fetch_aea_by_doi(session: requests.Session, doi: str) -> Optional[Dict[str, List[str]]]:
    """Follow DOI to landing page and try to parse JEL codes from AEA/landing HTML."""
    url = f"https://doi.org/{doi}"
    # cache DOI redirect target
    cache_target = safe_cache_path(f"doi_redirect_{doi}", CACHE_DIR)
    if cache_target.exists():
        final_url = cache_target.read_text(encoding='utf-8')
    else:
        try:
            resp = http_get(session, url, timeout=20)
            if resp is None:
                return None
            final_url = resp.url
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_target.write_text(final_url, encoding='utf-8')
        except Exception:
            return None

    # fetch HTML (cached)
    cache_html = safe_cache_path(f"html_{final_url}", CACHE_DIR)
    if cache_html.exists():
        html = cache_html.read_text(encoding='utf-8')
    else:
        resp = http_get(session, final_url)
        if resp is None:
            return None
        html = resp.text
        cache_html.write_text(html, encoding='utf-8')

    # parse for JEL labels
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text(separator=' \n ')
    jels = extract_jel_from_text(text)
    if jels:
        return {"jel_codes": jels, "jel_raw": text, "source": "aea_page", "url": final_url}
    return None


def fetch_crossref_by_doi(session: requests.Session, doi: str) -> Optional[Dict[str, List[str]]]:
    cache = safe_cache_path(f"crossref_{doi}.json", CACHE_DIR)
    if cache.exists():
        data = json.loads(cache.read_text(encoding='utf-8'))
    else:
        url = f"https://api.crossref.org/works/{doi}"
        resp = http_get(session, url)
        if resp is None:
            return None
        data = resp.json()
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(data), encoding='utf-8')
    # search in the Crossref message for JEL-like strings
    msg = data.get('message', {})
    txt = json.dumps(msg)
    jels = extract_jel_from_text(txt)
    if jels:
        return {"jel_codes": jels, "jel_raw": txt, "source": "crossref", "url": msg.get('URL')}
    return None


def fetch_openalex_jel(session: requests.Session, openalex_id: str) -> Optional[Dict[str, List[str]]]:
    cache = safe_cache_path(f"openalex_{openalex_id.replace('/', '_')}.json", CACHE_DIR)
    if cache.exists():
        data = json.loads(cache.read_text(encoding='utf-8'))
    else:
        url = f"https://api.openalex.org/works/{openalex_id.split('/')[-1]}"
        resp = http_get(session, url)
        if resp is None:
            return None
        data = resp.json()
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(data), encoding='utf-8')
    txt = json.dumps(data)
    jels = extract_jel_from_text(txt)
    if jels:
        return {"jel_codes": jels, "jel_raw": txt, "source": "openalex", "url": None}
    return None


def fetch_ideas_by_doi(session: requests.Session, doi: str) -> Optional[Dict[str, List[str]]]:
    # search IDEAS for DOI and scrape
    search_url = "https://ideas.repec.org/search.html"
    resp = http_get(session, search_url, params={"q": doi})
    if resp is None:
        return None
    soup = BeautifulSoup(resp.text, 'lxml')
    a = soup.find('a', href=True)
    if not a:
        return None
    href = a.get('href')
    if href.startswith('/'):
        record_url = 'https://ideas.repec.org' + href
    else:
        record_url = href
    r2 = http_get(session, record_url)
    if r2 is None:
        return None
    text = BeautifulSoup(r2.text, 'lxml').get_text(separator=' \n ')
    jels = extract_jel_from_text(text)
    if jels:
        return {"jel_codes": jels, "jel_raw": text, "source": "ideas", "url": record_url}
    return None


def process_all():
    session = requests.Session()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # find raw files
    raw_files = sorted(p for p in RAW_DIR.glob('openalex_AEJ_Micro_*.jsonl'))
    for p in raw_files:
        out_p = p.with_name(p.stem + '_enriched.jsonl')
        if out_p.exists():
            print('Skipping already enriched', out_p)
            continue
        with open(p, 'r', encoding='utf-8') as fh_in, open(out_p, 'w', encoding='utf-8') as fh_out:
            for line in fh_in:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                doi = obj.get('doi') or (obj.get('ids') or {}).get('doi')
                doi_norm = normalize_doi(doi)
                openalex_id = obj.get('id')
                result = None
                if doi_norm:
                    # 1) AEA/social article landing
                    result = fetch_aea_by_doi(session, doi_norm)
                    # small pause
                    time.sleep(0.2)
                if not result and doi_norm:
                    result = fetch_crossref_by_doi(session, doi_norm)
                    time.sleep(0.2)
                if not result and openalex_id:
                    result = fetch_openalex_jel(session, openalex_id)
                    time.sleep(0.2)
                if not result and doi_norm:
                    result = fetch_ideas_by_doi(session, doi_norm)
                    time.sleep(0.2)

                if result:
                    obj['jel_codes'] = result.get('jel_codes', [])
                    obj['jel_raw'] = result.get('jel_raw', '')
                    obj['jel_source'] = result.get('source')
                    obj['landing_url'] = result.get('url')
                else:
                    obj['jel_codes'] = []
                    obj['jel_raw'] = ''
                    obj['jel_source'] = None
                    obj['landing_url'] = None
                fh_out.write(json.dumps(obj, ensure_ascii=False) + '\n')
        print('Wrote enriched file', out_p)


if __name__ == '__main__':
    process_all()
