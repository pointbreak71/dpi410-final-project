"""Enrich OpenAlex raw files with JEL codes fetched from RePEc (IDEAS) when possible.

Run as: python -m src.fetch_repec_jel
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
import yaml
from typing import List, Dict, Any

from .utils import get_session, do_get
from bs4 import BeautifulSoup
import re


ROOT = Path(__file__).resolve().parents[1]


def load_config(path: Path | str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def fetch_jel_for_doi(session, doi: str, api_key: str) -> List[str]:
    """Best-effort: try RePEc/IDEAS search page for DOI and scrape JEL codes.

    Steps:
    1. If an `api_key` is present, keep the placeholder attempt (user can configure).
    2. Try IDEAS search page: https://ideas.repec.org/search.html?q={doi}
       - If a result page for the work is found, fetch it and scrape for JEL codes.
    Returns a list of JEL codes (may be empty).
    """
    # 1) Placeholder API attempt if an api_key is provided (keeps backward compatibility)
    if api_key:
        try:
            url = "https://api.repec.org/jel"
            params = {"doi": doi, "api_key": api_key}
            resp = do_get(session, url, params=params)
            data = resp.json()
            return data.get("jel") or []
        except Exception:
            pass

    # 2) Try IDEAS search scrape
    try:
        search_url = "https://ideas.repec.org/search.html"
        resp = do_get(session, search_url, params={"q": doi})
        text = resp.text
        soup = BeautifulSoup(text, "lxml")
        # Look for first result link to a record page
        a = soup.find("a", href=re.compile(r"/[^/]+/[^/]+/[^/]+\.html$"))
        if not a:
            # fallback: any link containing '/a/' or '/p/' or '/i/'
            a = soup.find("a", href=re.compile(r"/(a|p|i)/"))
        if not a:
            return []
        href = a.get("href")
        # Build absolute URL
        if href.startswith("/"):
            record_url = "https://ideas.repec.org" + href
        else:
            record_url = href
        r2 = do_get(session, record_url)
        s2 = BeautifulSoup(r2.text, "lxml")
        text2 = s2.get_text(separator=" \n ")
        # Search for JEL codes e.g., 'JEL Classification: D2, L1'
        m = re.search(r"JEL classification[:\s]*([A-Z0-9,\.\s]+)", text2, re.IGNORECASE)
        jel_codes = []
        if m:
            codes = m.group(1)
            # split by non-alphanumeric
            for part in re.split(r"[;,\s]+", codes):
                part = part.strip().upper()
                if re.match(r"^[A-Z]\d+", part):
                    jel_codes.append(part)
        else:
            # fallback: try to find anchor links to JEL pages
            for a in s2.find_all("a", href=True):
                if "/jel/" in a.get("href"):
                    t = a.get_text().strip().upper()
                    if re.match(r"^[A-Z]\d+", t):
                        jel_codes.append(t)
        return list(dict.fromkeys(jel_codes))
    except Exception:
        return []


def process_all_raw(config_path: str = "config.yaml"):
    cfg = load_config(config_path)
    rawdir = ROOT / "data" / "raw"
    api_key = os.environ.get("REPEC_API_KEY")
    # also check .env
    try:
        from dotenv import load_dotenv

        load_dotenv()
        api_key = api_key or os.environ.get("REPEC_API_KEY")
    except Exception:
        pass

    session = get_session()
    for p in rawdir.glob("openalex_*.jsonl"):
        out_p = p.with_name(p.stem + "_repec.jsonl")
        with open(p, "r", encoding="utf-8") as fh_in, open(out_p, "w", encoding="utf-8") as fh_out:
            for line in fh_in:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                doi = None
                # DOI often at 'doi' or in 'ids'
                if obj.get("doi"):
                    doi = obj.get("doi")
                else:
                    ids = obj.get("ids", {})
                    doi = ids.get("doi")
                jel_codes = []
                if doi:
                    jel_codes = fetch_jel_for_doi(session, doi, api_key)
                    # be gentle
                    time.sleep(0.5)
                obj["repec_jel"] = jel_codes
                fh_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
        print(f"Wrote enriched file {out_p}")


if __name__ == "__main__":
    process_all_raw()
