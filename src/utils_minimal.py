"""Minimal utilities: HTTP with retries, abstract reconstruction, JEL extraction."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


def http_get(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None, timeout: int = 15, retries: int = 3) -> Optional[requests.Response]:
    delay = 1.0
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == retries:
                return None
            time.sleep(delay)
            delay *= 2
    return None


def reconstruct_abstract(inverted_index: Optional[Dict[str, List[int]]]) -> str:
    if not inverted_index:
        return ""
    try:
        max_pos = 0
        for token, positions in inverted_index.items():
            if positions:
                max_pos = max(max_pos, max(positions))
        words = [""] * (max_pos + 1)
        for token, positions in inverted_index.items():
            for p in positions:
                words[p] = token
        return " ".join(w for w in words if w)
    except Exception:
        return ""


def normalize_doi(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = raw.strip()
    if s.lower().startswith("http"):
        # remove URL prefix
        s = re.sub(r"https?://(dx\.)?doi\.org/", "", s, flags=re.I)
    s = s.replace("doi:", "")
    s = s.strip().lower()
    return s or None


def extract_jel_from_text(text: str) -> List[str]:
    if not text:
        return []
    # capture A-Z followed by 1 or 2 digits, e.g., L1, L13, D43
    pattern = re.compile(r"\b([A-Z]\d{1,2})\b")
    found = pattern.findall(text.upper())
    # Filter plausible JEL codes: letter + 1-2 digits
    uniques = []
    for f in found:
        if f not in uniques:
            uniques.append(f)
    return uniques


def safe_cache_path(key: str, cache_dir: Path) -> Path:
    # create filename-safe key
    name = re.sub(r"[^0-9A-Za-z._-]", "_", key)
    return cache_dir / name


def save_jsonl(path: Path, items: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for it in items:
            fh.write(json.dumps(it, ensure_ascii=False) + "\n")


def search_aea_by_doi(session: requests.Session, doi: str) -> Optional[str]:
    """Search AEA website by DOI and return the first article URL found."""
    if not doi:
        return None
    try:
        from bs4 import BeautifulSoup
        # Construct AEA site search URL
        search_url = f"https://www.aeaweb.org/articles?id={doi.strip()}"
        # Try DOI-based search in AEA articles
        resp = http_get(session, "https://www.aeaweb.org/articles/search", params={"q": doi}, timeout=10, retries=2)
        if resp and resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for article links
            for link in soup.find_all("a", href=re.compile(r"/articles\?")):
                return "https://www.aeaweb.org" + link["href"] if link.get("href", "").startswith("/") else link.get("href")
    except Exception:
        pass
    return None


def extract_jel_from_aea_html(html: str) -> Optional[Dict[str, Any]]:
    """Extract JEL and abstract from AEA article HTML."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        
        # Look for JEL classification section
        jel_codes = []
        jel_raw = ""
        
        # Try common patterns for JEL display on AEA articles
        for section in soup.find_all(["div", "p"]):
            text = section.get_text(strip=True) if section else ""
            if any(label in text.upper() for label in ["JEL CLASSIF", "JEL CODE", "JEL:", "CLASSIF"]):
                jel_raw = text
                jel_codes = extract_jel_from_text(text)
                if jel_codes:
                    return {"jel_codes": jel_codes, "jel_raw": jel_raw}
        
        # Fallback: look for any pattern in visible text
        all_text = soup.get_text()
        for match in re.finditer(r"(?:JEL|classif)[:\s]+([A-Z0-9,\s]+?)(?:\n|$)", all_text, re.I):
            candidate = match.group(1)
            codes = extract_jel_from_text(candidate)
            if codes:
                return {"jel_codes": codes, "jel_raw": candidate[:200]}
        
    except Exception:
        pass
    return None


def fetch_crossref_by_doi(session: requests.Session, doi: str) -> Optional[Dict[str, Any]]:
    """Fetch paper metadata from Crossref API by DOI."""
    if not doi:
        return None
    try:
        doi_clean = normalize_doi(doi)
        if not doi_clean:
            return None
        url = f"https://api.crossref.org/works/{doi_clean}"
        resp = http_get(session, url, timeout=10, retries=2)
        if resp and resp.status_code == 200:
            data = resp.json()
            work = data.get("message", {})
            
            # Look for JEL in subject or keyword fields
            subjects = work.get("subject", [])
            jel_codes = []
            for subj in subjects:
                codes = extract_jel_from_text(subj)
                jel_codes.extend(codes)
            
            jel_codes = list(set(jel_codes))
            if jel_codes:
                return {"jel_codes": sorted(jel_codes), "jel_raw": ", ".join(subjects[:3]) if subjects else "", "jel_source": "crossref"}
    except Exception:
        pass
    return None

