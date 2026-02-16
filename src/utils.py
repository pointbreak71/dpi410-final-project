"""Utility helpers for data fetching and processing."""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "mpaid-final-project/0.1 (+https://example.com)"})
    return s


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30),
       retry=retry_if_exception_type((requests.exceptions.RequestException,)))
def do_get(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> requests.Response:
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp


def reconstruct_abstract(inverted_index: Dict[str, List[int]]) -> str:
    """Reconstruct a plain-text abstract from OpenAlex inverted index.

    OpenAlex gives inverted_index mapping token -> [positions]. We place tokens into a list
    at the given indices (positions start at 0) and join with spaces.
    """
    if not inverted_index:
        return ""
    try:
        # Determine required length
        max_pos = 0
        for token, positions in inverted_index.items():
            if positions:
                max_pos = max(max_pos, max(positions))
        words = [""] * (max_pos + 1)
        for token, positions in inverted_index.items():
            for pos in positions:
                words[pos] = token
        return " ".join(w for w in words if w)
    except Exception:
        return ""


def normalize_title(title: str) -> str:
    s = (title or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s


def deduplicate_papers(df):
    """Deduplicate a DataFrame of papers.

    Strategy: drop exact duplicate DOIs (case-insensitive). For missing DOIs, deduplicate by
    normalized (title, year, journal) keeping the first occurrence.
    """
    import pandas as pd

    d = df.copy()
    if "doi" in d.columns:
        d["doi_norm"] = d["doi"].fillna("").str.lower()
        # Keep first occurrence of each DOI (non-empty)
        has_doi = d["doi_norm"] != ""
        doi_unique = d[has_doi].drop_duplicates(subset=["doi_norm"]) if has_doi.any() else d[has_doi]
        no_doi = d[~has_doi].copy()
    else:
        doi_unique = d.iloc[0:0]
        no_doi = d

    if not no_doi.empty:
        no_doi["title_norm"] = no_doi["title"].fillna("").apply(normalize_title)
        no_doi = no_doi.drop_duplicates(subset=["title_norm", "year", "journal"])

    result = pd.concat([doi_unique, no_doi], ignore_index=True, sort=False)
    # drop helper cols
    for c in ["doi_norm", "title_norm"]:
        if c in result.columns:
            result = result.drop(columns=[c])
    return result


def extract_jel_prefixes(jel_codes: Iterable[str]) -> List[str]:
    prefixes = []
    if not jel_codes:
        return prefixes
    for code in jel_codes:
        if not code:
            continue
        m = re.match(r"^([A-Z])(\d)", code)
        if m:
            prefixes.append(m.group(1) + m.group(2))
        else:
            # fallback: first letter
            prefixes.append(code[0])
    return sorted(list(set(prefixes)))


def label_jel_codes(jel_codes: Iterable[str], include_l8: bool = False, include_m5: bool = False) -> str:
    """Return label 'market','firm','both','unclear' based on prefixes.

    Rules:
    - Market if any JEL begins with L1, L4, D4 (and optionally L8)
    - Firm if any JEL begins with D2, L2 (and optionally M5)
    - Both if matches both
    - Unclear otherwise
    """
    if not jel_codes:
        return "unclear"
    market_prefixes = {"L1", "L4", "D4"}
    firm_prefixes = {"D2", "L2"}
    if include_l8:
        market_prefixes.add("L8")
    if include_m5:
        firm_prefixes.add("M5")

    found_market = False
    found_firm = False
    for code in jel_codes:
        if not code:
            continue
        # Normalize like 'L12' -> 'L1' or 'D22' -> 'D2'
        m = re.match(r"^([A-Z])(\d)", code)
        prefix = None
        if m:
            prefix = m.group(1) + m.group(2)
        else:
            prefix = code[:2]
        if prefix in market_prefixes:
            found_market = True
        if prefix in firm_prefixes:
            found_firm = True

    if found_market and found_firm:
        return "both"
    if found_market:
        return "market"
    if found_firm:
        return "firm"
    return "unclear"
