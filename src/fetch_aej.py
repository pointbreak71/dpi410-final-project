"""Fetch AEJ: Micro works via OpenAlex and cache raw JSONL files per year.

Run: python -m src.fetch_aej
"""
from __future__ import annotations

import json
from pathlib import Path
import datetime

import requests

from .utils_minimal import http_get, reconstruct_abstract, safe_cache_path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"


def fetch_openalex_aej(year: int, session: requests.Session) -> int:
    """Fetch works for AEJ Micro for a given year and append to a JSONL file."""
    out_file = RAW_DIR / f"openalex_AEJ_Micro_{year}.jsonl"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    url = "https://api.openalex.org/works"
    # filter by publication_year and source display name
    params = {
        "filter": f'publication_year:{year},primary_location.source.display_name:"American Economic Journal: Microeconomics"',
        "per-page": 200,
        "cursor": "*",
    }
    written = 0
    while True:
        resp = http_get(session, url, params=params)
        if resp is None:
            break
        data = resp.json()
        results = data.get("results") or []
        if results:
            with open(out_file, "a", encoding="utf-8") as fh:
                for w in results:
                    fh.write(json.dumps(w, ensure_ascii=False) + "\n")
                    written += 1
        meta = data.get("meta", {})
        next_cursor = meta.get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
    return written


def main(start: int = 1995, end: int | None = None):
    session = requests.Session()
    if end is None:
        end = datetime.date.today().year - 1
    total = 0
    for y in range(start, end + 1):
        n = fetch_openalex_aej(y, session)
        print(f"Wrote {n} works to data/raw/openalex_AEJ_Micro_{y}.jsonl")
        total += n
    print(f"Total written: {total}")


if __name__ == '__main__':
    main()
