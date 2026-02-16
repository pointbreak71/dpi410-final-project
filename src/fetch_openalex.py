"""Fetch works from OpenAlex by journal and year and save JSONL files.

Run as: python -m src.fetch_openalex
"""
from __future__ import annotations

import json
import time
from pathlib import Path
import yaml
from typing import Dict

from .utils import get_session, do_get


def resolve_source_id(session, maybe_id: str, journal_name: str) -> str:
    """If the config value looks like a placeholder, try to find a source id by searching OpenAlex."""
    if maybe_id and not maybe_id.startswith("<"):
        return maybe_id
    # map common config names to display names
    name_map = {
        "AEJ_Micro": "American Economic Journal: Microeconomics",
        "American_Economic_Review": "American Economic Review",
        "Quarterly_Journal_of_Economics": "Quarterly Journal of Economics",
        "Journal_of_Political_Economy": "Journal of Political Economy",
        "Review_of_Economic_Studies": "Review of Economic Studies",
        "Review_of_Economics_and_Statistics": "The Review of Economics and Statistics",
        "RAND_Journal_of_Economics": "RAND Journal of Economics",
        "Journal_of_Industrial_Economics": "Journal of Industrial Economics",
    }
    display_name = name_map.get(journal_name, journal_name.replace("_", " "))
    # search OpenAlex sources by journal display name
    url = "https://api.openalex.org/sources"
    params = {"search": display_name, "per-page": 5}
    try:
        resp = do_get(session, url, params=params)
        data = resp.json()
        results = data.get("results") or []
        if results:
            # pick the best match (first)
            sid = results[0].get("id")
            return sid
    except Exception:
        pass
    return maybe_id


ROOT = Path(__file__).resolve().parents[1]


def load_config(path: Path | str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def fetch_and_save(openalex_source_id: str, journal_name: str, year: int, outdir: Path):
    session = get_session()
    outdir.mkdir(parents=True, exist_ok=True)
    out_file = outdir / f"openalex_{journal_name.replace(' ', '_')}_{year}.jsonl"
    url = "https://api.openalex.org/works"
    cursor = "*"
    # If openalex_source_id is a placeholder like '<...>' or empty, fall back to searching by display name
    if not openalex_source_id or (isinstance(openalex_source_id, str) and openalex_source_id.strip().startswith("<")):
        display_name = journal_name.replace("_", " ")
        # OpenAlex filter requires quoting string values
        params = {
            "filter": f'publication_year:{year},primary_location.source.display_name:"{display_name}"',
            "per-page": 200,
            "cursor": cursor,
        }
    else:
        params = {
            "filter": f"publication_year:{year},primary_location.source.id:{openalex_source_id}",
            "per-page": 200,
            "cursor": cursor,
        }
    written = 0
    while True:
        resp = do_get(session, url, params=params)
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
        # be polite
        time.sleep(1)
    print(f"Wrote {written} works to {out_file}")


def main(config_path: str = "config.yaml"):
    cfg = load_config(config_path)
    journals = cfg.get("journals", [])
    years = cfg.get("years", {})
    start = years.get("start", 1995)
    end = years.get("end")
    if end == "latest" or end is None:
        import datetime

        end = datetime.date.today().year - 1
    outdir = ROOT / "data" / "raw"
    session = get_session()
    for journal in journals:
        name = journal.get("name")
        source_id = journal.get("openalex_id")
        resolved = resolve_source_id(session, source_id, name)
        if not resolved:
            # allow fetch_and_save to fall back to display_name search
            print(f"Note: no ID resolved for {name}; will try display-name filter")
            resolved = source_id
        for y in range(start, end + 1):
            fetch_and_save(resolved, name, y, outdir)


if __name__ == "__main__":
    main()
