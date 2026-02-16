"""Quick dataset builder from raw OpenAlex JSONL files.

Run: python -m src.build_dataset_quick

Reads raw JSONL from data/raw/openalex/{journal_key}/{year}.jsonl,
builds a tidy dataframe (without JEL enrichment), deduplicates, and writes to:
  data/processed/papers_raw.parquet
  data/processed/papers_raw.csv
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config_multi.yaml"
RAW_DIR = ROOT / "data" / "raw" / "openalex"
PROC_DIR = ROOT / "data" / "processed"
PROC_DIR.mkdir(exist_ok=True)


def load_config() -> dict:
    """Load journals config and build lookup."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    
    key_to_journal = {}
    for j in cfg.get("journals", []):
        key_to_journal[j["key"]] = {"name": j["name"], "key": j["key"]}
    return key_to_journal


def collect_papers() -> List[Dict[str, Any]]:
    """Collect papers from all raw JSONL files."""
    rows = []
    journal_lookup = load_config()
    
    # Find all raw JSONL files (not enriched)
    for journal_dir in sorted(RAW_DIR.glob("*")):
        if not journal_dir.is_dir():
            continue
        journal_key = journal_dir.name
        if journal_key not in journal_lookup:
            continue
        
        journal_name = journal_lookup[journal_key]["name"]
        print(f"Reading {journal_key}...")
        
        for raw_file in sorted(journal_dir.glob("*.jsonl")):
            if "_enriched" in raw_file.name:
                continue  # Skip enriched files
            
            try:
                with open(raw_file, "r", encoding="utf-8") as fh:
                    for line in fh:
                        if not line.strip():
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        
                        # Extract minimal fields
                        title = obj.get("title")
                        if not title:
                            continue
                        
                        year = obj.get("publication_year")
                        doi = obj.get("doi")
                        authors = obj.get("authors", [])
                        
                        # Transform authors
                        author_list = []
                        if isinstance(authors, list):
                            for a in authors:
                                if isinstance(a, dict):
                                    if "author" in a and isinstance(a["author"], dict):
                                        name = a["author"].get("display_name", "")
                                    else:
                                        name = a.get("display_name", "")
                                elif isinstance(a, str):
                                    name = a
                                else:
                                    continue
                                if name and name != "Unknown Author":
                                    author_list.append(name)
                        
                        row = {
                            "year": year,
                            "journal": journal_name,
                            "journal_key": journal_key,
                            "title": title,
                            "doi": doi,
                            "authors": "|".join(author_list[:10]) if author_list else None,
                            "abstract": obj.get("abstract") or obj.get("abstract_inverted_index"),
                            "url": obj.get("landing_page_url"),
                            "jel_codes": None,
                            "jel_raw": None,
                            "jel_source": "missing",
                        }
                        rows.append(row)
            except Exception as e:
                print(f"  Error reading {raw_file}: {e}")
    
    return rows


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate papers by DOI within journal-year, then by title."""
    # Remove completely empty rows
    df = df.dropna(how="all", subset=["title"])
    
    # Deduplicate by DOI within journal-year
    if "doi" in df.columns and "journal_key" in df.columns and "year" in df.columns:
        df = df.drop_duplicates(subset=["doi", "journal_key", "year"], keep="first")
    
    # Deduplicate by title within journal-year
    df = df.drop_duplicates(subset=["title", "journal_key", "year"], keep="first")
    
    return df.reset_index(drop=True)


def main():
    """Build dataset."""
    print("Collecting papers from raw JSONL files...")
    papers = collect_papers()
    print(f"Collected {len(papers)} papers\n")
    
    if not papers:
        print("No papers found!")
        return
    
    df = pd.DataFrame(papers)
    print(f"Before deduplication: {len(df)} rows")
    
    df = deduplicate(df)
    print(f"After deduplication: {len(df)} rows\n")
    
    # Compute stats
    print(f"Papers by journal:")
    for journal_key in df["journal_key"].unique():
        count = len(df[df["journal_key"] == journal_key])
        journal_name = df[df["journal_key"] == journal_key]["journal"].iloc[0]
        print(f"  {journal_key}: {count} papers")
    
    print(f"\nYear range: {df['year'].min()} – {df['year'].max()}")
    print(f"Papers with DOI: {df['doi'].notna().sum()} ({100*df['doi'].notna().sum()/len(df):.1f}%)")
    print(f"Papers with authors: {df['authors'].notna().sum()} ({100*df['authors'].notna().sum()/len(df):.1f}%)")
    
    # Save
    out_parquet = PROC_DIR / "papers_raw.parquet"
    out_csv = PROC_DIR / "papers_raw.csv"
    
    df.to_parquet(out_parquet)
    df.to_csv(out_csv, index=False)
    
    print(f"\nWrote {out_parquet}")
    print(f"Wrote {out_csv}")


if __name__ == "__main__":
    main()
