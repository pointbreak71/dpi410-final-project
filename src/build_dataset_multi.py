"""Build final tidy dataset from enriched OpenAlex JSONL files.

Run: python -m src.build_dataset_multi

Reads enriched JSONL from data/raw/openalex/{journal_key}/{year}_enriched.jsonl,
builds a tidy dataframe, deduplicates, and writes to:
  data/processed/papers.parquet
  data/processed/papers.csv

Prints diagnostics: total papers, per-journal counts, % with DOI, % with JEL codes,
top 25 JEL codes, top 10 per journal.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from src.utils_minimal import normalize_doi, reconstruct_abstract

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config_multi.yaml"
RAW_DIR = ROOT / "data" / "raw" / "openalex"
PROC_DIR = ROOT / "data" / "processed"


def load_config() -> dict:
    """Load journals config and build lookup."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with open(CONFIG_PATH) as fh:
        cfg = yaml.safe_load(fh)
    
    key_to_journal = {}
    for j in cfg.get("journals", []):
        key_to_journal[j["key"]] = {
            "name": j["name"],
            "key": j["key"],
        }
    return key_to_journal


def collect_papers() -> List[Dict[str, Any]]:
    """Collect papers from all enriched JSONL files."""
    rows = []
    journal_lookup = load_config()
    
    # Find all enriched JSONL files
    for journal_dir in sorted(RAW_DIR.glob("*")):
        if not journal_dir.is_dir():
            continue
        journal_key = journal_dir.name
        if journal_key not in journal_lookup:
            continue
        
        journal_name = journal_lookup[journal_key]["name"]
        
        print(f"Reading {journal_key}...")
        
        for enriched_file in sorted(journal_dir.glob("*_enriched.jsonl")):
            try:
                with open(enriched_file, "r", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        
                        # Extract fields
                        title = obj.get("title")
                        if not title:
                            continue
                        
                        # Authors
                        authors = []
                        for a in (obj.get("authorships") or []):
                            au = a.get("author", {}).get("display_name")
                            if au:
                                authors.append(au)
                        
                        # DOI
                        doi = normalize_doi(obj.get("doi") or (obj.get("ids") or {}).get("doi"))
                        
                        # Abstract
                        abstract = ""
                        if obj.get("abstract_inverted_index"):
                            try:
                                abstract = reconstruct_abstract(obj.get("abstract_inverted_index"))
                            except Exception:
                                abstract = ""
                        
                        # Landing page URL
                        landing_url = obj.get("landing_page_url") or (obj.get("primary_location") or {}).get("url")
                        
                        # JEL
                        jel_codes = obj.get("jel_codes") or []
                        jel_raw = obj.get("jel_raw") or ""
                        jel_source = obj.get("jel_source") or "missing"
                        
                        # OpenAlex concepts
                        concepts = []
                        for c in (obj.get("concepts") or []):
                            cn = c.get("display_name")
                            if cn:
                                concepts.append(cn)
                        
                        row = {
                            "year": int(obj.get("publication_year")) or None,
                            "journal_key": journal_key,
                            "journal": journal_name,
                            "title": title,
                            "authors": "|".join(authors) if authors else "",
                            "doi": doi,
                            "url": landing_url or obj.get("id"),
                            "openalex_id": obj.get("id"),
                            "abstract": abstract,
                            "jel_codes": jel_codes,
                            "jel_raw": jel_raw,
                            "jel_source": jel_source,
                            "openalex_landing_page_url": landing_url,
                            "openalex_concepts": "|".join(concepts) if concepts else "",
                        }
                        rows.append(row)
            except Exception as e:
                print(f"  Error reading {enriched_file}: {e}")
    
    return rows


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate within each journal-year by DOI, then by (title, year)."""
    initial_count = len(df)
    
    # Normalize
    df["doi_norm"] = df["doi"].fillna("").str.lower()
    df["title_norm"] = df["title"].fillna("").str.lower()
    
    deduped = []
    for (journal_key, year), group in df.groupby(["journal_key", "year"]):
        # Deduplicate by DOI within journal-year
        with_doi = group[group["doi_norm"] != ""].drop_duplicates(subset=["doi_norm"], keep="first")
        without_doi = group[group["doi_norm"] == ""]
        
        # Deduplicate without DOI by title
        if not without_doi.empty:
            without_doi = without_doi.drop_duplicates(subset=["title_norm"], keep="first")
        
        deduped.append(pd.concat([with_doi, without_doi], ignore_index=True, sort=False))
    
    df = pd.concat(deduped, ignore_index=True, sort=False) if deduped else df
    df = df.drop(columns=["doi_norm", "title_norm"])
    
    final_count = len(df)
    print(f"Deduplicating: {initial_count} → {final_count} papers")
    
    return df


def compute_diagnostics(df: pd.DataFrame):
    """Print dataset diagnostics."""
    print("\n" + "=" * 60)
    print("DATASET DIAGNOSTICS")
    print("=" * 60)
    
    print(f"\nTotal papers: {len(df)}")
    print(f"Year range: {df['year'].min()} to {df['year'].max()}")
    
    print("\nPapers per journal:")
    for journal_key in sorted(df["journal_key"].unique()):
        count = len(df[df["journal_key"] == journal_key])
        print(f"  {journal_key}: {count}")
    
    # DOI coverage
    with_doi = (df["doi"].notna() & (df["doi"] != "")).sum()
    pct_doi = 100 * with_doi / len(df) if len(df) > 0 else 0
    print(f"\nDOI coverage: {with_doi} / {len(df)} ({pct_doi:.1f}%)")
    
    for journal_key in sorted(df["journal_key"].unique()):
        subset = df[df["journal_key"] == journal_key]
        with_doi_j = (subset["doi"].notna() & (subset["doi"] != "")).sum()
        pct_j = 100 * with_doi_j / len(subset) if len(subset) > 0 else 0
        print(f"  {journal_key}: {with_doi_j} / {len(subset)} ({pct_j:.1f}%)")
    
    # JEL coverage
    with_jel = (df["jel_codes"].str.len() > 0).sum()
    pct_jel = 100 * with_jel / len(df) if len(df) > 0 else 0
    print(f"\nJEL code coverage: {with_jel} / {len(df)} ({pct_jel:.1f}%)")
    
    for journal_key in sorted(df["journal_key"].unique()):
        subset = df[df["journal_key"] == journal_key]
        with_jel_j = (subset["jel_codes"].str.len() > 0).sum()
        pct_j = 100 * with_jel_j / len(subset) if len(subset) > 0 else 0
        print(f"  {journal_key}: {with_jel_j} / {len(subset)} ({pct_j:.1f}%)")
    
    # JEL sources
    print("\nJEL sources distribution (overall):")
    jel_sources = df["jel_source"].value_counts()
    for source, count in jel_sources.items():
        pct = 100 * count / len(df)
        print(f"  {source}: {count} ({pct:.1f}%)")
    
    # Top JEL codes
    all_codes = []
    for codes_list in df["jel_codes"]:
        if isinstance(codes_list, list):
            all_codes.extend(codes_list)
    
    if all_codes:
        jel_counter = {}
        for code in all_codes:
            jel_counter[code] = jel_counter.get(code, 0) + 1
        
        sorted_jel = sorted(jel_counter.items(), key=lambda x: x[1], reverse=True)
        
        print("\nTop 25 JEL codes (overall):")
        for code, count in sorted_jel[:25]:
            pct = 100 * count / len(df)
            print(f"  {code}: {count} ({pct:.1f}%)")
        
        # Per journal
        print("\nTop 10 JEL codes by journal:")
        for journal_key in sorted(df["journal_key"].unique()):
            subset = df[df["journal_key"] == journal_key]
            j_codes = []
            for codes_list in subset["jel_codes"]:
                if isinstance(codes_list, list):
                    j_codes.extend(codes_list)
            
            if j_codes:
                j_counter = {}
                for code in j_codes:
                    j_counter[code] = j_counter.get(code, 0) + 1
                sorted_j = sorted(j_counter.items(), key=lambda x: x[1], reverse=True)
                print(f"\n  {journal_key}:")
                for code, count in sorted_j[:10]:
                    print(f"    {code}: {count}")
    
    print("\n" + "=" * 60)


def main():
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    
    print("Collecting papers from enriched JSONL files...")
    rows = collect_papers()
    print(f"Collected {len(rows)} papers")
    
    df = pd.DataFrame(rows)
    
    # Deduplicate
    df = deduplicate(df)
    
    # Diagnostics
    compute_diagnostics(df)
    
    # Write outputs
    parquet_out = PROC_DIR / "papers.parquet"
    csv_out = PROC_DIR / "papers.csv"
    
    df.to_parquet(parquet_out, index=False)
    df.to_csv(csv_out, index=False)
    
    print(f"\nWrote {parquet_out}")
    print(f"Wrote {csv_out}")


if __name__ == "__main__":
    main()
