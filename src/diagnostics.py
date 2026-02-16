"""Diagnostic summary of processed papers dataset.

Run: python -m src.diagnostics
"""
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_FILE = ROOT / "data" / "processed" / "papers.parquet"


def main():
    if not PARQUET_FILE.exists():
        print(f"Error: {PARQUET_FILE} not found. Run pipeline first.")
        return
    
    df = pd.read_parquet(PARQUET_FILE)
    
    print("\n" + "=" * 80)
    print("DATASET DIAGNOSTICS")
    print("=" * 80)
    
    print(f"\nTotal papers: {len(df)}")
    print(f"Year range: {df['year'].min()} to {df['year'].max()}")
    print(f"Journals: {', '.join(sorted(df['journal_key'].unique()))}")
    
    # Per-journal summary
    print("\n--- Papers per journal ---")
    for jk in sorted(df["journal_key"].unique()):
        subset = df[df["journal_key"] == jk]
        print(f"  {jk}: {len(subset)}")
    
    # Per-year summary: count, % with DOI, % with JEL
    print("\n--- Per-year breakdown ---")
    print(f"{'Year':<6} {'Count':<8} {'%DOI':<8} {'%JEL':<8} {'Sources':<30}")
    print("-" * 60)
    
    for year in sorted(df["year"].unique()):
        year_df = df[df["year"] == year]
        count = len(year_df)
        with_doi = (year_df["doi"].notna() & (year_df["doi"] != "")).sum()
        pct_doi = 100 * with_doi / count if count > 0 else 0
        with_jel = (year_df["jel_codes"].apply(lambda x: len(x) if isinstance(x, list) else 0) > 0).sum()
        pct_jel = 100 * with_jel / count if count > 0 else 0
        
        sources = year_df["jel_source"].value_counts().to_dict()
        source_str = ", ".join(f"{k}:{v}" for k, v in sorted(sources.items()))[:28]
        
        print(f"{int(year):<6} {count:<8} {pct_doi:>6.1f}% {pct_jel:>6.1f}% {source_str:<30}")
    
    # Overall JEL coverage
    with_jel_total = (df["jel_codes"].apply(lambda x: len(x) if isinstance(x, list) else 0) > 0).sum()
    pct_jel_total = 100 * with_jel_total / len(df) if len(df) > 0 else 0
    print(f"\nOverall JEL coverage: {with_jel_total}/{len(df)} ({pct_jel_total:.1f}%)")
    
    # 10 random samples from 2009–2015 if available
    early_df = df[df["year"] <= 2015]
    if len(early_df) > 0:
        print(f"\n--- 10 random papers from 2009–2015 ---")
        sample = early_df.sample(min(10, len(early_df)), random_state=42)
        for idx, (_, row) in enumerate(sample.iterrows(), start=1):
            title_short = row["title"][:50] + "..." if len(str(row["title"])) > 50 else row["title"]
            doi_str = row["doi"] if row["doi"] else "N/A"
            jel_str = ", ".join(row["jel_codes"]) if isinstance(row["jel_codes"], list) and row["jel_codes"] else "[]"
            jel_raw_str = row["jel_raw"][:60] + "..." if row["jel_raw"] and len(str(row["jel_raw"])) > 60 else (row["jel_raw"] or "")
            
            print(f"\n{idx}. [{int(row['year'])}] {row['journal_key']}")
            print(f"   Title: {title_short}")
            print(f"   DOI: {doi_str}")
            print(f"   JEL codes: {jel_str}")
            print(f"   JEL source: {row['jel_source']}")
            if jel_raw_str:
                print(f"   JEL raw: {jel_raw_str}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
