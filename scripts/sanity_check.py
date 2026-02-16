"""Quick sanity check: load parquet and print 10 random rows + diagnostics.

Run: python scripts/sanity_check.py
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_FILE = ROOT / "data" / "processed" / "papers.parquet"


def main():
    if not PARQUET_FILE.exists():
        print(f"Error: {PARQUET_FILE} not found")
        raise SystemExit(1)
    
    df = pd.read_parquet(PARQUET_FILE)
    
    print(f"\n{'='*80}")
    print(f"DATASET SANITY CHECK")
    print(f"{'='*80}")
    
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
    print(f"Columns: {', '.join(df.columns)}")
    
    print(f"\nYear range: {df['year'].min()} to {df['year'].max()}")
    print(f"Journals: {', '.join(sorted(df['journal_key'].unique()))}")
    
    print(f"\nPapers per journal:")
    for jk in sorted(df["journal_key"].unique()):
        count = len(df[df["journal_key"] == jk])
        print(f"  {jk}: {count}")
    
    # JEL coverage
    with_jel = (df["jel_codes"].str.len() > 0).sum()
    print(f"\nPapers with JEL codes: {with_jel} / {len(df)} ({100*with_jel/len(df):.1f}%)")
    
    # JEL sources
    print(f"\nJEL sources:")
    for source, count in df["jel_source"].value_counts().items():
        print(f"  {source}: {count}")
    
    # Sample 10 random rows
    print(f"\n{'='*80}")
    print(f"SAMPLE: 10 Random Papers")
    print(f"{'='*80}\n")
    
    sample = df.sample(min(10, len(df)), random_state=42)
    for idx, (_, row) in enumerate(sample.iterrows(), start=1):
        title_trunc = row['title'][:70] + "..." if len(str(row['title'])) > 70 else row['title']
        doi_str = row['doi'] if row['doi'] else "N/A"
        jel_str = ", ".join(row['jel_codes']) if isinstance(row['jel_codes'], list) and row['jel_codes'] else "N/A"
        
        print(f"{idx}. [{row['year']}] {row['journal_key'].upper()}")
        print(f"   Title: {title_trunc}")
        print(f"   DOI: {doi_str}")
        print(f"   JEL ({row['jel_source']}): {jel_str}")
        print()


if __name__ == "__main__":
    main()
