"""Build tidy AEJ dataset from enriched raw files and save parquet + csv.

Run: python -m src.build_aej_dataset
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd
from src.utils_minimal import reconstruct_abstract

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / 'data' / 'raw'
PROC_DIR = ROOT / 'data' / 'processed'


def collect_enriched() -> pd.DataFrame:
    rows: List[dict] = []
    files = sorted(RAW_DIR.glob('openalex_AEJ_Micro_*_enriched.jsonl'))
    print(f"Found {len(files)} enriched files")
    for p in files:
        # infer year from filename
        year = None
        parts = p.stem.split('_')
        if len(parts) >= 4 and parts[3].isdigit():
            year = int(parts[3])
        print(f"Reading {p.name}...")
        try:
            with open(p, 'r', encoding='utf-8') as fh:
                for line in fh:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    title = obj.get('display_name') or obj.get('title')
                    doi = obj.get('doi') or (obj.get('ids') or {}).get('doi')
                    if doi:
                        doi = doi.strip().lower().replace('https://doi.org/', '').replace('doi:', '')
                    authors = []
                    for a in (obj.get('authorships') or []):
                        au = a.get('author', {}).get('display_name')
                        if au:
                            authors.append(au)
                    abstract = ''
                    if obj.get('abstract_inverted_index'):
                        # reconstruct silently
                        try:
                            abstract = reconstruct_abstract(obj.get('abstract_inverted_index'))
                        except Exception:
                            abstract = ''
                    row = {
                        'year': int(obj.get('publication_year')) if obj.get('publication_year') else year,
                        'journal': 'American Economic Journal: Microeconomics',
                        'title': title,
                        'authors': authors,
                        'doi': doi,
                        'url': obj.get('landing_url') or (obj.get('primary_location') or {}).get('url') or obj.get('id'),
                        'abstract': abstract,
                        'jel_codes': obj.get('jel_codes') or [],
                        'jel_raw': obj.get('jel_raw') or '',
                        'source': obj.get('jel_source') or '',
                    }
                    rows.append(row)
        except Exception as e:
            print(f"  Error reading {p.name}: {e}")
    print(f"Collected {len(rows)} rows")
    df = pd.DataFrame(rows)
    # normalize year to int and drop rows missing title or doi
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    # deduplicate by doi when present, else by title+year
    if 'doi' in df.columns:
        df['doi_norm'] = df['doi'].fillna('').str.lower()
        with_doi = df[df['doi_norm'] != ''].drop_duplicates(subset=['doi_norm'])
        without_doi = df[df['doi_norm'] == '']
        if not without_doi.empty:
            without_doi['title_norm'] = without_doi['title'].fillna('').str.lower()
            without_doi = without_doi.drop_duplicates(subset=['title_norm', 'year'])
            without_doi = without_doi.drop(columns=['title_norm'])
        df = pd.concat([with_doi, without_doi], ignore_index=True, sort=False)
        df = df.drop(columns=['doi_norm'])
    return df


def main():
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    df = collect_enriched()
    outp = PROC_DIR / 'papers.parquet'
    outcsv = PROC_DIR / 'papers.csv'
    df.to_parquet(outp, index=False)
    df.to_csv(outcsv, index=False)
    print('Wrote processed dataset to', outp)


if __name__ == '__main__':
    main()
