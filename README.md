# Multi-Journal JEL Code Extraction Pipeline

This project fetches papers from top economics journals (AER, QJE, AEJ: Micro) via OpenAlex and extracts JEL classification codes using robust multi-source fallbacks.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick Start

```bash
# 1. Fetch from OpenAlex (AER: 1975–2025, QJE: 1975–2025, AEJ Micro: 2009–2025)
python -m src.fetch_openalex_multi

# 2. Enrich with JEL codes (tries AEA → Crossref → OpenAlex → missing)
python -m src.enrich_jel_multi

# 3. Build final dataset
python -m src.build_dataset_multi

# 4. Check results
python scripts/sanity_check.py
```

## Output

- `data/processed/papers.parquet` — final tidy dataset
- `data/processed/papers.csv` — same in CSV format

Columns: year, journal_key, journal, title, authors, doi, url, openalex_id, abstract, jel_codes, jel_raw, jel_source, openalex_landing_page_url, openalex_concepts

## Config

Edit `config_multi.yaml` to adjust journal source IDs and year ranges. Pre-configured for AER, QJE, AEJ: Micro.

## Features

- **Robust JEL extraction**: AEA pages → AEA search → Crossref → OpenAlex concepts
- **Caching**: HTML cached locally to avoid re-fetching
- **Deduplication**: Per journal-year by DOI, then by title
- **Diagnostics**: Coverage stats and JEL frequency counts
- **No external tools**: requests, pandas, pyarrow, beautifulsoup4, pyyaml
