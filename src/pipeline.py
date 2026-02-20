"""Comprehensive pipeline for scraping economics journal metadata.

This unified script handles:
1. Fetching papers from OpenAlex with multiple fallback strategies
2. Enriching papers with JEL codes from multiple sources
3. Building final dataset with JEL hierarchical information
4. Progress tracking with tqdm
5. Robust error handling with exponential backoff
6. Robots.txt compliance
7. Comprehensive logging

Usage:
    python -m src.pipeline --config config_comprehensive.yaml
    python -m src.pipeline --config config_comprehensive.yaml --step fetch
    python -m src.pipeline --config config_comprehensive.yaml --step enrich
    python -m src.pipeline --config config_comprehensive.yaml --step build
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import pandas as pd
import requests
import yaml
from tqdm import tqdm

from .jel_decoder import JELDecoder
from .utils_minimal import (
    http_get,
    normalize_doi,
    extract_jel_from_text,
    reconstruct_abstract,
    safe_cache_path
)

ROOT = Path(__file__).resolve().parents[1]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RobotsChecker:
    """Check robots.txt for allowed/disallowed paths."""

    def __init__(self):
        self.parsers = {}

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """Check if URL can be fetched according to robots.txt."""
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        if base_url not in self.parsers:
            parser = RobotFileParser()
            parser.set_url(f"{base_url}/robots.txt")
            try:
                parser.read()
                self.parsers[base_url] = parser
            except Exception:
                # If can't fetch robots.txt, assume allowed
                return True

        return self.parsers[base_url].can_fetch(user_agent, url)


class Pipeline:
    """Main pipeline for scraping and processing journal metadata."""

    def __init__(self, config_path: str):
        """Initialize pipeline with configuration."""
        self.config_path = Path(config_path)
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.session = requests.Session()
        self.robots_checker = RobotsChecker()

        # Setup directories
        self.raw_dir = ROOT / "data" / "raw" / "openalex"
        self.cache_dir = ROOT / self.config['jel_enrichment']['cache_dir']
        self.output_dir = ROOT / self.config['output']['directory']

        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        log_file = ROOT / self.config['progress']['log_file']
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(fh)

        logger.info(f"Pipeline initialized with config: {config_path}")

    def fetch_papers(self):
        """Fetch papers from OpenAlex for all configured journals."""
        logger.info("="*60)
        logger.info("STEP 1: Fetching papers from OpenAlex")
        logger.info("="*60)

        journals = self.config['journals']
        total_journals = len(journals)

        for idx, journal in enumerate(journals, 1):
            journal_key = journal['key']
            journal_name = journal['name']
            source_id = journal.get('openalex_source_id', '')
            start_year = journal.get('start_year', self.config['years']['start'])
            end_year = journal.get('end_year', self.config['years']['end'])

            logger.info(f"\n[{idx}/{total_journals}] Processing {journal_name} ({journal_key})")
            logger.info(f"  Years: {start_year}-{end_year}")
            logger.info(f"  OpenAlex ID: {source_id}")

            # Create journal directory
            journal_dir = self.raw_dir / journal_key
            journal_dir.mkdir(parents=True, exist_ok=True)

            # Fetch for each year
            years = range(start_year, end_year + 1)
            year_progress = tqdm(years, desc=f"  {journal_key}", leave=False)

            for year in year_progress:
                year_progress.set_postfix({'year': year})
                output_file = journal_dir / f"{year}.jsonl"

                if output_file.exists():
                    logger.debug(f"    Skipping {year} (already exists)")
                    continue

                try:
                    papers = self._fetch_year(source_id, journal_name, year)
                    if papers:
                        with open(output_file, 'w', encoding='utf-8') as f:
                            for paper in papers:
                                f.write(json.dumps(paper, ensure_ascii=False) + '\n')
                        logger.info(f"    {year}: {len(papers)} papers")
                    else:
                        logger.warning(f"    {year}: No papers found")
                except Exception as e:
                    logger.error(f"    {year}: Error - {e}")

                # Rate limiting
                time.sleep(self.config['scraping']['rate_limit_delay'])

        logger.info("\nFetch complete!")

    def _fetch_year(self, source_id: str, journal_name: str, year: int) -> List[Dict]:
        """Fetch papers for a single journal-year from OpenAlex."""
        url = "https://api.openalex.org/works"
        papers = []

        # Check robots.txt
        if self.config['scraping']['respect_robots_txt']:
            if not self.robots_checker.can_fetch(url):
                logger.warning(f"Robots.txt disallows fetching from {url}")
                return papers

        cursor = "*"
        max_retries = self.config['scraping']['max_retries']
        timeout = self.config['scraping']['timeout']

        # Build filter - try source_id first, then display_name
        if source_id and not source_id.startswith('<'):
            filter_str = f"publication_year:{year},primary_location.source.id:{source_id}"
        else:
            display_name = journal_name
            filter_str = f'publication_year:{year},primary_location.source.display_name:"{display_name}"'

        while True:
            params = {
                "filter": filter_str,
                "per-page": 200,
                "cursor": cursor
            }

            resp = http_get(
                self.session,
                url,
                params=params,
                timeout=timeout,
                retries=max_retries
            )

            if not resp:
                logger.warning(f"Failed to fetch data for {journal_name} {year}")
                break

            try:
                data = resp.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON: {e}")
                break

            results = data.get('results', [])
            papers.extend(results)

            meta = data.get('meta', {})
            next_cursor = meta.get('next_cursor')

            if not next_cursor:
                break

            cursor = next_cursor
            time.sleep(0.5)  # Polite delay between pagination requests

        return papers

    def enrich_jel_codes(self):
        """Enrich all fetched papers with JEL codes."""
        logger.info("="*60)
        logger.info("STEP 2: Enriching papers with JEL codes")
        logger.info("="*60)

        # Find all raw JSONL files
        raw_files = list(self.raw_dir.glob("*/*.jsonl"))
        raw_files = [f for f in raw_files if not f.name.endswith('_enriched.jsonl')]

        logger.info(f"Found {len(raw_files)} files to enrich")

        for raw_file in tqdm(raw_files, desc="Enriching files"):
            enriched_file = raw_file.with_name(raw_file.stem + '_enriched.jsonl')

            # Skip only if fully enriched (same line count as raw file)
            if enriched_file.exists():
                raw_count = sum(1 for line in open(raw_file, 'r', encoding='utf-8') if line.strip())
                done_count = sum(1 for line in open(enriched_file, 'r', encoding='utf-8') if line.strip())
                if done_count >= raw_count:
                    logger.debug(f"Skipping {raw_file.name} (fully enriched: {done_count} papers)")
                    continue
                # Partial file — fall through to _enrich_file which will resume

            try:
                self._enrich_file(raw_file, enriched_file)
                logger.info(f"Enriched {raw_file.relative_to(self.raw_dir)}")
            except Exception as e:
                logger.error(f"Error enriching {raw_file}: {e}")

        logger.info("\nEnrichment complete!")

    def _enrich_file(self, input_file: Path, output_file: Path):
        """Enrich a single JSONL file with JEL codes.

        Supports mid-file resumption: if a partial output file exists (fewer
        lines than the input), already-enriched papers are skipped and the
        file is appended from where it left off.
        """
        # Read all raw papers
        papers = []
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    papers.append(json.loads(line))
                except Exception:
                    continue

        if not papers:
            return

        # Check how many papers are already enriched in a partial output file
        already_done = 0
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                already_done = sum(1 for line in f if line.strip())
            if already_done >= len(papers):
                logger.debug(f"  {input_file.name}: all {len(papers)} papers already enriched, skipping")
                return
            logger.info(f"  {input_file.name}: resuming from paper {already_done + 1}/{len(papers)}")

        # Enrich remaining papers, appending to the output file as we go
        remaining = papers[already_done:]
        with open(output_file, 'a', encoding='utf-8') as out_fh:
            for paper in tqdm(remaining, desc=f"  {input_file.name}",
                              initial=already_done, total=len(papers), leave=False):
                enriched_paper = self._enrich_paper(paper)
                out_fh.write(json.dumps(enriched_paper, ensure_ascii=False) + '\n')
                out_fh.flush()  # Ensure each paper is written immediately

    def _enrich_paper(self, paper: Dict) -> Dict:
        """Enrich a single paper with JEL codes from multiple sources."""
        doi = normalize_doi(paper.get('doi') or (paper.get('ids') or {}).get('doi'))
        openalex_id = paper.get('id')

        sources = self.config['jel_enrichment']['sources']
        result = None

        for source in sources:
            if result:
                break

            try:
                if source == 'aea_page' and doi:
                    result = self._fetch_jel_from_aea(doi)
                elif source == 'crossref' and doi:
                    result = self._fetch_jel_from_crossref(doi)
                elif source == 'openalex' and openalex_id:
                    result = self._fetch_jel_from_openalex(openalex_id)
                elif source == 'ideas_repec' and doi:
                    result = self._fetch_jel_from_ideas(doi)

                if result:
                    paper['jel_codes'] = result.get('jel_codes', [])
                    paper['jel_raw'] = result.get('jel_raw', '')
                    paper['jel_source'] = result.get('source')
                    break

                time.sleep(self.config['jel_enrichment']['retry_delay'])

            except Exception as e:
                logger.debug(f"Error with {source}: {e}")
                continue

        if not result:
            paper['jel_codes'] = []
            paper['jel_raw'] = ''
            paper['jel_source'] = 'missing'

        return paper

    def _fetch_jel_from_aea(self, doi: str) -> Optional[Dict]:
        """Fetch JEL codes from AEA article page."""
        from bs4 import BeautifulSoup

        cache_file = safe_cache_path(f"aea_{doi}.html", self.cache_dir)

        if cache_file.exists():
            html = cache_file.read_text(encoding='utf-8')
        else:
            url = f"https://doi.org/{doi}"
            resp = http_get(self.session, url, timeout=15, retries=2)
            if not resp:
                return None
            html = resp.text
            cache_file.write_text(html, encoding='utf-8')

        soup = BeautifulSoup(html, 'lxml')
        text = soup.get_text(separator=' \n ')
        jels = extract_jel_from_text(text)

        if jels:
            return {
                'jel_codes': jels,
                'jel_raw': text[:500],
                'source': 'aea_page'
            }
        return None

    def _fetch_jel_from_crossref(self, doi: str) -> Optional[Dict]:
        """Fetch JEL codes from Crossref API."""
        cache_file = safe_cache_path(f"crossref_{doi}.json", self.cache_dir)

        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding='utf-8'))
        else:
            url = f"https://api.crossref.org/works/{doi}"
            resp = http_get(self.session, url, timeout=10, retries=2)
            if not resp:
                return None
            data = resp.json()
            cache_file.write_text(json.dumps(data), encoding='utf-8')

        msg = data.get('message', {})
        txt = json.dumps(msg)
        jels = extract_jel_from_text(txt)

        if jels:
            return {
                'jel_codes': jels,
                'jel_raw': txt[:500],
                'source': 'crossref'
            }
        return None

    def _fetch_jel_from_openalex(self, openalex_id: str) -> Optional[Dict]:
        """Fetch JEL codes from OpenAlex metadata."""
        cache_file = safe_cache_path(f"openalex_{openalex_id.replace('/', '_')}.json", self.cache_dir)

        if cache_file.exists():
            data = json.loads(cache_file.read_text(encoding='utf-8'))
        else:
            url = f"https://api.openalex.org/works/{openalex_id.split('/')[-1]}"
            resp = http_get(self.session, url, timeout=10, retries=2)
            if not resp:
                return None
            data = resp.json()
            cache_file.write_text(json.dumps(data), encoding='utf-8')

        txt = json.dumps(data)
        jels = extract_jel_from_text(txt)

        if jels:
            return {
                'jel_codes': jels,
                'jel_raw': txt[:500],
                'source': 'openalex'
            }
        return None

    def _fetch_jel_from_ideas(self, doi: str) -> Optional[Dict]:
        """Fetch JEL codes from IDEAS/RePEc."""
        from bs4 import BeautifulSoup

        search_url = "https://ideas.repec.org/search.html"
        resp = http_get(self.session, search_url, params={"q": doi}, timeout=10, retries=2)

        if not resp:
            return None

        soup = BeautifulSoup(resp.text, 'lxml')
        text = soup.get_text(separator=' \n ')
        jels = extract_jel_from_text(text)

        if jels:
            return {
                'jel_codes': jels,
                'jel_raw': text[:500],
                'source': 'ideas'
            }
        return None

    def build_dataset(self):
        """Build final dataset from enriched papers."""
        logger.info("="*60)
        logger.info("STEP 3: Building final dataset")
        logger.info("="*60)

        # Collect all enriched files
        enriched_files = list(self.raw_dir.glob("*/*_enriched.jsonl"))
        logger.info(f"Found {len(enriched_files)} enriched files")

        # Build journal key mapping
        journal_lookup = {j['key']: j['name'] for j in self.config['journals']}

        # Collect papers
        rows = []
        for enriched_file in tqdm(enriched_files, desc="Reading files"):
            journal_key = enriched_file.parent.name

            if journal_key not in journal_lookup:
                continue

            journal_name = journal_lookup[journal_key]

            with open(enriched_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        paper = json.loads(line)
                    except Exception:
                        continue

                    row = self._paper_to_row(paper, journal_key, journal_name)
                    if row:
                        rows.append(row)

        logger.info(f"Collected {len(rows)} papers")

        # Create dataframe
        df = pd.DataFrame(rows)

        # Deduplicate
        if self.config['processing']['deduplicate']['enabled']:
            initial_count = len(df)
            df = self._deduplicate(df)
            logger.info(f"Deduplicated: {initial_count} → {len(df)} papers")

        # Enrich with JEL hierarchical information
        logger.info("Adding JEL hierarchical information...")
        decoder = JELDecoder()
        df = decoder.enrich_dataframe(df)

        # Save outputs
        for fmt in self.config['output']['formats']:
            if fmt == 'csv':
                output_file = self.output_dir / "papers.csv"
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {output_file}")
            elif fmt == 'parquet':
                output_file = self.output_dir / "papers.parquet"
                df.to_parquet(output_file, index=False)
                logger.info(f"Saved {output_file}")

        # Print diagnostics
        self._print_diagnostics(df)

        logger.info("\nDataset build complete!")

    def _paper_to_row(self, paper: Dict, journal_key: str, journal_name: str) -> Optional[Dict]:
        """Convert paper dict to row dict."""
        title = paper.get('title')
        if not title:
            return None

        # Authors
        authors = []
        for a in (paper.get('authorships') or []):
            au = a.get('author', {}).get('display_name')
            if au:
                authors.append(au)

        # DOI
        doi = normalize_doi(paper.get('doi') or (paper.get('ids') or {}).get('doi'))

        # Abstract
        abstract = ""
        if paper.get('abstract_inverted_index'):
            try:
                abstract = reconstruct_abstract(paper.get('abstract_inverted_index'))
            except Exception:
                pass

        # URL
        landing_url = paper.get('landing_page_url') or (paper.get('primary_location') or {}).get('url')

        # JEL
        jel_codes = paper.get('jel_codes', [])
        jel_raw = paper.get('jel_raw', '')
        jel_source = paper.get('jel_source', 'missing')

        # OpenAlex concepts
        concepts = []
        for c in (paper.get('concepts') or []):
            cn = c.get('display_name')
            if cn:
                concepts.append(cn)

        return {
            'year': int(paper.get('publication_year')) if paper.get('publication_year') else None,
            'journal_key': journal_key,
            'journal': journal_name,
            'title': title,
            'authors': '|'.join(authors) if authors else '',
            'doi': doi,
            'url': landing_url or paper.get('id'),
            'openalex_id': paper.get('id'),
            'abstract': abstract,
            'jel_codes': jel_codes,
            'jel_raw': jel_raw,
            'jel_source': jel_source,
            'openalex_landing_page_url': landing_url,
            'openalex_concepts': '|'.join(concepts) if concepts else '',
        }

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate dataframe."""
        df = df.copy()
        df['doi_norm'] = df['doi'].fillna('').str.lower()
        df['title_norm'] = df['title'].fillna('').str.lower()

        deduped = []
        for (journal_key, year), group in df.groupby(['journal_key', 'year']):
            with_doi = group[group['doi_norm'] != ''].drop_duplicates(subset=['doi_norm'], keep='first')
            without_doi = group[group['doi_norm'] == '']

            if not without_doi.empty:
                without_doi = without_doi.drop_duplicates(subset=['title_norm'], keep='first')

            deduped.append(pd.concat([with_doi, without_doi], ignore_index=True, sort=False))

        df = pd.concat(deduped, ignore_index=True, sort=False) if deduped else df
        df = df.drop(columns=['doi_norm', 'title_norm'])

        return df

    def _print_diagnostics(self, df: pd.DataFrame):
        """Print dataset diagnostics."""
        print("\n" + "="*60)
        print("DATASET DIAGNOSTICS")
        print("="*60)

        print(f"\nTotal papers: {len(df)}")
        print(f"Year range: {df['year'].min()} to {df['year'].max()}")

        print("\nPapers per journal:")
        for journal_key in sorted(df['journal_key'].unique()):
            count = len(df[df['journal_key'] == journal_key])
            print(f"  {journal_key}: {count}")

        # DOI coverage
        with_doi = (df['doi'].notna() & (df['doi'] != '')).sum()
        pct_doi = 100 * with_doi / len(df) if len(df) > 0 else 0
        print(f"\nDOI coverage: {with_doi} / {len(df)} ({pct_doi:.1f}%)")

        # JEL coverage
        with_jel = df['has_jel'].sum()
        pct_jel = 100 * with_jel / len(df) if len(df) > 0 else 0
        print(f"JEL code coverage: {with_jel} / {len(df)} ({pct_jel:.1f}%)")

        # Top JEL primary categories
        if 'jel_primary_letters' in df.columns:
            print("\nTop 10 primary JEL categories:")
            all_primaries = []
            for letters in df['jel_primary_letters']:
                if letters:
                    all_primaries.extend(letters.split('|'))

            from collections import Counter
            for letter, count in Counter(all_primaries).most_common(10):
                pct = 100 * count / len(df)
                print(f"  {letter}: {count} ({pct:.1f}%)")

        print("="*60)

    def run_full_pipeline(self):
        """Run the complete pipeline."""
        logger.info("Starting full pipeline...")

        self.fetch_papers()
        self.enrich_jel_codes()
        self.build_dataset()

        logger.info("\n" + "="*60)
        logger.info("PIPELINE COMPLETE!")
        logger.info("="*60)


def main():
    parser = argparse.ArgumentParser(description="Run economics journal metadata scraping pipeline")
    parser.add_argument(
        '--config',
        default='config_comprehensive.yaml',
        help='Path to configuration file (default: config_comprehensive.yaml)'
    )
    parser.add_argument(
        '--step',
        choices=['fetch', 'enrich', 'build', 'full'],
        default='full',
        help='Pipeline step to run (default: full)'
    )

    args = parser.parse_args()

    pipeline = Pipeline(args.config)

    if args.step == 'fetch':
        pipeline.fetch_papers()
    elif args.step == 'enrich':
        pipeline.enrich_jel_codes()
    elif args.step == 'build':
        pipeline.build_dataset()
    else:
        pipeline.run_full_pipeline()


if __name__ == '__main__':
    main()
