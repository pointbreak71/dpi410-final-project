"""JEL code decoder module for enriching papers with hierarchical JEL information.

This module loads the JEL classification system and provides functions to:
1. Decode JEL codes into their hierarchical components
2. Add enriched columns to dataframes with JEL descriptions
3. Extract primary categories, subcategories, and full descriptions

Usage:
    from src.jel_decoder import JELDecoder

    decoder = JELDecoder()
    enriched_df = decoder.enrich_dataframe(df)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
JEL_CODES_PATH = ROOT / "data" / "jel_codes.json"


class JELDecoder:
    """Decoder for JEL classification codes with hierarchical lookups."""

    def __init__(self, jel_codes_path: Optional[Path] = None):
        """Initialize the JEL decoder.

        Args:
            jel_codes_path: Path to jel_codes.json file. If None, uses default location.
        """
        if jel_codes_path is None:
            jel_codes_path = JEL_CODES_PATH

        if not jel_codes_path.exists():
            raise FileNotFoundError(
                f"JEL codes file not found at {jel_codes_path}. "
                f"Run 'python -m src.fetch_jel_codes' to generate it."
            )

        with open(jel_codes_path, 'r', encoding='utf-8') as f:
            self.jel_lookup = json.load(f)

        print(f"Loaded {len(self.jel_lookup)} JEL codes")

    def decode_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Decode a single JEL code.

        Args:
            code: JEL code (e.g., 'C13', 'L1', 'D')

        Returns:
            Dict with hierarchical information, or None if code not found
        """
        if not code:
            return None

        code_upper = code.strip().upper()
        return self.jel_lookup.get(code_upper)

    def decode_codes(self, codes: List[str]) -> List[Dict[str, Any]]:
        """Decode a list of JEL codes.

        Args:
            codes: List of JEL codes

        Returns:
            List of decoded information dicts (skips codes not found)
        """
        results = []
        for code in codes:
            decoded = self.decode_code(code)
            if decoded:
                results.append(decoded)
        return results

    def get_primary_categories(self, codes: List[str]) -> List[str]:
        """Extract unique primary letter categories from JEL codes.

        Args:
            codes: List of JEL codes

        Returns:
            Sorted list of unique primary letters (e.g., ['C', 'D', 'L'])
        """
        primaries = set()
        for code in codes:
            if code:
                primaries.add(code[0].upper())
        return sorted(primaries)

    def get_primary_descriptions(self, codes: List[str]) -> List[str]:
        """Get descriptions for primary categories.

        Args:
            codes: List of JEL codes

        Returns:
            List of primary category descriptions
        """
        primaries = self.get_primary_categories(codes)
        descriptions = []
        for letter in primaries:
            info = self.decode_code(letter)
            if info:
                descriptions.append(info['description'])
        return descriptions

    def enrich_row(self, jel_codes: List[str]) -> Dict[str, Any]:
        """Enrich a single row's JEL codes with hierarchical information.

        Args:
            jel_codes: List of JEL codes for this paper

        Returns:
            Dict with enrichment columns:
                - jel_primary_letters: pipe-separated primary letters (e.g., "C|D|L")
                - jel_primary_categories: pipe-separated primary descriptions
                - jel_full_descriptions: pipe-separated full code descriptions
                - jel_count: number of JEL codes
                - has_jel: boolean indicating if any JEL codes exist
        """
        if not jel_codes or not isinstance(jel_codes, list):
            return {
                'jel_primary_letters': '',
                'jel_primary_categories': '',
                'jel_full_descriptions': '',
                'jel_count': 0,
                'has_jel': False
            }

        # Get primary categories
        primary_letters = self.get_primary_categories(jel_codes)
        primary_descs = self.get_primary_descriptions(jel_codes)

        # Get full descriptions for all codes
        decoded = self.decode_codes(jel_codes)
        full_descs = [d['description'] for d in decoded]

        return {
            'jel_primary_letters': '|'.join(primary_letters),
            'jel_primary_categories': '|'.join(primary_descs),
            'jel_full_descriptions': '|'.join(full_descs),
            'jel_count': len(jel_codes),
            'has_jel': len(jel_codes) > 0
        }

    def enrich_dataframe(self, df: pd.DataFrame, jel_column: str = 'jel_codes') -> pd.DataFrame:
        """Enrich a dataframe with JEL hierarchical information.

        Args:
            df: DataFrame with a column containing JEL codes (as list)
            jel_column: Name of column containing JEL codes (default: 'jel_codes')

        Returns:
            DataFrame with additional columns:
                - jel_primary_letters
                - jel_primary_categories
                - jel_full_descriptions
                - jel_count
                - has_jel
        """
        if jel_column not in df.columns:
            raise ValueError(f"Column '{jel_column}' not found in dataframe")

        print(f"Enriching {len(df)} papers with JEL hierarchical information...")

        # Apply enrichment to each row
        enriched = df[jel_column].apply(self.enrich_row)

        # Convert to dataframe and join with original
        enriched_df = pd.DataFrame(enriched.tolist())

        # Combine with original dataframe
        result = pd.concat([df, enriched_df], axis=1)

        print(f"Added {len(enriched_df.columns)} JEL enrichment columns")
        print(f"Papers with JEL codes: {result['has_jel'].sum()} / {len(result)} ({100*result['has_jel'].sum()/len(result):.1f}%)")

        return result

    def get_code_stats(self, df: pd.DataFrame, jel_column: str = 'jel_codes') -> pd.DataFrame:
        """Generate statistics about JEL code usage in the dataset.

        Args:
            df: DataFrame with JEL codes
            jel_column: Name of column containing JEL codes

        Returns:
            DataFrame with columns: code, count, description, primary_category
        """
        # Flatten all JEL codes
        all_codes = []
        for codes in df[jel_column]:
            if isinstance(codes, list):
                all_codes.extend(codes)

        # Count occurrences
        from collections import Counter
        code_counts = Counter(all_codes)

        # Build stats dataframe
        stats = []
        for code, count in code_counts.most_common():
            info = self.decode_code(code)
            stats.append({
                'code': code,
                'count': count,
                'pct': 100 * count / len(df),
                'description': info['description'] if info else '',
                'primary_letter': code[0] if code else '',
                'primary_category': info['primary_description'] if info else ''
            })

        return pd.DataFrame(stats)

    def filter_by_primary_category(self, df: pd.DataFrame, primary_letter: str,
                                   jel_column: str = 'jel_codes') -> pd.DataFrame:
        """Filter papers by primary JEL category.

        Args:
            df: DataFrame with JEL codes
            primary_letter: Primary letter (e.g., 'C', 'L', 'D')
            jel_column: Name of column containing JEL codes

        Returns:
            Filtered dataframe containing only papers with codes starting with that letter
        """
        primary_upper = primary_letter.upper()

        def has_primary(codes):
            if not isinstance(codes, list):
                return False
            return any(c.startswith(primary_upper) for c in codes)

        mask = df[jel_column].apply(has_primary)
        return df[mask].copy()

    def get_primary_category_distribution(self, df: pd.DataFrame,
                                         jel_column: str = 'jel_codes') -> pd.DataFrame:
        """Get distribution of papers across primary JEL categories.

        Args:
            df: DataFrame with JEL codes
            jel_column: Name of column containing JEL codes

        Returns:
            DataFrame with columns: primary_letter, category_name, count, pct
        """
        # Count papers per primary category
        primary_counts = {}

        for codes in df[jel_column]:
            if not isinstance(codes, list):
                continue
            primaries = self.get_primary_categories(codes)
            for letter in primaries:
                primary_counts[letter] = primary_counts.get(letter, 0) + 1

        # Build distribution dataframe
        distribution = []
        for letter in sorted(primary_counts.keys()):
            info = self.decode_code(letter)
            distribution.append({
                'primary_letter': letter,
                'category_name': info['description'] if info else '',
                'count': primary_counts[letter],
                'pct': 100 * primary_counts[letter] / len(df)
            })

        return pd.DataFrame(distribution)


def main():
    """Test the JEL decoder."""
    decoder = JELDecoder()

    # Test individual codes
    print("\nTesting individual code decoding:")
    test_codes = ['C', 'C1', 'C13', 'L1', 'D43', 'E52']
    for code in test_codes:
        info = decoder.decode_code(code)
        if info:
            print(f"  {code}: {info['description']}")
            if info['primary_description']:
                print(f"       Primary: {info['primary_description']}")

    # Test enrichment
    print("\nTesting enrichment:")
    test_jel_codes = ['C13', 'C14', 'L1', 'D43']
    enriched = decoder.enrich_row(test_jel_codes)
    print(f"  Codes: {test_jel_codes}")
    print(f"  Primary letters: {enriched['jel_primary_letters']}")
    print(f"  Primary categories: {enriched['jel_primary_categories']}")
    print(f"  Full descriptions: {enriched['jel_full_descriptions'][:100]}...")


if __name__ == '__main__':
    main()
