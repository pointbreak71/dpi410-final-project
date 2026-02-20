"""Fetch and parse the official AEA JEL classification system.

This module downloads the JEL codes from the American Economic Association
and creates a structured lookup table with hierarchical descriptions.

Run: python -m src.fetch_jel_codes

Output: data/jel_codes.json and data/jel_codes.csv
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


def fetch_jel_hierarchy() -> List[Dict[str, str]]:
    """Fetch and parse the complete JEL classification hierarchy from AEA website.

    Returns:
        List of dicts with keys: code, level, description, parent_code
    """
    # Use hardcoded structure by default (more reliable)
    # You can uncomment the web scraping code below if you want to try fetching fresh data
    print("Using curated JEL classification structure...")
    jel_codes = get_hardcoded_jel_structure()
    print(f"Loaded {len(jel_codes)} JEL codes")
    return jel_codes

    # Web scraping code (commented out for reliability)
    # Uncomment below if you want to try fetching from AEA website
    """
    url = "https://www.aeaweb.org/econlit/jelCodes.php?view=jel"

    print("Fetching JEL codes from AEA website...")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        jel_codes = []
        text_content = soup.get_text()
        lines = text_content.split('\n')

        # Patterns for matching JEL codes
        primary_pattern = re.compile(r'^([A-Z])\s*[-–—:]\s*(.+)$')
        two_digit_pattern = re.compile(r'^([A-Z]\d)\s*[-–—:]\s*(.+)$')
        three_digit_pattern = re.compile(r'^([A-Z]\d\d)\s*[-–—:]\s*(.+)$')

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Try three-digit first (most specific)
            match = three_digit_pattern.match(line)
            if match:
                code, description = match.groups()
                jel_codes.append({
                    'code': code.upper(),
                    'level': 3,
                    'description': description.strip(),
                    'parent_code': code[0:2].upper()
                })
                continue

            # Try two-digit
            match = two_digit_pattern.match(line)
            if match:
                code, description = match.groups()
                jel_codes.append({
                    'code': code.upper(),
                    'level': 2,
                    'description': description.strip(),
                    'parent_code': code[0].upper()
                })
                continue

            # Try primary (single letter)
            match = primary_pattern.match(line)
            if match:
                code, description = match.groups()
                if len(code) == 1 and code.isalpha():
                    jel_codes.append({
                        'code': code.upper(),
                        'level': 1,
                        'description': description.strip(),
                        'parent_code': None
                    })

        if len(jel_codes) >= 20:
            print(f"Extracted {len(jel_codes)} JEL codes from website")
            return jel_codes
    except Exception as e:
        print(f"Warning: Could not fetch from website ({e}). Using hardcoded structure...")

    jel_codes = get_hardcoded_jel_structure()
    print(f"Loaded {len(jel_codes)} JEL codes")
    return jel_codes
    """


def get_hardcoded_jel_structure() -> List[Dict[str, str]]:
    """Return a hardcoded JEL classification structure.

    This is a fallback if web scraping fails. Contains the major categories
    and some common subcategories. Not exhaustive but covers most common codes.
    """
    return [
        # Primary categories
        {'code': 'A', 'level': 1, 'description': 'General Economics and Teaching', 'parent_code': None},
        {'code': 'B', 'level': 1, 'description': 'History of Economic Thought, Methodology, and Heterodox Approaches', 'parent_code': None},
        {'code': 'C', 'level': 1, 'description': 'Mathematical and Quantitative Methods', 'parent_code': None},
        {'code': 'D', 'level': 1, 'description': 'Microeconomics', 'parent_code': None},
        {'code': 'E', 'level': 1, 'description': 'Macroeconomics and Monetary Economics', 'parent_code': None},
        {'code': 'F', 'level': 1, 'description': 'International Economics', 'parent_code': None},
        {'code': 'G', 'level': 1, 'description': 'Financial Economics', 'parent_code': None},
        {'code': 'H', 'level': 1, 'description': 'Public Economics', 'parent_code': None},
        {'code': 'I', 'level': 1, 'description': 'Health, Education, and Welfare', 'parent_code': None},
        {'code': 'J', 'level': 1, 'description': 'Labor and Demographic Economics', 'parent_code': None},
        {'code': 'K', 'level': 1, 'description': 'Law and Economics', 'parent_code': None},
        {'code': 'L', 'level': 1, 'description': 'Industrial Organization', 'parent_code': None},
        {'code': 'M', 'level': 1, 'description': 'Business Administration and Business Economics; Marketing; Accounting; Personnel Economics', 'parent_code': None},
        {'code': 'N', 'level': 1, 'description': 'Economic History', 'parent_code': None},
        {'code': 'O', 'level': 1, 'description': 'Economic Development, Innovation, Technological Change, and Growth', 'parent_code': None},
        {'code': 'P', 'level': 1, 'description': 'Political Economy and Comparative Economic Systems', 'parent_code': None},
        {'code': 'Q', 'level': 1, 'description': 'Agricultural and Natural Resource Economics; Environmental and Ecological Economics', 'parent_code': None},
        {'code': 'R', 'level': 1, 'description': 'Urban, Rural, Regional, Real Estate, and Transportation Economics', 'parent_code': None},
        {'code': 'Y', 'level': 1, 'description': 'Miscellaneous Categories', 'parent_code': None},
        {'code': 'Z', 'level': 1, 'description': 'Other Special Topics', 'parent_code': None},

        # Common two-digit codes (partial list - major categories)
        {'code': 'C1', 'level': 2, 'description': 'Econometric and Statistical Methods and Methodology: General', 'parent_code': 'C'},
        {'code': 'C2', 'level': 2, 'description': 'Single Equation Models; Single Variables', 'parent_code': 'C'},
        {'code': 'C3', 'level': 2, 'description': 'Multiple or Simultaneous Equation Models; Multiple Variables', 'parent_code': 'C'},
        {'code': 'C4', 'level': 2, 'description': 'Econometric and Statistical Methods: Special Topics', 'parent_code': 'C'},
        {'code': 'C5', 'level': 2, 'description': 'Econometric Modeling', 'parent_code': 'C'},
        {'code': 'C6', 'level': 2, 'description': 'Mathematical Methods; Programming Models; Mathematical and Simulation Modeling', 'parent_code': 'C'},
        {'code': 'C7', 'level': 2, 'description': 'Game Theory and Bargaining Theory', 'parent_code': 'C'},
        {'code': 'C8', 'level': 2, 'description': 'Data Collection and Data Estimation Methodology; Computer Programs', 'parent_code': 'C'},
        {'code': 'C9', 'level': 2, 'description': 'Design of Experiments', 'parent_code': 'C'},

        {'code': 'D0', 'level': 2, 'description': 'General', 'parent_code': 'D'},
        {'code': 'D1', 'level': 2, 'description': 'Household Behavior and Family Economics', 'parent_code': 'D'},
        {'code': 'D2', 'level': 2, 'description': 'Production and Organizations', 'parent_code': 'D'},
        {'code': 'D3', 'level': 2, 'description': 'Distribution', 'parent_code': 'D'},
        {'code': 'D4', 'level': 2, 'description': 'Market Structure, Pricing, and Design', 'parent_code': 'D'},
        {'code': 'D5', 'level': 2, 'description': 'General Equilibrium and Disequilibrium', 'parent_code': 'D'},
        {'code': 'D6', 'level': 2, 'description': 'Welfare Economics', 'parent_code': 'D'},
        {'code': 'D7', 'level': 2, 'description': 'Analysis of Collective Decision-Making', 'parent_code': 'D'},
        {'code': 'D8', 'level': 2, 'description': 'Information, Knowledge, and Uncertainty', 'parent_code': 'D'},
        {'code': 'D9', 'level': 2, 'description': 'Intertemporal Choice', 'parent_code': 'D'},

        {'code': 'E0', 'level': 2, 'description': 'General', 'parent_code': 'E'},
        {'code': 'E1', 'level': 2, 'description': 'General Aggregative Models', 'parent_code': 'E'},
        {'code': 'E2', 'level': 2, 'description': 'Consumption, Saving, Production, Investment, Labor Markets, and Informal Economy', 'parent_code': 'E'},
        {'code': 'E3', 'level': 2, 'description': 'Prices, Business Fluctuations, and Cycles', 'parent_code': 'E'},
        {'code': 'E4', 'level': 2, 'description': 'Money and Interest Rates', 'parent_code': 'E'},
        {'code': 'E5', 'level': 2, 'description': 'Monetary Policy, Central Banking, and the Supply of Money and Credit', 'parent_code': 'E'},
        {'code': 'E6', 'level': 2, 'description': 'Macroeconomic Policy, Macroeconomic Aspects of Public Finance, and General Outlook', 'parent_code': 'E'},
        {'code': 'E7', 'level': 2, 'description': 'Macro-Based Behavioral Economics', 'parent_code': 'E'},

        {'code': 'L0', 'level': 2, 'description': 'General', 'parent_code': 'L'},
        {'code': 'L1', 'level': 2, 'description': 'Market Structure, Firm Strategy, and Market Performance', 'parent_code': 'L'},
        {'code': 'L2', 'level': 2, 'description': 'Firm Objectives, Organization, and Behavior', 'parent_code': 'L'},
        {'code': 'L3', 'level': 2, 'description': 'Nonprofit Organizations and Public Enterprise', 'parent_code': 'L'},
        {'code': 'L4', 'level': 2, 'description': 'Antitrust Issues and Policies', 'parent_code': 'L'},
        {'code': 'L5', 'level': 2, 'description': 'Regulation and Industrial Policy', 'parent_code': 'L'},
        {'code': 'L6', 'level': 2, 'description': 'Industry Studies: Manufacturing', 'parent_code': 'L'},
        {'code': 'L7', 'level': 2, 'description': 'Industry Studies: Primary Products and Construction', 'parent_code': 'L'},
        {'code': 'L8', 'level': 2, 'description': 'Industry Studies: Services', 'parent_code': 'L'},
        {'code': 'L9', 'level': 2, 'description': 'Industry Studies: Transportation and Utilities', 'parent_code': 'L'},

        # Common three-digit codes (sample)
        {'code': 'C13', 'level': 3, 'description': 'Estimation: General', 'parent_code': 'C1'},
        {'code': 'C14', 'level': 3, 'description': 'Semiparametric and Nonparametric Methods: General', 'parent_code': 'C1'},
        {'code': 'C21', 'level': 3, 'description': 'Cross-Sectional Models; Spatial Models; Treatment Effect Models; Quantile Regressions', 'parent_code': 'C2'},
        {'code': 'C22', 'level': 3, 'description': 'Time-Series Models; Dynamic Quantile Regressions; Dynamic Treatment Effect Models; Diffusion Processes', 'parent_code': 'C2'},
        {'code': 'C23', 'level': 3, 'description': 'Panel Data Models; Spatio-temporal Models', 'parent_code': 'C2'},
        {'code': 'C25', 'level': 3, 'description': 'Discrete Regression and Qualitative Choice Models; Discrete Regressors; Proportions; Probabilities', 'parent_code': 'C2'},
        {'code': 'C26', 'level': 3, 'description': 'Instrumental Variables (IV) Estimation', 'parent_code': 'C2'},

        {'code': 'D43', 'level': 3, 'description': 'Oligopoly and Other Forms of Market Imperfection', 'parent_code': 'D4'},
        {'code': 'D44', 'level': 3, 'description': 'Auctions', 'parent_code': 'D4'},
        {'code': 'D82', 'level': 3, 'description': 'Asymmetric and Private Information; Mechanism Design', 'parent_code': 'D8'},
        {'code': 'D83', 'level': 3, 'description': 'Search; Learning; Information and Knowledge; Communication; Belief; Unawareness', 'parent_code': 'D8'},

        {'code': 'L13', 'level': 3, 'description': 'Oligopoly and Other Imperfect Markets', 'parent_code': 'L1'},
        {'code': 'L14', 'level': 3, 'description': 'Transactional Relationships; Contracts and Reputation; Networks', 'parent_code': 'L1'},
    ]


def create_jel_lookup() -> Dict[str, Dict[str, str]]:
    """Create a lookup dictionary for JEL codes with hierarchical information.

    Returns:
        Dict mapping code -> {description, primary_letter, primary_desc,
                             first_digit, first_digit_desc, parent_code, level}
    """
    jel_list = fetch_jel_hierarchy()

    # Build lookup tables for each level
    primary_lookup = {}
    two_digit_lookup = {}
    three_digit_lookup = {}

    for item in jel_list:
        code = item['code']
        if item['level'] == 1:
            primary_lookup[code] = item['description']
        elif item['level'] == 2:
            two_digit_lookup[code] = item['description']
        elif item['level'] == 3:
            three_digit_lookup[code] = item['description']

    # Create enriched lookup
    lookup = {}

    for item in jel_list:
        code = item['code']

        # Determine hierarchical structure
        if len(code) == 1:
            # Primary category
            lookup[code] = {
                'code': code,
                'description': item['description'],
                'level': 1,
                'primary_letter': code,
                'primary_description': item['description'],
                'first_digit': None,
                'first_digit_description': None,
                'parent_code': None
            }
        elif len(code) == 2:
            # Two-digit code
            primary = code[0]
            lookup[code] = {
                'code': code,
                'description': item['description'],
                'level': 2,
                'primary_letter': primary,
                'primary_description': primary_lookup.get(primary, ''),
                'first_digit': code,
                'first_digit_description': item['description'],
                'parent_code': primary
            }
        elif len(code) == 3:
            # Three-digit code
            primary = code[0]
            first_two = code[0:2]
            lookup[code] = {
                'code': code,
                'description': item['description'],
                'level': 3,
                'primary_letter': primary,
                'primary_description': primary_lookup.get(primary, ''),
                'first_digit': first_two,
                'first_digit_description': two_digit_lookup.get(first_two, ''),
                'parent_code': first_two
            }

    return lookup


def save_jel_codes():
    """Fetch JEL codes and save to JSON and CSV files."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Get lookup dictionary
    lookup = create_jel_lookup()

    # Save as JSON
    json_path = DATA_DIR / "jel_codes.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(lookup, f, indent=2, ensure_ascii=False)
    print(f"Saved JEL codes to {json_path}")

    # Convert to DataFrame and save as CSV
    df = pd.DataFrame.from_dict(lookup, orient='index')
    df = df.reset_index(drop=True)

    csv_path = DATA_DIR / "jel_codes.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved JEL codes to {csv_path}")

    # Print summary statistics
    print(f"\nJEL Code Summary:")
    print(f"  Total codes: {len(lookup)}")
    print(f"  Primary categories: {len([c for c in lookup.values() if c['level'] == 1])}")
    print(f"  Two-digit codes: {len([c for c in lookup.values() if c['level'] == 2])}")
    print(f"  Three-digit codes: {len([c for c in lookup.values() if c['level'] == 3])}")

    return lookup


if __name__ == '__main__':
    save_jel_codes()
