import pandas as pd

from src.utils import deduplicate_papers


def test_deduplication():
    rows = [
        {"title": "A Study", "year": 2020, "journal": "A", "doi": "10.1/abc"},
        {"title": "A Study", "year": 2020, "journal": "A", "doi": "10.1/ABC"},
        {"title": "Similar", "year": 2020, "journal": "A", "doi": None},
        {"title": "Similar ", "year": 2020, "journal": "A", "doi": None},
    ]
    df = pd.DataFrame(rows)
    out = deduplicate_papers(df)
    # Expect 2 unique: one for DOI, one for title-based
    assert len(out) == 2
