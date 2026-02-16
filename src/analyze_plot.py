"""Analyze processed dataset and produce the main figure.

Run as: python -m src.analyze_plot
"""
from __future__ import annotations

from pathlib import Path
import argparse
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]


def plot_share(df: pd.DataFrame, by_journal: bool = False, include_unclear: bool = False, outpath: Path | str = None):
    df = df.copy()
    if not include_unclear:
        df = df[df["label_firm_market"] != "unclear"]
    if by_journal:
        grouped = df.groupby(["year", "journal"])['label_firm_market'].apply(lambda s: (s=="market").mean()).unstack('journal')
        ax = grouped.plot()
    else:
        series = df.groupby('year')['label_firm_market'].apply(lambda s: (s=="market").mean())
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(series.index, series.values, marker='o')
        ax.set_ylabel('Share market')
        ax.set_xlabel('Year')
        ax.set_ylim(0, 1)
        ax.grid(True)
    if outpath:
        plt.tight_layout()
        plt.savefig(outpath)
        print(f"Saved figure to {outpath}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--by-journal', action='store_true')
    parser.add_argument('--include-unclear', action='store_true')
    parser.add_argument('--infile', default=str(ROOT / 'data' / 'processed' / 'papers.parquet'))
    parser.add_argument('--out', default=str(ROOT / 'figs' / 'share_market_over_time.png'))
    args = parser.parse_args()
    df = pd.read_parquet(args.infile)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    plot_share(df, by_journal=args.by_journal, include_unclear=args.include_unclear, outpath=args.out)


if __name__ == '__main__':
    main()
