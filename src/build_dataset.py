"""Combine raw OpenAlex files, deduplicate, construct tidy dataset, and save parquet.

Run as: python -m src.build_dataset
"""
from __future__ import annotations

import json
from pathlib import Path
import yaml
import pandas as pd

from .utils import reconstruct_abstract, deduplicate_papers, extract_jel_prefixes, label_jel_codes


ROOT = Path(__file__).resolve().parents[1]


def load_config(path: Path | str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def collect_raw(rawdir: Path) -> pd.DataFrame:
    rows = []
    for p in rawdir.glob("openalex_*_repec.jsonl"):
        # infer journal and year from filename
        parts = p.stem.split("_")
        # openalex_{journal}_{year}_repec
        if len(parts) < 3:
            continue
        journal = parts[1]
        year = int(parts[2]) if parts[2].isdigit() else None
        with open(p, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                title = obj.get("title")
                doi = obj.get("doi") or (obj.get("ids") or {}).get("doi")
                openalex_id = obj.get("id")
                abstract = reconstruct_abstract(obj.get("abstract_inverted_index") or {})
                jel = obj.get("repec_jel") or []
                rows.append({
                    "year": year,
                    "journal": journal,
                    "title": title,
                    "doi": doi,
                    "openalex_id": openalex_id,
                    "abstract": abstract,
                    "jel_codes": jel,
                })
    df = pd.DataFrame(rows)
    return df


def build_and_save(config_path: str = "config.yaml"):
    cfg = load_config(config_path)
    rawdir = ROOT / "data" / "raw"
    procdir = ROOT / "data" / "processed"
    procdir.mkdir(parents=True, exist_ok=True)
    df = collect_raw(rawdir)
    if df.empty:
        print("No raw files found — run fetch_openalex and fetch_repec_jel first.")
        return
    df = deduplicate_papers(df)
    # compute jel_primary_letter and prefixes and label
    df["jel_primary_letter"] = df["jel_codes"].apply(lambda x: (x[0][0] if x else "") )
    df["jel_prefixes"] = df["jel_codes"].apply(lambda x: extract_jel_prefixes(x))
    df["label_firm_market"] = df["jel_codes"].apply(lambda x: label_jel_codes(x))
    out = procdir / "papers.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote processed dataset to {out}")


if __name__ == "__main__":
    build_and_save()
