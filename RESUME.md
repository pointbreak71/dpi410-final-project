# Pipeline Resume Instructions

## Current Status (as of 2026-02-20)

**Pipeline Status**: PAUSED during enrichment step

**Last Activity**: Enriching JPE (Journal of Political Economy) files at ~07:28am on 2026-02-18

**What's Complete**:
- ✅ All papers fetched from OpenAlex (15 journals, various year ranges)
- ✅ Partial enrichment completed for several journals
- ⏳ Enrichment in progress for remaining journals

**What's Remaining**:
- Finish enriching remaining journal files (JPE, QJE, REstat, REStud, and others)
- Build final dataset with JEL hierarchical information
- Export to CSV and Parquet

## To Resume

```bash
cd /Users/maxglanville/Desktop/MPAID/2025-spring/DPI-410/final-project
source .venv/bin/activate
python -m src.pipeline --config config_comprehensive.yaml
```

The pipeline will automatically:
1. Skip all already-fetched papers
2. Skip all fully-enriched files
3. Resume any partially-enriched files from the exact paper it left off on
4. Continue until completion

## Estimated Time Remaining

Approximately 10-15 hours depending on:
- How many journals are already fully enriched
- Network speed and API availability
- Rate limiting delays

## Output Location

When complete, final dataset will be at:
- `data/processed/papers.csv`
- `data/processed/papers.parquet`

## Monitor Progress

```bash
# Watch real-time log
tail -f logs/scraping.log

# Check last 20 log entries
tail -20 logs/scraping.log

# Check if process is running
ps aux | grep "src.pipeline"
```

## Data Currently Collected

Raw data is in: `data/raw/openalex/`
- Each journal has its own subdirectory (e.g., `aer/`, `qje/`, `jpe/`)
- Each year has a `.jsonl` file with raw OpenAlex data
- Enriched files are named `*_enriched.jsonl`

## Troubleshooting

If the pipeline fails or hangs:

1. **Check logs**: `tail -50 logs/scraping.log`
2. **Check which file it's on**: Last line in log shows current file
3. **Safe to restart**: Just re-run the command above - resumption is automatic
4. **Clear partial file**: If a specific file is corrupted, delete its `*_enriched.jsonl` file and it will re-enrich from scratch

## Key Features

- **Mid-file resumption**: If interrupted during a file, picks up at the exact paper it left off
- **Multi-source fallback**: Tries AEA → Crossref → OpenAlex → IDEAS for JEL codes
- **Progress bars**: Shows real-time progress with tqdm
- **Comprehensive logging**: All activity logged to `logs/scraping.log`

## Quick Stats

To see current progress:

```bash
# Count raw vs enriched files per journal
for d in data/raw/openalex/*/; do
  j=$(basename $d)
  raw=$(ls $d | grep -v enriched | wc -l | tr -d ' ')
  enr=$(ls $d | grep enriched | wc -l | tr -d ' ')
  echo "$j: $raw raw, $enr enriched"
done
```

## Final Note

Once complete, you'll have a comprehensive dataset of ~50,000-100,000 economics papers from 1975-2025 with:
- Full metadata (title, authors, DOI, abstract, etc.)
- JEL codes from multiple authoritative sources
- Hierarchical JEL classifications (primary category, subcategories, descriptions)
- Ready for analysis in CSV or Parquet format
