# Economics Journal Metadata Scraper

A comprehensive Python project for scraping, enriching, and analyzing metadata from top economics journals (1975-2025). Features robust multi-source JEL code extraction, hierarchical classification, progress tracking, and intelligent fallback strategies.

## 🎯 Features

- **Multi-Source Scraping**: Fetches paper metadata from OpenAlex with automatic fallback to display-name search
- **Comprehensive JEL Enrichment**: Extracts JEL classification codes from multiple sources:
  - AEA article landing pages
  - Crossref API metadata
  - OpenAlex metadata
  - IDEAS/RePEc database
- **Hierarchical JEL Decoding**: Automatically enriches papers with:
  - Primary letter categories (e.g., 'C' = Mathematical and Quantitative Methods)
  - First-digit subcategories (e.g., 'C1' = Econometric Methods)
  - Second-digit subcategories (e.g., 'C13' = Estimation: General)
  - Human-readable descriptions at all levels
- **Progress Tracking**: Real-time progress bars with tqdm
- **Robust Error Handling**: Exponential backoff, retry logic, and graceful degradation
- **Robots.txt Compliance**: Respects website crawling policies
- **Caching**: Local HTML/API response caching to minimize re-fetching
- **Flexible Configuration**: YAML-based configuration for easy customization
- **Multiple Output Formats**: CSV and Parquet
- **Comprehensive Diagnostics**: Coverage statistics, JEL frequency analysis, and quality checks

## 📊 Supported Journals

### Top 5 Economics Journals
- American Economic Review (AER) - 1975-2025
- Quarterly Journal of Economics (QJE) - 1975-2025
- Journal of Political Economy (JPE) - 1975-2025
- Econometrica - 1975-2025
- Review of Economic Studies (REStat) - 1975-2025

### American Economic Journal Series
- AEJ: Applied Economics - 2009-2025
- AEJ: Macroeconomics - 2009-2025
- AEJ: Microeconomics - 2009-2025
- AEJ: Economic Policy - 2009-2025

### Additional Top Journals
- Review of Economics and Statistics
- Journal of the European Economic Association
- Journal of Labor Economics
- Journal of Applied Econometrics
- Journal of Finance
- Journal of Financial Economics

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- pip or conda

### Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd final-project

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

#### Option 1: Run the Complete Pipeline (Recommended)

```bash
# Run the full pipeline with default configuration
python -m src.pipeline --config config_comprehensive.yaml

# This will:
# 1. Fetch papers from OpenAlex for all configured journals (1975-2025)
# 2. Enrich papers with JEL codes from multiple sources
# 3. Build final dataset with hierarchical JEL information
# 4. Save to data/processed/papers.csv and papers.parquet
```

#### Option 2: Run Individual Steps

```bash
# Step 1: Fetch papers from OpenAlex
python -m src.pipeline --config config_comprehensive.yaml --step fetch

# Step 2: Enrich with JEL codes
python -m src.pipeline --config config_comprehensive.yaml --step enrich

# Step 3: Build final dataset
python -m src.pipeline --config config_comprehensive.yaml --step build
```

#### Option 3: Use Existing Modular Scripts

```bash
# Fetch papers (uses config_multi.yaml)
python -m src.fetch_openalex_multi

# Enrich with JEL codes
python -m src.enrich_jel_multi

# Build dataset
python -m src.build_dataset_multi
```

## 📁 Project Structure

```
final-project/
├── config_comprehensive.yaml    # Main configuration file (1975-2025, all journals)
├── config_multi.yaml            # Alternative config (subset of journals)
├── requirements.txt             # Python dependencies
├── README_COMPREHENSIVE.md      # This file
│
├── src/
│   ├── pipeline.py              # **NEW** Unified pipeline script with progress bars
│   ├── fetch_jel_codes.py       # **NEW** Download AEA JEL classification system
│   ├── jel_decoder.py           # **NEW** JEL hierarchical decoder
│   ├── fetch_openalex_multi.py  # Fetch from OpenAlex (modular)
│   ├── enrich_jel_multi.py      # Enrich with JEL codes (modular)
│   ├── build_dataset_multi.py   # Build final dataset (modular)
│   ├── utils_minimal.py         # Utility functions
│   └── ...
│
├── data/
│   ├── jel_codes.json           # **NEW** JEL classification lookup
│   ├── jel_codes.csv            # **NEW** JEL classification CSV
│   ├── raw/
│   │   ├── openalex/
│   │   │   ├── aer/             # Raw OpenAlex data per journal
│   │   │   │   ├── 1975.jsonl
│   │   │   │   ├── 1975_enriched.jsonl
│   │   │   │   └── ...
│   │   │   ├── qje/
│   │   │   └── ...
│   │   └── cache/               # Cached HTML/API responses
│   └── processed/
│       ├── papers.csv           # Final dataset (CSV)
│       └── papers.parquet       # Final dataset (Parquet)
│
└── logs/
    └── scraping.log             # Detailed execution logs
```

## ⚙️ Configuration

The pipeline uses YAML configuration files. Edit `config_comprehensive.yaml` to customize:

### Date Range

```yaml
years:
  start: 1975
  end: 2025
```

### Scraping Settings

```yaml
scraping:
  rate_limit_delay: 1.0        # Seconds between requests
  max_retries: 3               # Retry attempts for failed requests
  timeout: 30                  # Request timeout
  backoff_multiplier: 2.0      # Exponential backoff factor
  respect_robots_txt: true     # Check robots.txt before scraping
```

### JEL Enrichment Sources

```yaml
jel_enrichment:
  sources:
    - aea_page      # Try AEA article page first
    - crossref      # Then Crossref API
    - openalex      # Then OpenAlex
    - ideas_repec   # Finally IDEAS/RePEc
```

### Adding/Removing Journals

```yaml
journals:
  - key: custom_journal
    name: "My Custom Journal"
    openalex_source_id: "S123456789"  # Find ID at openalex.org
    start_year: 2000
    end_year: 2025
```

## 📊 Output Dataset Schema

The final dataset (`papers.csv` / `papers.parquet`) contains:

### Basic Metadata
- `year`: Publication year
- `journal_key`: Short journal identifier (e.g., "aer")
- `journal`: Full journal name
- `title`: Paper title
- `authors`: Pipe-separated author list
- `doi`: Digital Object Identifier
- `url`: Landing page URL
- `abstract`: Abstract text (if available)

### JEL Classification (Original)
- `jel_codes`: List of JEL codes (e.g., ["C13", "L1", "D43"])
- `jel_raw`: Raw text from which JEL codes were extracted
- `jel_source`: Source of JEL codes (aea_page, crossref, openalex, ideas, missing)

### JEL Hierarchical Enrichment (NEW)
- `jel_primary_letters`: Primary categories (e.g., "C|D|L")
- `jel_primary_categories`: Primary category descriptions
- `jel_full_descriptions`: Full descriptions for all codes
- `jel_count`: Number of JEL codes
- `has_jel`: Boolean indicating JEL code presence

### OpenAlex Metadata
- `openalex_id`: OpenAlex work ID
- `openalex_landing_page_url`: OpenAlex landing page
- `openalex_concepts`: OpenAlex concept tags

## 🔍 Using the JEL Decoder

The JEL decoder can be used independently:

```python
from src.jel_decoder import JELDecoder

# Initialize decoder
decoder = JELDecoder()

# Decode individual codes
info = decoder.decode_code('C13')
print(info['description'])  # "Estimation: General"
print(info['primary_description'])  # "Mathematical and Quantitative Methods"

# Enrich a dataframe
import pandas as pd
df = pd.read_csv('data/processed/papers.csv')
enriched_df = decoder.enrich_dataframe(df)

# Filter by primary category
c_papers = decoder.filter_by_primary_category(df, 'C')

# Get distribution statistics
distribution = decoder.get_primary_category_distribution(df)
print(distribution)

# Get detailed JEL code statistics
stats = decoder.get_code_stats(df)
print(stats.head(20))  # Top 20 most common JEL codes
```

## 🐛 Troubleshooting

### Issue: "JEL codes file not found"

**Solution**: Run the JEL codes fetcher first:
```bash
python -m src.fetch_jel_codes
```

### Issue: "No papers found for journal X"

**Possible causes**:
1. **Incorrect OpenAlex source ID**: Check the journal's OpenAlex page and update the ID in config
2. **Display name mismatch**: The journal name doesn't match OpenAlex's display name
3. **Year range issue**: Journal didn't exist in that year range

**Solution**:
```bash
# Test with a single journal-year
python -c "from src.pipeline import Pipeline; p = Pipeline('config_comprehensive.yaml'); p._fetch_year('S4210188956', 'American Economic Review', 2020)"
```

### Issue: "Rate limiting / Too many requests"

**Solution**: Increase rate limiting delay in config:
```yaml
scraping:
  rate_limit_delay: 2.0  # Increase from 1.0 to 2.0 seconds
```

### Issue: "Low JEL code coverage"

This is expected for older papers (pre-2000). JEL codes became more systematically recorded in the 2000s.

**To check coverage by year**:
```python
import pandas as pd
df = pd.read_csv('data/processed/papers.csv')
coverage = df.groupby('year')['has_jel'].mean() * 100
print(coverage)
```

### Issue: "Script hangs or times out"

**Possible causes**:
1. Network connectivity issues
2. Website is down or blocking requests
3. Robots.txt is preventing access

**Solution**:
1. Check internet connection
2. Try running individual steps separately
3. Check logs: `tail -f logs/scraping.log`
4. Temporarily disable robots.txt checking:
   ```yaml
   scraping:
     respect_robots_txt: false
   ```

### Issue: "Memory error with large datasets"

**Solution**: Process journals in smaller batches by creating a custom config with fewer journals, or use the `--step` flag to run steps separately.

### Issue: "Invalid OpenAlex source ID"

**Solution**:
1. Visit https://openalex.org/
2. Search for the journal
3. Copy the source ID from the URL (e.g., `S123456789`)
4. Update `config_comprehensive.yaml`

Alternatively, set the ID to `<placeholder>` and the script will attempt auto-resolution.

### Issue: "BeautifulSoup/lxml parsing errors"

**Solution**:
```bash
pip install --upgrade beautifulsoup4 lxml
```

## 📈 Performance Tips

### Optimize for Speed

1. **Use caching**: The pipeline automatically caches API responses. On subsequent runs, it will use cached data.
2. **Parallel processing**: While not implemented in the current version, you could modify the pipeline to process journals in parallel using `multiprocessing`.
3. **Skip already-enriched files**: The pipeline automatically skips files that have already been enriched.

### Optimize for Coverage

1. **Run enrichment multiple times**: JEL sources may be temporarily unavailable. Run enrichment again to catch papers that were missed.
2. **Add more sources**: Edit `config_comprehensive.yaml` to add custom JEL extraction sources.

## 📊 Example Analysis

```python
import pandas as pd
from src.jel_decoder import JELDecoder

# Load dataset
df = pd.read_csv('data/processed/papers.csv')

# Basic statistics
print(f"Total papers: {len(df)}")
print(f"Year range: {df['year'].min()}-{df['year'].max()}")
print(f"JEL coverage: {df['has_jel'].sum() / len(df) * 100:.1f}%")

# Papers per journal
print("\nPapers per journal:")
print(df['journal_key'].value_counts())

# Top primary JEL categories
decoder = JELDecoder()
distribution = decoder.get_primary_category_distribution(df)
print("\nTop primary categories:")
print(distribution.head(10))

# Trend analysis: JEL code usage over time
import matplotlib.pyplot as plt

yearly_coverage = df.groupby('year')['has_jel'].mean() * 100
plt.figure(figsize=(12, 6))
plt.plot(yearly_coverage.index, yearly_coverage.values)
plt.xlabel('Year')
plt.ylabel('JEL Coverage (%)')
plt.title('JEL Code Coverage Over Time')
plt.grid(True)
plt.savefig('figs/jel_coverage_trend.png', dpi=300, bbox_inches='tight')

# Most common JEL codes
stats = decoder.get_code_stats(df)
print("\nTop 20 JEL codes:")
print(stats.head(20))

# Papers in a specific category (e.g., Econometric Methods)
c_papers = decoder.filter_by_primary_category(df, 'C')
print(f"\nPapers in 'C' (Mathematical and Quantitative Methods): {len(c_papers)}")
```

## 🔬 Advanced Usage

### Custom JEL Extraction

You can add custom JEL extraction functions to `src/pipeline.py`:

```python
def _fetch_jel_from_custom_source(self, doi: str) -> Optional[Dict]:
    """Fetch JEL codes from your custom source."""
    # Your implementation here
    pass
```

Then add it to the sources list in your config:
```yaml
jel_enrichment:
  sources:
    - aea_page
    - crossref
    - custom_source  # Your new source
    - openalex
```

### Exporting for Analysis

```bash
# Export to Stata
python -c "import pandas as pd; df = pd.read_csv('data/processed/papers.csv'); df.to_stata('papers.dta')"

# Export to Excel
python -c "import pandas as pd; df = pd.read_csv('data/processed/papers.csv'); df.to_excel('papers.xlsx', index=False)"

# Export summary statistics
python -c "import pandas as pd; df = pd.read_csv('data/processed/papers.csv'); df.describe().to_csv('summary_stats.csv')"
```

### Filtering and Subsetting

```python
import pandas as pd

df = pd.read_csv('data/processed/papers.csv')

# Get only top 5 journals
top5 = df[df['journal_key'].isin(['aer', 'qje', 'jpe', 'econometrica', 'restud'])]

# Get papers from recent years
recent = df[df['year'] >= 2015]

# Get papers with specific JEL codes
def has_code(codes, target):
    return isinstance(codes, list) and target in codes

c13_papers = df[df['jel_codes'].apply(lambda c: has_code(c, 'C13'))]

# Export subset
top5_recent = df[(df['journal_key'].isin(['aer', 'qje', 'jpe'])) & (df['year'] >= 2015)]
top5_recent.to_csv('data/processed/top5_recent.csv', index=False)
```

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

1. **Additional journals**: Add more economics journals to the configuration
2. **Better JEL extraction**: Improve regex patterns or add ML-based classification
3. **Parallel processing**: Speed up scraping with concurrent requests
4. **Data validation**: Add more comprehensive data quality checks
5. **Visualization**: Create automatic charts and dashboards
6. **Testing**: Add unit tests and integration tests

## 📝 Citation

If you use this project in your research, please cite:

```bibtex
@software{econ_journal_scraper,
  author = {Your Name},
  title = {Economics Journal Metadata Scraper},
  year = {2025},
  url = {https://github.com/yourusername/yourrepo}
}
```

## 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

## ⚠️ Legal and Ethical Considerations

- **Rate Limiting**: The pipeline includes polite rate limiting to avoid overloading servers.
- **Robots.txt**: Respects robots.txt by default. Keep this enabled unless you have explicit permission.
- **Terms of Service**: Ensure you comply with the terms of service of all data sources (OpenAlex, Crossref, AEA, IDEAS/RePEc).
- **Data Usage**: This tool is intended for academic research purposes. Check licensing terms before commercial use.
- **Attribution**: Always cite data sources in your research.

## 🆘 Support

For issues, questions, or feature requests:

1. Check this README and troubleshooting section
2. Check logs: `logs/scraping.log`
3. Open an issue on GitHub
4. Contact: your.email@institution.edu

## 🗺️ Roadmap

- [ ] Add support for more journals
- [ ] Implement parallel processing for faster scraping
- [ ] Add interactive web dashboard
- [ ] Create pre-trained ML model for JEL classification
- [ ] Add citation network analysis
- [ ] Implement automatic updates (cron job)
- [ ] Add support for other academic databases (Web of Science, Scopus)
