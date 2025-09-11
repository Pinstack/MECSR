# MECSR Streamlined Mall Scraper

A fast, efficient web scraper for the Middle East Council of Shopping Centres & Retailers (MECSR) directory.

## Features

- ⚡ **4x faster** than browser-based scraping (1.57 req/sec vs 0.31 req/sec)
- 🔄 **Concurrent processing** with optimized HTTP connections
- 📊 **Clean JSON output** with essential mall data only
- 🏪 **1001 malls** coverage across all directory pages
- 🎯 **84% success rate** with robust error handling

## Quick Start

```bash
# Install dependencies
make install

# Run test batch (100 malls)
make test

# Run full scraper (1001 malls)
make run
```

## Output Format

Each mall record contains:

```json
{
  "url": "https://www.mecsr.org/directory-shopping-centres/example",
  "name": "Example Mall",
  "external_url": "https://www.example-mall.com/",
  "mall_type": "Community Centre",
  "development_status": "Existing Mall",
  "property_details": {
    "type_of_property": "Community Centre",
    "mall_size_sqm": 163107,
    "gla_sqm": 55127,
    "retail_outlets": "116",
    "year_built": "2019 Nov",
    "anchor_tenants": "Beymen, Vakko, Boyner, Migros, LCW"
  },
  "location": {
    "latitude": 25.2899867,
    "longitude": 55.4974664,
    "address": "Full address"
  },
  "tenants": [
    {"name": "Beymen", "category": "luxury"},
    {"name": "Vakko", "category": "fashion"}
  ],
  "total_tenants": 5,
  "first_image": "https://image-url.jpg"
}
```

## Performance

- **Test Batch (100 malls)**: ~64 seconds (1.57 req/sec)
- **Full Run (1001 malls)**: ~11 minutes (1.57 req/sec)
- **Success Rate**: 70-84% depending on mall availability

## Project Structure

```
├── simple_mecsr_scraper.py     # Main scraper
├── enhanced_extractor.py       # Data extraction methods
├── scrapers/
│   ├── pagination_crawler.py   # HTTP-based crawling
│   └── detail_extractor.py    # URL collection
├── utils/
│   ├── constants.py           # Configuration constants
│   ├── helpers.py            # Utility functions
│   └── reporting.py          # Report generation
├── config.py                  # Application configuration
├── pyproject.toml            # Dependencies
└── Makefile                  # Build automation
```

## Dependencies

- `aiohttp` - Fast async HTTP client
- `beautifulsoup4` + `lxml` - HTML parsing
- `pydantic` - Data validation

## Development

```bash
# Format code
make format

# Lint code
make lint

# Clean up
make clean
```

## Architecture

1. **Directory Discovery**: Collect all mall URLs from paginated directory
2. **Concurrent Scraping**: HTTP-based parallel requests with connection pooling
3. **Data Extraction**: CSS selector-based parsing with BeautifulSoup
4. **Result Storage**: Clean JSON output with essential fields only