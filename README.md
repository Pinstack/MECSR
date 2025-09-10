# MECSR Directory Scraper

A comprehensive web scraping solution for extracting shopping centre data from the Middle East Council of Shopping Centres & Retailers (MECSR) directory.

## Overview

This project scrapes and structures data from [MECSR.org](https://www.mecsr.org/directory-shopping-centres), providing clean, validated datasets of shopping centre information across the Middle East and North Africa region.

## Features

- **Complete Coverage**: Extracts all 1,001+ shopping centre listings
- **Structured Data**: Clean, validated data suitable for analysis
- **Multiple Formats**: JSON, CSV, SQLite, and Parquet export options
- **Rate Limiting**: Respectful scraping with intelligent rate limiting
- **Error Resilience**: Robust error handling and retry mechanisms
- **Progress Monitoring**: Real-time progress tracking and reporting

## Quick Start

### Prerequisites

- Python 3.11+
- uv package manager (recommended)

### Quick Start with Makefile (Recommended)

1. **Clone and setup in one command**
   ```bash
   git clone https://github.com/Pinstack/MECSR.git
   cd MECSR
   make setup
   ```

2. **Run the scraper**
   ```bash
   make run                    # Basic scraping to JSON
   make scrape-sample          # Scrape first 10 pages only
   ```

### Manual Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Pinstack/MECSR.git
   cd MECSR
   ```

2. **Set up environment**
   ```bash
   # Create virtual environment
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   # .venv\Scripts\activate   # Windows

   # Install uv if not already available
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Install dependencies
   uv sync
   ```

3. **Run the scraper**
   ```bash
   # Basic usage - scrape to JSON
   python main.py --output-format json

   # Scrape first 10 pages to CSV
   python main.py --output-format csv --max-pages 10

   # Resume from page 50
   python main.py --resume-from 50
   ```

## Usage Examples

### Command Line Options

```bash
python main.py [OPTIONS]

Options:
  --output-format {json,csv,sqlite,parquet}
                        Output format for scraped data (default: json)
  --max-pages INTEGER   Maximum number of pages to scrape
  --resume-from INTEGER Resume scraping from specific page (default: 1)
  --output-dir TEXT     Output directory (default: ./data)
  --verbose             Enable verbose logging
  --dry-run            Perform dry run without scraping
  --help               Show help message
```

### Environment Variables

```bash
# Browser settings
export BROWSER_HEADLESS=true
export BROWSER_VIEWPORT_WIDTH=1920
export BROWSER_VIEWPORT_HEIGHT=1080

# Rate limiting
export REQUESTS_PER_MINUTE=30
export MAX_CONCURRENT_REQUESTS=5

# Retry configuration
export MAX_RETRIES=3
export BASE_RETRY_DELAY=1.0
export MAX_RETRY_DELAY=60.0

# Logging
export LOG_LEVEL=INFO
export LOG_FILE=./logs/scraper.log

# Output configuration
export OUTPUT_DIRECTORY=./data
export OUTPUT_FORMAT=json
```

## Project Structure

```
MECSR/
├── main.py                 # Entry point with CLI ✅
├── config.py              # Configuration management ✅
├── pyproject.toml         # Project dependencies and settings ✅
├── scrapers/              # Data extraction modules
│   └── __init__.py        # ✅ (modules to be implemented)
├── processors/            # Data validation and cleaning
│   └── __init__.py        # ✅ (modules to be implemented)
├── storage/               # Persistence and export
│   └── __init__.py        # ✅ (modules to be implemented)
├── monitoring/            # Observability and logging
│   └── __init__.py        # ✅ (modules to be implemented)
├── utils/                 # Shared utilities
│   └── __init__.py        # ✅ (modules to be implemented)
├── tests/                 # Test suite
│   └── __init__.py        # ✅ (tests to be added)
├── docs/                  # Documentation (empty)
├── memory-bank/           # Project documentation ✅
│   ├── projectbrief.md    # Project overview and requirements
│   ├── productContext.md  # User experience and market context
│   ├── activeContext.md   # Current work focus and next steps
│   ├── systemPatterns.md  # Architecture and technical patterns
│   ├── techContext.md     # Technologies and technical constraints
│   └── progress.md        # Current status and development tracking
└── data/                  # Output directory (created automatically)
```

## Data Schema

Each shopping centre listing includes:

- **Basic Information**: Name, location, property type, status
- **Physical Details**: GLA, total size, number of floors
- **Geographic Data**: Address, city, country, coordinates
- **Business Info**: Website, contact information
- **Metadata**: Scraped date, source URL, data quality scores

## Development

### Makefile Commands (Recommended)

```bash
# Setup development environment
make setup              # Create .venv and install all dependencies
make dev-install        # Install development dependencies only

# Quality checks
make check              # Run all quality checks (lint + format)
make lint               # Run linting only
make format             # Format code only

# Testing
make test               # Run test suite

# Development workflow
make dev                # Run quality checks + tests
make status             # Show project status
```

### Manual Development Commands

```bash
# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Run linting
uv run black .
uv run flake8 .

# Type checking
uv run mypy .
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Set up development environment: `make setup`
4. Make your changes and add tests
5. Run quality checks: `make check`
6. Run tests: `make test`
7. Commit your changes: `git commit -am 'Add your feature'`
8. Push to the branch: `git push origin feature/your-feature`
9. Submit a pull request

## Performance & Limitations

### Performance Targets
- **Throughput**: Reliable extraction of all 1,001 listings
- **Success Rate**: Majority of listings successfully extracted
- **Error Rate**: Basic error handling for common issues

### Rate Limiting
- **Default**: 30 requests per minute
- **Configurable**: Can be adjusted based on needs
- **Respectful**: Includes delays and human-like patterns

### Known Limitations
- Requires JavaScript execution for dynamic content
- Subject to MECSR website structure changes
- Geographic data limited to provided coordinates

## Legal & Ethical Considerations

### Compliance
- Adheres to website terms of service
- Respects robots.txt directives
- Implements ethical scraping practices

### Data Usage
- Proper attribution to MECSR as data source
- Appropriate use for research and analysis
- Respect for privacy and data protection

## Support & Documentation

- **Issues**: [GitHub Issues](https://github.com/Pinstack/MECSR/issues)
- **Documentation**: See `memory-bank/` directory for detailed specifications
- **Memory Bank**: Comprehensive project documentation in `memory-bank/`

## License

This project is for educational and research purposes. Please ensure compliance with MECSR terms of service and applicable laws when using this scraper.

---

**Built with**: [Crawl4AI](https://github.com/unclecode/crawl4ai) | **Python 3.11+** | **uv** package manager
