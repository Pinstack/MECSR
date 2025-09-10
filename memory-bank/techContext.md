# Technical Context: MECSR Directory Scraper

## Technology Stack

### Core Framework: Crawl4AI (✅ ENHANCED WITH PLAYWRIGHT MCP)
**Version**: 0.7.0+ (latest stable with aggressive optimization)
**Purpose**: Enhanced web crawling and scraping framework with comprehensive data extraction
**Key Capabilities**:
- Aggressive async-first architecture optimized for high-performance scraping
- Enhanced browser automation with Playwright MCP for hidden data discovery
- Advanced LLM-friendly data extraction strategies with Post Details, tenants, images
- Native support for comprehensive structured data extraction
- Intelligent rate limiting (45 req/min) with no detection
- Multiple extraction strategies (CSS, XPath, LLM-based, Playwright MCP)

**Installation Requirements**:
```bash
# Core installation
uv add crawl4ai[all]>=0.7.0

# Post-installation setup
crawl4ai-setup
crawl4ai-doctor  # Health check
```

**Dependencies**:
- **Playwright**: Browser automation engine
- **AsyncIO**: Python's asynchronous I/O framework
- **Pydantic**: Data validation and serialization
- **BeautifulSoup4/lxml**: HTML parsing engines
- **aiohttp**: HTTP client for async requests

### Programming Language & Runtime

#### Python 3.11+
**Rationale**:
- Excellent async/await support for I/O-bound operations
- Rich ecosystem of data processing libraries
- Strong typing support with modern type hints
- Mature web scraping and data science libraries

**Key Python Libraries**:
```python
# Core Dependencies (pyproject.toml)
crawl4ai[all]>=0.7.0          # Web crawling & scraping
pydantic>=2.0                 # Data validation & models
pandas>=2.0                   # Data manipulation
rich>=13.0                    # Enhanced CLI output
loguru>=0.7                   # Structured logging
asyncio                        # Async operations (built-in)
pathlib                        # Path handling (built-in)

# Optional but recommended
sentence-transformers          # Text processing
torch>=2.0                     # ML operations
transformers                   # LLM integration
```

### Development Environment

#### Local Development Setup
**Requirements**:
- **OS**: macOS, Linux, or Windows with WSL
- **Python**: 3.11+ with uv installed
- **Memory**: 4GB+ RAM (8GB recommended)
- **Storage**: 5GB+ free space
- **Network**: Stable internet connection

**Development Tools**:
```bash
# Python environment management with .venv
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Development dependencies with uv
uv pip install -e .
uv add --dev pytest pytest-asyncio black flake8 mypy pre-commit
pre-commit install  # Code quality hooks

# Alternative: Install from pyproject.toml
uv sync
```


### Data Processing Technologies

#### Data Validation: Pydantic
**Usage**: Strong typing and automatic validation for extracted data
```python
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime

class MallListing(BaseModel):
    mall_id: str = Field(..., regex=r'^[a-z0-9_-]+$')
    name: str = Field(..., min_length=1, max_length=200)
    gla_sqm: Optional[int] = Field(None, gt=0, le=1000000)
    scraped_at: datetime = Field(default_factory=datetime.now)

    @validator('gla_sqm')
    def validate_gla_range(cls, v):
        if v and (v < 100 or v > 1000000):
            raise ValueError('GLA must be between 100 and 1,000,000 sqm')
        return v
```

#### Enhanced Data Processing: EnhancedDataExtractor (✅ NEW CAPABILITY)
**Usage**: Comprehensive data extraction with multiple data sources and categorization
```python
class EnhancedDataExtractor:
    def extract_comprehensive_mall_data(self, html: str, url: str) -> Dict[str, Any]:
        # Enhanced extraction capabilities
        return {
            'property_details': self._extract_property_details_comprehensive(soup),  # Post Details
            'tenant_data': self._extract_tenant_data_comprehensive(soup),  # 78+ tenants
            'media_content': self._extract_media_content_comprehensive(soup),  # 47+ images
            'contact_info': self._extract_contact_info_comprehensive(soup),  # External URLs
            'structured_data': self._extract_structured_data_comprehensive(soup)  # JSON-LD
        }

    def _categorize_tenant(self, tenant_name: str) -> str:
        # Intelligent tenant categorization
        categories = {
            'fashion': ['h&m', 'zara', 'forever21', 'centrepoint'],
            'food': ['starbucks', 'carrefour', 'mcdonalds', 'kfc'],
            'electronics': ['sharaf dg', 'jumbo'],
            # 10+ categories with 50+ keywords
        }
```

**Capabilities**:
- Post Details table extraction (15+ specifications per mall)
- Tenant categorization (fashion, food, electronics, entertainment, etc.)
- Property image extraction with metadata (dimensions, alt text)
- External URL extraction (mall websites, social media)
- Structured data extraction (JSON-LD, microdata)
- Comprehensive validation and quality scoring

#### Data Manipulation: Pandas
**Usage**: Efficient data processing and export to multiple formats
```python
import pandas as pd
from typing import List

class DataExporter:
    def export_to_csv(self, listings: List[MallListing], filename: str):
        df = pd.DataFrame([listing.dict() for listing in listings])
        df.to_csv(filename, index=False)

    def export_to_json(self, listings: List[MallListing], filename: str):
        df = pd.DataFrame([listing.dict() for listing in listings])
        df.to_json(filename, orient='records', indent=2)

    def export_to_excel(self, listings: List[MallListing], filename: str):
        df = pd.DataFrame([listing.dict() for listing in listings])
        df.to_excel(filename, index=False)
```

### Storage Technologies

#### JSON Format (YAGNI-Compliant)
**Simple JSON Output** (Initial Implementation):
- Human-readable structure
- Easy to work with in most programming languages
- Supports all required data fields
- Simple file I/O operations

**Future Expansion** (When Needed):
- CSV format for Excel compatibility
- SQLite for complex queries
- Parquet for large-scale analytics

### Monitoring & Logging Technologies

#### Structured Logging: Loguru
**Configuration**:
```python
from loguru import logger
import sys

# Console logging with colors
logger.add(sys.stdout, format="{time} | {level} | {module}:{function}:{line} | {message}",
           level="INFO", colorize=True)

# File logging with rotation
logger.add("logs/crawler_{time}.log", rotation="1 day", retention="7 days",
           format="{time} | {level} | {module}:{function}:{line} | {message}")

# Structured JSON logging for analysis
logger.add("logs/crawler.json", rotation="1 day", serialize=True)
```

#### Progress Monitoring: Rich
**Usage**: Beautiful console output with progress bars
```python
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table

console = Console()

# Progress bar for crawling
with Progress(
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
) as progress:
    task = progress.add_task("Crawling pages...", total=84)
    # Update progress as pages are processed

# Data quality table
table = Table(title="Data Quality Report")
table.add_column("Metric", style="cyan")
table.add_column("Value", style="magenta")
table.add_row("Total Listings", "1,001")
table.add_row("Success Rate", "98.5%")
console.print(table)
```

### Testing Framework

#### pytest with Async Support
**Configuration**:
```python
# pytest.ini
[tool:pytest.ini_options]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --tb=short --asyncio-mode=auto
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
```

**Test Structure**:
```python
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

class TestPaginationCrawler:
    @pytest.fixture
    async def crawler(self):
        return PaginationCrawler()

    @pytest.mark.asyncio
    async def test_crawl_page_success(self, crawler):
        # Mock successful page crawl
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.html = "<html><body>Test content</body></html>"

        with patch.object(crawler, '_crawl_single_page', return_value=mock_result):
            result = await crawler.crawl_page(1)
            assert result.success is True
            assert "Test content" in result.html
```

### Performance Optimization Technologies

#### Aggressive Async Processing Patterns (✅ ENHANCED)
**Concurrent Request Management with Intelligent Rate Limiting**:
```python
import asyncio
from typing import List, TypeVar

T = TypeVar('T')

class AggressiveConcurrentProcessor:
    def __init__(self, max_concurrent: int = 15, rate_limit_rpm: int = 45):  # Aggressive settings
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = RateLimiter(requests_per_minute=rate_limit_rpm)

    async def process_batch_aggressive(self, items: List[T]) -> List[T]:
        async def process_item(item: T) -> T:
            async with self.semaphore:
                await self.rate_limiter.acquire_token()  # Intelligent rate limiting
                return await self._process_single_item_enhanced(item)  # Enhanced extraction

        tasks = [process_item(item) for item in items]
        return await asyncio.gather(*tasks, return_exceptions=True)
```

#### Memory Management
**Streaming Processing for Large Datasets**:
```python
import json
from typing import Iterator, List
from pathlib import Path

class StreamingProcessor:
    def __init__(self, chunk_size: int = 100):
        self.chunk_size = chunk_size

    async def process_large_dataset(self, source_file: Path, output_file: Path):
        """Process large JSON files without loading everything into memory"""
        with open(source_file, 'r') as infile, open(output_file, 'w') as outfile:
            outfile.write('[\n')

            first_item = True
            for chunk in self._read_json_chunks(infile):
                processed_chunk = await self._process_chunk(chunk)

                for item in processed_chunk:
                    if not first_item:
                        outfile.write(',\n')
                    json.dump(item, outfile, indent=2)
                    first_item = False

            outfile.write('\n]')

    def _read_json_chunks(self, file) -> Iterator[List[dict]]:
        """Read JSON file in chunks"""
        buffer = []
        for line in file:
            buffer.append(json.loads(line.strip()))
            if len(buffer) >= self.chunk_size:
                yield buffer
                buffer = []
        if buffer:
            yield buffer
```

### Security Considerations

#### Rate Limiting & Anti-Bot Measures
**Implementation Strategy**:
```python
class RateLimiter:
    def __init__(self, requests_per_minute: int = 30):
        self.requests_per_minute = requests_per_minute
        self.tokens = requests_per_minute
        self.last_refill = time.time()

    async def acquire_token(self):
        """Implement token bucket algorithm"""
        now = time.time()
        time_passed = now - self.last_refill
        self.tokens = min(
            self.requests_per_minute,
            self.tokens + time_passed * (self.requests_per_minute / 60)
        )

        if self.tokens >= 1:
            self.tokens -= 1
            self.last_refill = now
            return True
        else:
            # Wait for next token
            wait_time = (1 - self.tokens) * (60 / self.requests_per_minute)
            await asyncio.sleep(wait_time)
            return await self.acquire_token()
```

#### User Agent & Browser Fingerprinting
**Configuration**:
```python
browser_config = BrowserConfig(
    headless=True,
    user_agent="MECSR-Research-Crawler/1.0 (Educational Project)",
    viewport_width=1920,
    viewport_height=1080,
    java_script_enabled=True,
    # Additional anti-detection measures
    ignore_https_errors=True,
    bypass_csp=True
)
```

### Local Execution

#### Environment Setup
**Production Execution**:
```bash
# Activate virtual environment
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Run the scraper
python main.py --output-format json

# Or with custom options
python main.py \
  --output-format csv \
  --max-pages 50 \
  --resume-from 25
```

**Environment Variables**:
```bash
# Set environment variables for production
export BROWSER_HEADLESS=true
export REQUESTS_PER_MINUTE=20
export LOG_LEVEL=WARNING
export OUTPUT_FORMAT=json
```

### Version Control & CI/CD

#### Git Workflow
**Branch Strategy**:
- `main`: Production-ready code
- `develop`: Integration branch
- `feature/*`: Feature branches
- `hotfix/*`: Critical fixes

#### CI/CD Pipeline
```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --frozen --no-install-project
          uv run crawl4ai-setup
      - name: Run tests
        run: uv run pytest
      - name: Run linting
        run: uv run black --check . && uv run flake8 .
```

### Maintenance & Updates

#### Dependency Management
**pyproject.toml Structure**:
```toml
[project]
name = "mecsr-scraper"
version = "0.1.0"
description = "MECSR Directory Web Scraper"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "crawl4ai[all]>=0.7.0",
    "pydantic>=2.0.0",
    "pandas>=2.0.0",
    "rich>=13.0.0",
    "loguru>=0.7.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "black>=23.0.0",
    "flake8>=6.0.0",
    "mypy>=1.0.0",
    "pre-commit>=3.0.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

**Dependency Commands**:
```bash
# Add new dependency
uv add package-name

# Add development dependency
uv add --dev package-name

# Update all dependencies
uv lock --upgrade

# Sync environment with lockfile
uv sync
```

#### Update Strategy
- **Monthly**: Security updates with `uv lock --upgrade`
- **Quarterly**: Major dependency updates and uv version upgrades
- **Annually**: Framework upgrades and breaking changes

---
*This technical context provides the complete technology foundation for building, deploying, and maintaining the MECSR scraping solution.*
