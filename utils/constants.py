"""Constants for MECSR scraper."""

from pathlib import Path

# URLs
BASE_URL = "https://www.mecsr.org"
DIRECTORY_URL = f"{BASE_URL}/directory-shopping-centres"

# Default settings
DEFAULT_MAX_CONCURRENT = 10
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_BATCH_SIZE = 10
DEFAULT_RATE_LIMIT_DELAY = 2.0  # seconds

# Output settings
OUTPUT_DIR = "output"
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# File patterns
OUTPUT_FILE_PATTERN = "mecsr_scraping_{timestamp}.json"
MALLS_DB_FILENAME = "mecsr_malls_{timestamp}.json"
FILE_EXTENSION_JSON = ".json"

# HTTP settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 1

# Progress settings
PROGRESS_UPDATE_INTERVAL = 10

# Data validation
MAX_PROPERTY_SIZE_SQM = 1000000  # 1 million sqm
MIN_PROPERTY_SIZE_SQM = 1000     # 1,000 sqm
MAX_LATITUDE = 90.0
MIN_LATITUDE = -90.0
MAX_LONGITUDE = 180.0
MIN_LONGITUDE = -180.0

# Error messages
ERROR_TIMEOUT = "Request timeout"
ERROR_PARSE = "Failed to parse response"
ERROR_NETWORK = "Network error"
ERROR_UNKNOWN = "Unknown error"
ERROR_VALIDATION = "Data validation failed"
ERROR_DUPLICATE = "Duplicate record found"

# Success messages
MSG_STARTING = "🚀 Starting MECSR scraper..."
MSG_COMPLETED = "✅ Scraping completed!"
MSG_SAVED_TO = "💾 Results saved to: {file}"
MSG_PROCESSING_STARTED = "🔄 Processing started..."
MSG_PROCESSING_FINISHED = "✅ Processing finished!"

# Progress messages
MSG_DISCOVERING = "🔍 Discovering mall URLs..."
MSG_PROCESSING_BATCH = "📦 Processing batch {batch}/{total} ({size} items)"
MSG_PROGRESS = "Progress: {current}/{total} ({percentage:.1f}%)"

# Scraping scope constants
TOTAL_MALLS_TO_SCRAPE = 1001
MALLS_PER_PAGE = 12
MAX_PAGES_TO_SCRAPE = 84  # ceil(1001 / 12)

# Test data constants
TEST_LATITUDE = 24.3331427
TEST_LONGITUDE = 54.5239257
TEST_POST_ID = "123"
TEST_USER_ID = "456"
TEST_DATA_ID = "789"
