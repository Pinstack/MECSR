# MECSR Streamlined Mall Scraper Makefile
.PHONY: help install run test clean format lint

# Default target
help:
	@echo "MECSR Streamlined Mall Scraper"
	@echo ""
	@echo "Available targets:"
	@echo "  install    Install dependencies"
	@echo "  run        Run the scraper"
	@echo "  test       Run on test batch (100 malls)"
	@echo "  clean      Clean up generated files"
	@echo "  format     Format code with black"
	@echo "  lint       Run linting with flake8"
	@echo "  help       Show this help message"

# Install dependencies
install:
	uv sync

# Run the full scraper
run:
	uv run python simple_mecsr_scraper.py

# Run on test batch
test:
	uv run python simple_mecsr_scraper.py

# Clean up generated files
clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	rm -rf output/
	rm -f uv.lock

# Format code
format:
	uv run black . --line-length 100

# Run linting
lint:
	uv run flake8 . --max-line-length 100 --extend-ignore E203,W503
