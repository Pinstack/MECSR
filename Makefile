# MECSR Directory Scraper Makefile
# YAGNI-compliant build system for simple, efficient development workflow

.PHONY: help setup install dev-install run test lint format clean check-env docs build

# Default target
help:
	@echo "MECSR Directory Scraper - Available commands:"
	@echo ""
	@echo "Environment Setup:"
	@echo "  setup         - Create .venv and install dependencies"
	@echo "  install       - Install production dependencies only"
	@echo "  dev-install   - Install development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  run           - Run the scraper"
	@echo "  test          - Run test suite"
	@echo "  lint          - Run linting checks"
	@echo "  format        - Format code with black"
	@echo "  check         - Run all quality checks (lint + format)"
	@echo ""
	@echo "Utilities:"
	@echo "  clean         - Remove generated files and cache"
	@echo "  check-env     - Verify environment setup"
	@echo "  docs          - Generate/update documentation"
	@echo "  build         - Build distribution package"
	@echo ""
	@echo "Examples:"
	@echo "  make setup && make run"
	@echo "  make check && make test"

# Environment setup
setup: check-env
	@echo "🐍 Setting up Python virtual environment..."
	python3 -m venv .venv
	@echo "📦 Installing dependencies with uv..."
	. .venv/bin/activate && uv sync
	@echo "🔧 Setting up Crawl4AI..."
	. .venv/bin/activate && uv run crawl4ai-setup
	@echo "✅ Setup complete! Activate with: source .venv/bin/activate"

install:
	@echo "📦 Installing production dependencies..."
	uv sync --no-dev

dev-install:
	@echo "📦 Installing development dependencies..."
	uv sync

# Development workflow
run:
	@echo "🚀 Running MECSR scraper..."
	uv run python main.py

test:
	@echo "🧪 Running tests..."
	uv run pytest tests/ -v

lint:
	@echo "🔍 Running linting checks..."
	uv run flake8 src/ tests/
	uv run mypy src/

format:
	@echo "🎨 Formatting code..."
	uv run black src/ tests/
	uv run isort src/ tests/

check: lint format
	@echo "✅ All quality checks passed!"

# Utility commands
clean:
	@echo "🧹 Cleaning up..."
	rm -rf dist/ build/ *.egg-info/
	rm -rf .pytest_cache/ .mypy_cache/ __pycache__/
	rm -rf data/*.json data/*.csv
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

check-env:
	@echo "🔍 Checking environment..."
	@python3 --version >/dev/null 2>&1 || python --version >/dev/null 2>&1 || { echo "❌ Python not found. Install Python 3.11+ first."; exit 1; }
	@(python3 --version 2>/dev/null || python --version 2>/dev/null) | grep -q "Python 3.11\|Python 3.12\|Python 3.13" || { echo "❌ Python 3.11+ required."; exit 1; }
	@command -v uv >/dev/null 2>&1 || { echo "❌ uv not found. Install uv first."; exit 1; }
	@echo "✅ Environment OK"

docs:
	@echo "📚 Generating documentation..."
	@echo "Documentation available in memory-bank/ directory"

build:
	@echo "📦 Building distribution package..."
	uv build

# Quick development cycle
dev: check test
	@echo "🎯 Development cycle complete!"

# Data operations (for when scraper is implemented)
scrape:
	@echo "🕷️ Starting scraping process..."
	uv run python main.py --output-format json

scrape-sample:
	@echo "🕷️ Scraping sample data (first 10 pages)..."
	uv run python main.py --output-format json --max-pages 10

# Environment activation helper
activate:
	@echo "To activate the virtual environment, run:"
	@echo "source .venv/bin/activate"

# Show project status
status:
	@echo "📊 Project Status:"
	@echo "- Virtual environment: $(shell [ -d .venv ] && echo '✅ Created' || echo '❌ Missing')"
	@echo "- Dependencies: $(shell [ -f uv.lock ] && echo '✅ Installed' || echo '❌ Missing')"
	@echo "- Main script: $(shell [ -f main.py ] && echo '✅ Ready' || echo '❌ Missing')"
	@echo "- Config: $(shell [ -f config.py ] && echo '✅ Ready' || echo '❌ Missing')"
	@echo "- Data directory: $(shell [ -d data ] && echo '✅ Ready' || echo '❌ Missing')"
