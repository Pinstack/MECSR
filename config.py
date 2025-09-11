"""
Configuration module for MECSR scraper.

This module provides centralized configuration management using Pydantic
for strong typing and validation.
"""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class ScrapingSettings(BaseSettings):
    """Application settings with environment variable support"""

    # Crawler Configuration
    browser_headless: bool = Field(default=True, env="BROWSER_HEADLESS")
    browser_viewport_width: int = Field(default=1920, env="BROWSER_VIEWPORT_WIDTH")
    browser_viewport_height: int = Field(default=1080, env="BROWSER_VIEWPORT_HEIGHT")

    # Scraping Scope Configuration
    total_malls_to_scrape: int = Field(default=1001, env="TOTAL_MALLS_TO_SCRAPE")
    malls_per_page: int = Field(default=12, env="MALLS_PER_PAGE")
    max_pages_to_scrape: int = Field(default=84, env="MAX_PAGES_TO_SCRAPE")  # 1001 / 12 = ~84 pages

    # Rate Limiting (optimized for large-scale scraping)
    requests_per_minute: int = Field(default=60, env="REQUESTS_PER_MINUTE")  # Increased for 1001 malls
    max_concurrent_requests: int = Field(default=10, env="MAX_CONCURRENT_REQUESTS")  # Increased concurrency
    batch_size: int = Field(default=20, env="BATCH_SIZE")  # Larger batches for efficiency

    # Retry Configuration
    max_retries: int = Field(default=5, env="MAX_RETRIES")  # More retries for large-scale
    base_retry_delay: float = Field(default=2.0, env="BASE_RETRY_DELAY")  # Longer delay
    max_retry_delay: float = Field(default=120.0, env="MAX_RETRY_DELAY")  # Max 2 minutes

    # Storage Configuration
    output_directory: str = Field(default="./output", env="OUTPUT_DIRECTORY")
    output_format: str = Field(default="json", env="OUTPUT_FORMAT")

    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")

    # Target Configuration
    base_url: str = Field(default="https://www.mecsr.org", env="BASE_URL")
    target_endpoint: str = Field(default="/directory-shopping-centres", env="TARGET_ENDPOINT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = ScrapingSettings()


class CrawlerConfig:
    """Configuration for Crawl4AI crawler with full 1001 mall support"""

    def __init__(self):
        self.browser_config = {
            "headless": settings.browser_headless,
            "viewport_width": settings.browser_viewport_width,
            "viewport_height": settings.browser_viewport_height,
            "java_script_enabled": True,
            "user_agent": "MECSR-Research-Crawler/1.0 (Educational Project)"
        }

        self.run_config = {
            "cache_mode": "bypass",  # Always fresh data for large-scale scraping
            "page_timeout": 45000,   # 45 seconds for larger pages
            "word_count_threshold": 10,
            "process_iframes": True,
            "remove_overlay_elements": True,
            "verbose": False
        }

        # Rate limiting settings optimized for 1001 malls
        self.rate_limit = {
            "requests_per_minute": settings.requests_per_minute,
            "max_concurrent": settings.max_concurrent_requests,
            "batch_size": settings.batch_size
        }

        # Scraping scope settings
        self.scraping_scope = {
            "total_malls_to_scrape": settings.total_malls_to_scrape,
            "malls_per_page": settings.malls_per_page,
            "max_pages_to_scrape": settings.max_pages_to_scrape,
            "estimated_total_pages": max(1, (settings.total_malls_to_scrape + settings.malls_per_page - 1) // settings.malls_per_page)
        }

        # Retry configuration for large-scale operations
        self.retry_config = {
            "max_retries": settings.max_retries,
            "base_delay": settings.base_retry_delay,
            "max_delay": settings.max_retry_delay
        }

        # Target URLs
        self.base_url = settings.base_url.rstrip('/')
        self.target_url = f"{self.base_url}{settings.target_endpoint}"

        # Page URLs for pagination
        self.page_urls = self._generate_page_urls()

    def _generate_page_urls(self) -> list[str]:
        """Generate all page URLs for scraping all 1001 malls"""
        urls = []
        for page_num in range(1, self.scraping_scope["max_pages_to_scrape"] + 1):
            if page_num == 1:
                url = self.target_url
            else:
                url = f"{self.target_url}?page={page_num}"
            urls.append(url)
        return urls

    def get_scraping_estimate(self) -> dict:
        """Get time and resource estimates for scraping all 1001 malls"""
        total_pages = self.scraping_scope["max_pages_to_scrape"]
        total_malls = self.scraping_scope["total_malls_to_scrape"]

        # Estimate based on rate limiting
        requests_per_minute = self.rate_limit["requests_per_minute"]
        estimated_minutes = total_pages / requests_per_minute
        estimated_hours = estimated_minutes / 60

        return {
            "total_malls": total_malls,
            "total_pages": total_pages,
            "estimated_duration_hours": round(estimated_hours, 1),
            "estimated_duration_minutes": round(estimated_minutes, 1),
            "requests_per_minute": requests_per_minute,
            "max_concurrent": self.rate_limit["max_concurrent"],
            "batch_size": self.rate_limit["batch_size"]
        }


# Create global config instance
crawler_config = CrawlerConfig()

