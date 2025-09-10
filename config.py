"""
Configuration module for MECSR scraper.

This module provides centralized configuration management using Pydantic
for strong typing and validation.
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class ScrapingSettings(BaseSettings):
    """Application settings with environment variable support"""

    # Crawler Configuration
    browser_headless: bool = Field(default=True, env="BROWSER_HEADLESS")
    browser_viewport_width: int = Field(default=1920, env="BROWSER_VIEWPORT_WIDTH")
    browser_viewport_height: int = Field(default=1080, env="BROWSER_VIEWPORT_HEIGHT")

    # Rate Limiting
    requests_per_minute: int = Field(default=30, env="REQUESTS_PER_MINUTE")
    max_concurrent_requests: int = Field(default=5, env="MAX_CONCURRENT_REQUESTS")

    # Retry Configuration
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    base_retry_delay: float = Field(default=1.0, env="BASE_RETRY_DELAY")
    max_retry_delay: float = Field(default=60.0, env="MAX_RETRY_DELAY")

    # Storage Configuration
    output_directory: str = Field(default="./data", env="OUTPUT_DIRECTORY")
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
    """Configuration for Crawl4AI crawler"""

    def __init__(self):
        self.browser_config = {
            "headless": settings.browser_headless,
            "viewport_width": settings.browser_viewport_width,
            "viewport_height": settings.browser_viewport_height,
            "java_script_enabled": True,
            "user_agent": "MECSR-Research-Crawler/1.0 (Educational Project)"
        }

        self.run_config = {
            "cache_mode": "bypass",  # Always fresh data
            "page_timeout": 30000,   # 30 seconds
            "word_count_threshold": 10,
            "process_iframes": True,
            "remove_overlay_elements": True,
            "verbose": False
        }

        # Rate limiting settings
        self.rate_limit = {
            "requests_per_minute": settings.requests_per_minute,
            "max_concurrent": settings.max_concurrent_requests
        }

        # Target URLs
        self.base_url = settings.base_url.rstrip('/')
        self.target_url = f"{self.base_url}{settings.target_endpoint}"


# Create global config instance
crawler_config = CrawlerConfig()

