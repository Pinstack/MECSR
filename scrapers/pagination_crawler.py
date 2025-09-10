"""
PaginationCrawler component for MECSR directory scraping.
Handles paginated page crawling with parallel processing and rate limiting.
"""

from typing import List, Optional, Any, Dict
import asyncio
from unittest.mock import MagicMock
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig


class PaginationCrawler:
    """Crawler for handling paginated MECSR directory pages"""

    def __init__(self,
                 base_url: str = "https://www.mecsr.org",
                 endpoint: str = "/directory-shopping-centres",
                 max_concurrent_requests: int = 5):
        """
        Initialize the PaginationCrawler

        Args:
            base_url: Base URL for MECSR website
            endpoint: Directory endpoint path
            max_concurrent_requests: Maximum concurrent requests
        """
        self.base_url = base_url
        self.endpoint = endpoint
        self.max_concurrent_requests = max_concurrent_requests

        # Browser configuration for respectful scraping
        self.browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            user_agent="MECSR-Research-Crawler/1.0 (Educational Project)"
        )

        # Mock rate limiter for now - will be implemented later
        self.rate_limiter = MagicMock()

    async def crawl_single_page(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Crawl a single page using Crawl4AI and return structured result

        Args:
            url: URL to crawl

        Returns:
            Dictionary with crawl result or None if invalid URL
        """
        # Validate URL
        if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            return None

        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(url=url)

                # Check if crawl was successful
                if result.success:
                    return {
                        "url": url,
                        "success": True,
                        "html": result.html,
                        "status_code": getattr(result, 'status_code', 200),
                        "response_time": getattr(result, 'response_time', None),
                        "links_found": len(result.links) if hasattr(result, 'links') else 0
                    }
                else:
                    return {
                        "url": url,
                        "success": False,
                        "error": getattr(result, 'error_message', 'Unknown error'),
                        "status_code": getattr(result, 'status_code', None)
                    }

        except Exception as e:
            # Handle various exceptions (network errors, timeouts, etc.)
            error_type = type(e).__name__
            return {
                "url": url,
                "success": False,
                "error": str(e),
                "error_type": error_type,
                "status_code": None
            }

    def generate_page_url(self, page_number: int) -> str:
        """
        Generate URL for a specific page number

        Args:
            page_number: Page number (must be >= 1)

        Returns:
            Complete URL for the page

        Raises:
            ValueError: If page_number is invalid
        """
        if page_number < 1:
            raise ValueError(f"Page number must be >= 1, got {page_number}")

        if page_number == 1:
            return f"{self.base_url}{self.endpoint}"
        else:
            return f"{self.base_url}{self.endpoint}?page={page_number}"

    def generate_page_urls(self, start_page: int, end_page: int) -> List[str]:
        """
        Generate URLs for a range of pages

        Args:
            start_page: First page number (must be >= 1)
            end_page: Last page number (must be >= start_page)

        Returns:
            List of URLs for the page range

        Raises:
            ValueError: If page range is invalid
        """
        if start_page < 1:
            raise ValueError(f"Start page must be >= 1, got {start_page}")
        if end_page < start_page:
            raise ValueError(f"End page ({end_page}) must be >= start page ({start_page})")

        urls = []
        for page_num in range(start_page, end_page + 1):
            urls.append(self.generate_page_url(page_num))

        return urls

    async def crawl_pages(self, urls: List[str]) -> List[Any]:
        """
        Crawl multiple pages concurrently with semaphore-based rate limiting

        Args:
            urls: List of URLs to crawl

        Returns:
            List of crawl results
        """
        if not urls:
            return []

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def crawl_with_semaphore(url: str) -> Optional[Dict[str, Any]]:
            """Crawl a single URL with semaphore control"""
            async with semaphore:
                return await self.crawl_single_page(url)

        # Process all URLs concurrently with semaphore control
        tasks = [crawl_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None results and exceptions, keep only successful results
        valid_results = []
        for result in results:
            if result is not None and not isinstance(result, Exception):
                valid_results.append(result)

        return valid_results
