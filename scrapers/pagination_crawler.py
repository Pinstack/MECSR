"""
PaginationCrawler component for MECSR directory scraping.
Handles paginated page crawling with parallel processing and rate limiting.
"""

from typing import List, Optional, Any, Dict
import asyncio
import aiohttp
from unittest.mock import MagicMock
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

        # HTTP headers to mimic real browser and avoid blocking
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }

        # Connector will be created when session is created
        self.session = None
        self.connector = None

    async def crawl_single_page(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Crawl a single page using aiohttp for fast HTTP requests

        Args:
            url: URL to crawl

        Returns:
            Dictionary with crawl result or None if invalid URL
        """
        # Validate URL
        if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
            return None

        # Create session and connector if not exists
        if self.session is None:
            if self.connector is None:
                self.connector = aiohttp.TCPConnector(
                    limit=self.max_concurrent_requests * 2,  # Connection pool size
                    limit_per_host=self.max_concurrent_requests,
                    ttl_dns_cache=300,  # DNS cache for 5 minutes
                    use_dns_cache=True,
                    keepalive_timeout=60,
                )
            self.session = aiohttp.ClientSession(
                connector=self.connector,
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=30, connect=10)
            )

        import time
        start_time = time.time()

        try:
            async with self.session.get(url) as response:
                response_time = time.time() - start_time

                if response.status == 200:
                    html = await response.text()
                    return {
                        "url": url,
                        "success": True,
                        "html": html,
                        "status_code": response.status,
                        "response_time": response_time,
                        "content_length": len(html),
                        "content_type": response.headers.get('content-type', '')
                    }
                else:
                    return {
                        "url": url,
                        "success": False,
                        "error": f"HTTP {response.status}",
                        "status_code": response.status,
                        "response_time": response_time
                    }

        except Exception as e:
            response_time = time.time() - start_time
            error_type = type(e).__name__
            return {
                "url": url,
                "success": False,
                "error": str(e),
                "error_type": error_type,
                "response_time": response_time
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

    async def cleanup(self):
        """Clean up aiohttp session and connector"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
        if self.connector and not self.connector.closed:
            await self.connector.close()
            self.connector = None
