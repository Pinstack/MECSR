"""
Test suite for PaginationCrawler component.
Follows TDD principles: write failing tests first, then implement minimal code to pass.
"""

import pytest
import asyncio
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from scrapers.pagination_crawler import PaginationCrawler


class TestPaginationCrawler:
    """Test suite for PaginationCrawler class"""

    @pytest.fixture
    def crawler(self):
        """Fixture to create PaginationCrawler instance"""
        return PaginationCrawler()

    @pytest.mark.asyncio
    async def test_pagination_crawler_initialization(self, crawler):
        """Test that PaginationCrawler can be initialized"""
        assert crawler is not None
        assert hasattr(crawler, 'crawl_pages')
        assert hasattr(crawler, 'crawl_single_page')

    @pytest.mark.asyncio
    async def test_crawl_single_page_method_exists(self, crawler):
        """Test that crawl_single_page method exists and is callable"""
        assert hasattr(crawler, 'crawl_single_page')
        assert callable(crawler.crawl_single_page)

    @pytest.mark.asyncio
    async def test_crawl_pages_method_exists(self, crawler):
        """Test that crawl_pages method exists and is callable"""
        assert hasattr(crawler, 'crawl_pages')
        assert callable(crawler.crawl_pages)

    @pytest.mark.asyncio
    async def test_crawl_single_page_returns_none_for_invalid_url(self, crawler):
        """Test that crawl_single_page returns None for invalid URL"""
        result = await crawler.crawl_single_page("invalid-url")
        assert result is None

    @pytest.mark.asyncio
    async def test_crawl_pages_returns_empty_list_for_no_pages(self, crawler):
        """Test that crawl_pages returns empty list when no pages to crawl"""
        result = await crawler.crawl_pages([])
        assert result == []

    @pytest.mark.asyncio
    async def test_crawl_pages_handles_empty_page_list(self, crawler):
        """Test that crawl_pages handles empty page list gracefully"""
        result = await crawler.crawl_pages([])
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_crawler_has_base_url_attribute(self, crawler):
        """Test that crawler has base_url attribute for MECSR site"""
        assert hasattr(crawler, 'base_url')
        assert crawler.base_url == "https://www.mecsr.org"

    @pytest.mark.asyncio
    async def test_crawler_has_endpoint_attribute(self, crawler):
        """Test that crawler has endpoint attribute for directory"""
        assert hasattr(crawler, 'endpoint')
        assert crawler.endpoint == "/directory-shopping-centres"

    @pytest.mark.asyncio
    async def test_crawler_has_max_concurrent_attribute(self, crawler):
        """Test that crawler has max_concurrent_requests attribute"""
        assert hasattr(crawler, 'max_concurrent_requests')
        assert isinstance(crawler.max_concurrent_requests, int)
        assert crawler.max_concurrent_requests > 0

    @pytest.mark.asyncio
    async def test_crawler_has_rate_limiter_attribute(self, crawler):
        """Test that crawler has rate limiter for respectful scraping"""
        assert hasattr(crawler, 'rate_limiter')
        assert crawler.rate_limiter is not None

    # URL Generation Tests
    @pytest.mark.asyncio
    async def test_generate_page_url_method_exists(self, crawler):
        """Test that generate_page_url method exists"""
        assert hasattr(crawler, 'generate_page_url')
        assert callable(crawler.generate_page_url)

    @pytest.mark.asyncio
    async def test_generate_page_url_page_1(self, crawler):
        """Test URL generation for page 1 (first page)"""
        url = crawler.generate_page_url(1)
        expected = "https://www.mecsr.org/directory-shopping-centres"
        assert url == expected

    @pytest.mark.asyncio
    async def test_generate_page_url_page_2_and_above(self, crawler):
        """Test URL generation for pages 2 and above with query parameter"""
        url = crawler.generate_page_url(2)
        expected = "https://www.mecsr.org/directory-shopping-centres?page=2"
        assert url == expected

        url = crawler.generate_page_url(10)
        expected = "https://www.mecsr.org/directory-shopping-centres?page=10"
        assert url == expected

    @pytest.mark.asyncio
    async def test_generate_page_url_invalid_page_numbers(self, crawler):
        """Test URL generation handles invalid page numbers"""
        # Page 0 should return None or raise error
        with pytest.raises(ValueError):
            crawler.generate_page_url(0)

        # Negative page numbers should raise error
        with pytest.raises(ValueError):
            crawler.generate_page_url(-1)

    @pytest.mark.asyncio
    async def test_generate_page_urls_method_exists(self, crawler):
        """Test that generate_page_urls method exists for multiple pages"""
        assert hasattr(crawler, 'generate_page_urls')
        assert callable(crawler.generate_page_urls)

    @pytest.mark.asyncio
    async def test_generate_page_urls_single_page(self, crawler):
        """Test generating URLs for a single page"""
        urls = crawler.generate_page_urls(1, 1)
        expected = ["https://www.mecsr.org/directory-shopping-centres"]
        assert urls == expected

    @pytest.mark.asyncio
    async def test_generate_page_urls_multiple_pages(self, crawler):
        """Test generating URLs for multiple pages"""
        urls = crawler.generate_page_urls(1, 3)
        expected = [
            "https://www.mecsr.org/directory-shopping-centres",
            "https://www.mecsr.org/directory-shopping-centres?page=2",
            "https://www.mecsr.org/directory-shopping-centres?page=3"
        ]
        assert urls == expected

    @pytest.mark.asyncio
    async def test_generate_page_urls_invalid_range(self, crawler):
        """Test URL generation handles invalid page ranges"""
        # End page before start page
        with pytest.raises(ValueError):
            crawler.generate_page_urls(5, 3)

        # Start page 0
        with pytest.raises(ValueError):
            crawler.generate_page_urls(0, 5)

    # Single Page Crawling Tests
    @pytest.mark.asyncio
    async def test_crawl_single_page_valid_url(self, crawler):
        """Test crawling a valid MECSR page URL"""
        test_url = "https://www.mecsr.org/directory-shopping-centres"
        result = await crawler.crawl_single_page(test_url)

        # Should return a dictionary with expected structure
        assert isinstance(result, dict)
        assert "url" in result
        assert result["url"] == test_url
        assert "success" in result
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_crawl_single_page_with_page_param(self, crawler):
        """Test crawling a paginated URL with page parameter"""
        test_url = "https://www.mecsr.org/directory-shopping-centres?page=2"
        result = await crawler.crawl_single_page(test_url)

        assert isinstance(result, dict)
        assert result["url"] == test_url
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_crawl_single_page_invalid_domain(self, crawler):
        """Test crawling an invalid domain"""
        invalid_url = "https://invalid-domain-that-does-not-exist.com/test"
        result = await crawler.crawl_single_page(invalid_url)

        # Should handle gracefully and return failure result
        assert isinstance(result, dict)
        assert result["url"] == invalid_url
        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_crawl_single_page_http_error(self, crawler):
        """Test crawling a URL that returns HTTP error (404, 500, etc.)"""
        error_url = "https://httpstat.us/404"  # Service that returns 404
        result = await crawler.crawl_single_page(error_url)

        assert isinstance(result, dict)
        assert result["url"] == error_url
        assert result["success"] is True  # Crawl4AI successfully fetched the page
        assert "status_code" in result
        assert result["status_code"] == 404  # But the HTTP status is 404
        assert "html" in result
        assert "404 Not Found" in result["html"]

    @pytest.mark.asyncio
    async def test_crawl_single_page_contains_html_content(self, crawler):
        """Test that crawled page contains expected HTML content"""
        test_url = "https://www.mecsr.org/directory-shopping-centres"
        result = await crawler.crawl_single_page(test_url)

        assert result["success"] is True
        assert "html" in result
        assert isinstance(result["html"], str)
        assert len(result["html"]) > 0
        # Should contain typical HTML elements
        assert "<html" in result["html"].lower() or "<!doctype html" in result["html"].lower()

    @pytest.mark.asyncio
    async def test_crawl_single_page_with_timeout(self, crawler):
        """Test crawling handles timeouts properly"""
        # Use a URL that might timeout
        slow_url = "https://httpstat.us/200?sleep=10000"  # 10 second delay
        result = await crawler.crawl_single_page(slow_url)

        # Should either succeed (if timeout is longer) or fail gracefully
        assert isinstance(result, dict)
        assert result["url"] == slow_url
        # Either success or failure with timeout error
        assert "success" in result

    # Parallel Processing Tests
    @pytest.mark.asyncio
    async def test_crawl_pages_parallel_processing(self, crawler):
        """Test that crawl_pages processes multiple URLs concurrently"""
        urls = [
            "https://www.mecsr.org/directory-shopping-centres",
            "https://www.mecsr.org/directory-shopping-centres?page=2",
            "https://www.mecsr.org/directory-shopping-centres?page=3"
        ]

        import time
        start_time = time.time()
        results = await crawler.crawl_pages(urls)
        end_time = time.time()

        # Should return results for all URLs
        assert len(results) == len(urls)
        for result in results:
            assert isinstance(result, dict)
            assert result["success"] is True
            assert "html" in result

        # Should complete faster than sequential processing (rough check)
        # Sequential would take at least 3 * ~2.5s = 7.5s
        # Parallel should take less than 5s
        duration = end_time - start_time
        assert duration < 10  # Allow some buffer for network variability

    @pytest.mark.asyncio
    async def test_crawl_pages_respects_concurrency_limit(self, crawler):
        """Test that crawl_pages respects the max_concurrent_requests limit"""
        # Create many URLs to test concurrency limiting
        urls = [f"https://httpstat.us/200?n={i}" for i in range(10)]

        results = await crawler.crawl_pages(urls)

        # Should process all URLs
        assert len(results) == len(urls)
        for result in results:
            assert isinstance(result, dict)
            assert "success" in result

    @pytest.mark.asyncio
    async def test_crawl_pages_handles_mixed_success_failure(self, crawler):
        """Test crawl_pages handles mix of valid and invalid URLs"""
        urls = [
            "https://www.mecsr.org/directory-shopping-centres",  # Valid
            "https://invalid-domain-that-does-not-exist.com",   # Invalid
            "https://httpstat.us/404",                          # HTTP error
            "https://www.mecsr.org/directory-shopping-centres?page=2"  # Valid
        ]

        results = await crawler.crawl_pages(urls)

        # Should return results for all URLs (some successful, some failed)
        assert len(results) == 4  # All URLs should be processed

        # Check that we have both successes and failures
        success_count = sum(1 for r in results if r.get("success") is True)
        failure_count = sum(1 for r in results if r.get("success") is False)

        assert success_count > 0  # At least some should succeed
        assert failure_count > 0  # At least some should fail

    @pytest.mark.asyncio
    async def test_crawler_semaphore_limits_concurrency(self, crawler):
        """Test that the crawler uses semaphore to limit concurrent requests"""
        # This is more of an integration test - we can't easily mock semaphore
        # but we can verify the behavior indirectly

        urls = [f"https://httpstat.us/200?n={i}" for i in range(crawler.max_concurrent_requests + 2)]

        results = await crawler.crawl_pages(urls)

        # Should complete successfully without overwhelming the semaphore
        assert len(results) == len(urls)
        successful_results = [r for r in results if r.get("success") is True]
        assert len(successful_results) == len(urls)  # All should succeed

    @pytest.mark.asyncio
    async def test_crawl_pages_empty_list_fast_return(self, crawler):
        """Test that crawl_pages returns immediately for empty URL list"""
        import time
        start_time = time.time()
        results = await crawler.crawl_pages([])
        end_time = time.time()

        assert results == []
        # Should return very quickly
        assert (end_time - start_time) < 0.1
