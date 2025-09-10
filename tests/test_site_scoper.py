"""
Test suite for SiteScoper component.
Tests site analysis, URL discovery, and structure mapping functionality.
Follows TDD principles: write failing tests first, then implement minimal code to pass.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import List

from scrapers.site_scoper import SiteScoper, SiteStructure, UrlDiscoveryResult


class TestSiteScoper:
    """Test suite for SiteScoper class"""

    @pytest.fixture
    def scoper(self):
        """Fixture to create SiteScoper instance"""
        return SiteScoper()

    @pytest.fixture
    def sample_directory_html(self):
        """Sample HTML containing MECSR directory structure"""
        return """
        <!DOCTYPE html>
        <html>
        <head><title>MECSR Directory</title></head>
        <body>
            <nav>
                <a href="/directory-shopping-centres">Shopping Centres</a>
                <a href="/directory-retail-brands">Retail Brands</a>
                <a href="/directory-services-suppliers">Services</a>
                <a href="/about">About</a>
                <a href="https://events.mecsr.org">Events</a>
            </nav>

            <div class="pagination">
                <a href="?page=1">1</a>
                <a href="?page=2">2</a>
                <a href="?page=3">3</a>
                <a href="?page=4">4</a>
                <a href="?page=5">5</a>
            </div>

            <div class="search-result-item">
                <a href="/directory-shopping-centres/dalma-mall">Dalma Mall</a>
            </div>
            <div class="search-result-item">
                <a href="/directory-shopping-centres/360-kuwait">360 Kuwait</a>
            </div>

            <a href="/sitemap.xml">Sitemap</a>
        </body>
        </html>
        """

    @pytest.mark.asyncio
    async def test_site_scoper_initialization(self, scoper):
        """Test that SiteScoper can be initialized"""
        assert scoper is not None
        assert scoper.base_url == "https://www.mecsr.org"
        assert scoper.max_discovery_depth == 3
        assert hasattr(scoper, 'analyze_site_structure')
        assert hasattr(scoper, 'discover_urls')

    @pytest.mark.asyncio
    async def test_analyze_site_structure_method_exists(self, scoper):
        """Test that analyze_site_structure method exists"""
        assert hasattr(scoper, 'analyze_site_structure')
        assert callable(scoper.analyze_site_structure)

    @pytest.mark.asyncio
    async def test_discover_urls_method_exists(self, scoper):
        """Test that discover_urls method exists"""
        assert hasattr(scoper, 'discover_urls')
        assert callable(scoper.discover_urls)

    @pytest.mark.asyncio
    async def test_analyze_site_structure_with_sample_html(self, scoper, sample_directory_html):
        """Test site structure analysis with sample HTML"""
        # Mock the crawler to return our sample HTML
        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': sample_directory_html
            }

            with patch.object(scoper, '_check_robots_txt', return_value=True):
                structure = await scoper.analyze_site_structure()

                assert isinstance(structure, SiteStructure)
                assert structure.base_url == "https://www.mecsr.org"
                assert structure.total_pages >= 1
                assert isinstance(structure.directory_patterns, list)
                assert isinstance(structure.sitemap_urls, list)
                assert structure.robots_allowed is True

    @pytest.mark.asyncio
    async def test_find_directory_patterns(self, scoper, sample_directory_html):
        """Test directory pattern discovery"""
        patterns = scoper._find_directory_patterns(sample_directory_html)

        assert isinstance(patterns, list)
        assert len(patterns) > 0
        assert any('directory-shopping-centres' in pattern for pattern in patterns)
        assert any('directory-retail-brands' in pattern for pattern in patterns)

    @pytest.mark.asyncio
    async def test_analyze_pagination_structure(self, scoper, sample_directory_html):
        """Test pagination structure analysis"""
        pattern = scoper._analyze_pagination_structure(sample_directory_html)

        assert isinstance(pattern, str)
        assert 'page' in pattern.lower()

    @pytest.mark.asyncio
    async def test_estimate_site_size(self, scoper, sample_directory_html):
        """Test site size estimation"""
        total_pages, estimated_malls = scoper._estimate_site_size(sample_directory_html)

        assert isinstance(total_pages, int)
        assert isinstance(estimated_malls, int)
        assert total_pages >= 1
        assert estimated_malls >= 0

    @pytest.mark.asyncio
    async def test_discover_urls_basic_functionality(self, scoper):
        """Test basic URL discovery functionality"""
        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': '<html><body><a href="/test">Test Link</a></body></html>'
            }

            result = await scoper.discover_urls('https://www.mecsr.org/test')

            assert isinstance(result, UrlDiscoveryResult)
            assert isinstance(result.directory_urls, list)
            assert isinstance(result.pagination_urls, list)
            assert isinstance(result.external_links, list)
            assert isinstance(result.internal_links, list)
            assert isinstance(result.discovered_pages, int)

    @pytest.mark.asyncio
    async def test_discover_urls_with_directory_links(self, scoper):
        """Test URL discovery with directory links"""
        html_with_directories = """
        <html><body>
            <a href="/directory-shopping-centres">Shopping Centres</a>
            <a href="/directory-retail-brands">Retail Brands</a>
            <a href="/about">About</a>
            <a href="https://external.com">External Link</a>
        </body></html>
        """

        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': html_with_directories
            }

            result = await scoper.discover_urls()

            assert len(result.directory_urls) >= 1
            assert len(result.external_links) >= 1
            assert len(result.internal_links) >= 1

    @pytest.mark.asyncio
    async def test_discover_urls_with_pagination(self, scoper):
        """Test URL discovery with pagination links"""
        html_with_pagination = """
        <html><body>
            <a href="?page=1">Page 1</a>
            <a href="?page=2">Page 2</a>
            <a href="?page=3">Page 3</a>
            <a href="/directory-shopping-centres">Directory</a>
        </body></html>
        """

        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': html_with_pagination
            }

            result = await scoper.discover_urls()

            assert len(result.pagination_urls) >= 1
            assert len(result.directory_urls) >= 1

    @pytest.mark.asyncio
    async def test_check_robots_txt_allowed(self, scoper):
        """Test robots.txt checking when crawling is allowed"""
        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': "User-agent: *\nAllow: /"
            }

            allowed = await scoper._check_robots_txt()
            assert allowed is True

    @pytest.mark.asyncio
    async def test_check_robots_txt_disallowed(self, scoper):
        """Test robots.txt checking when crawling is disallowed"""
        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': "User-agent: *\nDisallow: /directory-shopping-centres"
            }

            allowed = await scoper._check_robots_txt()
            assert allowed is False

    @pytest.mark.asyncio
    async def test_check_robots_txt_error_handling(self, scoper):
        """Test robots.txt error handling"""
        with patch.object(scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = None  # Simulate failure

            allowed = await scoper._check_robots_txt()
            assert allowed is True  # Default to allowed on error

    @pytest.mark.asyncio
    async def test_find_sitemap_urls(self, scoper):
        """Test sitemap URL discovery"""
        html_with_sitemap = """
        <html><body>
            <a href="/sitemap.xml">Sitemap</a>
            <a href="/sitemap-shopping-centres.xml">Mall Sitemap</a>
            <a href="/about">About</a>
        </body></html>
        """

        sitemaps = scoper._find_sitemap_urls(html_with_sitemap)

        assert isinstance(sitemaps, list)
        assert len(sitemaps) >= 1
        assert any('sitemap' in url for url in sitemaps)

    @pytest.mark.asyncio
    async def test_url_categorization(self, scoper):
        """Test URL categorization logic"""
        html_with_mixed_links = """
        <html><body>
            <a href="/directory-shopping-centres">Internal Directory</a>
            <a href="/about">Internal Page</a>
            <a href="https://events.mecsr.org">Internal Subdomain</a>
            <a href="https://external.com">External Link</a>
            <a href="mailto:test@example.com">Email Link</a>
            <a href="#anchor">Anchor Link</a>
        </body></html>
        """

        visited = set()
        to_visit = set()
        directory_urls = []
        pagination_urls = []
        external_links = []
        internal_links = []

        scoper._extract_urls_from_html(
            html_with_mixed_links, scoper.base_url,
            visited, to_visit, directory_urls,
            pagination_urls, external_links, internal_links
        )

        assert len(internal_links) >= 2  # Internal links
        assert len(external_links) >= 1  # External links
        assert len(directory_urls) >= 1  # Directory links

    @pytest.mark.asyncio
    async def test_discovery_depth_limit(self, scoper):
        """Test that URL discovery respects depth limits"""
        # Create a scoper with very limited depth
        limited_scoper = SiteScoper(max_discovery_depth=1)

        html_with_deep_links = """
        <html><body>
            <a href="/level1">Level 1</a>
            <a href="/level1/level2">Level 2</a>
            <a href="/level1/level2/level3">Level 3</a>
        </body></html>
        """

        with patch.object(limited_scoper.crawler, 'crawl_single_page') as mock_crawl:
            mock_crawl.return_value = {
                'success': True,
                'html': html_with_deep_links
            }

            result = await limited_scoper.discover_urls()

            # Should discover some URLs but respect depth limit
            assert isinstance(result, UrlDiscoveryResult)
            assert result.discovered_pages >= 1

    @pytest.mark.asyncio
    async def test_empty_html_handling(self, scoper):
        """Test handling of empty or minimal HTML"""
        # Test with empty HTML
        patterns = scoper._find_directory_patterns("")
        assert patterns == []

        patterns = scoper._find_directory_patterns("<html></html>")
        assert patterns == []

        # Test pagination analysis with empty HTML
        pagination_pattern = scoper._analyze_pagination_structure("")
        assert isinstance(pagination_pattern, str)

    @pytest.mark.asyncio
    async def test_mixed_url_formats(self, scoper):
        """Test handling of various URL formats"""
        html_with_mixed_urls = """
        <html><body>
            <a href="/directory-shopping-centres">Relative Path</a>
            <a href="https://www.mecsr.org/directory-retail-brands">Absolute URL</a>
            <a href="//www.mecsr.org/directory-services-suppliers">Protocol-relative</a>
            <a href="https://external.com/link">External</a>
        </body></html>
        """

        patterns = scoper._find_directory_patterns(html_with_mixed_urls)

        assert len(patterns) >= 2  # Should find multiple directory patterns
        assert all('mecsr.org' in pattern for pattern in patterns)
