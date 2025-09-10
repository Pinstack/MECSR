"""
SiteScoper component for MECSR directory scraping.
Handles initial site analysis, URL discovery, and site structure mapping.
"""

from typing import List, Dict, Optional, Set
import asyncio
import re
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from bs4 import BeautifulSoup

from scrapers.pagination_crawler import PaginationCrawler


@dataclass
class SiteStructure:
    """Data class for site structure information"""
    base_url: str
    total_pages: int
    directory_patterns: List[str]
    pagination_pattern: str
    robots_allowed: bool
    sitemap_urls: List[str]
    estimated_malls: int


@dataclass
class UrlDiscoveryResult:
    """Data class for URL discovery results"""
    directory_urls: List[str]
    pagination_urls: List[str]
    external_links: List[str]
    internal_links: List[str]
    discovered_pages: int


class SiteScoper:
    """Scoper for analyzing MECSR website structure and discovering URLs"""

    def __init__(self,
                 base_url: str = "https://www.mecsr.org",
                 max_discovery_depth: int = 3):
        """
        Initialize the SiteScoper

        Args:
            base_url: Base URL of the target website
            max_discovery_depth: Maximum depth for URL discovery
        """
        self.base_url = base_url.rstrip('/')
        self.max_discovery_depth = max_discovery_depth
        self.crawler = PaginationCrawler()

        # Common patterns for MECSR directory structure
        self.directory_patterns = [
            r'/directory-shopping-centres',
            r'/directory-retail-brands',
            r'/directory-services-suppliers',
            r'/directory.*centres',
            r'/directory.*centers'
        ]

        self.pagination_patterns = [
            r'\?page=\d+',
            r'/page/\d+',
            r'/\d+/?$'
        ]

    async def analyze_site_structure(self) -> SiteStructure:
        """
        Analyze the overall site structure

        Returns:
            SiteStructure object with comprehensive site analysis
        """
        print("🔍 Analyzing MECSR site structure...")

        # Get main directory page
        result = await self.crawler.crawl_single_page(f"{self.base_url}/directory-shopping-centres")

        if not result or not result.get('success'):
            raise ValueError("Failed to access main MECSR directory page")

        html = result['html']

        # Analyze directory patterns
        directory_patterns = self._find_directory_patterns(html)

        # Analyze pagination structure
        pagination_pattern = self._analyze_pagination_structure(html)

        # Estimate total pages and malls
        total_pages, estimated_malls = self._estimate_site_size(html)

        # Check robots.txt
        robots_allowed = await self._check_robots_txt()

        # Find sitemap URLs
        sitemap_urls = self._find_sitemap_urls(html)

        return SiteStructure(
            base_url=self.base_url,
            total_pages=total_pages,
            directory_patterns=directory_patterns,
            pagination_pattern=pagination_pattern,
            robots_allowed=robots_allowed,
            sitemap_urls=sitemap_urls,
            estimated_malls=estimated_malls
        )

    async def discover_urls(self, start_url: Optional[str] = None) -> UrlDiscoveryResult:
        """
        Discover all relevant URLs on the site

        Args:
            start_url: URL to start discovery from (defaults to main directory)

        Returns:
            UrlDiscoveryResult with discovered URLs
        """
        if not start_url:
            start_url = f"{self.base_url}/directory-shopping-centres"

        print(f"🔗 Discovering URLs starting from: {start_url}")

        visited: Set[str] = set()
        to_visit: Set[str] = {start_url}

        directory_urls: List[str] = []
        pagination_urls: List[str] = []
        external_links: List[str] = []
        internal_links: List[str] = []

        depth = 0
        while to_visit and depth < self.max_discovery_depth:
            current_batch = list(to_visit)
            to_visit.clear()

            print(f"📄 Processing {len(current_batch)} URLs at depth {depth}...")

            # Process URLs in parallel with semaphore
            semaphore = asyncio.Semaphore(5)  # Limit concurrent requests

            async def process_url(url: str) -> Optional[str]:
                async with semaphore:
                    try:
                        result = await self.crawler.crawl_single_page(url)
                        if result and result.get('success'):
                            return result['html']
                    except Exception:
                        pass
                return None

            # Process batch concurrently
            tasks = [process_url(url) for url in current_batch]
            html_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Extract URLs from HTML results
            for url, html_result in zip(current_batch, html_results):
                if isinstance(html_result, str):
                    self._extract_urls_from_html(
                        html_result, url, visited, to_visit,
                        directory_urls, pagination_urls,
                        external_links, internal_links
                    )

            depth += 1

        return UrlDiscoveryResult(
            directory_urls=sorted(list(set(directory_urls))),
            pagination_urls=sorted(list(set(pagination_urls))),
            external_links=sorted(list(set(external_links))),
            internal_links=sorted(list(set(internal_links))),
            discovered_pages=len(visited)
        )

    def _find_directory_patterns(self, html: str) -> List[str]:
        """Find directory-related URL patterns in HTML"""
        patterns = []

        # Look for navigation links to directories
        soup = BeautifulSoup(html, 'html.parser')
        nav_links = soup.find_all('a', href=True)

        for link in nav_links:
            href = link.get('href')
            if href:
                for pattern in self.directory_patterns:
                    if re.search(pattern, href, re.IGNORECASE):
                        full_url = urljoin(self.base_url, href)
                        if full_url not in patterns:
                            patterns.append(full_url)
                        break

        return patterns

    def _analyze_pagination_structure(self, html: str) -> str:
        """Analyze how pagination is implemented on the site"""
        soup = BeautifulSoup(html, 'html.parser')

        # Look for pagination links
        pagination_links = soup.find_all('a', href=re.compile(r'[?&]page=\d+'))

        if pagination_links:
            # Extract the pattern from the first pagination link
            href = pagination_links[0].get('href')
            if '?page=' in href:
                return "?page={page}"
            elif '/page/' in href:
                return "/page/{page}"

        # Fallback to common patterns
        return "?page={page}"

    def _estimate_site_size(self, html: str) -> tuple[int, int]:
        """Estimate total pages and malls from initial page analysis"""
        soup = BeautifulSoup(html, 'html.parser')

        # Look for pagination information
        pagination_info = soup.find('div', class_=re.compile(r'pagination|pager'))
        total_pages = 1

        if pagination_info:
            # Try to find the last page number
            page_links = pagination_info.find_all('a', href=re.compile(r'[?&]page=\d+'))
            if page_links:
                page_numbers = []
                for link in page_links:
                    href = link.get('href')
                    match = re.search(r'[?&]page=(\d+)', href)
                    if match:
                        page_numbers.append(int(match.group(1)))

                if page_numbers:
                    total_pages = max(page_numbers)

        # Estimate malls per page (rough heuristic)
        mall_containers = soup.find_all('div', class_=re.compile(r'search.*result|mall.*item'))
        malls_per_page = max(len(mall_containers), 10)  # At least 10 malls per page

        estimated_total_malls = total_pages * malls_per_page

        return total_pages, estimated_total_malls

    async def _check_robots_txt(self) -> bool:
        """Check if crawling is allowed by robots.txt"""
        try:
            robots_url = f"{self.base_url}/robots.txt"
            result = await self.crawler.crawl_single_page(robots_url)

            if result and result.get('success'):
                robots_content = result['html']
                # Simple check for MECSR directory allowance
                if 'Disallow:' in robots_content:
                    # Check if our specific paths are disallowed
                    disallowed = False
                    for line in robots_content.split('\n'):
                        line = line.strip()
                        if line.startswith('Disallow:'):
                            path = line.replace('Disallow:', '').strip()
                            if path in ['/directory-shopping-centres', '/directory', '/']:
                                disallowed = True
                                break
                    return not disallowed
                else:
                    # No disallow directives found, assume allowed
                    return True
            else:
                # Can't access robots.txt, assume allowed
                return True
        except Exception:
            return True  # Default to allowed on error

    def _find_sitemap_urls(self, html: str) -> List[str]:
        """Find sitemap URLs in the HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        sitemaps = []

        # Look for sitemap links
        sitemap_links = soup.find_all('a', href=re.compile(r'sitemap', re.IGNORECASE))
        for link in sitemap_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                sitemaps.append(full_url)

        return sitemaps

    def _extract_urls_from_html(self, html: str, base_url: str,
                               visited: Set[str], to_visit: Set[str],
                               directory_urls: List[str], pagination_urls: List[str],
                               external_links: List[str], internal_links: List[str]):
        """Extract various types of URLs from HTML content"""
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a', href=True)

        for link in links:
            href = link.get('href')
            if not href:
                continue

            # Convert relative URLs to absolute
            full_url = urljoin(base_url, href)

            # Skip if already visited
            if full_url in visited:
                continue

            # Skip anchors and javascript
            if full_url.startswith('#') or full_url.startswith('javascript:'):
                continue

            # Skip mailto and other protocols
            parsed = urlparse(full_url)
            if parsed.scheme not in ['http', 'https']:
                continue

            # Categorize URLs
            if parsed.netloc == urlparse(self.base_url).netloc:
                # Internal link
                internal_links.append(full_url)

                # Check if it's a directory URL
                for pattern in self.directory_patterns:
                    if re.search(pattern, full_url, re.IGNORECASE):
                        directory_urls.append(full_url)
                        break

                # Check if it's a pagination URL
                for pattern in self.pagination_patterns:
                    if re.search(pattern, full_url):
                        pagination_urls.append(full_url)
                        break

                # Add to visit queue if not visited and within depth
                if full_url not in visited and len(to_visit) < 100:  # Limit queue size
                    to_visit.add(full_url)

            else:
                # External link
                external_links.append(full_url)

            visited.add(full_url)
