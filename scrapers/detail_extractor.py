"""
DetailExtractor component for MECSR directory scraping.
Extracts individual mall data from HTML pages, focusing on links, coordinates, and detailed property information.
"""

from typing import List, Dict, Optional, Tuple
import re
import asyncio
from bs4 import BeautifulSoup

from scrapers.pagination_crawler import PaginationCrawler


class DetailExtractor:
    """Extractor for individual mall data from MECSR directory HTML"""

    def __init__(self):
        """Initialize the DetailExtractor"""
        pass

    def extract_mall_links(self, html: str) -> List[str]:
        """
        Extract mall links from HTML

        Args:
            html: Raw HTML content

        Returns:
            List of mall URLs
        """
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        links = []

        # Find all links to mall detail pages
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and '/directory-shopping-centres/' in href:
                # Keep relative URLs for consistency
                if href.startswith('http'):
                    # Convert to relative URL
                    href = href.replace('https://www.mecsr.org', '')
                if href not in links:  # Avoid duplicates
                    links.append(href)

        return links

    def extract_mall_data(self, html: str) -> List[Dict[str, any]]:
        """
        Extract complete mall data from HTML

        Args:
            html: Raw HTML content

        Returns:
            List of mall data dictionaries
        """
        if not html:
            return []

        soup = BeautifulSoup(html, 'html.parser')
        malls = []

        # Find all mall listing containers
        # Try multiple possible container classes
        mall_containers = soup.find_all('div', class_=lambda x: x and ('search_result' in x or 'search-result-item' in x))

        for container in mall_containers:
            mall_data = self._extract_single_mall_data(container)
            if mall_data:
                malls.append(mall_data)

        # Remove duplicates based on URL to avoid processing the same mall multiple times
        seen_urls = set()
        unique_malls = []
        for mall in malls:
            url = mall.get('url')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_malls.append(mall)

        return unique_malls

    def _extract_single_mall_data(self, container) -> Optional[Dict[str, any]]:
        """
        Extract data from a single mall container

        Args:
            container: BeautifulSoup element containing mall data

        Returns:
            Mall data dictionary or None if extraction fails
        """
        try:
            mall_data = {}

            # Extract mall name and URL - find the best link with title/text
            mall_links = container.find_all('a', href=re.compile(r'/directory-shopping-centres/'))

            # Prefer links that have both title and meaningful text
            best_link = None
            for link in mall_links:
                title = link.get('title')
                text = link.get_text(strip=True)
                if title and text and len(text) > 3:  # Meaningful text
                    best_link = link
                    break
                elif not best_link and (title or text):
                    best_link = link

            if best_link:
                mall_data['name'] = best_link.get('title') or best_link.get_text(strip=True)
                href = best_link.get('href')
                if href:
                    # Keep relative URLs for consistency
                    if href.startswith('http'):
                        href = href.replace('https://www.mecsr.org', '')
                    mall_data['url'] = href

            # Extract property type
            property_type_elem = container.find('span', class_=lambda x: x and 'pull-left' in x)
            if property_type_elem:
                mall_data['property_type'] = property_type_elem.get_text(strip=True)

            # Extract status
            status_elem = container.find('span', class_=lambda x: x and 'badge' in x)
            if status_elem:
                mall_data['status'] = status_elem.get_text(strip=True)

            # Extract postItem data attributes (contains IDs and potentially coordinates)
            post_item = container.find('span', class_='postItem')
            if post_item:
                mall_data['post_id'] = post_item.get('data-postid')
                mall_data['user_id'] = post_item.get('data-userid')
                mall_data['data_id'] = post_item.get('data-dataid')
                mall_data['data_type'] = post_item.get('data-datatype')

                # Extract coordinates if available in data attributes
                lat = post_item.get('data-lat')
                lng = post_item.get('data-lng')
                if lat and lng:
                    mall_data['latitude'] = float(lat)
                    mall_data['longitude'] = float(lng)

            # If coordinates not found in data attributes, try to get them from detail page
            if 'latitude' not in mall_data and mall_data.get('url'):
                try:
                    # Note: This is synchronous call within async context - would need to be refactored
                    # For now, we'll handle coordinates separately or use a different approach
                    pass
                except Exception:
                    pass

            # Only return mall data if we have at least a name and URL
            if mall_data.get('name') and mall_data.get('url'):
                return mall_data

        except Exception as e:
            # Log error but continue processing other malls
            print(f"Error extracting mall data: {e}")
            pass

        return None

    async def get_mall_coordinates(self, mall_url: str) -> Optional[Dict[str, float]]:
        """
        Get coordinates for a specific mall by visiting its detail page

        Args:
            mall_url: URL of the mall detail page

        Returns:
            Dictionary with lat/lng coordinates or None
        """
        try:
            # Convert relative URL to absolute URL
            if not mall_url.startswith('http'):
                mall_url = f"https://www.mecsr.org{mall_url}"

            # Import here to avoid circular imports
            from scrapers.pagination_crawler import PaginationCrawler

            crawler = PaginationCrawler()
            result = await crawler.crawl_single_page(mall_url)

            if result and result.get('success') and result.get('html'):
                html = result['html']

                # Extract coordinates from Google Maps JavaScript
                lat_match = re.search(r'parseFloat\(([0-9.-]+)\)', html)
                lng_match = re.search(r'parseFloat\(([0-9.-]+)\)', html[html.find('parseFloat') + 1:])

                if lat_match and lng_match:
                    try:
                        latitude = float(lat_match.group(1))
                        longitude = float(lng_match.group(1))

                        # Validate coordinate ranges
                        if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                            return {
                                'latitude': latitude,
                                'longitude': longitude
                            }
                    except ValueError:
                        pass

        except Exception as e:
            print(f"Error extracting coordinates from {mall_url}: {e}")

        return None

    async def extract_external_urls(self, mall_html: str) -> List[str]:
        """
        Extract external URLs from mall detail pages (More Details buttons)

        Args:
            mall_html: HTML content of a mall detail page

        Returns:
            List of external URLs pointing to mall websites
        """
        if not mall_html:
            return []

        soup = BeautifulSoup(mall_html, 'html.parser')
        external_urls = []

        # Look for "More Details" buttons that link to external sites
        more_details_buttons = soup.find_all('a', class_=lambda x: x and 'btn' in x and 'primary' in x)

        for button in more_details_buttons:
            href = button.get('href')
            title = button.get('title', '')

            # Check if it's an external link (not pointing to MECSR)
            if href and href.startswith('http') and 'mecsr.org' not in href:
                # Verify it's actually a "More Details" button
                button_text = button.get_text(strip=True).lower()
                if 'more details' in button_text or 'external-link' in str(button):
                    external_urls.append(href)

        return list(set(external_urls))  # Remove duplicates

    async def extract_detailed_mall_info(self, mall_html: str) -> Dict[str, any]:
        """
        Extract detailed property information from individual mall pages

        Args:
            mall_html: HTML content of a mall detail page

        Returns:
            Dictionary with detailed mall information
        """
        if not mall_html:
            return {}

        soup = BeautifulSoup(mall_html, 'html.parser')
        details = {}

        # Extract property details from various sections
        # Look for specification tables or detail sections
        detail_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(term in str(x).lower() for term in ['detail', 'spec', 'info', 'property']))

        for section in detail_sections:
            # Extract various property details
            self._extract_property_specifications(section, details)

        # Look for structured data (JSON-LD)
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('@type') in ['Place', 'LocalBusiness', 'Organization']:
                    self._extract_from_json_ld(data, details)
            except (json.JSONDecodeError, AttributeError):
                continue

        # Extract from meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name', '').lower()
            content = meta.get('content', '')

            if 'description' in name and content:
                details['description'] = content
            elif 'keywords' in name and content:
                details['keywords'] = content.split(',')

        return details

    def _extract_property_specifications(self, section, details: Dict[str, any]):
        """Extract property specifications from a section"""
        # Look for common property detail patterns
        text_content = section.get_text().lower()

        # GLA (Gross Leasable Area)
        gla_patterns = [
            r'gla[:\s]*([\d,]+)\s*(?:sqm|sq\.m\.?|square meters?)',
            r'gross leasable area[:\s]*([\d,]+)\s*(?:sqm|sq\.m\.?|square meters?)',
            r'total area[:\s]*([\d,]+)\s*(?:sqm|sq\.m\.?|square meters?)',
            r'lettable area[:\s]*([\d,]+)\s*(?:sqm|sq\.m\.?|square meters?)'
        ]

        for pattern in gla_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                gla_value = match.group(1).replace(',', '')
                try:
                    details['gla_sqm'] = int(gla_value)
                    # Approximate conversion to sq ft (1 sqm ≈ 10.764 sq ft)
                    details['gla_sqft'] = int(float(gla_value) * 10.764)
                except ValueError:
                    pass
                break

        # Year built/opened
        year_patterns = [
            r'year[:\s]*(?:built|opened|established)[:\s]*(\d{4})',
            r'opened[:\s]*(?:in)?[:\s]*(\d{4})',
            r'established[:\s]*(?:in)?[:\s]*(\d{4})',
            r'(\d{4})[^\d]*mall',
            r'(\d{4})[^\d]*centre'
        ]

        for pattern in year_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                year = match.group(1)
                try:
                    details['opening_year'] = int(year)
                except ValueError:
                    pass
                break

        # Number of stores/shops
        store_patterns = [
            r'(\d+)[\s]*(?:stores?|shops?|retailers?|tenants?)',
            r'stores?[:\s]*(\d+)',
            r'shops?[:\s]*(\d+)',
            r'tenants?[:\s]*(\d+)'
        ]

        for pattern in store_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                stores = match.group(1)
                try:
                    details['stores_count'] = int(stores)
                except ValueError:
                    pass
                break

        # Parking spaces
        parking_patterns = [
            r'(\d+)[\s]*(?:parking|car)?[\s]*spaces?',
            r'parking[:\s]*(\d+)',
            r'car parks?[:\s]*(\d+)'
        ]

        for pattern in parking_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                parking = match.group(1)
                try:
                    details['parking_spaces'] = int(parking)
                except ValueError:
                    pass
                break

    def _extract_from_json_ld(self, json_data: Dict, details: Dict[str, any]):
        """Extract information from JSON-LD structured data"""
        # Address information
        if 'address' in json_data:
            address = json_data['address']
            if isinstance(address, dict):
                if 'streetAddress' in address:
                    details['address'] = address['streetAddress']
                if 'addressLocality' in address:
                    details['city'] = address['addressLocality']
                if 'addressCountry' in address:
                    details['country'] = address['addressCountry']

        # Contact information
        if 'telephone' in json_data:
            details['phone'] = json_data['telephone']
        if 'email' in json_data:
            details['email'] = json_data['email']
        if 'url' in json_data:
            details['website'] = json_data['url']

        # Description
        if 'description' in json_data:
            details['description'] = json_data['description']

    async def scrape_mall_details_batch(self, mall_urls: List[str], batch_size: int = 5) -> List[Dict[str, any]]:
        """
        Scrape detailed information from multiple mall URLs using async batching

        Args:
            mall_urls: List of mall URLs to scrape
            batch_size: Number of concurrent requests

        Returns:
            List of detailed mall information dictionaries
        """
        if not mall_urls:
            return []

        print(f"🔄 Starting batch scraping of {len(mall_urls)} mall pages (batch size: {batch_size})")

        crawler = PaginationCrawler()
        semaphore = asyncio.Semaphore(batch_size)
        results = []

        async def scrape_single_mall(url: str) -> Dict[str, any]:
            async with semaphore:
                try:
                    print(f"  📄 Scraping: {url.split('/')[-1]}")

                    # Scrape the mall page
                    result = await crawler.crawl_single_page(url)

                    if not result or not result.get('success'):
                        return {'url': url, 'error': 'Failed to fetch page'}

                    html = result['html']
                    mall_details = {
                        'url': url,
                        'mall_name': url.split('/')[-1].replace('-', ' ').title(),
                        'scraped_at': result.get('response_time'),
                        'page_size': len(html)
                    }

                    # Extract external URLs
                    external_urls = await self.extract_external_urls(html)
                    if external_urls:
                        mall_details['external_urls'] = external_urls

                    # Extract detailed property information
                    detailed_info = await self.extract_detailed_mall_info(html)
                    mall_details.update(detailed_info)

                    # Extract coordinates
                    coordinates = await self.get_mall_coordinates(url)
                    if coordinates:
                        mall_details['latitude'] = coordinates['latitude']
                        mall_details['longitude'] = coordinates['longitude']

                    print(f"    ✅ Extracted: {len(mall_details)} fields")
                    return mall_details

                except Exception as e:
                    print(f"    ❌ Error: {e}")
                    return {'url': url, 'error': str(e)}

        # Process all URLs concurrently in batches
        tasks = [scrape_single_mall(url) for url in mall_urls]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and collect successful results
        for result in batch_results:
            if isinstance(result, dict) and 'error' not in result:
                results.append(result)
            elif isinstance(result, Exception):
                print(f"❌ Batch processing error: {result}")

        print(f"✅ Completed batch scraping: {len(results)}/{len(mall_urls)} successful")
        return results

    async def collect_sample_mall_urls(self, sample_size: int = 20, max_pages: int = 5) -> List[str]:
        """
        Collect a sample of mall URLs from the first few pages of MECSR directory

        Args:
            sample_size: Number of mall URLs to return
            max_pages: Maximum number of pages to scan

        Returns:
            Sample list of mall URLs
        """
        print(f"🔍 Collecting sample of {sample_size} mall URLs from first {max_pages} pages...")

        crawler = PaginationCrawler()
        all_mall_urls = []

        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                page_url = "https://www.mecsr.org/directory-shopping-centres"
            else:
                page_url = f"https://www.mecsr.org/directory-shopping-centres?page={page_num}"

            print(f"  📄 Scanning page {page_num}: {page_url}")

            result = await crawler.crawl_single_page(page_url)

            if not result or not result.get('success'):
                print(f"    ❌ Failed to fetch page {page_num}")
                break

            html = result['html']
            mall_links = self.extract_mall_links(html)

            if not mall_links:
                print(f"    ⚠️ No mall links found on page {page_num}")
                break

            # Convert to absolute URLs
            for link in mall_links:
                if not link.startswith('http'):
                    full_url = f"https://www.mecsr.org{link}"
                else:
                    full_url = link
                all_mall_urls.append(full_url)

            print(f"    ✅ Found {len(mall_links)} malls on page {page_num} (total so far: {len(all_mall_urls)})")

            if len(all_mall_urls) >= sample_size:
                break

        # Return sample of requested size
        sample_urls = all_mall_urls[:sample_size]
        print(f"🎯 Collected {len(sample_urls)} sample mall URLs")
        return sample_urls

    async def collect_all_mall_urls(self, base_url: str = "https://www.mecsr.org/directory-shopping-centres") -> List[str]:
        """
        Collect all mall URLs from the MECSR directory by paginating through all pages

        Args:
            base_url: Base directory URL to start from

        Returns:
            Complete list of all mall URLs
        """
        print("🔍 Collecting all mall URLs from MECSR directory...")

        crawler = PaginationCrawler()
        all_mall_urls = set()
        page_num = 1

        while True:
            if page_num == 1:
                page_url = base_url
            else:
                page_url = f"{base_url}?page={page_num}"

            print(f"  📄 Scanning page {page_num}: {page_url}")

            result = await crawler.crawl_single_page(page_url)

            if not result or not result.get('success'):
                print(f"    ❌ Failed to fetch page {page_num}")
                break

            html = result['html']
            mall_links = self.extract_mall_links(html)

            if not mall_links:
                print(f"    ⚠️ No mall links found on page {page_num}")
                break

            # Convert to absolute URLs and add to set
            for link in mall_links:
                if not link.startswith('http'):
                    full_url = f"https://www.mecsr.org{link}"
                else:
                    full_url = link
                all_mall_urls.add(full_url)

            print(f"    ✅ Found {len(mall_links)} malls on page {page_num} (total unique: {len(all_mall_urls)})")

            # Check if we've reached the last page
            soup = BeautifulSoup(html, 'html.parser')
            next_link = soup.find('a', text=re.compile(r'next|»|>', re.IGNORECASE))
            if not next_link:
                break

            page_num += 1

            # Safety check to avoid infinite loops
            if page_num > 100:  # Assuming max 100 pages
                print("⚠️ Reached page limit (100), stopping collection")
                break

        final_urls = sorted(list(all_mall_urls))
        print(f"🎯 Collected {len(final_urls)} unique mall URLs total")
        return final_urls
