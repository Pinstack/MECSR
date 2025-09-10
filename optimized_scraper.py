#!/usr/bin/env python3
"""
Optimized MECSR Scraper with Enhanced Data Extraction & Performance

This script provides production-ready scraping with:
- Complete data extraction (images, descriptions, tenants, Post Detail table, external URLs)
- Aggressive async batching with optimal performance
- Checkpointing and resume capability
- Intelligent rate limiting
- Comprehensive error handling
"""

import asyncio
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import aiofiles
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from bs4 import BeautifulSoup

# Import our components
from scrapers.site_scoper import SiteScoper
from scrapers.pagination_crawler import PaginationCrawler
from scrapers.detail_extractor import DetailExtractor
from processors.data_processor import DataProcessor, MallData
from storage.storage_handler import StorageHandler
from config import settings

console = Console()

@dataclass
class ScrapingCheckpoint:
    """Checkpoint for resuming interrupted scraping sessions"""
    timestamp: datetime
    completed_urls: List[str]
    failed_urls: List[str]
    current_batch: int
    total_batches: int
    performance_stats: Dict[str, Any]
    last_successful_url: Optional[str] = None

@dataclass
class MallDataEnhanced:
    """Enhanced mall data structure with all required fields"""
    # Basic info
    name: str
    url: str
    mall_id: Optional[str] = None

    # Location data
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    country: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None

    # Property details
    property_type: Optional[str] = None
    status: Optional[str] = None
    gla_sqm: Optional[int] = None
    gla_sqft: Optional[int] = None
    stores_count: Optional[int] = None
    parking_spaces: Optional[int] = None
    opening_year: Optional[int] = None
    floors: Optional[int] = None
    total_size_sqm: Optional[int] = None

    # Contact information
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None

    # Content
    description: Optional[str] = None
    keywords: Optional[List[str]] = None

    # Media and external links
    images: Optional[List[str]] = None
    external_urls: Optional[List[str]] = None
    tenant_list: Optional[List[str]] = None

    # Metadata
    post_id: Optional[str] = None
    user_id: Optional[str] = None
    data_id: Optional[str] = None
    data_type: Optional[str] = None
    scraped_at: datetime = None
    data_quality_score: Optional[float] = None

    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now()

class OptimizedScraper:
    """Production-ready scraper with enhanced capabilities"""

    def __init__(self,
                 checkpoint_dir: str = "checkpoints",
                 max_concurrent: int = 10,
                 rate_limit_rpm: int = 30,
                 batch_size: int = 10):
        """
        Initialize the optimized scraper

        Args:
            checkpoint_dir: Directory for storing checkpoints
            max_concurrent: Maximum concurrent requests
            rate_limit_rpm: Rate limit in requests per minute
            batch_size: Optimal batch size for processing
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)

        self.max_concurrent = max_concurrent
        self.rate_limit_rpm = rate_limit_rpm
        self.batch_size = batch_size

        # Initialize components
        self.site_scoper = SiteScoper()
        self.crawler = PaginationCrawler(max_concurrent_requests=max_concurrent)
        self.extractor = DetailExtractor()
        self.processor = DataProcessor()
        self.storage = StorageHandler()

        # Rate limiting
        self.last_request_time = 0
        self.request_interval = 60 / rate_limit_rpm

        # Progress tracking
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TextColumn("[bold green]{task.completed}/{task.total} processed"),
            console=console
        )

        # Statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_urls': 0,
            'processed_urls': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'data_quality_scores': [],
            'performance_metrics': {
                'avg_response_time': 0,
                'throughput_rps': 0,
                'total_requests': 0
            }
        }

    async def scrape_comprehensive(self,
                                  mall_urls: List[str],
                                  output_file: str = None,
                                  resume_checkpoint: str = None) -> Dict[str, Any]:
        """
        Perform comprehensive scraping with all enhancements

        Args:
            mall_urls: List of mall URLs to scrape
            output_file: Output file path (optional)
            resume_checkpoint: Checkpoint file to resume from (optional)

        Returns:
            Comprehensive scraping results
        """
        console.print("\n🚀 [bold blue]Optimized MECSR Scraper[/bold blue]")
        console.print(f"📊 Processing {len(mall_urls)} mall URLs")
        console.print(f"⚡ Configuration: {self.max_concurrent} concurrent, {self.rate_limit_rpm} req/min, batch size {self.batch_size}")

        self.stats['start_time'] = datetime.now()
        self.stats['total_urls'] = len(mall_urls)

        # Load checkpoint if resuming
        if resume_checkpoint:
            checkpoint = await self._load_checkpoint(resume_checkpoint)
            if checkpoint:
                mall_urls = [url for url in mall_urls if url not in checkpoint.completed_urls]
                console.print(f"📋 Resuming from checkpoint: {len(checkpoint.completed_urls)} already processed")

        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(self.max_concurrent)

        # Process URLs in optimized batches
        all_results = []
        processed_count = 0

        with self.progress:
            task = self.progress.add_task("Scraping malls...", total=len(mall_urls), processed=0, total_urls=len(mall_urls))

            # Process in batches
            for i in range(0, len(mall_urls), self.batch_size):
                batch_urls = mall_urls[i:i + self.batch_size]
                console.print(f"\n🔄 Processing batch {i//self.batch_size + 1}/{(len(mall_urls) + self.batch_size - 1)//self.batch_size}")

                # Process batch with enhanced extraction
                batch_results = await self._process_batch_enhanced(batch_urls, semaphore)

                # Update results and statistics
                all_results.extend(batch_results)
                successful = len([r for r in batch_results if 'error' not in r and r.get('data')])
                processed_count += len(batch_urls)

                self.stats['processed_urls'] += len(batch_urls)
                self.stats['successful_extractions'] += successful
                self.stats['failed_extractions'] += (len(batch_urls) - successful)

                # Update progress
                self.progress.update(task, advance=len(batch_urls), processed=processed_count)

                # Save checkpoint every 10 batches
                if (i // self.batch_size + 1) % 10 == 0:
                    await self._save_checkpoint(all_results, i // self.batch_size + 1, len(mall_urls) // self.batch_size + 1)

                # Rate limiting delay between batches
                await asyncio.sleep(1)

        # Finalize statistics
        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        self.stats['performance_metrics']['throughput_rps'] = len(all_results) / duration if duration > 0 else 0

        # Save final results
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/comprehensive_mecsr_scraping_{timestamp}.json"

        await self._save_comprehensive_results(all_results, output_file)

        # Generate report
        return await self._generate_comprehensive_report(all_results)

    async def _process_batch_enhanced(self, batch_urls: List[str], semaphore) -> List[Dict[str, Any]]:
        """
        Process a batch of URLs with enhanced data extraction

        Args:
            batch_urls: URLs to process
            semaphore: Async semaphore for rate limiting

        Returns:
            List of enhanced extraction results
        """
        async def process_single_url(url: str) -> Dict[str, Any]:
            async with semaphore:
                # Rate limiting
                await self._rate_limit()

                try:
                    # Enhanced extraction with all data fields
                    mall_data = await self._extract_mall_data_comprehensive(url)

                    return {
                        'url': url,
                        'success': True,
                        'data': mall_data,
                        'scraped_at': datetime.now(),
                        'response_time': 0  # Could be enhanced with actual timing
                    }

                except Exception as e:
                    console.print(f"❌ Failed to extract {url}: {e}")
                    return {
                        'url': url,
                        'success': False,
                        'error': str(e),
                        'scraped_at': datetime.now()
                    }

        # Process batch concurrently
        tasks = [process_single_url(url) for url in batch_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return results
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                console.print(f"💥 Batch processing exception: {result}")
            else:
                valid_results.append(result)

        return valid_results

    async def _extract_mall_data_comprehensive(self, url: str) -> MallDataEnhanced:
        """
        Extract comprehensive mall data including all required fields

        Args:
            url: Mall URL to extract data from

        Returns:
            Enhanced MallDataEnhanced object
        """
        # Get raw page content
        result = await self.crawler.crawl_single_page(url)

        if not result or not result.get('success'):
            raise ValueError(f"Failed to fetch page: {result.get('error', 'Unknown error')}")

        html = result['html']
        soup = BeautifulSoup(html, 'html.parser')

        # Initialize data structure
        mall_data = MallDataEnhanced(
            name=self._extract_mall_name(soup),
            url=url
        )

        # Extract all data fields
        mall_data.mall_id = self._extract_mall_id(url)

        # Location data
        coordinates = await self.extractor.get_mall_coordinates(url)
        if coordinates:
            mall_data.latitude = coordinates['latitude']
            mall_data.longitude = coordinates['longitude']

        # Enhanced property details
        mall_data.property_type = self._extract_property_type(soup)
        mall_data.status = self._extract_status(soup)

        # Post Detail table data
        post_details = self._extract_post_detail_table(soup)
        mall_data.gla_sqm = post_details.get('gla_sqm')
        mall_data.stores_count = post_details.get('stores_count')
        mall_data.parking_spaces = post_details.get('parking_spaces')
        mall_data.opening_year = post_details.get('opening_year')
        mall_data.floors = post_details.get('floors')
        mall_data.total_size_sqm = post_details.get('total_size_sqm')

        # Contact information
        contact_info = self._extract_contact_info(soup)
        mall_data.phone = contact_info.get('phone')
        mall_data.email = contact_info.get('email')
        mall_data.website = contact_info.get('website')

        # Content
        mall_data.description = self._extract_description(soup)
        mall_data.keywords = self._extract_keywords(soup)

        # Media and external links
        mall_data.images = self._extract_images(soup)
        mall_data.external_urls = await self.extractor.extract_external_urls(html)
        mall_data.tenant_list = self._extract_tenants(soup)

        # Metadata
        mall_data.post_id = self._extract_post_id(soup)
        mall_data.user_id = self._extract_user_id(soup)
        mall_data.data_id = self._extract_data_id(soup)
        mall_data.data_type = self._extract_data_type(soup)

        # Calculate data quality score
        mall_data.data_quality_score = self._calculate_data_quality_score(mall_data)

        # Convert GLA to square feet if available
        if mall_data.gla_sqm:
            mall_data.gla_sqft = int(mall_data.gla_sqm * 10.764)

        return mall_data

    def _extract_mall_name(self, soup: BeautifulSoup) -> str:
        """Extract mall name from page"""
        # Try multiple selectors for mall name
        selectors = [
            'h1.page-title',
            'h1.entry-title',
            '.mall-name',
            '.property-title',
            'h1'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 3:  # Meaningful name
                    return text

        # Fallback: extract from URL
        return " ".join(self._extract_mall_id_from_url(soup.find('link', rel='canonical')['href'] if soup.find('link', rel='canonical') else '').split('-')).title()

    def _extract_mall_id(self, url: str) -> str:
        """Extract mall ID from URL"""
        return url.split('/')[-1]

    def _extract_property_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract property type"""
        # Look for property type in various locations
        selectors = [
            '.property-type',
            '.type-badge',
            '.category',
            '[data-property-type]'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)

        return None

    def _extract_status(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract property status"""
        selectors = [
            '.status-badge',
            '.property-status',
            '.status'
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)

        return None

    def _extract_post_detail_table(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract data from Post Detail table"""
        details = {}

        # Look for tables with property details
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    # Extract specific fields
                    if 'gla' in key and 'sqm' in key:
                        try:
                            details['gla_sqm'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass
                    elif 'store' in key and 'count' in key:
                        try:
                            details['stores_count'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass
                    elif 'parking' in key:
                        try:
                            details['parking_spaces'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass
                    elif 'year' in key and 'open' in key:
                        try:
                            details['opening_year'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass
                    elif 'floor' in key:
                        try:
                            details['floors'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass
                    elif 'total' in key and 'size' in key:
                        try:
                            details['total_size_sqm'] = int(''.join(filter(str.isdigit, value)))
                        except ValueError:
                            pass

        return details

    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract contact information"""
        contact = {}

        # Phone
        phone_selectors = ['.phone', '.tel', '.telephone', '[href^="tel:"]']
        for selector in phone_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'a' and element.get('href', '').startswith('tel:'):
                    contact['phone'] = element['href'].replace('tel:', '')
                else:
                    contact['phone'] = element.get_text(strip=True)
                break

        # Email
        email_selectors = ['.email', '[href^="mailto:"]']
        for selector in email_selectors:
            element = soup.select_one(selector)
            if element:
                if element.name == 'a' and element.get('href', '').startswith('mailto:'):
                    contact['email'] = element['href'].replace('mailto:', '')
                else:
                    contact['email'] = element.get_text(strip=True)
                break

        # Website
        website_selectors = ['.website', '.url', '[href^="http"]']
        for selector in website_selectors:
            element = soup.select_one(selector)
            if element and element.name == 'a':
                href = element.get('href')
                if href and href.startswith('http') and 'mecsr.org' not in href:
                    contact['website'] = href
                    break

        return contact

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract mall description"""
        # Look for description in meta tags first
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content']

        # Look for description in content areas
        desc_selectors = [
            '.description',
            '.content',
            '.property-description',
            '.about',
            'article p'
        ]

        for selector in desc_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 50:  # Substantial description
                    return text

        return None

    def _extract_keywords(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract keywords"""
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            return [kw.strip() for kw in meta_keywords['content'].split(',') if kw.strip()]

        return None

    def _extract_images(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract image URLs"""
        images = []

        # Find all images
        img_tags = soup.find_all('img', src=True)
        for img in img_tags:
            src = img.get('src')
            if src:
                # Convert relative URLs to absolute
                if not src.startswith('http'):
                    src = f"https://www.mecsr.org{src}"
                # Filter out icons, logos, etc. - focus on property images
                if any(term in src.lower() for term in ['property', 'mall', 'centre', 'center']) or 'uploads' in src:
                    images.append(src)

        # Also check for image galleries or carousels
        gallery_selectors = ['.gallery img', '.carousel img', '.property-images img']
        for selector in gallery_selectors:
            gallery_imgs = soup.select(selector)
            for img in gallery_imgs:
                src = img.get('src')
                if src and src not in images:
                    if not src.startswith('http'):
                        src = f"https://www.mecsr.org{src}"
                    images.append(src)

        return images if images else None

    def _extract_tenants(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract tenant/store list"""
        tenants = []

        # Look for tenant lists, store directories, etc.
        tenant_selectors = [
            '.tenants li',
            '.stores li',
            '.retailers li',
            '.tenant-list a',
            '.store-list a'
        ]

        for selector in tenant_selectors:
            tenant_elements = soup.select(selector)
            for element in tenant_elements:
                tenant_name = element.get_text(strip=True)
                if tenant_name and len(tenant_name) > 2:
                    tenants.append(tenant_name)

        # Also check for structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict) and 'containsPlace' in data:
                    for place in data['containsPlace']:
                        if 'name' in place:
                            tenants.append(place['name'])
            except:
                continue

        return list(set(tenants)) if tenants else None

    def _extract_post_id(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract post ID"""
        post_item = soup.find('span', class_='postItem')
        if post_item:
            return post_item.get('data-postid')
        return None

    def _extract_user_id(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract user ID"""
        post_item = soup.find('span', class_='postItem')
        if post_item:
            return post_item.get('data-userid')
        return None

    def _extract_data_id(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract data ID"""
        post_item = soup.find('span', class_='postItem')
        if post_item:
            return post_item.get('data-dataid')
        return None

    def _extract_data_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract data type"""
        post_item = soup.find('span', class_='postItem')
        if post_item:
            return post_item.get('data-datatype')
        return None

    def _calculate_data_quality_score(self, mall_data: MallDataEnhanced) -> float:
        """Calculate data quality score"""
        score = 0.0
        total_weight = 0.0

        # Required fields (high weight)
        if mall_data.name:
            score += 1.0
        total_weight += 1.0

        if mall_data.url:
            score += 1.0
        total_weight += 1.0

        # Important fields (medium weight)
        if mall_data.latitude and mall_data.longitude:
            score += 0.9
        total_weight += 0.9

        if mall_data.description:
            score += 0.7
        total_weight += 0.7

        # Additional fields (lower weight)
        if mall_data.gla_sqm:
            score += 0.6
        total_weight += 0.6

        if mall_data.stores_count:
            score += 0.5
        total_weight += 0.5

        if mall_data.website:
            score += 0.4
        total_weight += 0.4

        if mall_data.images:
            score += 0.3
        total_weight += 0.3

        if mall_data.tenant_list:
            score += 0.3
        total_weight += 0.3

        return score / total_weight if total_weight > 0 else 0.0

    async def _rate_limit(self):
        """Implement intelligent rate limiting"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.request_interval:
            sleep_time = self.request_interval - time_since_last_request
            await asyncio.sleep(sleep_time)

        self.last_request_time = time.time()

    async def _save_checkpoint(self, results: List[Dict], current_batch: int, total_batches: int):
        """Save checkpoint for resume capability"""
        checkpoint = ScrapingCheckpoint(
            timestamp=datetime.now(),
            completed_urls=[r['url'] for r in results if r.get('success')],
            failed_urls=[r['url'] for r in results if not r.get('success')],
            current_batch=current_batch,
            total_batches=total_batches,
            performance_stats=self.stats['performance_metrics']
        )

        checkpoint_file = self.checkpoint_dir / f"checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        async with aiofiles.open(checkpoint_file, 'w') as f:
            await f.write(json.dumps(asdict(checkpoint), default=str, indent=2))

    async def _load_checkpoint(self, checkpoint_file: str) -> Optional[ScrapingCheckpoint]:
        """Load checkpoint for resuming"""
        checkpoint_path = Path(checkpoint_file)
        if not checkpoint_path.exists():
            return None

        async with aiofiles.open(checkpoint_path, 'r') as f:
            data = await f.read()

        checkpoint_dict = json.loads(data)
        return ScrapingCheckpoint(**checkpoint_dict)

    async def _save_comprehensive_results(self, results: List[Dict], output_file: str):
        """Save comprehensive results to file"""
        output_data = {
            'metadata': {
                'scraped_at': datetime.now().isoformat(),
                'total_malls': len(results),
                'successful_extractions': len([r for r in results if r.get('success')]),
                'scraper_config': {
                    'max_concurrent': self.max_concurrent,
                    'rate_limit_rpm': self.rate_limit_rpm,
                    'batch_size': self.batch_size
                },
                'performance_stats': self.stats
            },
            'malls': []
        }

        for result in results:
            if result.get('success') and result.get('data'):
                mall_dict = asdict(result['data'])
                # Convert datetime to ISO string
                if 'scraped_at' in mall_dict and isinstance(mall_dict['scraped_at'], datetime):
                    mall_dict['scraped_at'] = mall_dict['scraped_at'].isoformat()
                output_data['malls'].append(mall_dict)

        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(output_data, indent=2, ensure_ascii=False, default=str))

    async def _generate_comprehensive_report(self, results: List[Dict]) -> Dict[str, Any]:
        """Generate comprehensive scraping report"""
        successful_results = [r for r in results if r.get('success') and r.get('data')]

        # Data completeness analysis
        completeness_stats = {
            'images': len([r for r in successful_results if r['data'].images]),
            'descriptions': len([r for r in successful_results if r['data'].description]),
            'tenants': len([r for r in successful_results if r['data'].tenant_list]),
            'coordinates': len([r for r in successful_results if r['data'].latitude and r['data'].longitude]),
            'contact_info': len([r for r in successful_results if r['data'].phone or r['data'].email or r['data'].website]),
            'property_details': len([r for r in successful_results if r['data'].gla_sqm or r['data'].stores_count])
        }

        # Performance analysis
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() if self.stats['end_time'] else 0
        throughput = len(results) / duration if duration > 0 else 0

        report = {
            'summary': {
                'total_urls_processed': len(results),
                'successful_extractions': len(successful_results),
                'failed_extractions': len(results) - len(successful_results),
                'success_rate': len(successful_results) / len(results) * 100 if results else 0,
                'duration_seconds': duration,
                'throughput_per_second': throughput
            },
            'data_completeness': {
                field: {
                    'count': count,
                    'percentage': count / len(successful_results) * 100 if successful_results else 0
                }
                for field, count in completeness_stats.items()
            },
            'performance_metrics': self.stats['performance_metrics'],
            'quality_scores': {
                'average': sum(r['data'].data_quality_score for r in successful_results if r['data'].data_quality_score) / len(successful_results) if successful_results else 0,
                'distribution': {
                    'excellent': len([r for r in successful_results if r['data'].data_quality_score and r['data'].data_quality_score >= 0.9]),
                    'good': len([r for r in successful_results if r['data'].data_quality_score and 0.7 <= r['data'].data_quality_score < 0.9]),
                    'fair': len([r for r in successful_results if r['data'].data_quality_score and 0.5 <= r['data'].data_quality_score < 0.7]),
                    'poor': len([r for r in successful_results if r['data'].data_quality_score and r['data'].data_quality_score < 0.5])
                }
            }
        }

        # Display report
        console.print("\n📊 [bold]Comprehensive Scraping Report[/bold]")

        table = Table(title="📈 Scraping Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total URLs", str(report['summary']['total_urls_processed']))
        table.add_row("Successful", str(report['summary']['successful_extractions']))
        table.add_row("Success Rate", ".1f")
        table.add_row("Duration", ".1f")
        table.add_row("Throughput", ".2f")

        console.print(table)

        # Data completeness table
        console.print("\n📋 [bold]Data Completeness[/bold]")
        completeness_table = Table()
        completeness_table.add_column("Data Field", style="cyan")
        completeness_table.add_column("Count", style="green")
        completeness_table.add_column("Percentage", style="magenta")

        for field, stats in report['data_completeness'].items():
            completeness_table.add_row(
                field.replace('_', ' ').title(),
                str(stats['count']),
                ".1f"
            )

        console.print(completeness_table)

        # Quality distribution
        console.print("\n🎯 [bold]Data Quality Distribution[/bold]")
        quality_dist = report['quality_scores']['distribution']
        quality_table = Table()
        quality_table.add_column("Quality Level", style="cyan")
        quality_table.add_column("Count", style="green")

        quality_table.add_row("Excellent (≥90%)", str(quality_dist['excellent']))
        quality_table.add_row("Good (70-89%)", str(quality_dist['good']))
        quality_table.add_row("Fair (50-69%)", str(quality_dist['fair']))
        quality_table.add_row("Poor (<50%)", str(quality_dist['poor']))

        console.print(quality_table)

        return report


async def main():
    """Main entry point for optimized scraper"""
    # Get mall URLs (could be from file, command line, or generated)
    # For testing, use our validated URLs
    test_mall_urls = [
        "https://www.mecsr.org/directory-shopping-centres/dalma-mall",
        "https://www.mecsr.org/directory-shopping-centres/01-burda",
        "https://www.mecsr.org/directory-shopping-centres/al-bahar-towers-shopping-centre"
    ]

    # Initialize optimized scraper
    scraper = OptimizedScraper(
        max_concurrent=10,  # Optimal batch size from validation
        rate_limit_rpm=30,  # Respectful rate limiting
        batch_size=10
    )

    # Run comprehensive scraping
    results = await scraper.scrape_comprehensive(test_mall_urls)

    console.print("\n✅ [bold green]Optimized scraping completed![/bold green]")
    console.print("[dim]Check output directory for comprehensive results[/dim]")


if __name__ == "__main__":
    asyncio.run(main())
