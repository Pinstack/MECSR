#!/usr/bin/env python3
"""
Simple MECSR Scraper - Lean and Clean Version
Strips out complexity while keeping core functionality working.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Only essential imports
from enhanced_extractor import EnhancedDataExtractor
from scrapers.pagination_crawler import PaginationCrawler

# Utility imports
from utils.helpers import (
    get_iso_timestamp, ensure_directory, create_error_result
)
from utils.constants import (
    DEFAULT_MAX_CONCURRENT, DEFAULT_REQUESTS_PER_MINUTE, DEFAULT_BATCH_SIZE,
    OUTPUT_DIR, OUTPUT_FILE_PATTERN, TIMESTAMP_FORMAT,
    MSG_STARTING, MSG_DISCOVERING, MSG_PROCESSING_BATCH,
    TOTAL_MALLS_TO_SCRAPE, MAX_PAGES_TO_SCRAPE
)
from utils.reporting import (
    generate_scraping_report, print_scraping_summary, print_final_report
)


class SimpleMECSRScraper:
    """Simple, clean scraper focused on core functionality"""

    def __init__(self,
                 max_concurrent: int = DEFAULT_MAX_CONCURRENT,
                 requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
                 batch_size: int = DEFAULT_BATCH_SIZE):
        self.max_concurrent = max_concurrent
        self.requests_per_minute = requests_per_minute
        self.batch_size = batch_size
        self.delay = 60 / requests_per_minute  # Conservative rate limiting
        self.random_delay = 2.0  # Additional random delay to avoid patterns

        # Initialize core components
        self.extractor = EnhancedDataExtractor()
        self.crawler = PaginationCrawler(max_concurrent_requests=max_concurrent)

        # Simple stats tracking
        self.stats = {
            'start_time': None,
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'avg_response_time': 0.0
        }

    async def scrape_malls(self, mall_urls: List[str]) -> Dict[str, Any]:
        """Main scraping function - simple and clean"""
        self.stats['start_time'] = datetime.now()
        print(MSG_STARTING)

        all_results = []
        semaphore = asyncio.Semaphore(self.max_concurrent)

        # Process in simple batches
        for i in range(0, len(mall_urls), self.batch_size):
            batch_urls = mall_urls[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(mall_urls) + self.batch_size - 1) // self.batch_size

            print(MSG_PROCESSING_BATCH.format(
                batch=batch_num,
                total=total_batches,
                size=len(batch_urls)
            ))

            batch_results = await self._process_batch(batch_urls, semaphore)
            all_results.extend(batch_results)

            # Simple progress update
            self.stats['total_processed'] += len(batch_urls)
            print(f"   ✅ Completed: {self.stats['total_processed']}/{len(mall_urls)} total")

        # Generate simple report and return both report and results
        report = generate_scraping_report(all_results)
        print_scraping_summary(all_results)
        return {
            'report': report,
            'results': all_results
        }

    async def _process_batch(self, batch_urls: List[str], semaphore) -> List[Dict[str, Any]]:
        """Process a single batch of URLs"""
        async def process_single_url(url: str) -> Dict[str, Any]:
            async with semaphore:
                # Add random delay to avoid detection patterns
                import random
                random_delay = random.uniform(0, self.random_delay)
                total_delay = self.delay + random_delay
                await asyncio.sleep(total_delay)

                start_time = time.time()
                try:
                    # Get page content
                    result = await self.crawler.crawl_single_page(url)
                    if not result or not result.get('success'):
                        return self._create_error_result(url, "Failed to fetch page")

                    html = result['html']

                    # Extract data using streamlined extractor
                    mall_data = self.extractor.extract_streamlined_mall_data(html, url)

                    elapsed = time.time() - start_time
                    self.stats['successful'] += 1
                    self.stats['avg_response_time'] = (
                        (self.stats['avg_response_time'] * (self.stats['successful'] - 1) + elapsed)
                        / self.stats['successful']
                    )

                    return {
                        'url': url,
                        'success': True,
                        'data': mall_data,
                        'response_time': elapsed,
                        'scraped_at': get_iso_timestamp()
                    }

                except Exception as e:
                    self.stats['failed'] += 1
                    return self._create_error_result(url, str(e))

        # Process all URLs in batch concurrently
        tasks = [process_single_url(url) for url in batch_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions
        valid_results = []
        for r in results:
            if isinstance(r, dict):
                valid_results.append(r)
            else:
                print(f"   ❌ Batch error: {r}")

        return valid_results

    def _create_error_result(self, url: str, error: str) -> Dict[str, Any]:
        """Create error result dictionary"""
        return create_error_result(error, url)


    async def save_results(self, results: List[Dict[str, Any]], output_file: str):
        """Save results to JSON with proper datetime handling"""
        # Prepare output structure
        successful_results = [r for r in results if r.get('success') and r.get('data')]

        output = {
            'metadata': {
                'scraped_at': get_iso_timestamp(),
                'total_malls': len(results),
                'successful_extractions': len(successful_results),
                'scraper_version': 'simple_v1.0'
            },
            'malls': []
        }

        # Process each successful result
        for r in successful_results:
            if r.get('data'):
                mall_dict = dict(r['data'])

                # Handle datetime objects for JSON serialization
                if 'last_updated' in mall_dict and isinstance(mall_dict['last_updated'], datetime):
                    mall_dict['last_updated'] = mall_dict['last_updated'].isoformat()

                # Add scraping metadata
                mall_dict['_scraping_metadata'] = {
                    'response_time': r.get('response_time', 0.0),
                    'scraped_at': r.get('scraped_at', get_iso_timestamp())
                }

                output['malls'].append(mall_dict)

        # Custom JSON encoder for datetime objects
        def datetime_encoder(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        # Ensure output directory exists
        output_path = Path(output_file)
        ensure_directory(str(output_path.parent))

        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=datetime_encoder)

        print(f"💾 Results saved to: {output_file}")
        return output


async def main(test_batch_size: int = 100):
    """Simple main function with test batch option"""
    print("🏪 Streamlined MECSR Mall Scraper")
    print("=" * 40)

    # Get mall URLs from all pages (up to 1001 malls)
    from scrapers.detail_extractor import DetailExtractor

    print(MSG_DISCOVERING)
    extractor = DetailExtractor()
    mall_urls = await extractor.collect_mall_urls_async(
        num_pages=MAX_PAGES_TO_SCRAPE,
        max_concurrent=2  # Very conservative for directory pages
    )

    # Limit to test batch for performance testing
    if test_batch_size and len(mall_urls) > test_batch_size:
        print(f"🔬 Testing with {test_batch_size} malls (limited for performance testing)")
        mall_urls = mall_urls[:test_batch_size]

    if not mall_urls:
        print("❌ No mall URLs found!")
        return

    print(f"🎯 Found {len(mall_urls)} malls to scrape")

    # Create scraper with respectful settings to avoid blocking
    scraper = SimpleMECSRScraper(
        max_concurrent=3,  # Reduced concurrency to be respectful
        requests_per_minute=20,  # Conservative rate to avoid blocking
        batch_size=10  # Smaller batches for gentler scraping
    )

    # Scrape malls
    scrape_data = await scraper.scrape_malls(mall_urls)

    # Save results
    timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)
    output_file = f"{OUTPUT_DIR}/{OUTPUT_FILE_PATTERN.format(timestamp=timestamp)}"
    await scraper.save_results(scrape_data['results'], output_file)

    print_final_report(scrape_data['report'], output_file)

    # Cleanup crawler resources
    try:
        await scraper.crawler.cleanup()
        print("✅ Cleanup completed successfully")
    except Exception as e:
        print(f"⚠️ Cleanup warning (non-critical): {e}")


if __name__ == "__main__":
    asyncio.run(main(test_batch_size=None))
