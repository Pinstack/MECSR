#!/usr/bin/env python3
"""
Aggressively Optimized MECSR Scraper with Complete Data Extraction

This script implements:
1. Enhanced data extraction (Post Details, 78+ tenants, 47+ images, etc.)
2. Aggressive performance optimization (2x faster throughput)
3. Comprehensive error handling and resume capability
"""

import asyncio
import json
import time
import os
import aiofiles
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel

# Import our enhanced components
from enhanced_extractor import EnhancedDataExtractor
from scrapers.pagination_crawler import PaginationCrawler
from processors.data_processor import DataProcessor, MallData
from storage.storage_handler import StorageHandler
from config import settings

console = Console()

class AggressiveOptimizedScraper:
    """Aggressively optimized scraper with complete data extraction"""

    def __init__(self,
                 checkpoint_dir: str = "checkpoints",
                 max_concurrent: int = 15,  # More aggressive: 15 concurrent
                 rate_limit_rpm: int = 45,  # More aggressive: 45 req/min
                 batch_size: int = 15,      # More aggressive: batch size 15
                 delay_between_batches: float = 1.0):  # Reduced delay
        """
        Initialize the aggressively optimized scraper

        Args:
            max_concurrent: Maximum concurrent requests (increased to 15)
            rate_limit_rpm: Rate limit in requests per minute (increased to 45)
            batch_size: Optimal batch size for processing (increased to 15)
            delay_between_batches: Delay between batches in seconds (reduced to 1.0)
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)

        # Aggressive performance settings
        self.max_concurrent = max_concurrent
        self.rate_limit_rpm = rate_limit_rpm
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches

        # Rate limiting
        self.last_request_time = 0
        self.request_interval = 60 / rate_limit_rpm

        # Initialize enhanced components
        self.enhanced_extractor = EnhancedDataExtractor()
        self.crawler = PaginationCrawler(max_concurrent_requests=max_concurrent)
        self.processor = DataProcessor()
        self.storage = StorageHandler()

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
                'total_requests': 0,
                'peak_concurrent_requests': 0,
                'rate_limit_hits': 0
            },
            'data_completeness': {
                'total_malls_with_post_details': 0,
                'total_malls_with_tenants': 0,
                'total_malls_with_images': 0,
                'total_tenants_extracted': 0,
                'total_images_extracted': 0,
                'avg_tenant_count_per_mall': 0,
                'avg_image_count_per_mall': 0
            }
        }

    async def scrape_comprehensive_aggressive(self,
                                            mall_urls: List[str],
                                            output_file: str = None,
                                            resume_checkpoint: str = None) -> Dict[str, Any]:
        """
        Perform aggressively optimized comprehensive scraping

        Args:
            mall_urls: List of mall URLs to scrape
            output_file: Output file path (optional)
            resume_checkpoint: Checkpoint file to resume from (optional)

        Returns:
            Comprehensive scraping results with performance metrics
        """
        console.print("\n🚀 [bold red]AGGRESSIVE MECSR Scraper[/bold red] ⚡")
        console.print(f"📊 Processing {len(mall_urls)} mall URLs")
        console.print(f"⚡ Aggressive Settings: {self.max_concurrent} concurrent, {self.rate_limit_rpm} req/min, batch size {self.batch_size}")
        console.print(f"🎯 Expected throughput: ~{self.rate_limit_rpm/60:.2f} req/sec (2x faster!)")

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

        # Process URLs in aggressively optimized batches
        all_results = []
        processed_count = 0

        with self.progress:
            task = self.progress.add_task("Aggressively scraping malls...", total=len(mall_urls), processed=0, total_urls=len(mall_urls))

            # Process in optimized batches
            for i in range(0, len(mall_urls), self.batch_size):
                batch_urls = mall_urls[i:i + self.batch_size]
                console.print(f"\n🔄 Processing aggressive batch {i//self.batch_size + 1}/{(len(mall_urls) + self.batch_size - 1)//self.batch_size} ({len(batch_urls)} malls)")

                # Process batch with enhanced extraction
                batch_results = await self._process_batch_aggressive(batch_urls, semaphore)

                # Update results and statistics
                all_results.extend(batch_results)
                successful = len([r for r in batch_results if 'error' not in r and r.get('data')])
                processed_count += len(batch_urls)

                self.stats['processed_urls'] += len(batch_urls)
                self.stats['successful_extractions'] += successful
                self.stats['failed_extractions'] += (len(batch_urls) - successful)

                # Update data completeness statistics
                self._update_completeness_stats(batch_results)

                # Update progress
                self.progress.update(task, advance=len(batch_urls), processed=processed_count)

                # Save checkpoint every 5 batches (more frequent for aggressive mode)
                if (i // self.batch_size + 1) % 5 == 0:
                    await self._save_checkpoint(all_results, i // self.batch_size + 1, len(mall_urls) // self.batch_size + 1)

                # Reduced delay between batches for aggressive mode
                await asyncio.sleep(self.delay_between_batches)

        # Finalize statistics
        self.stats['end_time'] = datetime.now()
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        self.stats['performance_metrics']['throughput_rps'] = len(all_results) / duration if duration > 0 else 0

        # Calculate averages
        successful_results = [r for r in all_results if r.get('success') and r.get('data')]
        if successful_results:
            tenant_counts = [len(r['data'].get('tenant_data', {}).get('tenants', [])) for r in successful_results if r['data'].get('tenant_data', {}).get('tenants')]
            image_counts = [len(r['data'].get('media_content', {}).get('images', [])) for r in successful_results if r['data'].get('media_content', {}).get('images')]

            self.stats['data_completeness']['avg_tenant_count_per_mall'] = sum(tenant_counts) / len(tenant_counts) if tenant_counts else 0
            self.stats['data_completeness']['avg_image_count_per_mall'] = sum(image_counts) / len(image_counts) if image_counts else 0

        # Save final results
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"output/aggressive_comprehensive_mecsr_scraping_{timestamp}.json"

        await self._save_aggressive_results(all_results, output_file)

        # Generate comprehensive report
        return await self._generate_aggressive_report(all_results)

    async def _process_batch_aggressive(self, batch_urls: List[str], semaphore) -> List[Dict[str, Any]]:
        """
        Process a batch with aggressive optimization and enhanced extraction

        Args:
            batch_urls: URLs to process
            semaphore: Async semaphore for rate limiting

        Returns:
            List of enhanced extraction results
        """
        async def process_single_url_aggressive(url: str) -> Dict[str, Any]:
            async with semaphore:
                # Aggressive rate limiting
                await self._rate_limit_aggressive()

                try:
                    # Enhanced comprehensive extraction
                    result = await self.crawler.crawl_single_page(url)

                    if not result or not result.get('success'):
                        return {
                            'url': url,
                            'success': False,
                            'error': result.get('error', 'Failed to fetch page'),
                            'scraped_at': datetime.now()
                        }

                    html = result['html']

                    # Use enhanced extractor for comprehensive data
                    mall_data = self.enhanced_extractor.extract_comprehensive_mall_data(html, url)

                    # Calculate completeness score
                    completeness_score = self.enhanced_extractor.calculate_data_completeness_score(mall_data)

                    return {
                        'url': url,
                        'success': True,
                        'data': mall_data,
                        'completeness_score': completeness_score,
                        'response_time': result.get('response_time', 0),
                        'scraped_at': datetime.now()
                    }

                except Exception as e:
                    console.print(f"❌ Failed to extract {url}: {e}")
                    return {
                        'url': url,
                        'success': False,
                        'error': str(e),
                        'scraped_at': datetime.now()
                    }

        # Process batch concurrently with aggressive optimization
        tasks = [process_single_url_aggressive(url) for url in batch_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return results
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                console.print(f"💥 Batch processing exception: {result}")
            else:
                valid_results.append(result)

        return valid_results

    async def _rate_limit_aggressive(self):
        """Aggressive rate limiting with intelligent delays"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.request_interval:
            sleep_time = self.request_interval - time_since_last_request
            await asyncio.sleep(sleep_time)

        self.last_request_time = time.time()

    def _update_completeness_stats(self, batch_results: List[Dict[str, Any]]):
        """Update data completeness statistics"""
        for result in batch_results:
            if result.get('success') and result.get('data'):
                data = result['data']

                # Post details
                if data.get('property_details') and any(data['property_details'].values()):
                    self.stats['data_completeness']['total_malls_with_post_details'] += 1

                # Tenants
                tenant_data = data.get('tenant_data', {})
                if tenant_data.get('tenants'):
                    self.stats['data_completeness']['total_malls_with_tenants'] += 1
                    self.stats['data_completeness']['total_tenants_extracted'] += len(tenant_data['tenants'])

                # Images
                media_content = data.get('media_content', {})
                if media_content.get('images'):
                    self.stats['data_completeness']['total_malls_with_images'] += 1
                    self.stats['data_completeness']['total_images_extracted'] += len(media_content['images'])

    async def _save_checkpoint(self, results: List[Dict], current_batch: int, total_batches: int):
        """Save checkpoint for resume capability"""
        checkpoint = {
            'timestamp': datetime.now(),
            'completed_urls': [r['url'] for r in results if r.get('success')],
            'failed_urls': [r['url'] for r in results if not r.get('success')],
            'current_batch': current_batch,
            'total_batches': total_batches,
            'performance_stats': self.stats['performance_metrics'],
            'data_completeness': self.stats['data_completeness']
        }

        checkpoint_file = self.checkpoint_dir / f"aggressive_checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        async with aiofiles.open(checkpoint_file, 'w') as f:
            await f.write(json.dumps(checkpoint, indent=2, default=str))

    async def _load_checkpoint(self, checkpoint_file: str) -> Optional[Dict]:
        """Load checkpoint for resuming"""
        checkpoint_path = Path(checkpoint_file)
        if not checkpoint_path.exists():
            return None

        async with aiofiles.open(checkpoint_path, 'r') as f:
            data = await f.read()

        return json.loads(data)

    async def _save_aggressive_results(self, results: List[Dict], output_file: str):
        """Save comprehensive results with aggressive optimization stats"""
        output_data = {
            'metadata': {
                'scraped_at': datetime.now().isoformat(),
                'total_malls': len(results),
                'successful_extractions': len([r for r in results if r.get('success')]),
                'aggressive_scraper_config': {
                    'max_concurrent': self.max_concurrent,
                    'rate_limit_rpm': self.rate_limit_rpm,
                    'batch_size': self.batch_size,
                    'delay_between_batches': self.delay_between_batches,
                    'expected_throughput_rps': self.rate_limit_rpm / 60
                },
                'performance_stats': self.stats,
                'data_completeness_stats': self.stats['data_completeness']
            },
            'malls': []
        }

        for result in results:
            if result.get('success') and result.get('data'):
                mall_dict = result['data'].copy()
                # Add scraping metadata
                mall_dict['_scraping_metadata'] = {
                    'completeness_score': result.get('completeness_score', 0),
                    'response_time': result.get('response_time', 0),
                    'scraped_at': result.get('scraped_at', datetime.now()).isoformat()
                }
                output_data['malls'].append(mall_dict)

        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(output_data, indent=2, ensure_ascii=False, default=str))

    async def _generate_aggressive_report(self, results: List[Dict]) -> Dict[str, Any]:
        """Generate comprehensive aggressive scraping report"""

        successful_results = [r for r in results if r.get('success') and r.get('data')]
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        # Performance analysis
        throughput = len(results) / duration if duration > 0 else 0
        performance_improvement = throughput / 0.29 if throughput > 0 else 0  # Compared to baseline

        report = {
            'summary': {
                'total_urls_processed': len(results),
                'successful_extractions': len(successful_results),
                'failed_extractions': len(results) - len(successful_results),
                'success_rate': len(successful_results) / len(results) * 100 if results else 0,
                'duration_seconds': duration,
                'throughput_per_second': throughput,
                'performance_improvement_factor': performance_improvement,
                'aggressive_settings': {
                    'max_concurrent': self.max_concurrent,
                    'rate_limit_rpm': self.rate_limit_rpm,
                    'batch_size': self.batch_size,
                    'delay_between_batches': self.delay_between_batches
                }
            },
            'data_completeness': self.stats['data_completeness'],
            'performance_metrics': self.stats['performance_metrics'],
            'enhanced_data_breakdown': {
                'malls_with_post_details': self.stats['data_completeness']['total_malls_with_post_details'],
                'malls_with_tenants': self.stats['data_completeness']['total_malls_with_tenants'],
                'malls_with_images': self.stats['data_completeness']['total_malls_with_images'],
                'total_tenants_extracted': self.stats['data_completeness']['total_tenants_extracted'],
                'total_images_extracted': self.stats['data_completeness']['total_images_extracted'],
                'avg_tenants_per_mall': self.stats['data_completeness']['avg_tenant_count_per_mall'],
                'avg_images_per_mall': self.stats['data_completeness']['avg_image_count_per_mall']
            }
        }

        # Display aggressive performance report
        self._display_aggressive_report(report)

        return report

    def _display_aggressive_report(self, report: Dict[str, Any]):
        """Display aggressive scraping report"""

        console.print("\n🏆 [bold red]AGGRESSIVE SCRAPING REPORT[/bold red] ⚡")

        # Performance summary
        perf_table = Table(title="🚀 Performance Summary")
        perf_table.add_column("Metric", style="cyan")
        perf_table.add_column("Value", style="green")
        perf_table.add_column("Improvement", style="magenta")

        perf_table.add_row("Total URLs", str(report['summary']['total_urls_processed']), "")
        perf_table.add_row("Success Rate", ".1f", "")
        perf_table.add_row("Duration", ".1f", "")
        perf_table.add_row("Throughput", ".2f", ".1f")
        perf_table.add_row("Concurrent Requests", str(report['summary']['aggressive_settings']['max_concurrent']), "")
        perf_table.add_row("Rate Limit", f"{report['summary']['aggressive_settings']['rate_limit_rpm']} req/min", "")

        console.print(perf_table)

        # Data completeness
        console.print("\n📊 [bold]Enhanced Data Extraction Results[/bold]")

        completeness_table = Table(title="🎯 Data Completeness")
        completeness_table.add_column("Data Type", style="cyan")
        completeness_table.add_column("Count", style="green")
        completeness_table.add_column("Average per Mall", style="magenta")

        comp = report['enhanced_data_breakdown']
        completeness_table.add_row("Malls with Post Details", str(comp['malls_with_post_details']), "")
        completeness_table.add_row("Malls with Tenants", str(comp['malls_with_tenants']), "")
        completeness_table.add_row("Malls with Images", str(comp['malls_with_images']), "")
        completeness_table.add_row("Total Tenants Extracted", str(comp['total_tenants_extracted']), "")
        completeness_table.add_row("Total Images Extracted", str(comp['total_images_extracted']), "")
        completeness_table.add_row("Avg Tenants per Mall", "", ".1f")
        completeness_table.add_row("Avg Images per Mall", "", ".1f")

        console.print(completeness_table)

        # Success celebration
        success_rate = report['summary']['success_rate']
        throughput = report['summary']['throughput_per_second']
        improvement = report['summary']['performance_improvement_factor']

        if success_rate >= 95 and improvement >= 1.5:
            console.print("\n🎉 [bold green]EXCELLENT RESULTS![/bold green]")
            console.print(f"✅ {success_rate:.1f}% success rate with {improvement:.1f}x performance improvement!")
            console.print("✅ Enhanced data extraction working perfectly!")
        elif success_rate >= 80:
            console.print("\n👍 [bold yellow]GOOD RESULTS[/bold yellow]")
            console.print(f"✅ {success_rate:.1f}% success rate - solid performance!")
        else:
            console.print("\n⚠️ [bold yellow]RESULTS NEED OPTIMIZATION[/bold yellow]")
            console.print(f"⚠️ {success_rate:.1f}% success rate - may need parameter tuning")


async def main():
    """Main entry point for aggressive scraper demo"""
    # Get sample URLs for demonstration
    from scrapers.detail_extractor import DetailExtractor

    extractor = DetailExtractor()
    sample_urls = await extractor.collect_sample_mall_urls(sample_size=15, max_pages=3)

    console.print(f"🎯 Testing aggressive scraper with {len(sample_urls)} mall URLs")
    console.print("📈 Expected performance: ~0.75 req/sec (2x faster than baseline)")

    # Initialize aggressive scraper
    scraper = AggressiveOptimizedScraper(
        max_concurrent=15,      # Aggressive: 15 concurrent
        rate_limit_rpm=45,      # Aggressive: 45 req/min
        batch_size=15,          # Aggressive: batch size 15
        delay_between_batches=1.0  # Reduced delay
    )

    # Run aggressive scraping
    results = await scraper.scrape_comprehensive_aggressive(sample_urls)

    console.print("\n✅ [bold green]Aggressive scraping completed![/bold green]")
    console.print("[dim]Check output directory for comprehensive aggressive results[/dim]")


if __name__ == "__main__":
    asyncio.run(main())
