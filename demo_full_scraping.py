#!/usr/bin/env python3
"""
Demo: Full MECSR Directory Scraping with Optimized Performance

This script demonstrates how to scrape all 696+ mall URLs from the MECSR directory
using the optimized scraper with performance enhancements and protective measures.
"""

import asyncio
from optimized_scraper import OptimizedScraper
from datetime import datetime

async def demo_full_scraping():
    """Demo full MECSR directory scraping"""

    print("🏪 MECSR Full Directory Scraping Demo")
    print("=" * 50)

    # Configuration optimized for production use
    scraper = OptimizedScraper(
        max_concurrent=8,      # Balanced concurrency (not too aggressive)
        rate_limit_rpm=25,     # Respectful rate limiting
        batch_size=8          # Optimal batch size for processing
    )

    # Step 1: Collect all mall URLs from the directory
    print("\n📋 Step 1: Collecting all mall URLs...")
    from scrapers.detail_extractor import DetailExtractor

    extractor = DetailExtractor()
    all_mall_urls = await extractor.collect_all_mall_urls()

    print(f"✅ Found {len(all_mall_urls)} mall URLs total")

    # Step 2: Scrape comprehensive data from all malls
    print("
🚀 Step 2: Starting comprehensive scraping..."    print(f"Configuration: {scraper.max_concurrent} concurrent, {scraper.rate_limit_rpm} req/min")
    print(f"Expected completion time: ~{len(all_mall_urls) // scraper.rate_limit_rpm * 60 // 60} hours")

    # Generate timestamped output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"output/full_mecsr_scraping_{timestamp}.json"

    # Run the comprehensive scraping
    results = await scraper.scrape_comprehensive(
        all_mall_urls[:50],  # Limit to first 50 for demo (remove limit for full run)
        output_file=output_file
    )

    print(f"\n📊 Final Results:")
    print(f"  Total URLs processed: {results['summary']['total_urls_processed']}")
    print(f"  Success rate: {results['summary']['success_rate']:.1f}%")
    print(f"  Throughput: {results['summary']['throughput_per_second']:.2f} req/sec")
    print(f"  Output saved to: {output_file}")

    print("
📋 Data Completeness:"    completeness = results['data_completeness']
    for field, stats in completeness.items():
        print(".1f"
    print("
🎯 Quality Distribution:"    quality_dist = results['quality_scores']['distribution']
    for level, count in quality_dist.items():
        print(f"  {level}: {count}")

    print("
✅ Demo completed successfully!"    print("💡 For full production run, remove the [:50] limit and run overnight")

if __name__ == "__main__":
    asyncio.run(demo_full_scraping())
