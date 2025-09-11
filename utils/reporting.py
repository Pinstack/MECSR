"""Reporting utilities for MECSR scraper."""

from typing import Dict, Any, List
from datetime import datetime
from .helpers import format_percentage, format_throughput, calculate_success_rate


def generate_scraping_report(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate comprehensive scraping report."""
    total = len(results)
    successes = sum(1 for r in results if r.get('success', False))
    success_rate = calculate_success_rate(successes, total)

    # Calculate timing
    start_time = min(r.get('scraped_at') for r in results if r.get('scraped_at'))
    end_time = max(r.get('scraped_at') for r in results if r.get('scraped_at'))

    if start_time and end_time:
        try:
            start = datetime.fromisoformat(start_time)
            end = datetime.fromisoformat(end_time)
            duration_seconds = (end - start).total_seconds()
            throughput_per_second = total / duration_seconds if duration_seconds > 0 else 0
        except:
            duration_seconds = 0
            throughput_per_second = 0
    else:
        duration_seconds = 0
        throughput_per_second = 0

    return {
        'summary': {
            'total_urls_processed': total,
            'successful_scrapes': successes,
            'failed_scrapes': total - successes,
            'success_rate': success_rate,
            'duration_seconds': duration_seconds,
            'throughput_per_second': throughput_per_second
        },
        'timestamp': datetime.now().isoformat()
    }


def print_scraping_summary(results: List[Dict[str, Any]]) -> None:
    """Print human-readable scraping summary."""
    report = generate_scraping_report(results)

    print("\n📊 SCRAPING SUMMARY")
    print(f"Total processed: {report['summary']['total_urls_processed']}")
    print(f"Successful: {report['summary']['successful_scrapes']}")
    print(f"Failed: {report['summary']['failed_scrapes']}")
    print(f"Success rate: {format_percentage(report['summary']['success_rate'])}")
    print(f"Duration: {report['summary']['duration_seconds']:.1f}s")
    print(f"Throughput: {format_throughput(report['summary']['throughput_per_second'])}")


def print_progress(current: int, total: int, prefix: str = "Progress") -> None:
    """Print progress with percentage."""
    percentage = (current / total * 100) if total > 0 else 0
    print(f"\r{prefix}: {current}/{total} ({percentage:.1f}%)", end="", flush=True)


def print_batch_progress(batch_num: int, total_batches: int, batch_size: int) -> None:
    """Print batch processing progress."""
    print(f"📦 Processing batch {batch_num}/{total_batches} ({batch_size} items)")


def print_final_report(report: Dict[str, Any], output_file: str) -> None:
    """Print final completion report."""
    print("\n✅ SCRAPING COMPLETE!")
    print(f"📊 Total processed: {report['summary']['total_urls_processed']}")
    print(f"🎯 Success rate: {format_percentage(report['summary']['success_rate'])}")
    print(f"⚡ Throughput: {format_throughput(report['summary']['throughput_per_second'])}")
    print(f"💾 Results saved to: {output_file}")
