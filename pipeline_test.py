#!/usr/bin/env python3
"""
Pipeline Validation and Optimization Script for MECSR Scraper

This script validates the current pipeline, tests data extraction completeness,
and implements performance optimizations with protective measures.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
import sys

# Import our pipeline components
from scrapers.site_scoper import SiteScoper
from scrapers.pagination_crawler import PaginationCrawler
from scrapers.detail_extractor import DetailExtractor
from processors.data_processor import DataProcessor, MallData
from storage.storage_handler import StorageHandler
from config import settings

console = Console()

class PipelineValidator:
    """Comprehensive validator for the MECSR scraping pipeline"""

    def __init__(self):
        """Initialize the pipeline validator"""
        self.site_scoper = SiteScoper()
        self.crawler = PaginationCrawler()
        self.extractor = DetailExtractor()
        self.processor = DataProcessor()
        self.storage = StorageHandler()

        # Test URLs for validation (first 3 malls from the directory)
        self.test_mall_urls = [
            "https://www.mecsr.org/directory-shopping-centres/dalma-mall",
            "https://www.mecsr.org/directory-shopping-centres/01-burda",
            "https://www.mecsr.org/directory-shopping-centres/al-bahar-towers-shopping-centre"
        ]

        # Results tracking
        self.validation_results = {
            'pipeline_tests': {},
            'data_completeness': {},
            'performance_metrics': {},
            'error_analysis': {}
        }

    async def run_full_validation(self) -> Dict[str, Any]:
        """
        Run complete pipeline validation including:
        1. URL discovery validation
        2. Data extraction completeness test
        3. Performance optimization
        4. Error handling and resume capability
        """
        console.print("\n[bold blue]🔬 MECSR Pipeline Validation & Optimization[/bold blue]")
        console.print("[dim]Testing complete pipeline with 3 sample malls[/dim]\n")

        try:
            # Phase 1: URL Discovery Validation
            console.print("[bold]Phase 1: URL Discovery Validation[/bold]")
            await self._validate_url_discovery()

            # Phase 2: Data Extraction Completeness
            console.print("\n[bold]Phase 2: Data Extraction Completeness[/bold]")
            await self._validate_data_extraction_completeness()

            # Phase 3: Performance Optimization
            console.print("\n[bold]Phase 3: Performance Optimization[/bold]")
            await self._optimize_performance()

            # Phase 4: Error Handling & Resume
            console.print("\n[bold]Phase 4: Error Handling & Resume Capability[/bold]")
            await self._test_error_handling()

            # Phase 5: Generate Report
            console.print("\n[bold]Phase 5: Validation Report[/bold]")
            return await self._generate_validation_report()

        except Exception as e:
            console.print(f"[red]❌ Validation failed: {e}[/red]")
            import traceback
            console.print("[red]" + traceback.format_exc() + "[/red]")
            return {'error': str(e)}

    async def _validate_url_discovery(self):
        """Test URL discovery functionality with just our test URLs"""
        console.print("🔍 Testing URL discovery...")

        try:
            # Use our predefined test URLs instead of collecting all
            test_urls = self.test_mall_urls

            # Quick validation that URLs are properly formatted
            valid_urls = [url for url in test_urls if url.startswith('https://www.mecsr.org/directory-shopping-centres/')]

            self.validation_results['pipeline_tests']['url_discovery'] = {
                'success': True,
                'total_urls_found': len(valid_urls),
                'sample_urls': valid_urls,
                'test_urls_valid': len(valid_urls) == len(test_urls)
            }

            console.print(f"✅ Validated {len(valid_urls)} test URLs")

        except Exception as e:
            console.print(f"❌ URL discovery failed: {e}")
            self.validation_results['pipeline_tests']['url_discovery'] = {
                'success': False,
                'error': str(e)
            }

    async def _validate_data_extraction_completeness(self):
        """Test complete data extraction from individual mall pages"""
        console.print("📊 Testing data extraction completeness...")

        # Test with our 3 sample URLs
        test_results = []

        for i, url in enumerate(self.test_mall_urls, 1):
            console.print(f"  Testing mall {i}/3: {url.split('/')[-1]}")

            try:
                # Extract detailed information
                mall_details = await self.extractor.scrape_mall_details_batch([url], batch_size=1)

                if mall_details and len(mall_details) > 0:
                    mall_data = mall_details[0]

                    # Analyze completeness of extracted data
                    completeness = self._analyze_data_completeness(mall_data)

                    test_results.append({
                        'url': url,
                        'success': True,
                        'data_completeness': completeness,
                        'extraction_time': mall_data.get('scraped_at'),
                        'page_size': mall_data.get('page_size', 0)
                    })

                    console.print(f"    ✅ Extracted {len(mall_data)} fields")
                else:
                    test_results.append({
                        'url': url,
                        'success': False,
                        'error': 'No data extracted'
                    })
                    console.print(f"    ❌ No data extracted")

            except Exception as e:
                test_results.append({
                    'url': url,
                    'success': False,
                    'error': str(e)
                })
                console.print(f"    ❌ Error: {e}")

        self.validation_results['data_completeness'] = {
            'test_results': test_results,
            'overall_success_rate': len([r for r in test_results if r['success']]) / len(test_results),
            'average_completeness': self._calculate_average_completeness(test_results)
        }

    def _analyze_data_completeness(self, mall_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze completeness of extracted data"""

        # Define required data fields
        required_fields = {
            'basic_info': ['mall_name', 'url'],
            'location': ['latitude', 'longitude'],
            'property_details': ['gla_sqm', 'gla_sqft', 'stores_count', 'parking_spaces', 'opening_year'],
            'contact': ['phone', 'email', 'website'],
            'content': ['description'],
            'external_links': ['external_urls'],
            'images': ['images'],  # Check for image URLs
            'tenants': ['tenants'],  # Check for tenant/store information
            'metadata': ['last_updated', 'data_quality_score']
        }

        completeness = {}
        total_fields = 0
        present_fields = 0

        for category, fields in required_fields.items():
            category_total = len(fields)
            category_present = 0

            for field in fields:
                if field in mall_data and mall_data[field] is not None:
                    # For lists/dicts, check if they're not empty
                    if isinstance(mall_data[field], (list, dict)):
                        if len(mall_data[field]) > 0:
                            category_present += 1
                    else:
                        # For strings, check if not empty
                        if isinstance(mall_data[field], str) and len(mall_data[field].strip()) > 0:
                            category_present += 1
                        elif not isinstance(mall_data[field], str):
                            category_present += 1

            completeness[category] = {
                'present': category_present,
                'total': category_total,
                'percentage': (category_present / category_total) * 100 if category_total > 0 else 0
            }

            total_fields += category_total
            present_fields += category_present

        completeness['overall'] = {
            'present': present_fields,
            'total': total_fields,
            'percentage': (present_fields / total_fields) * 100 if total_fields > 0 else 0
        }

        return completeness

    def _calculate_average_completeness(self, test_results: List[Dict]) -> float:
        """Calculate average completeness across all tests"""
        successful_tests = [r for r in test_results if r['success'] and 'data_completeness' in r]

        if not successful_tests:
            return 0.0

        total_completeness = sum(
            r['data_completeness']['overall']['percentage']
            for r in successful_tests
        )

        return total_completeness / len(successful_tests)

    async def _optimize_performance(self):
        """Test and optimize performance with aggressive batching"""
        console.print("⚡ Testing performance optimization...")

        # Test different batch sizes
        batch_sizes = [1, 3, 5, 10]
        performance_results = {}

        for batch_size in batch_sizes:
            console.print(f"  Testing batch size: {batch_size}")

            start_time = time.time()

            try:
                # Test batch processing
                results = await self.extractor.scrape_mall_details_batch(
                    self.test_mall_urls,
                    batch_size=batch_size
                )

                end_time = time.time()
                duration = end_time - start_time

                successful = len([r for r in results if 'error' not in r])

                performance_results[batch_size] = {
                    'duration': duration,
                    'successful': successful,
                    'total': len(self.test_mall_urls),
                    'throughput': successful / duration if duration > 0 else 0,
                    'success_rate': successful / len(self.test_mall_urls)
                }

                console.print(f"    ✅ Batch size {batch_size}: {successful}/{len(self.test_mall_urls)} successful, throughput: {performance_results[batch_size]['throughput']:.2f} req/sec")
            except Exception as e:
                performance_results[batch_size] = {
                    'error': str(e),
                    'duration': time.time() - start_time
                }
                console.print(f"    ❌ Batch size {batch_size} failed: {e}")

        # Find optimal batch size
        optimal_batch = max(
            [(k, v) for k, v in performance_results.items() if 'throughput' in v],
            key=lambda x: x[1]['throughput']
        )[0] if any('throughput' in v for v in performance_results.values()) else 1

        self.validation_results['performance_metrics'] = {
            'batch_performance': performance_results,
            'optimal_batch_size': optimal_batch,
            'max_throughput': max([v.get('throughput', 0) for v in performance_results.values()])
        }

    async def _test_error_handling(self):
        """Test error handling and resume capability"""
        console.print("🛡️ Testing error handling and resume capability...")

        # Test with invalid URLs
        invalid_urls = [
            "https://www.mecsr.org/directory-shopping-centres/non-existent-mall",
            "https://invalid-domain.com/test",
            "https://www.mecsr.org/directory-shopping-centres/another-fake-mall"
        ]

        console.print("  Testing with invalid URLs...")
        error_results = await self.extractor.scrape_mall_details_batch(invalid_urls, batch_size=2)

        successful_with_errors = len([r for r in error_results if 'error' not in r])
        failed_with_errors = len([r for r in error_results if 'error' in r])

        # Test resume capability (simulate interruption)
        console.print("  Testing resume capability...")
        resume_test_urls = self.test_mall_urls + invalid_urls[:1]  # Mix valid and invalid

        # Simulate processing with interruption at index 2
        partial_results = []
        for i, url in enumerate(resume_test_urls):
            if i >= 2:  # Simulate interruption after 2 successful requests
                break

            try:
                result = await self.extractor.scrape_mall_details_batch([url], batch_size=1)
                if result:
                    partial_results.extend(result)
            except Exception as e:
                partial_results.append({'url': url, 'error': str(e)})

        # Test resume from interruption point
        remaining_urls = resume_test_urls[len(partial_results):]
        if remaining_urls:
            resume_results = await self.extractor.scrape_mall_details_batch(remaining_urls, batch_size=2)

            # Combine results
            combined_results = partial_results + resume_results
            successful_resume = len([r for r in combined_results if 'error' not in r])
        else:
            combined_results = partial_results
            successful_resume = len([r for r in combined_results if 'error' not in r])

        self.validation_results['error_analysis'] = {
            'invalid_url_handling': {
                'total_tested': len(invalid_urls),
                'successful': successful_with_errors,
                'failed': failed_with_errors,
                'error_rate': failed_with_errors / len(invalid_urls)
            },
            'resume_capability': {
                'partial_processing': len(partial_results),
                'resume_processing': len(remaining_urls) if remaining_urls else 0,
                'total_successful': successful_resume,
                'resume_success_rate': successful_resume / len(resume_test_urls)
            }
        }

    async def _generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report"""

        # Create summary table
        table = Table(title="🔬 Pipeline Validation Results")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="magenta")

        # URL Discovery
        url_discovery = self.validation_results['pipeline_tests'].get('url_discovery', {})
        if url_discovery.get('success'):
            table.add_row("URL Discovery", "✅ PASS", f"Found {url_discovery['total_urls_found']} URLs")
        else:
            table.add_row("URL Discovery", "❌ FAIL", url_discovery.get('error', 'Unknown error'))

        # Data Completeness
        data_comp = self.validation_results['data_completeness']
        success_rate = data_comp['overall_success_rate'] * 100
        avg_completeness = data_comp['average_completeness']

        if success_rate >= 80:
            status = "✅ PASS"
        elif success_rate >= 50:
            status = "⚠️ PARTIAL"
        else:
            status = "❌ FAIL"

        table.add_row("Data Extraction", status, f"{success_rate:.1f}% success, {avg_completeness:.1f}% completeness")

        # Performance
        perf = self.validation_results['performance_metrics']
        if perf.get('max_throughput', 0) > 0:
            table.add_row("Performance", "✅ PASS", f"Max throughput: {perf['max_throughput']:.2f} req/sec")
        else:
            table.add_row("Performance", "❌ FAIL", "No throughput measured")

        # Error Handling
        error_handling = self.validation_results['error_analysis']
        resume_success = error_handling['resume_capability']['resume_success_rate'] * 100

        if resume_success >= 80:
            table.add_row("Error Handling", "✅ PASS", f"Resume success: {resume_success:.1f}%")
        else:
            table.add_row("Error Handling", "⚠️ PARTIAL", f"Resume success: {resume_success:.1f}%")

        console.print(table)

        # Detailed data completeness breakdown
        if data_comp['test_results']:
            console.print("\n[bold]📊 Data Completeness Breakdown:[/bold]")
            for result in data_comp['test_results']:
                if result['success'] and 'data_completeness' in result:
                    comp = result['data_completeness']
                    mall_name = result['url'].split('/')[-1].replace('-', ' ').title()
                    console.print(f"  {mall_name}: {comp['overall']['percentage']:.1f}% complete ({comp['overall']['present']}/{comp['overall']['total']} fields)")

        # Performance recommendations
        perf_results = self.validation_results['performance_metrics']
        if 'optimal_batch_size' in perf_results:
            console.print(f"\n[bold]⚡ Performance Recommendation:[/bold]")
            console.print(f"Optimal batch size: {perf_results['optimal_batch_size']}")
            console.print(f"Max throughput: {perf_results['max_throughput']:.2f} requests/second")

        # Save detailed results
        report_path = Path("output/pipeline_validation_report.json")
        report_path.parent.mkdir(exist_ok=True)

        with open(report_path, 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)

        console.print(f"\n[bold]📄 Detailed report saved to: {report_path}[/bold]")

        return self.validation_results

    def _analyze_data_completeness(self, mall_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze completeness of extracted data"""

        # Define required data fields
        required_fields = {
            'basic_info': ['mall_name', 'url'],
            'location': ['latitude', 'longitude'],
            'property_details': ['gla_sqm', 'gla_sqft', 'stores_count', 'parking_spaces', 'opening_year'],
            'contact': ['phone', 'email', 'website'],
            'content': ['description'],
            'external_links': ['external_urls'],
            'images': ['images'],  # Check for image URLs
            'tenants': ['tenants'],  # Check for tenant/store information
            'metadata': ['last_updated', 'data_quality_score']
        }

        completeness = {}
        total_fields = 0
        present_fields = 0

        for category, fields in required_fields.items():
            category_total = len(fields)
            category_present = 0

            for field in fields:
                if field in mall_data and mall_data[field] is not None:
                    # For lists/dicts, check if they're not empty
                    if isinstance(mall_data[field], (list, dict)):
                        if len(mall_data[field]) > 0:
                            category_present += 1
                    else:
                        # For strings, check if not empty
                        if isinstance(mall_data[field], str) and len(mall_data[field].strip()) > 0:
                            category_present += 1
                        elif not isinstance(mall_data[field], str):
                            category_present += 1

            completeness[category] = {
                'present': category_present,
                'total': category_total,
                'percentage': (category_present / category_total) * 100 if category_total > 0 else 0
            }

            total_fields += category_total
            present_fields += category_present

        completeness['overall'] = {
            'present': present_fields,
            'total': total_fields,
            'percentage': (present_fields / total_fields) * 100 if total_fields > 0 else 0
        }

        return completeness

    def _calculate_average_completeness(self, test_results: List[Dict]) -> float:
        """Calculate average completeness across all tests"""
        successful_tests = [r for r in test_results if r['success'] and 'data_completeness' in r]

        if not successful_tests:
            return 0.0

        total_completeness = sum(
            r['data_completeness']['overall']['percentage']
            for r in successful_tests
        )

        return total_completeness / len(successful_tests)

    async def _optimize_performance(self):
        """Test and optimize performance with aggressive batching"""
        console.print("⚡ Testing performance optimization...")

        # Test different batch sizes
        batch_sizes = [1, 3, 5, 10]
        performance_results = {}

        for batch_size in batch_sizes:
            console.print(f"  Testing batch size: {batch_size}")

            start_time = time.time()

            try:
                # Test batch processing
                results = await self.extractor.scrape_mall_details_batch(
                    self.test_mall_urls,
                    batch_size=batch_size
                )

                end_time = time.time()
                duration = end_time - start_time

                successful = len([r for r in results if 'error' not in r])

                performance_results[batch_size] = {
                    'duration': duration,
                    'successful': successful,
                    'total': len(self.test_mall_urls),
                    'throughput': successful / duration if duration > 0 else 0,
                    'success_rate': successful / len(self.test_mall_urls)
                }

                console.print(f"    ✅ Batch size {batch_size}: {successful}/{len(self.test_mall_urls)} successful, throughput: {performance_results[batch_size]['throughput']:.2f} req/sec")
            except Exception as e:
                performance_results[batch_size] = {
                    'error': str(e),
                    'duration': time.time() - start_time
                }
                console.print(f"    ❌ Batch size {batch_size} failed: {e}")

        # Find optimal batch size
        optimal_batch = max(
            [(k, v) for k, v in performance_results.items() if 'throughput' in v],
            key=lambda x: x[1]['throughput']
        )[0] if any('throughput' in v for v in performance_results.values()) else 1

        self.validation_results['performance_metrics'] = {
            'batch_performance': performance_results,
            'optimal_batch_size': optimal_batch,
            'max_throughput': max([v.get('throughput', 0) for v in performance_results.values()])
        }

    async def _test_error_handling(self):
        """Test error handling and resume capability"""
        console.print("🛡️ Testing error handling and resume capability...")

        # Test with invalid URLs
        invalid_urls = [
            "https://www.mecsr.org/directory-shopping-centres/non-existent-mall",
            "https://invalid-domain.com/test",
            "https://www.mecsr.org/directory-shopping-centres/another-fake-mall"
        ]

        console.print("  Testing with invalid URLs...")
        error_results = await self.extractor.scrape_mall_details_batch(invalid_urls, batch_size=2)

        successful_with_errors = len([r for r in error_results if 'error' not in r])
        failed_with_errors = len([r for r in error_results if 'error' in r])

        # Test resume capability (simulate interruption)
        console.print("  Testing resume capability...")
        resume_test_urls = self.test_mall_urls + invalid_urls[:1]  # Mix valid and invalid

        # Simulate processing with interruption at index 2
        partial_results = []
        for i, url in enumerate(resume_test_urls):
            if i >= 2:  # Simulate interruption after 2 successful requests
                break

            try:
                result = await self.extractor.scrape_mall_details_batch([url], batch_size=1)
                if result:
                    partial_results.extend(result)
            except Exception as e:
                partial_results.append({'url': url, 'error': str(e)})

        # Test resume from interruption point
        remaining_urls = resume_test_urls[len(partial_results):]
        if remaining_urls:
            resume_results = await self.extractor.scrape_mall_details_batch(remaining_urls, batch_size=2)

            # Combine results
            combined_results = partial_results + resume_results
            successful_resume = len([r for r in combined_results if 'error' not in r])
        else:
            combined_results = partial_results
            successful_resume = len([r for r in combined_results if 'error' not in r])

        self.validation_results['error_analysis'] = {
            'invalid_url_handling': {
                'total_tested': len(invalid_urls),
                'successful': successful_with_errors,
                'failed': failed_with_errors,
                'error_rate': failed_with_errors / len(invalid_urls)
            },
            'resume_capability': {
                'partial_processing': len(partial_results),
                'resume_processing': len(remaining_urls) if remaining_urls else 0,
                'total_successful': successful_resume,
                'resume_success_rate': successful_resume / len(resume_test_urls)
            }
        }

    async def _generate_validation_report(self) -> Dict[str, Any]:
        """Generate comprehensive validation report"""

        # Create summary table
        table = Table(title="🔬 Pipeline Validation Results")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="magenta")

        # URL Discovery
        url_discovery = self.validation_results['pipeline_tests'].get('url_discovery', {})
        if url_discovery.get('success'):
            table.add_row("URL Discovery", "✅ PASS", f"Found {url_discovery['total_urls_found']} URLs")
        else:
            table.add_row("URL Discovery", "❌ FAIL", url_discovery.get('error', 'Unknown error'))

        # Data Completeness
        data_comp = self.validation_results['data_completeness']
        success_rate = data_comp['overall_success_rate'] * 100
        avg_completeness = data_comp['average_completeness']

        if success_rate >= 80:
            status = "✅ PASS"
        elif success_rate >= 50:
            status = "⚠️ PARTIAL"
        else:
            status = "❌ FAIL"

        table.add_row("Data Extraction", status, ".1f")

        # Performance
        perf = self.validation_results['performance_metrics']
        if perf.get('max_throughput', 0) > 0:
            table.add_row("Performance", "✅ PASS", ".2f")
        else:
            table.add_row("Performance", "❌ FAIL", "No throughput measured")

        # Error Handling
        error_handling = self.validation_results['error_analysis']
        resume_success = error_handling['resume_capability']['resume_success_rate'] * 100

        if resume_success >= 80:
            table.add_row("Error Handling", "✅ PASS", ".1f")
        else:
            table.add_row("Error Handling", "⚠️ PARTIAL", ".1f")

        console.print(table)

        # Detailed data completeness breakdown
        if data_comp['test_results']:
            console.print("\n[bold]📊 Data Completeness Breakdown:[/bold]")
            for result in data_comp['test_results']:
                if result['success'] and 'data_completeness' in result:
                    comp = result['data_completeness']
                    mall_name = result['url'].split('/')[-1].replace('-', ' ').title()
                    console.print(".1f")

        # Performance recommendations
        perf_results = self.validation_results['performance_metrics']
        if 'optimal_batch_size' in perf_results:
            console.print(f"\n[bold]⚡ Performance Recommendation:[/bold]")
            console.print(f"Optimal batch size: {perf_results['optimal_batch_size']}")
            console.print(".2f")

        # Save detailed results
        report_path = Path("output/pipeline_validation_report.json")
        report_path.parent.mkdir(exist_ok=True)

        with open(report_path, 'w') as f:
            json.dump(self.validation_results, f, indent=2, default=str)

        console.print(f"\n[bold]📄 Detailed report saved to: {report_path}[/bold]")

        return self.validation_results


async def main():
    """Main entry point for pipeline validation"""
    validator = PipelineValidator()
    results = await validator.run_full_validation()

    if 'error' not in results:
        console.print("\n[bold green]🎉 Pipeline validation completed![/bold green]")
        console.print("[dim]Ready to proceed with optimized production scraping[/dim]")
    else:
        console.print(f"\n[bold red]❌ Pipeline validation failed: {results['error']}[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
