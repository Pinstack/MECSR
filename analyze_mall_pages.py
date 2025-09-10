#!/usr/bin/env python3
"""
Analyze Individual Mall Pages with Playwright MCP

This script uses playwright MCP tools to examine what data can be extracted
from individual mall pages that we're not currently capturing.
"""

import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Import our components
from scrapers.detail_extractor import DetailExtractor
from scrapers.pagination_crawler import PaginationCrawler

console = Console()

class MallPageAnalyzer:
    """Analyze individual mall pages for extractable data"""

    def __init__(self):
        self.crawler = PaginationCrawler()
        self.extractor = DetailExtractor()

    async def analyze_sample_malls(self, sample_size: int = 15) -> Dict[str, Any]:
        """
        Analyze a sample of mall pages to discover extractable data

        Args:
            sample_size: Number of malls to analyze

        Returns:
            Comprehensive analysis of extractable data
        """
        console.print("\n🔍 [bold blue]Mall Page Analysis with Playwright[/bold blue]")
        console.print(f"📊 Analyzing {sample_size} sample mall pages...")

        # Get sample URLs
        sample_urls = await self.extractor.collect_sample_mall_urls(
            sample_size=sample_size,
            max_pages=3
        )

        analysis_results = {
            'sample_size': len(sample_urls),
            'analyzed_urls': sample_urls,
            'data_discovery': {},
            'content_analysis': {},
            'structure_patterns': {},
            'extraction_opportunities': []
        }

        console.print("\n📄 Analyzing individual mall pages...")

        for i, url in enumerate(sample_urls, 1):
            console.print(f"\n  [{i}/{len(sample_urls)}] Analyzing: {url.split('/')[-1]}")

            try:
                # Get page content
                result = await self.crawler.crawl_single_page(url)

                if not result or not result.get('success'):
                    console.print(f"    ❌ Failed to fetch: {url}")
                    continue

                html = result['html']

                # Analyze page content
                page_analysis = await self._analyze_page_content(html, url)

                # Update discovery results
                self._update_discovery_results(analysis_results, page_analysis)

                console.print(f"    ✅ Found {len(page_analysis.get('extractable_elements', []))} data elements")

            except Exception as e:
                console.print(f"    ❌ Error analyzing {url}: {e}")

        # Generate comprehensive report
        return await self._generate_analysis_report(analysis_results)

    async def _analyze_page_content(self, html: str, url: str) -> Dict[str, Any]:
        """Analyze the content of a single mall page"""

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        analysis = {
            'url': url,
            'mall_name': url.split('/')[-1].replace('-', ' ').title(),
            'extractable_elements': [],
            'content_sections': [],
            'data_tables': [],
            'images': [],
            'links': [],
            'structured_data': {}
        }

        # Analyze different content sections
        analysis.update(self._analyze_basic_info(soup))
        analysis.update(self._analyze_property_details(soup))
        analysis.update(self._analyze_location_data(soup))
        analysis.update(self._analyze_media_content(soup))
        analysis.update(self._analyze_contact_info(soup))
        analysis.update(self._analyze_tenant_data(soup))
        analysis.update(self._analyze_structured_data(soup))

        return analysis

    def _analyze_basic_info(self, soup) -> Dict[str, Any]:
        """Analyze basic mall information"""
        basic_info = {
            'basic_info_elements': []
        }

        # Look for mall name variations
        name_selectors = [
            'h1.page-title', 'h1.entry-title', '.mall-name',
            '.property-title', '.mall-title', 'h1'
        ]

        for selector in name_selectors:
            elements = soup.select(selector)
            if elements:
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 3:
                        basic_info['basic_info_elements'].append({
                            'type': 'mall_name',
                            'selector': selector,
                            'text': text,
                            'tag': elem.name
                        })

        # Look for descriptions
        desc_selectors = [
            '.description', '.content', '.property-description',
            '.about', '.overview', 'article p', '.summary'
        ]

        for selector in desc_selectors:
            elements = soup.select(selector)
            if elements:
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 20:
                        basic_info['basic_info_elements'].append({
                            'type': 'description',
                            'selector': selector,
                            'text_length': len(text),
                            'tag': elem.name
                        })

        return basic_info

    def _analyze_property_details(self, soup) -> Dict[str, Any]:
        """Analyze property specification details"""
        property_details = {
            'property_elements': []
        }

        # Look for specification tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            table_data = []

            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)

                    table_data.append({
                        'key': key,
                        'value': value,
                        'table_index': i
                    })

                    # Check for property-related data
                    property_keywords = [
                        'gla', 'area', 'size', 'store', 'tenant',
                        'parking', 'floor', 'year', 'opened', 'built'
                    ]

                    if any(keyword in key for keyword in property_keywords):
                        property_details['property_elements'].append({
                            'type': 'property_spec',
                            'key': key,
                            'value': value,
                            'table_index': i
                        })

            if table_data:
                property_details['property_elements'].append({
                    'type': 'data_table',
                    'table_index': i,
                    'rows': len(table_data),
                    'sample_data': table_data[:3]
                })

        return property_details

    def _analyze_location_data(self, soup) -> Dict[str, Any]:
        """Analyze location and geographic data"""
        location_data = {
            'location_elements': []
        }

        # Look for address information
        address_selectors = [
            '.address', '.location', '.contact-address',
            '[data-address]', '.property-address'
        ]

        for selector in address_selectors:
            elements = soup.select(selector)
            if elements:
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text:
                        location_data['location_elements'].append({
                            'type': 'address',
                            'selector': selector,
                            'text': text
                        })

        # Look for map data or coordinates
        map_elements = soup.find_all(['div', 'iframe'], class_=lambda x: x and ('map' in str(x).lower() or 'google' in str(x).lower()))
        if map_elements:
            location_data['location_elements'].append({
                'type': 'map_embedded',
                'count': len(map_elements),
                'elements': [{'tag': elem.name, 'classes': elem.get('class', [])} for elem in map_elements[:3]]
            })

        # Look for coordinate data in scripts
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.get_text() if script.get_text() else ''
            if 'parseFloat' in script_text and ('lat' in script_text.lower() or 'lng' in script_text.lower()):
                location_data['location_elements'].append({
                    'type': 'coordinates_script',
                    'script_length': len(script_text)
                })

        return location_data

    def _analyze_media_content(self, soup) -> Dict[str, Any]:
        """Analyze images, videos, and other media"""
        media_content = {
            'media_elements': []
        }

        # Analyze images
        images = soup.find_all('img')
        property_images = []
        gallery_images = []
        logo_images = []

        for img in images:
            src = img.get('src', '')
            alt = img.get('alt', '').lower()
            classes = img.get('class', [])

            if src:
                img_info = {
                    'src': src if src.startswith('http') else f"https://www.mecsr.org{src}",
                    'alt': alt,
                    'classes': classes
                }

                # Categorize images
                if any(term in alt for term in ['mall', 'property', 'centre', 'center']):
                    property_images.append(img_info)
                elif any(cls and 'gallery' in cls for cls in classes):
                    gallery_images.append(img_info)
                elif any(term in alt for term in ['logo', 'brand']):
                    logo_images.append(img_info)

        if property_images:
            media_content['media_elements'].append({
                'type': 'property_images',
                'count': len(property_images),
                'samples': property_images[:3]
            })

        if gallery_images:
            media_content['media_elements'].append({
                'type': 'gallery_images',
                'count': len(gallery_images),
                'samples': gallery_images[:3]
            })

        # Look for video content
        videos = soup.find_all(['video', 'iframe'], src=lambda x: x and ('youtube' in x or 'vimeo' in x))
        if videos:
            media_content['media_elements'].append({
                'type': 'videos',
                'count': len(videos),
                'platforms': list(set(['youtube' if 'youtube' in v.get('src', '') else 'vimeo' if 'vimeo' in v.get('src', '') else 'other' for v in videos]))
            })

        return media_content

    def _analyze_contact_info(self, soup) -> Dict[str, Any]:
        """Analyze contact information"""
        contact_info = {
            'contact_elements': []
        }

        # Phone numbers
        phone_links = soup.find_all('a', href=lambda x: x and x.startswith('tel:'))
        if phone_links:
            phones = [link['href'].replace('tel:', '') for link in phone_links]
            contact_info['contact_elements'].append({
                'type': 'phone_numbers',
                'count': len(phones),
                'numbers': phones[:3]
            })

        # Email addresses
        email_links = soup.find_all('a', href=lambda x: x and x.startswith('mailto:'))
        if email_links:
            emails = [link['href'].replace('mailto:', '') for link in email_links]
            contact_info['contact_elements'].append({
                'type': 'email_addresses',
                'count': len(emails),
                'emails': emails[:3]
            })

        # Website links (external)
        website_links = soup.find_all('a', href=lambda x: x and x.startswith('http') and 'mecsr.org' not in x)
        if website_links:
            websites = list(set([link['href'] for link in website_links]))
            contact_info['contact_elements'].append({
                'type': 'external_websites',
                'count': len(websites),
                'urls': websites[:5]
            })

        return contact_info

    def _analyze_tenant_data(self, soup) -> Dict[str, Any]:
        """Analyze tenant and store information"""
        tenant_data = {
            'tenant_elements': []
        }

        # Look for tenant lists
        tenant_selectors = [
            '.tenants li', '.stores li', '.retailers li',
            '.tenant-list a', '.store-list a',
            '.brand-directory a', '.shop-directory a'
        ]

        all_tenants = []
        for selector in tenant_selectors:
            elements = soup.select(selector)
            for elem in elements:
                tenant_name = elem.get_text(strip=True)
                if tenant_name and len(tenant_name) > 2:
                    all_tenants.append({
                        'name': tenant_name,
                        'selector': selector,
                        'url': elem.get('href')
                    })

        if all_tenants:
            tenant_data['tenant_elements'].append({
                'type': 'tenant_list',
                'count': len(all_tenants),
                'selectors_found': list(set([t['selector'] for t in all_tenants])),
                'sample_tenants': all_tenants[:10]
            })

        # Look for store categories
        category_selectors = [
            '.categories li', '.store-categories a',
            '.retail-categories li', '.brand-categories a'
        ]

        categories = []
        for selector in category_selectors:
            elements = soup.select(selector)
            for elem in elements:
                cat_name = elem.get_text(strip=True)
                if cat_name:
                    categories.append(cat_name)

        if categories:
            tenant_data['tenant_elements'].append({
                'type': 'store_categories',
                'count': len(categories),
                'categories': list(set(categories))[:10]
            })

        return tenant_data

    def _analyze_structured_data(self, soup) -> Dict[str, Any]:
        """Analyze structured data (JSON-LD, microdata)"""
        structured_data = {
            'structured_elements': []
        }

        # JSON-LD structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for i, script in enumerate(json_scripts):
            try:
                import json
                data = json.loads(script.get_text())

                structured_data['structured_elements'].append({
                    'type': 'json_ld',
                    'index': i,
                    'data_type': data.get('@type') if isinstance(data, dict) else 'array',
                    'keys': list(data.keys()) if isinstance(data, dict) else [f'item_{j}' for j in range(len(data))] if isinstance(data, list) else []
                })

            except (json.JSONDecodeError, AttributeError):
                continue

        # Microdata
        microdata_elements = soup.find_all(attrs={'itemtype': True})
        if microdata_elements:
            structured_data['structured_elements'].append({
                'type': 'microdata',
                'count': len(microdata_elements),
                'itemtypes': list(set([elem['itemtype'] for elem in microdata_elements]))
            })

        return structured_data

    def _update_discovery_results(self, analysis_results: Dict[str, Any], page_analysis: Dict[str, Any]):
        """Update the overall discovery results with page analysis"""

        # Update content analysis
        for key, value in page_analysis.items():
            if key not in ['url', 'mall_name'] and isinstance(value, list):
                if key not in analysis_results['content_analysis']:
                    analysis_results['content_analysis'][key] = []

                analysis_results['content_analysis'][key].extend(value)

    async def _generate_analysis_report(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive analysis report"""

        # Summarize findings
        report = {
            'summary': {
                'total_malls_analyzed': analysis_results['sample_size'],
                'timestamp': datetime.now().isoformat(),
                'data_discovery_summary': {}
            },
            'detailed_findings': {},
            'extraction_recommendations': [],
            'implementation_priority': []
        }

        # Analyze content patterns
        content_analysis = analysis_results['content_analysis']

        # Basic information patterns
        if 'basic_info_elements' in content_analysis:
            basic_elements = content_analysis['basic_info_elements']
            mall_names = [e for e in basic_elements if e['type'] == 'mall_name']
            descriptions = [e for e in basic_elements if e['type'] == 'description']

            report['detailed_findings']['basic_info'] = {
                'mall_name_patterns': len(set([e['selector'] for e in mall_names])),
                'description_patterns': len(set([e['selector'] for e in descriptions])),
                'description_avg_length': sum([e.get('text_length', 0) for e in descriptions]) / len(descriptions) if descriptions else 0
            }

        # Property details
        if 'property_elements' in content_analysis:
            prop_elements = content_analysis['property_elements']
            data_tables = [e for e in prop_elements if e['type'] == 'data_table']
            specs = [e for e in prop_elements if e['type'] == 'property_spec']

            report['detailed_findings']['property_details'] = {
                'data_tables_found': len(data_tables),
                'property_specs_found': len(specs),
                'common_specs': list(set([e['key'] for e in specs])) if specs else []
            }

        # Location data
        if 'location_elements' in content_analysis:
            loc_elements = content_analysis['location_elements']
            addresses = [e for e in loc_elements if e['type'] == 'address']
            maps = [e for e in loc_elements if e['type'] == 'map_embedded']
            coords = [e for e in loc_elements if e['type'] == 'coordinates_script']

            report['detailed_findings']['location_data'] = {
                'address_patterns': len(addresses),
                'map_embeds': len(maps),
                'coordinate_scripts': len(coords)
            }

        # Media content
        if 'media_elements' in content_analysis:
            media_elements = content_analysis['media_elements']
            property_imgs = [e for e in media_elements if e['type'] == 'property_images']
            galleries = [e for e in media_elements if e['type'] == 'gallery_images']
            videos = [e for e in media_elements if e['type'] == 'videos']

            report['detailed_findings']['media_content'] = {
                'property_images_avg': sum([e['count'] for e in property_imgs]) / len(property_imgs) if property_imgs else 0,
                'galleries_found': len(galleries),
                'videos_found': len(videos)
            }

        # Contact information
        if 'contact_elements' in content_analysis:
            contact_elements = content_analysis['contact_elements']
            phones = [e for e in contact_elements if e['type'] == 'phone_numbers']
            emails = [e for e in contact_elements if e['type'] == 'email_addresses']
            websites = [e for e in contact_elements if e['type'] == 'external_websites']

            report['detailed_findings']['contact_info'] = {
                'phone_numbers_avg': sum([e['count'] for e in phones]) / len(phones) if phones else 0,
                'email_addresses_avg': sum([e['count'] for e in emails]) / len(emails) if emails else 0,
                'external_websites_avg': sum([e['count'] for e in websites]) / len(websites) if websites else 0
            }

        # Tenant data
        if 'tenant_elements' in content_analysis:
            tenant_elements = content_analysis['tenant_elements']
            tenant_lists = [e for e in tenant_elements if e['type'] == 'tenant_list']
            categories = [e for e in tenant_elements if e['type'] == 'store_categories']

            report['detailed_findings']['tenant_data'] = {
                'tenant_lists_found': len(tenant_lists),
                'avg_tenants_per_mall': sum([e['count'] for e in tenant_lists]) / len(tenant_lists) if tenant_lists else 0,
                'store_categories_found': len(categories)
            }

        # Generate recommendations
        report['extraction_recommendations'] = self._generate_recommendations(report['detailed_findings'])

        # Display results
        self._display_analysis_report(report)

        return report

    def _generate_recommendations(self, findings: Dict[str, Any]) -> List[str]:
        """Generate extraction recommendations based on findings"""
        recommendations = []

        if 'basic_info' in findings:
            if findings['basic_info']['description_patterns'] > 0:
                recommendations.append("✅ High priority: Extract detailed mall descriptions (found in multiple locations)")
            if findings['basic_info']['mall_name_patterns'] > 1:
                recommendations.append("✅ High priority: Use multiple selectors for mall name extraction")

        if 'property_details' in findings:
            if findings['property_details']['data_tables_found'] > 0:
                recommendations.append("✅ High priority: Parse property specification tables for GLA, stores, parking")
            if findings['property_details']['property_specs_found'] > 0:
                recommendations.append("✅ High priority: Extract structured property specifications")

        if 'location_data' in findings:
            if findings['location_data']['coordinate_scripts'] > 0:
                recommendations.append("✅ Medium priority: Extract coordinates from JavaScript/embedded maps")
            if findings['location_data']['address_patterns'] > 0:
                recommendations.append("✅ Medium priority: Extract detailed address information")

        if 'media_content' in findings:
            if findings['media_content']['property_images_avg'] > 0:
                recommendations.append("✅ Medium priority: Extract property images and photo galleries")
            if findings['media_content']['galleries_found'] > 0:
                recommendations.append("✅ Low priority: Extract image galleries for visual content")

        if 'contact_info' in findings:
            contact = findings['contact_info']
            if contact['phone_numbers_avg'] > 0 or contact['email_addresses_avg'] > 0:
                recommendations.append("✅ Medium priority: Extract comprehensive contact information")
            if contact['external_websites_avg'] > 0:
                recommendations.append("✅ High priority: Extract external mall websites")

        if 'tenant_data' in findings:
            if findings['tenant_data']['tenant_lists_found'] > 0:
                recommendations.append("✅ High priority: Extract tenant/store directory information")
            if findings['tenant_data']['store_categories_found'] > 0:
                recommendations.append("✅ Medium priority: Extract store categories and classifications")

        return recommendations

    def _display_analysis_report(self, report: Dict[str, Any]):
        """Display the analysis report in a formatted way"""

        console.print("\n📊 [bold]Mall Page Analysis Report[/bold]")
        console.print(f"📈 Analyzed {report['summary']['total_malls_analyzed']} mall pages")

        # Summary table
        table = Table(title="📋 Data Discovery Summary")
        table.add_column("Data Category", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Details", style="magenta")

        findings = report['detailed_findings']

        # Basic info
        if 'basic_info' in findings:
            basic = findings['basic_info']
            table.add_row("Basic Information", "✅ Found",
                         f"Names: {basic['mall_name_patterns']} patterns, Descriptions: {basic['description_avg_length']:.0f} avg chars")

        # Property details
        if 'property_details' in findings:
            prop = findings['property_details']
            table.add_row("Property Details", "✅ Found" if prop['data_tables_found'] > 0 else "❌ Missing",
                         f"Tables: {prop['data_tables_found']}, Specs: {len(prop['common_specs'])} types")

        # Location data
        if 'location_data' in findings:
            loc = findings['location_data']
            table.add_row("Location Data", "✅ Found" if loc['address_patterns'] > 0 else "⚠️ Limited",
                         f"Addresses: {loc['address_patterns']}, Maps: {loc['map_embeds']}, Coords: {loc['coordinate_scripts']}")

        # Media content
        if 'media_content' in findings:
            media = findings['media_content']
            table.add_row("Media Content", "✅ Found" if media['property_images_avg'] > 0 else "❌ Missing",
                         ".1f")

        # Contact info
        if 'contact_info' in findings:
            contact = findings['contact_info']
            has_contact = contact['phone_numbers_avg'] > 0 or contact['email_addresses_avg'] > 0
            table.add_row("Contact Info", "✅ Found" if has_contact else "❌ Missing",
                         ".1f")

        # Tenant data
        if 'tenant_data' in findings:
            tenant = findings['tenant_data']
            table.add_row("Tenant Data", "✅ Found" if tenant['tenant_lists_found'] > 0 else "❌ Missing",
                         ".1f")

        console.print(table)

        # Recommendations
        if report['extraction_recommendations']:
            console.print("\n🎯 [bold]Extraction Recommendations:[/bold]")
            for i, rec in enumerate(report['extraction_recommendations'], 1):
                console.print(f"{i}. {rec}")

        # Save detailed report
        output_file = f"output/mall_page_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        console.print(f"\n📄 Detailed report saved to: {output_file}")


async def main():
    """Main entry point for mall page analysis"""
    analyzer = MallPageAnalyzer()

    # Analyze sample mall pages
    results = await analyzer.analyze_sample_malls(sample_size=15)

    console.print("\n✅ [bold green]Mall page analysis completed![/bold green]")
    console.print("[dim]Enhanced scraper can now extract much more comprehensive data[/dim]")


if __name__ == "__main__":
    asyncio.run(main())
