#!/usr/bin/env python3
"""
Enhanced Data Extractor with Complete Data Coverage

This module provides comprehensive data extraction from individual mall pages,
capturing all available data including Post Details, tenant lists, images, and more.
"""

import re
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup

from scrapers.detail_extractor import DetailExtractor


class EnhancedDataExtractor:
    """Enhanced extractor that captures all available data from mall pages"""

    def __init__(self):
        self.base_extractor = DetailExtractor()

    def extract_comprehensive_mall_data(self, html: str, url: str) -> Dict[str, Any]:
        """
        Extract comprehensive data from mall page HTML

        Args:
            html: Raw HTML content
            url: Mall URL

        Returns:
            Dictionary with all extractable data
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Start with basic extraction
        mall_data = {
            'url': url,
            'mall_name': self._extract_mall_name_enhanced(soup),
            'basic_info': self._extract_basic_info_enhanced(soup),
            'property_details': self._extract_property_details_comprehensive(soup),
            'location_data': self._extract_location_data_enhanced(soup),
            'contact_info': self._extract_contact_info_comprehensive(soup),
            'tenant_data': self._extract_tenant_data_comprehensive(soup),
            'media_content': self._extract_media_content_comprehensive(soup),
            'structured_data': self._extract_structured_data_comprehensive(soup),
            'metadata': self._extract_metadata_enhanced(soup)
        }

        return mall_data

    def _extract_mall_name_enhanced(self, soup: BeautifulSoup) -> Optional[str]:
        """Enhanced mall name extraction with multiple fallbacks"""
        # Primary: Look for main heading
        title_elem = soup.find('h1')
        if title_elem:
            title_text = title_elem.get_text(strip=True)
            # Clean up common suffixes
            title_text = re.sub(r'\s*-\s*Shopping Centre.*', '', title_text, flags=re.IGNORECASE)
            title_text = re.sub(r'\s*-\s*Retail Properties.*', '', title_text, flags=re.IGNORECASE)
            if len(title_text) > 3:
                return title_text

        # Secondary: Look for specific mall name patterns
        name_patterns = [
            r'Mall Size in SQM:\s*\d+.*?([A-Za-z\s]+?)(?:\s*-|\s*\|)',
            r'([A-Za-z\s]+?)(?:\s*-|\s*\|).*?Mall'
        ]

        for pattern in name_patterns:
            match = re.search(pattern, soup.get_text(), re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if len(name) > 3 and not any(word in name.lower() for word in ['posted', 'by', 'jefferson']):
                    return name

        return None

    def _extract_basic_info_enhanced(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract enhanced basic information"""
        basic_info = {}

        # Mall size from the header
        size_match = re.search(r'Mall Size in SQM:\s*([0-9,]+)', soup.get_text())
        if size_match:
            basic_info['mall_size_sqm'] = int(size_match.group(1).replace(',', ''))

        # Property type and status
        type_status_elem = soup.find('div', class_=lambda x: x and 'pull-left' in str(x))
        if type_status_elem:
            text = type_status_elem.get_text(strip=True)
            if 'Super Regional' in text:
                basic_info['property_type'] = 'Super Regional Centre'
            elif 'Regional' in text:
                basic_info['property_type'] = 'Regional Centre'
            elif 'Community' in text:
                basic_info['property_type'] = 'Community Centre'

        # Status
        status_elem = soup.find('span', class_=lambda x: x and 'badge' in str(x))
        if status_elem:
            basic_info['status'] = status_elem.get_text(strip=True)

        # Address
        address_elem = soup.find('div', class_=lambda x: x and 'post_location_map' in str(x))
        if address_elem:
            address_text = address_elem.get_text(strip=True)
            if len(address_text) > 10:
                basic_info['address'] = address_text

        return basic_info

    def _extract_property_details_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract comprehensive property details from Post Details section"""
        property_details = {}

        # Find the Post Details section
        post_details_section = None

        # Look for various possible selectors for the Post Details section
        selectors = [
            '[class*="post-details"]',
            '#post-details',
            '.post-details',
            'div[class*="detail"]',
            'section[class*="detail"]'
        ]

        for selector in selectors:
            post_details_section = soup.select_one(selector)
            if post_details_section:
                break

        if not post_details_section:
            # Fallback: look for any div containing "Property 360 View" or similar
            all_divs = soup.find_all('div')
            for div in all_divs:
                if 'Property 360 View' in div.get_text() or 'Type of Property' in div.get_text():
                    post_details_section = div
                    break

        if post_details_section:
            # Method 1: Extract using structured HTML parsing (handles expansion automatically)
            structured_details = self._extract_structured_table_data(post_details_section)
            property_details.update(structured_details)

            # Method 2: Fallback to regex patterns for any missed fields
            text_content = post_details_section.get_text()
            fallback_patterns = self._get_fallback_regex_patterns()

            for key, pattern in fallback_patterns.items():
                if key not in property_details:  # Only use fallback if not already captured
                    match = re.search(pattern, text_content, re.IGNORECASE | re.MULTILINE)
                    if match:
                        value = match.group(1).strip()
                        if value and not value.startswith('http'):  # Skip URLs for now
                            # Clean up numeric values
                            if key in ['mall_size_sqm', 'gla_sqm', 'levels', 'car_parks', 'retail_outlets', 'annual_footfall']:
                                value = re.sub(r'[^\d]', '', value)
                                if value.isdigit():
                                    value = int(value)
                            property_details[key] = value

        return property_details

    def _extract_structured_table_data(self, post_details_section) -> Dict[str, Any]:
        """
        Extract property details using structured HTML parsing.
        This method automatically handles table expansion by parsing the HTML structure directly.
        """
        structured_data = {}

        # Find all table-view-group divs (each represents a field)
        table_groups = post_details_section.find_all('div', class_=lambda x: x and 'table-view-group' in str(x))

        for group in table_groups:
            try:
                # Extract field name from the bold div
                field_name_div = group.find('div', class_=lambda x: x and 'bold' in str(x))
                if not field_name_div:
                    continue

                field_name = field_name_div.get_text(strip=True)

                # Extract field value from the span elements
                value_div = group.find('div', class_=lambda x: x and 'col-sm-8' in str(x))
                if not value_div:
                    continue

                # Get the actual value from various span types
                value = None

                # Try different value extraction methods based on HTML structure
                if value_div.find('a', href=True):
                    # Handle URLs (like Property 360 View Link)
                    link = value_div.find('a', href=True)
                    if link:
                        value = link.get('href')
                        if value and not value.startswith('http'):
                            value = f"https://www.mecsr.org{value}"
                elif value_div.find('span'):
                    # Handle span values
                    span = value_div.find('span')
                    if span:
                        value = span.get_text(strip=True)
                        # Handle special cases
                        if 'select' in str(span.get('class', [])):
                            value = span.get_text(strip=True)
                        elif 'number' in str(span.get('class', [])):
                            # Clean numeric values
                            numeric_value = re.sub(r'[^\d]', '', value)
                            if numeric_value.isdigit():
                                value = int(numeric_value)
                        elif 'textbox' in str(span.get('class', [])):
                            value = span.get_text(strip=True)

                if value and field_name:
                    # Convert field name to snake_case key
                    key = self._field_name_to_key(field_name)
                    structured_data[key] = value

            except Exception as e:
                # Skip problematic groups and continue
                continue

        return structured_data

    def _field_name_to_key(self, field_name: str) -> str:
        """Convert human-readable field names to snake_case keys"""
        # Remove icons and special characters
        clean_name = re.sub(r'<[^>]+>', '', field_name)  # Remove HTML tags
        clean_name = re.sub(r'[^\w\s]', '', clean_name)  # Remove special chars
        clean_name = clean_name.strip()

        # Handle specific field name mappings
        field_mappings = {
            'Property 360 View Link': 'property_360_link',
            'Type of Property': 'type_of_property',
            'Mall Size in SQM': 'mall_size_sqm',
            'GLA in SQM': 'gla_sqm',
            'No of Level': 'levels',
            'No of Car Parks': 'car_parks',
            'No Retail Outlets': 'retail_outlets',
            'Annual Footfall EstimatedActual': 'annual_footfall',
            'Year Built': 'year_built',
            'AnchorNotable Tenants': 'anchor_tenants',
            'Owner Company Name': 'owner_company',
            'Managing Agent Company Name': 'managing_agent',
            'Leasing Agent Company Name': 'leasing_agent',
            'Main Contractor': 'main_contractor',
            'Retail Solutions Provider Service Provider Footfall Retail Analytics Technology Others': 'retail_solutions_provider'
        }

        # Try exact match first
        if clean_name in field_mappings:
            return field_mappings[clean_name]

        # Fallback to automatic conversion
        key = clean_name.lower().replace(' ', '_')
        key = re.sub(r'_+', '_', key)  # Remove multiple underscores
        key = key.strip('_')  # Remove leading/trailing underscores

        return key

    def _get_fallback_regex_patterns(self) -> Dict[str, str]:
        """Get fallback regex patterns for any fields missed by structured parsing"""
        return {
            'property_360_link': r'Property 360 View Link[:\s]*([^\n]+)',
            'type_of_property': r'Type of Property[:\s]*([^\n]+)',
            'mall_size_sqm': r'Mall Size in SQM[:\s]*([0-9,]+)',
            'gla_sqm': r'GLA in SQM[:\s]*([0-9,]+)',
            'levels': r'No\.?\s*of\s*Level[:\s]*([0-9]+)',
            'car_parks': r'No\.?\s*of\s*Car\s*Park[:\s]*([0-9,]+)',
            'retail_outlets': r'No\.?\s*Retail\s*Outlets?[:\s]*([0-9,]+)',
            'annual_footfall': r'Annual\s*Footfall[:\s]*([0-9,]+)',
            'year_built': r'Year\s*Built[:\s]*([^\n]+)',
            'owner_company': r'Owner\s*Company\s*Name[:\s]*([^\n]+)',
            'managing_agent': r'Managing\s*Agent[:\s]*([^\n]+)',
            'leasing_agent': r'Leasing\s*Agent[:\s]*([^\n]+)',
            'main_contractor': r'Main\s*Contractor[:\s]*([^\n]+)',
            'retail_solutions_provider': r'Retail\s*Solutions\s*Provider[:\s]*([^\n]+)'
        }

    def _extract_location_data_enhanced(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Enhanced location data extraction"""
        location_data = {}

        # Extract coordinates from JavaScript
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.get_text() if script.get_text() else ''
            lat_match = re.search(r'parseFloat\(([0-9.-]+)\)', script_text)
            lng_match = re.search(r'parseFloat\(([0-9.-]+)\)', script_text[script_text.find('parseFloat') + 1:] if 'parseFloat' in script_text else '')

            if lat_match and lng_match:
                try:
                    location_data['latitude'] = float(lat_match.group(1))
                    location_data['longitude'] = float(lng_match.group(1))
                    break
                except ValueError:
                    continue

        # Extract address components
        address_text = None
        address_elem = soup.find('div', class_=lambda x: x and 'post_location_map' in str(x))
        if address_elem:
            address_text = address_elem.get_text(strip=True)

        if address_text:
            location_data['full_address'] = address_text

            # Try to extract city and country
            if 'United Arab Emirates' in address_text:
                location_data['country'] = 'United Arab Emirates'
                # Extract city from common UAE cities
                uae_cities = ['Dubai', 'Abu Dhabi', 'Sharjah', 'Ajman', 'Ras Al Khaimah', 'Fujairah', 'Umm Al Quwain']
                for city in uae_cities:
                    if city in address_text:
                        location_data['city'] = city
                        break

        return location_data

    def _extract_contact_info_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Comprehensive contact information extraction"""
        contact_info = {}

        # Phone numbers
        phone_patterns = [
            r'tel:([+\d\s\-\(\)]+)',
            r'phone:?\s*([+\d\s\-\(\)]+)',
            r'\+[\d\s\-\(\)]{10,}'
        ]

        for pattern in phone_patterns:
            matches = re.findall(pattern, soup.get_text(), re.IGNORECASE)
            if matches:
                contact_info['phone_numbers'] = list(set(matches[:3]))  # Remove duplicates, limit to 3
                break

        # Email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, soup.get_text())
        if emails:
            contact_info['emails'] = list(set(emails[:3]))  # Remove duplicates, limit to 3

        # Website URLs
        website_links = soup.find_all('a', href=lambda x: x and x.startswith('http') and 'mecsr.org' not in x)
        websites = []
        for link in website_links:
            url = link.get('href')
            if url and len(url) > 10:  # Reasonable URL length
                websites.append(url)

        if websites:
            contact_info['external_websites'] = list(set(websites[:5]))  # Remove duplicates, limit to 5

        return contact_info

    def _extract_tenant_data_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Comprehensive tenant data extraction"""
        tenant_data = {}

        # Extract tenant links (the main tenant directory)
        tenant_links = soup.find_all('a', href=lambda x: x and '?q=' in str(x))
        tenants = []

        for link in tenant_links:
            tenant_name = link.get_text(strip=True)
            search_url = link.get('href')

            if tenant_name and len(tenant_name) > 2 and search_url:
                tenants.append({
                    'name': tenant_name,
                    'search_url': search_url,
                    'category': self._categorize_tenant(tenant_name)
                })

        if tenants:
            tenant_data['tenants'] = tenants
            tenant_data['total_tenant_count'] = len(tenants)

            # Group by category
            categories = {}
            for tenant in tenants:
                cat = tenant['category']
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(tenant['name'])

            tenant_data['tenant_categories'] = categories

        return tenant_data

    def _extract_media_content_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Comprehensive media content extraction"""
        media_content = {}

        # Extract images
        images = []
        img_elements = soup.find_all('img')

        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            alt = img.get('alt', '')

            if src and not src.startswith('data:'):  # Skip data URLs
                # Convert relative URLs to absolute
                if not src.startswith('http'):
                    src = f"https://www.mecsr.org{src}"

                # Skip logos and icons
                if not any(skip_word in alt.lower() or skip_word in src.lower()
                          for skip_word in ['logo', 'icon', 'banner', 'button']):
                    images.append({
                        'url': src,
                        'alt': alt,
                        'width': img.get('width'),
                        'height': img.get('height'),
                        'type': 'property_image'
                    })

        if images:
            media_content['images'] = images
            media_content['image_count'] = len(images)

        # Look for video content
        videos = soup.find_all(['video', 'iframe'], src=lambda x: x and ('youtube' in str(x).lower() or 'vimeo' in str(x).lower()))
        if videos:
            media_content['videos'] = [{
                'url': video.get('src'),
                'type': 'youtube' if 'youtube' in str(video).lower() else 'vimeo'
            } for video in videos[:3]]

        return media_content

    def _extract_structured_data_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract structured data (JSON-LD, microdata)"""
        structured_data = {}

        # JSON-LD structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        if json_scripts:
            structured_data['json_ld_count'] = len(json_scripts)
            structured_data['json_ld_data'] = []

            for script in json_scripts:
                try:
                    import json
                    data = json.loads(script.get_text())
                    structured_data['json_ld_data'].append(data)
                except:
                    continue

        # Microdata
        microdata_elements = soup.find_all(attrs={'itemtype': True})
        if microdata_elements:
            structured_data['microdata_count'] = len(microdata_elements)
            structured_data['microdata_types'] = list(set([elem['itemtype'] for elem in microdata_elements]))

        return structured_data

    def _extract_metadata_enhanced(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract enhanced metadata"""
        metadata = {}

        # Extract from meta tags
        meta_description = soup.find('meta', attrs={'name': 'description'})
        if meta_description:
            metadata['meta_description'] = meta_description.get('content')

        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            metadata['meta_keywords'] = meta_keywords.get('content', '').split(',')

        # Page title
        if soup.title:
            metadata['page_title'] = soup.title.get_text(strip=True)

        # Open Graph data
        og_title = soup.find('meta', attrs={'property': 'og:title'})
        if og_title:
            metadata['og_title'] = og_title.get('content')

        og_description = soup.find('meta', attrs={'property': 'og:description'})
        if og_description:
            metadata['og_description'] = og_description.get('content')

        og_image = soup.find('meta', attrs={'property': 'og:image'})
        if og_image:
            metadata['og_image'] = og_image.get('content')

        return metadata

    def _categorize_tenant(self, tenant_name: str) -> str:
        """Categorize tenant by type based on name"""
        name_lower = tenant_name.lower()

        categories = {
            'fashion': ['h&m', 'zara', 'uniqlo', 'gap', 'forever21', 'mango', 'bershka', 'stradivarius', 'pull&bear', 'massimo dutti', 'oysho', 'lefties', 'giordano', 'gant', 'r&b', 'american eagle', 'mothercare', 'monsoon', 'loccitane', 'sephora', 'mac', 'kiko milano', 'the body shop', 'victoria secret', 'bath&body works', 'nayomi', 'aldo', 'mini bounce'],
            'food': ['starbucks', 'tim hortons', 'caffe nero', 'costa', 'third avenue', 'dip n dip', 'india palace', 'gazebo', 'la brioche', 'coffee club', 'galito', 'chilli', 'nandos', 'bursa kebap evi', 'shake shack', 'barbeque nation', 'mcdonalds', 'burger king', 'pizza hut', 'kfc', 'sushi library', 'villa beirut'],
            'hypermarket': ['carrefour'],
            'department_store': ['centrepoint', 'riva', 'red tag', 'matalan', 'max', 'twenty4'],
            'home_improvement': ['home centre', '2xl home', 'pan home', 'chattles & more'],
            'entertainment': ['adventure zone', 'zeal entertainment centre', 'fun city', 'royal cinemas', 'e-max'],
            'electronics': ['sharaf dg', 'jumbo'],
            'books': ['borders'],
            'sports': ['fitness first', 'sun & sand sports', 'nike', 'adidas', 'reebok', 'under armour', 'puma', 'skechers'],
            'pharmacy': ['life pharmacy', 'watsons'],
            'services': ['yas clinic', 'united furniture']
        }

        for category, keywords in categories.items():
            if any(keyword in name_lower for keyword in keywords):
                return category

        return 'other'

    def calculate_data_completeness_score(self, mall_data: Dict[str, Any]) -> float:
        """Calculate data completeness score"""
        score = 0.0
        total_weight = 0.0

        # Basic information (high weight)
        if mall_data.get('mall_name'):
            score += 1.0
        total_weight += 1.0

        if mall_data.get('basic_info', {}).get('mall_size_sqm'):
            score += 0.8
        total_weight += 0.8

        # Property details (high weight)
        property_details = mall_data.get('property_details', {})
        if property_details.get('gla_sqm'):
            score += 0.9
        total_weight += 0.9

        if property_details.get('type_of_property'):
            score += 0.7
        total_weight += 0.7

        # Location data (medium weight)
        location_data = mall_data.get('location_data', {})
        if location_data.get('latitude') and location_data.get('longitude'):
            score += 0.8
        total_weight += 0.8

        # Contact info (medium weight)
        contact_info = mall_data.get('contact_info', {})
        if contact_info.get('external_websites'):
            score += 0.6
        total_weight += 0.6

        # Tenant data (medium weight)
        tenant_data = mall_data.get('tenant_data', {})
        if tenant_data.get('tenants'):
            tenant_count = len(tenant_data['tenants'])
            score += min(tenant_count / 50, 1.0) * 0.7  # Scale by tenant count
        total_weight += 0.7

        # Media content (lower weight)
        media_content = mall_data.get('media_content', {})
        if media_content.get('images'):
            score += 0.4
        total_weight += 0.4

        return score / total_weight if total_weight > 0 else 0.0

    def extract_streamlined_mall_data(self, html: str, url: str) -> Dict[str, Any]:
        """
        Extract only essential mall data in streamlined format.
        Produces clean JSON with only the requested fields.
        """
        soup = BeautifulSoup(html, 'lxml')

        # Extract core fields
        mall_data = {
            'url': url,
            'name': self._extract_mall_name_enhanced(soup),
            'external_url': self._extract_filtered_external_url(soup),
            'property_details': self._extract_property_details_comprehensive(soup),
            'location': self._extract_location_for_streamlined(soup),
            'tenants': self._extract_clean_tenant_data(soup),
            'first_image': self._extract_first_image_url(soup)
        }

        # Add parsed status fields
        mall_type, development_status = self._parse_status_fields(soup)
        mall_data['mall_type'] = mall_type
        mall_data['development_status'] = development_status

        # Add total tenants count
        mall_data['total_tenants'] = len(mall_data['tenants']) if mall_data['tenants'] else 0

        return mall_data

    def _parse_status_fields(self, soup: BeautifulSoup) -> tuple[str, str]:
        """Parse status field into mall_type and development_status"""
        mall_type = None
        development_status = None

        # Get the combined status string
        status_elem = soup.find('span', class_=lambda x: x and 'badge' in str(x))
        if status_elem:
            status_text = status_elem.get_text(strip=True)

            # Split on the separator
            if ' - ' in status_text:
                parts = status_text.split(' - ')
                if len(parts) == 2:
                    mall_type = parts[0].strip()
                    development_status = parts[1].strip()
            else:
                # Fallback: try to extract from property type
                type_elem = soup.find('div', class_=lambda x: x and 'pull-left' in str(x))
                if type_elem:
                    type_text = type_elem.get_text(strip=True)
                    if 'Super Regional' in type_text:
                        mall_type = 'Super Regional Centre'
                    elif 'Regional' in type_text:
                        mall_type = 'Regional Centre'
                    elif 'Community' in type_text:
                        mall_type = 'Community Centre'

                # Assume existing if we can't determine
                if not development_status:
                    development_status = 'Existing Mall'

        return mall_type, development_status

    def _extract_filtered_external_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract only the mall's official website, filter out social media and Google"""
        # Look for links in contact sections or with specific attributes
        website_links = soup.find_all('a', href=lambda x: x and x.startswith('http') and 'mecsr.org' not in x)

        candidates = []
        for link in website_links:
            url = link.get('href')
            if url:
                # Skip social media and Google services
                skip_domains = [
                    'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com',
                    'youtube.com', 'google.com', 'maps.google.com', 'x.com',
                    'ik.imagekit.io', 'imagekit.io'  # Skip image hosting services
                ]

                # Skip if it's in the skip domains
                if any(domain in url.lower() for domain in skip_domains):
                    continue

                # Skip URLs that are likely images or files
                if any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp']):
                    continue

                # Skip very long URLs (likely tracking/redirect URLs)
                if len(url) > 300:
                    continue

                # Prioritize URLs that look like official websites
                link_text = link.get_text(strip=True).lower()
                parent_text = link.parent.get_text(strip=True).lower() if link.parent else ""

                # Look for website-related keywords in link text or parent
                website_keywords = ['website', 'official', 'visit', 'www.', '.com', '.net', '.org']

                if any(keyword in link_text or keyword in parent_text or keyword in url.lower()
                       for keyword in website_keywords):
                    return url  # High priority match

                # Collect other potential website URLs
                candidates.append(url)

        # Return first candidate if no high-priority match
        return candidates[0] if candidates else None

    def _extract_location_for_streamlined(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract location data for streamlined format"""
        location_data = {}

        # Extract coordinates from JavaScript
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.get_text() if script.get_text() else ''
            lat_match = re.search(r'parseFloat\(([0-9.-]+)\)', script_text)
            lng_match = re.search(r'parseFloat\(([0-9.-]+)\)', script_text[script_text.find('parseFloat') + 1:] if 'parseFloat' in script_text else '')

            if lat_match and lng_match:
                try:
                    location_data['latitude'] = float(lat_match.group(1))
                    location_data['longitude'] = float(lng_match.group(1))
                    break
                except ValueError:
                    continue

        # Extract address
        address_elem = soup.find('div', class_=lambda x: x and 'post_location_map' in str(x))
        if address_elem:
            address_text = address_elem.get_text(strip=True)
            if len(address_text) > 10:
                location_data['address'] = address_text

        return location_data

    def _extract_clean_tenant_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract tenant data with only name and category (no search URLs)"""
        tenant_data = []

        # Extract tenant links (the main tenant directory)
        tenant_links = soup.find_all('a', href=lambda x: x and '?q=' in str(x))
        tenants = []

        for link in tenant_links:
            tenant_name = link.get_text(strip=True)
            search_url = link.get('href')

            if tenant_name and len(tenant_name) > 2 and search_url:
                tenants.append({
                    'name': tenant_name,
                    'category': self._categorize_tenant(tenant_name)
                })

        return tenants

    def _extract_first_image_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract only the first property image URL"""
        img_elements = soup.find_all('img')

        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            alt = img.get('alt', '')

            if src and not src.startswith('data:'):
                # Convert relative URLs to absolute
                if not src.startswith('http'):
                    src = f"https://www.mecsr.org{src}"

                # Skip logos and icons
                if not any(skip_word in alt.lower() or skip_word in src.lower()
                          for skip_word in ['logo', 'icon', 'banner', 'button']):
                    return src

        return None
