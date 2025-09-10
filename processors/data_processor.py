"""
DataProcessor component for MECSR directory scraping.
Handles data validation, transformation, enrichment, and quality assurance.
"""

from typing import List, Dict, Optional, Any, Union
import asyncio
import re
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, Field, validator


class MallStatus(str, Enum):
    """Enumeration of possible mall statuses"""
    EXISTING = "existing"
    UPCOMING = "upcoming"
    UNDER_CONSTRUCTION = "under_construction"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class PropertyType(str, Enum):
    """Enumeration of possible property types"""
    SUPER_REGIONAL_CENTRE = "super_regional_centre"
    REGIONAL_CENTRE = "regional_centre"
    COMMUNITY_CENTRE = "community_centre"
    NEIGHBOURHOOD_CENTRE = "neighbourhood_centre"
    RETAIL_PARK = "retail_park"
    OUTLET_CENTRE = "outlet_centre"
    LIFESTYLE_CENTRE = "lifestyle_centre"
    POWER_CENTRE = "power_centre"
    BULK_WAREHOUSE = "bulk_warehouse"
    UNKNOWN = "unknown"


class MallData(BaseModel):
    """Pydantic model for mall data validation"""
    name: str = Field(..., min_length=1, max_length=200)
    url: str = Field(..., min_length=1)
    property_type: Optional[str] = None
    status: Optional[str] = None
    latitude: Optional[float] = Field(None, ge=-90, le=90)
    longitude: Optional[float] = Field(None, ge=-180, le=180)
    post_id: Optional[str] = None
    user_id: Optional[str] = None
    data_id: Optional[str] = None
    data_type: Optional[str] = None

    # Additional enriched fields
    country: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    gla_sqft: Optional[int] = Field(None, ge=0)
    gla_sqm: Optional[int] = Field(None, ge=0)
    stores_count: Optional[int] = Field(None, ge=0)
    parking_spaces: Optional[int] = Field(None, ge=0)
    opening_year: Optional[int] = Field(None, ge=1800, le=datetime.now().year + 10)
    last_updated: Optional[datetime] = None
    data_quality_score: Optional[float] = Field(None, ge=0.0, le=1.0)

    @validator('property_type')
    def validate_property_type(cls, v):
        """Validate property type against known values"""
        if v is None:
            return v
        # Normalize common variations
        v = v.lower().strip()
        property_type_mapping = {
            'super regional centre': PropertyType.SUPER_REGIONAL_CENTRE,
            'super regional center': PropertyType.SUPER_REGIONAL_CENTRE,
            'super-regional centre': PropertyType.SUPER_REGIONAL_CENTRE,
            'super-regional center': PropertyType.SUPER_REGIONAL_CENTRE,
            'regional centre': PropertyType.REGIONAL_CENTRE,
            'regional center': PropertyType.REGIONAL_CENTRE,
            'community centre': PropertyType.COMMUNITY_CENTRE,
            'community center': PropertyType.COMMUNITY_CENTRE,
            'neighbourhood centre': PropertyType.NEIGHBOURHOOD_CENTRE,
            'neighborhood centre': PropertyType.NEIGHBOURHOOD_CENTRE,
            'retail park': PropertyType.RETAIL_PARK,
            'outlet centre': PropertyType.OUTLET_CENTRE,
            'outlet center': PropertyType.OUTLET_CENTRE,
            'lifestyle centre': PropertyType.LIFESTYLE_CENTRE,
            'lifestyle center': PropertyType.LIFESTYLE_CENTRE,
            'power centre': PropertyType.POWER_CENTRE,
            'power center': PropertyType.POWER_CENTRE,
            'bulk warehouse': PropertyType.BULK_WAREHOUSE,
        }
        return property_type_mapping.get(v, PropertyType.UNKNOWN)

    @validator('status')
    def validate_status(cls, v):
        """Validate status against known values"""
        if v is None:
            return v
        # Normalize common variations
        v = v.lower().strip()
        status_mapping = {
            'existing mall': MallStatus.EXISTING,
            'existing': MallStatus.EXISTING,
            'upcoming mall': MallStatus.UPCOMING,
            'upcoming': MallStatus.UPCOMING,
            'under construction': MallStatus.UNDER_CONSTRUCTION,
            'construction': MallStatus.UNDER_CONSTRUCTION,
            'closed': MallStatus.CLOSED,
            'temporarily closed': MallStatus.CLOSED,
        }
        return status_mapping.get(v, MallStatus.UNKNOWN)

    @validator('url')
    def validate_url(cls, v):
        """Validate URL format"""
        if not v:
            return v
        # Ensure URL has proper format
        if not v.startswith(('http://', 'https://')):
            v = f"https://www.mecsr.org{v}"
        return v


@dataclass
class ProcessingResult:
    """Data class for processing results"""
    valid_records: List[MallData]
    invalid_records: List[Dict[str, Any]]
    duplicates_removed: int
    enriched_records: int
    processing_stats: Dict[str, Any]


class DataProcessor:
    """Processor for validating, transforming, and enriching mall data"""

    def __init__(self):
        """Initialize the DataProcessor"""
        self.country_patterns = {
            'united arab emirates': 'UAE',
            'uae': 'UAE',
            'dubai': 'UAE',
            'abu dhabi': 'UAE',
            'sharjah': 'UAE',
            'ajman': 'UAE',
            'ras al khaimah': 'UAE',
            'fujairah': 'UAE',
            'umm al quwain': 'UAE',
            'saudi arabia': 'Saudi Arabia',
            'riyadh': 'Saudi Arabia',
            'jeddah': 'Saudi Arabia',
            'mecca': 'Saudi Arabia',
            'medina': 'Saudi Arabia',
            'dammam': 'Saudi Arabia',
            'khobar': 'Saudi Arabia',
            'taif': 'Saudi Arabia',
            'tabuk': 'Saudi Arabia',
            'buraidah': 'Saudi Arabia',
            'khamis mushait': 'Saudi Arabia',
            'al khobar': 'Saudi Arabia',
            'al qatif': 'Saudi Arabia',
            'yanbu': 'Saudi Arabia',
            'hail': 'Saudi Arabia',
            'najran': 'Saudi Arabia',
            'jizan': 'Saudi Arabia',
            'abha': 'Saudi Arabia',
            'arar': 'Saudi Arabia',
            'jizan': 'Saudi Arabia',
            'kuwait': 'Kuwait',
            'qatar': 'Qatar',
            'doha': 'Qatar',
            'bahrain': 'Bahrain',
            'oman': 'Oman',
            'muscat': 'Oman',
            'jordan': 'Jordan',
            'amman': 'Jordan',
            'lebanon': 'Lebanon',
            'beirut': 'Lebanon',
            'iraq': 'Iraq',
            'baghdad': 'Iraq',
            'turkey': 'Turkey',
            'istanbul': 'Turkey',
            'ankara': 'Turkey',
            'izmir': 'Turkey',
            'egypt': 'Egypt',
            'cairo': 'Egypt',
            'alexandria': 'Egypt',
        }

    async def process_mall_data(self, raw_data: List[Dict[str, Any]]) -> ProcessingResult:
        """
        Process raw mall data through validation, deduplication, and enrichment

        Args:
            raw_data: List of raw mall data dictionaries

        Returns:
            ProcessingResult with processed data and statistics
        """
        print(f"🔄 Processing {len(raw_data)} raw mall records...")

        # Step 1: Validate and convert to MallData objects
        valid_records = []
        invalid_records = []

        for record in raw_data:
            try:
                mall_data = MallData(**record)
                valid_records.append(mall_data)
            except Exception as e:
                invalid_records.append({
                    'original_data': record,
                    'error': str(e),
                    'error_type': type(e).__name__
                })

        print(f"✅ Validated {len(valid_records)} records, {len(invalid_records)} invalid")

        # Step 2: Remove duplicates
        original_count = len(valid_records)
        valid_records = self._remove_duplicates(valid_records)
        duplicates_removed = original_count - len(valid_records)

        print(f"🗑️ Removed {duplicates_removed} duplicate records")

        # Step 3: Enrich data
        enriched_count = 0
        for record in valid_records:
            if self._enrich_record(record):
                enriched_count += 1

        print(f"✨ Enriched {enriched_count} records with additional data")

        # Step 4: Calculate data quality scores
        for record in valid_records:
            record.data_quality_score = self._calculate_quality_score(record)

        # Step 5: Generate processing statistics
        stats = self._generate_processing_stats(valid_records, invalid_records, duplicates_removed, enriched_count)

        return ProcessingResult(
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicates_removed=duplicates_removed,
            enriched_records=enriched_count,
            processing_stats=stats
        )

    def _remove_duplicates(self, records: List[MallData]) -> List[MallData]:
        """Remove duplicate records based on URL"""
        seen_urls = set()
        unique_records = []

        for record in records:
            if record.url not in seen_urls:
                seen_urls.add(record.url)
                unique_records.append(record)

        return unique_records

    def _enrich_record(self, record: MallData) -> bool:
        """
        Enrich a single record with additional data

        Returns:
            True if record was enriched, False otherwise
        """
        enriched = False

        # Extract country and city from URL or name
        if not record.country:
            country, city = self._extract_location_info(record)
            if country:
                record.country = country
                enriched = True
            if city:
                record.city = city
                enriched = True

        # Normalize property type
        if record.property_type and record.property_type == PropertyType.UNKNOWN:
            normalized_type = self._normalize_property_type(record.name, record.property_type)
            if normalized_type != PropertyType.UNKNOWN:
                record.property_type = normalized_type
                enriched = True

        # Set last_updated timestamp
        if not record.last_updated:
            record.last_updated = datetime.now()
            enriched = True

        return enriched

    def _extract_location_info(self, record: MallData) -> tuple[Optional[str], Optional[str]]:
        """Extract country and city information from record"""
        text_to_search = f"{record.name} {record.url}".lower()

        # Look for country patterns
        for pattern, country in self.country_patterns.items():
            if pattern in text_to_search:
                # Try to extract city as well
                city = self._extract_city_from_text(text_to_search, country)
                return country, city

        return None, None

    def _extract_city_from_text(self, text: str, country: str) -> Optional[str]:
        """Extract city name from text based on country"""
        # Common city patterns by country
        city_patterns = {
            'UAE': ['dubai', 'abu dhabi', 'sharjah', 'ajman', 'ras al khaimah', 'fujairah', 'umm al quwain'],
            'Saudi Arabia': ['riyadh', 'jeddah', 'mecca', 'medina', 'dammam', 'khobar', 'taif', 'tabuk', 'buraidah', 'khamis mushait', 'al khobar', 'al qatif', 'yanbu', 'hail', 'najran', 'jizan', 'abha', 'arar'],
            'Turkey': ['istanbul', 'ankara', 'izmir', 'antalya', 'bursa', 'adana', 'gaziantep', 'konya', 'antalya', 'kayseri', 'mersin', 'eskişehir', 'denizli', 'samsun', 'sakarya', 'trabzon', 'erzurum', 'kastamonu'],
        }

        if country in city_patterns:
            for city in city_patterns[country]:
                if city in text:
                    return city.title()

        return None

    def _normalize_property_type(self, name: str, current_type: str) -> str:
        """Attempt to normalize property type based on name"""
        name_lower = name.lower()

        # Keywords that suggest property types
        type_keywords = {
            PropertyType.OUTLET_CENTRE: ['outlet', 'factory', 'discount'],
            PropertyType.RETAIL_PARK: ['retail park', 'park'],
            PropertyType.LIFESTYLE_CENTRE: ['lifestyle', 'village', 'town center'],
            PropertyType.POWER_CENTRE: ['power center', 'big box', 'category killers'],
            PropertyType.BULK_WAREHOUSE: ['warehouse', 'wholesale', 'bulk'],
            PropertyType.COMMUNITY_CENTRE: ['community', 'local', 'neighborhood'],
            PropertyType.REGIONAL_CENTRE: ['regional', 'major', 'central'],
            PropertyType.SUPER_REGIONAL_CENTRE: ['super regional', 'mega', 'super', 'large scale'],
        }

        for prop_type, keywords in type_keywords.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return prop_type

        return current_type

    def _calculate_quality_score(self, record: MallData) -> float:
        """Calculate data quality score (0.0 to 1.0)"""
        score = 0.0
        total_weight = 0.0

        # Required fields (high weight)
        if record.name:
            score += 1.0
        total_weight += 1.0

        if record.url:
            score += 1.0
        total_weight += 1.0

        # Important fields (medium weight)
        if record.property_type and record.property_type != PropertyType.UNKNOWN:
            score += 0.8
        total_weight += 0.8

        if record.status and record.status != MallStatus.UNKNOWN:
            score += 0.8
        total_weight += 0.8

        if record.latitude and record.longitude:
            score += 0.9
        total_weight += 0.9

        # Additional fields (lower weight)
        if record.country:
            score += 0.5
        total_weight += 0.5

        if record.city:
            score += 0.5
        total_weight += 0.5

        if record.gla_sqft or record.gla_sqm:
            score += 0.6
        total_weight += 0.6

        if record.stores_count:
            score += 0.4
        total_weight += 0.4

        if record.opening_year:
            score += 0.3
        total_weight += 0.3

        return score / total_weight if total_weight > 0 else 0.0

    def _generate_processing_stats(self, valid_records: List[MallData],
                                 invalid_records: List[Dict[str, Any]],
                                 duplicates_removed: int,
                                 enriched_count: int) -> Dict[str, Any]:
        """Generate comprehensive processing statistics"""
        stats = {
            'total_input_records': len(valid_records) + len(invalid_records) + duplicates_removed,
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'duplicates_removed': duplicates_removed,
            'enriched_records': enriched_count,
            'validation_success_rate': 0.0,
            'property_type_distribution': {},
            'status_distribution': {},
            'country_distribution': {},
            'quality_score_distribution': {
                'excellent': 0,  # >= 0.9
                'good': 0,       # >= 0.7
                'fair': 0,       # >= 0.5
                'poor': 0        # < 0.5
            },
            'coordinates_available': 0,
            'average_quality_score': 0.0,
        }

        if stats['total_input_records'] > 0:
            stats['validation_success_rate'] = len(valid_records) / stats['total_input_records']

        # Analyze valid records
        total_quality_score = 0.0
        for record in valid_records:
            total_quality_score += record.data_quality_score or 0.0

            # Property type distribution
            prop_type = record.property_type or 'unknown'
            stats['property_type_distribution'][prop_type] = stats['property_type_distribution'].get(prop_type, 0) + 1

            # Status distribution
            status = record.status or 'unknown'
            stats['status_distribution'][status] = stats['status_distribution'].get(status, 0) + 1

            # Country distribution
            country = record.country or 'unknown'
            stats['country_distribution'][country] = stats['country_distribution'].get(country, 0) + 1

            # Coordinates
            if record.latitude and record.longitude:
                stats['coordinates_available'] += 1

            # Quality score distribution
            quality = record.data_quality_score or 0.0
            if quality >= 0.9:
                stats['quality_score_distribution']['excellent'] += 1
            elif quality >= 0.7:
                stats['quality_score_distribution']['good'] += 1
            elif quality >= 0.5:
                stats['quality_score_distribution']['fair'] += 1
            else:
                stats['quality_score_distribution']['poor'] += 1

        if valid_records:
            stats['average_quality_score'] = total_quality_score / len(valid_records)

        return stats

    async def validate_coordinates(self, records: List[MallData]) -> Dict[str, Any]:
        """
        Validate and enhance coordinate data

        Args:
            records: List of mall records to validate coordinates for

        Returns:
            Dictionary with validation results
        """
        results = {
            'total_records': len(records),
            'valid_coordinates': 0,
            'invalid_coordinates': 0,
            'coordinates_added': 0,
            'validation_errors': []
        }

        for record in records:
            if record.latitude and record.longitude:
                # Validate coordinate ranges
                if (-90 <= record.latitude <= 90 and -180 <= record.longitude <= 180):
                    results['valid_coordinates'] += 1
                else:
                    results['invalid_coordinates'] += 1
                    results['validation_errors'].append({
                        'name': record.name,
                        'url': record.url,
                        'latitude': record.latitude,
                        'longitude': record.longitude,
                        'error': 'Coordinates out of valid range'
                    })
            else:
                results['invalid_coordinates'] += 1

        return results
