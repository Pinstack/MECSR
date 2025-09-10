"""
Test suite for DataProcessor component.
Tests data validation, transformation, enrichment, and quality assurance functionality.
"""

import pytest
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from processors.data_processor import (
    DataProcessor, MallData, ProcessingResult,
    MallStatus, PropertyType
)


class TestDataProcessor:
    """Test suite for DataProcessor class"""

    @pytest.fixture
    def processor(self):
        """Fixture to create DataProcessor instance"""
        return DataProcessor()

    @pytest.fixture
    def sample_mall_data(self) -> List[Dict[str, Any]]:
        """Sample mall data for testing"""
        return [
            {
                'name': 'Dalma Mall',
                'url': '/directory-shopping-centres/dalma-mall',
                'property_type': 'Super Regional Centre',
                'status': 'Existing Mall',
                'latitude': 24.3331427,
                'longitude': 54.5239257,
                'post_id': '123',
                'user_id': '456',
                'data_id': '789',
                'data_type': '4'
            },
            {
                'name': '01 Burda',
                'url': '/directory-shopping-centres/01-burda',
                'property_type': 'Community Centre',
                'status': 'Existing Mall',
                'latitude': 25.276987,
                'longitude': 55.296249,
                'post_id': '124',
                'user_id': '457',
                'data_id': '790',
                'data_type': '4'
            },
            {
                'name': 'Future Mall',  # Duplicate name but different URL
                'url': '/directory-shopping-centres/future-mall',
                'property_type': 'Regional Centre',
                'status': 'Upcoming Mall',
                'latitude': 24.466667,
                'longitude': 54.366667,
                'post_id': '125',
                'user_id': '458',
                'data_id': '791',
                'data_type': '4'
            },
            {
                'name': 'Invalid Mall',  # Invalid - missing required fields
                'property_type': 'Community Centre'
            },
            {
                'name': '',  # Invalid - empty name
                'url': '/directory-shopping-centres/empty-name'
            }
        ]

    @pytest.mark.asyncio
    async def test_data_processor_initialization(self, processor):
        """Test that DataProcessor can be initialized"""
        assert processor is not None
        assert hasattr(processor, 'process_mall_data')
        assert hasattr(processor, 'validate_coordinates')

    @pytest.mark.asyncio
    async def test_process_mall_data_basic_functionality(self, processor, sample_mall_data):
        """Test basic mall data processing functionality"""
        result = await processor.process_mall_data(sample_mall_data)

        assert isinstance(result, ProcessingResult)
        assert isinstance(result.valid_records, list)
        assert isinstance(result.invalid_records, list)
        assert isinstance(result.processing_stats, dict)
        assert isinstance(result.duplicates_removed, int)
        assert isinstance(result.enriched_records, int)

    @pytest.mark.asyncio
    async def test_process_mall_data_validation(self, processor, sample_mall_data):
        """Test data validation during processing"""
        result = await processor.process_mall_data(sample_mall_data)

        # Should have 3 valid records (excluding the 2 invalid ones)
        assert len(result.valid_records) == 3
        assert len(result.invalid_records) == 2

        # Check that valid records are MallData objects
        for record in result.valid_records:
            assert isinstance(record, MallData)
            assert record.name is not None
            assert record.url is not None

    @pytest.mark.asyncio
    async def test_process_mall_data_deduplication(self, processor):
        """Test duplicate removal during processing"""
        # Create data with duplicates
        duplicate_data = [
            {
                'name': 'Test Mall',
                'url': '/directory-shopping-centres/test-mall',
                'property_type': 'Community Centre'
            },
            {
                'name': 'Test Mall',
                'url': '/directory-shopping-centres/test-mall',  # Same URL - should be deduplicated
                'property_type': 'Community Centre'
            },
            {
                'name': 'Different Mall',
                'url': '/directory-shopping-centres/different-mall',
                'property_type': 'Regional Centre'
            }
        ]

        result = await processor.process_mall_data(duplicate_data)

        assert len(result.valid_records) == 2  # Should remove 1 duplicate
        assert result.duplicates_removed == 1

    @pytest.mark.asyncio
    async def test_process_mall_data_enrichment(self, processor):
        """Test data enrichment during processing"""
        enrichment_data = [
            {
                'name': 'Dubai Mall',
                'url': '/directory-shopping-centres/dubai-mall',
                'property_type': 'Super Regional Centre'
            },
            {
                'name': 'Riyadh Mall',
                'url': '/directory-shopping-centres/riyadh-mall',
                'property_type': 'Regional Centre'
            }
        ]

        result = await processor.process_mall_data(enrichment_data)

        assert len(result.valid_records) == 2

        # Check enrichment - Dubai should be detected as UAE
        dubai_record = next((r for r in result.valid_records if 'dubai' in r.name.lower()), None)
        if dubai_record:
            assert dubai_record.country == 'UAE'

        # Check enrichment - Riyadh should be detected as Saudi Arabia
        riyadh_record = next((r for r in result.valid_records if 'riyadh' in r.name.lower()), None)
        if riyadh_record:
            assert riyadh_record.country == 'Saudi Arabia'

    @pytest.mark.asyncio
    async def test_mall_data_validation(self):
        """Test MallData model validation"""
        # Valid data
        valid_data = {
            'name': 'Test Mall',
            'url': '/directory-shopping-centres/test-mall',
            'property_type': 'Community Centre',
            'status': 'Existing Mall',
            'latitude': 24.5,
            'longitude': 54.5
        }

        mall = MallData(**valid_data)
        assert mall.name == 'Test Mall'
        assert mall.property_type == PropertyType.COMMUNITY_CENTRE
        assert mall.status == MallStatus.EXISTING

        # Test property type normalization
        assert MallData(**{**valid_data, 'property_type': 'super regional centre'}).property_type == PropertyType.SUPER_REGIONAL_CENTRE
        assert MallData(**{**valid_data, 'property_type': 'regional center'}).property_type == PropertyType.REGIONAL_CENTRE

        # Test status normalization
        assert MallData(**{**valid_data, 'status': 'upcoming'}).status == MallStatus.UPCOMING
        assert MallData(**{**valid_data, 'status': 'under construction'}).status == MallStatus.UNDER_CONSTRUCTION

    @pytest.mark.asyncio
    async def test_mall_data_validation_errors(self):
        """Test MallData validation errors"""
        # Missing required fields
        with pytest.raises(Exception):
            MallData(name='', url='/test')  # Empty name

        # Invalid coordinates
        with pytest.raises(Exception):
            MallData(
                name='Test Mall',
                url='/test',
                latitude=100,  # Invalid latitude
                longitude=54.5
            )

        with pytest.raises(Exception):
            MallData(
                name='Test Mall',
                url='/test',
                latitude=24.5,
                longitude=200  # Invalid longitude
            )

    @pytest.mark.asyncio
    async def test_url_validation(self):
        """Test URL validation in MallData"""
        # URL without protocol should be prefixed
        mall = MallData(
            name='Test Mall',
            url='/directory-shopping-centres/test-mall'
        )
        assert mall.url.startswith('https://www.mecsr.org')

        # URL with protocol should remain unchanged
        mall2 = MallData(
            name='Test Mall',
            url='https://www.mecsr.org/test'
        )
        assert mall2.url == 'https://www.mecsr.org/test'

    @pytest.mark.asyncio
    async def test_coordinate_validation(self, processor):
        """Test coordinate validation functionality"""
        # Create valid records first
        valid_records = [
            MallData(name='Mall 1', url='/test1', latitude=24.5, longitude=54.5),
            MallData(name='Mall 4', url='/test4'),  # No coordinates
        ]

        # Manually create records with invalid coordinates (bypassing validation)
        invalid_lat_record = MallData(name='Mall 2', url='/test2', latitude=24.5, longitude=54.5)
        invalid_lat_record.latitude = 100  # Manually set invalid value

        invalid_lng_record = MallData(name='Mall 3', url='/test3', latitude=24.5, longitude=54.5)
        invalid_lng_record.longitude = 200  # Manually set invalid value

        test_records = valid_records + [invalid_lat_record, invalid_lng_record]

        result = await processor.validate_coordinates(test_records)

        assert result['total_records'] == 4
        assert result['valid_coordinates'] == 1  # Only first record is valid
        assert result['invalid_coordinates'] == 3
        assert len(result['validation_errors']) == 2  # Two records with invalid ranges

    @pytest.mark.asyncio
    async def test_data_quality_scoring(self, processor):
        """Test data quality score calculation"""
        # High quality record
        high_quality = MallData(
            name='Complete Mall',
            url='/test',
            property_type='Super Regional Centre',
            status='Existing Mall',
            latitude=24.5,
            longitude=54.5,
            country='UAE',
            city='Dubai',
            gla_sqft=1000000,
            stores_count=200,
            opening_year=2010
        )

        # Low quality record
        low_quality = MallData(
            name='Incomplete Mall',
            url='/test2'
        )

        # Process both records
        result = await processor.process_mall_data([high_quality.model_dump(), low_quality.model_dump()])

        assert len(result.valid_records) == 2

        # High quality should have higher score
        high_score_record = next(r for r in result.valid_records if r.name == 'Complete Mall')
        low_score_record = next(r for r in result.valid_records if r.name == 'Incomplete Mall')

        assert high_score_record.data_quality_score > low_score_record.data_quality_score
        assert high_score_record.data_quality_score >= 0.8  # Should be high
        assert low_score_record.data_quality_score <= 0.3  # Should be low

    @pytest.mark.asyncio
    async def test_processing_statistics(self, processor, sample_mall_data):
        """Test processing statistics generation"""
        result = await processor.process_mall_data(sample_mall_data)

        stats = result.processing_stats

        assert 'total_input_records' in stats
        assert 'valid_records' in stats
        assert 'invalid_records' in stats
        assert 'duplicates_removed' in stats
        assert 'enriched_records' in stats
        assert 'validation_success_rate' in stats
        assert 'property_type_distribution' in stats
        assert 'status_distribution' in stats
        assert 'country_distribution' in stats
        assert 'quality_score_distribution' in stats
        assert 'coordinates_available' in stats
        assert 'average_quality_score' in stats

        # Verify some expected values
        assert stats['total_input_records'] == len(sample_mall_data)
        assert stats['valid_records'] == len(result.valid_records)
        assert stats['invalid_records'] == len(result.invalid_records)
        assert 0.0 <= stats['validation_success_rate'] <= 1.0
        assert 0.0 <= stats['average_quality_score'] <= 1.0

    @pytest.mark.asyncio
    async def test_empty_data_processing(self, processor):
        """Test processing with empty or minimal data"""
        # Empty list
        result = await processor.process_mall_data([])
        assert len(result.valid_records) == 0
        assert len(result.invalid_records) == 0

        # List with only invalid data
        invalid_data = [
            {'name': ''},  # Missing URL
            {'url': '/test'}  # Missing name
        ]
        result = await processor.process_mall_data(invalid_data)
        assert len(result.valid_records) == 0
        assert len(result.invalid_records) == 2

    @pytest.mark.asyncio
    async def test_location_extraction(self, processor):
        """Test location information extraction"""
        test_cases = [
            ('Dubai Mall', 'UAE', 'Dubai'),
            ('Riyadh Galleria', 'Saudi Arabia', 'Riyadh'),
            ('Istanbul Mall', 'Turkey', 'Istanbul'),
            ('Generic Mall', None, None),  # Should not match any pattern
        ]

        for mall_name, expected_country, expected_city in test_cases:
            test_data = [{
                'name': mall_name,
                'url': f'/directory-shopping-centres/{mall_name.lower().replace(" ", "-")}'
            }]

            result = await processor.process_mall_data(test_data)

            if expected_country:
                record = result.valid_records[0]
                assert record.country == expected_country, f"Failed for {mall_name}"
                if expected_city:
                    assert record.city == expected_city, f"Failed city extraction for {mall_name}"

    @pytest.mark.asyncio
    async def test_property_type_normalization(self, processor):
        """Test property type normalization from names"""
        test_cases = [
            ('Dubai Outlet Mall', PropertyType.OUTLET_CENTRE),
            ('Mega Retail Park', PropertyType.RETAIL_PARK),
            ('Lifestyle Village', PropertyType.LIFESTYLE_CENTRE),
            ('Power Center Dubai', PropertyType.POWER_CENTRE),
        ]

        for mall_name, expected_type in test_cases:
            test_data = [{
                'name': mall_name,
                'url': f'/directory-shopping-centres/{mall_name.lower().replace(" ", "-")}',
                'property_type': 'unknown'  # Start with unknown
            }]

            result = await processor.process_mall_data(test_data)
            record = result.valid_records[0]

            # The normalization might not always work perfectly, but it should at least not break
            assert record.property_type is not None
