"""
Test suite for DetailExtractor component.
Extracts individual mall data from MECSR directory HTML.
Follows TDD principles: write failing tests first, then implement minimal code to pass.
"""

import pytest
from typing import List, Dict, Optional
from unittest.mock import MagicMock, patch

from scrapers.detail_extractor import DetailExtractor


class TestDetailExtractor:
    """Test suite for DetailExtractor class"""

    @pytest.fixture
    def extractor(self):
        """Fixture to create DetailExtractor instance"""
        return DetailExtractor()

    @pytest.fixture
    def sample_mall_html(self):
        """Sample HTML containing mall listings for testing"""
        return """
        <div class="search_results">
            <div class="col-md-6 col-sm-12">
                <div class="well search-result-item">
                    <span class="postItem" data-userid="431" data-datatype="4" data-dataid="12" data-postid="111"></span>
                    <button class="item-post-list-111 favorite fa fa-heart"></button>

                    <div class="img_section col-sm-4 nopad sm-bmargin">
                        <div class="btn-sm bg-primary line-height-xl bold no-radius-bottom">
                            <span class="pull-left">Super Regional Centre</span>
                            <span class="pull-right badge">Existing Mall</span>
                        </div>
                        <div class="alert-secondary btn-block text-center">
                            <a href="/directory-shopping-centres/dalma-mall">
                                <img class="search_result_image center-block" alt="Dalma Mall" title="Dalma Mall" src="https://example.com/image.jpg">
                            </a>
                        </div>
                    </div>

                    <div class="mid_section xs-nopad col-sm-8">
                        <a class="h3 bold bmargin center-block" title="Dalma Mall" href="/directory-shopping-centres/dalma-mall">
                            Dalma Mall
                        </a>
                        <p>Dalma Mall is Abu Dhabi's iconic super-regional shopping destination...</p>
                        <a class="inline-block bold" title="Dalma Mall" href="/directory-shopping-centres/dalma-mall">
                            View More
                        </a>
                    </div>
                </div>
            </div>
        </div>
        """

    @pytest.mark.asyncio
    async def test_detail_extractor_initialization(self, extractor):
        """Test that DetailExtractor can be initialized"""
        assert extractor is not None
        assert hasattr(extractor, 'extract_mall_data')
        assert hasattr(extractor, 'extract_mall_links')

    @pytest.mark.asyncio
    async def test_extract_mall_links_method_exists(self, extractor):
        """Test that extract_mall_links method exists"""
        assert hasattr(extractor, 'extract_mall_links')
        assert callable(extractor.extract_mall_links)

    @pytest.mark.asyncio
    async def test_extract_mall_data_method_exists(self, extractor):
        """Test that extract_mall_data method exists"""
        assert hasattr(extractor, 'extract_mall_data')
        assert callable(extractor.extract_mall_data)

    @pytest.mark.asyncio
    async def test_extract_mall_links_from_html(self, extractor, sample_mall_html):
        """Test extracting mall links from HTML"""
        links = extractor.extract_mall_links(sample_mall_html)

        assert isinstance(links, list)
        assert len(links) > 0
        assert "/directory-shopping-centres/dalma-mall" in links

    @pytest.mark.asyncio
    async def test_extract_mall_data_from_html(self, extractor, sample_mall_html):
        """Test extracting complete mall data from HTML"""
        malls = extractor.extract_mall_data(sample_mall_html)

        assert isinstance(malls, list)
        assert len(malls) > 0

        # Check first mall data structure
        mall = malls[0]
        assert isinstance(mall, dict)
        assert "name" in mall
        assert "url" in mall
        assert "property_type" in mall
        assert "status" in mall

    @pytest.mark.asyncio
    async def test_extract_mall_name(self, extractor, sample_mall_html):
        """Test extracting mall name from HTML"""
        malls = extractor.extract_mall_data(sample_mall_html)

        # Should find Dalma Mall
        mall_names = [mall.get("name") for mall in malls]
        assert "Dalma Mall" in mall_names

    @pytest.mark.asyncio
    async def test_extract_property_type(self, extractor, sample_mall_html):
        """Test extracting property type from HTML"""
        malls = extractor.extract_mall_data(sample_mall_html)

        # Should find Super Regional Centre
        property_types = [mall.get("property_type") for mall in malls]
        assert "Super Regional Centre" in property_types

    @pytest.mark.asyncio
    async def test_extract_mall_status(self, extractor, sample_mall_html):
        """Test extracting mall status from HTML"""
        malls = extractor.extract_mall_data(sample_mall_html)

        # Should find Existing Mall
        statuses = [mall.get("status") for mall in malls]
        assert "Existing Mall" in statuses

    @pytest.mark.asyncio
    async def test_extract_mall_links_with_external_urls(self, extractor, sample_mall_html):
        """Test that extracted links are proper external URLs"""
        links = extractor.extract_mall_links(sample_mall_html)

        for link in links:
            assert link.startswith("/directory-shopping-centres/")
            assert len(link) > len("/directory-shopping-centres/")

    @pytest.mark.asyncio
    async def test_extract_mall_data_with_coordinates(self, extractor):
        """Test extracting coordinates from mall data"""
        # Mock HTML with coordinate data
        html_with_coords = """
        <div class="search_results">
            <div class="col-md-6 col-sm-12">
                <div class="well search-result-item">
                    <span class="postItem" data-userid="431" data-datatype="4" data-dataid="12" data-postid="111" data-lat="25.1234" data-lng="55.5678"></span>
                    <a href="/directory-shopping-centres/test-mall">Test Mall</a>
                </div>
            </div>
        </div>
        """

        malls = extractor.extract_mall_data(html_with_coords)

        assert len(malls) > 0
        mall = malls[0]
        assert "latitude" in mall or "lat" in mall
        assert "longitude" in mall or "lng" in mall

    @pytest.mark.asyncio
    async def test_extract_mall_data_empty_html(self, extractor):
        """Test handling empty HTML"""
        malls = extractor.extract_mall_data("")
        assert malls == []

        malls = extractor.extract_mall_data("<html></html>")
        assert malls == []

    @pytest.mark.asyncio
    async def test_extract_mall_links_empty_html(self, extractor):
        """Test handling empty HTML for links"""
        links = extractor.extract_mall_links("")
        assert links == []

        links = extractor.extract_mall_links("<html></html>")
        assert links == []

    @pytest.mark.asyncio
    async def test_extract_mall_data_multiple_malls(self, extractor):
        """Test extracting data from multiple mall listings"""
        multi_mall_html = """
        <div class="search_results">
            <div class="col-md-6 col-sm-12">
                <div class="well search-result-item">
                    <span class="postItem" data-userid="431" data-datatype="4" data-dataid="12" data-postid="111"></span>
                    <a class="h3 bold" href="/directory-shopping-centres/mall-1">Mall One</a>
                    <span class="pull-left">Regional Centre</span>
                    <span class="pull-right badge">Existing Mall</span>
                </div>
            </div>
            <div class="col-md-6 col-sm-12">
                <div class="well search-result-item">
                    <span class="postItem" data-userid="432" data-datatype="4" data-dataid="12" data-postid="112"></span>
                    <a class="h3 bold" href="/directory-shopping-centres/mall-2">Mall Two</a>
                    <span class="pull-left">Community Centre</span>
                    <span class="pull-right badge">Upcoming Mall</span>
                </div>
            </div>
        </div>
        """

        malls = extractor.extract_mall_data(multi_mall_html)

        assert len(malls) == 2

        # Check both malls
        mall_names = [mall.get("name") for mall in malls]
        assert "Mall One" in mall_names
        assert "Mall Two" in mall_names

        property_types = [mall.get("property_type") for mall in malls]
        assert "Regional Centre" in property_types
        assert "Community Centre" in property_types

        statuses = [mall.get("status") for mall in malls]
        assert "Existing Mall" in statuses
        assert "Upcoming Mall" in statuses

    @pytest.mark.asyncio
    async def test_extract_mall_data_with_malformed_html(self, extractor):
        """Test handling malformed HTML gracefully"""
        malformed_html = """
        <div class="search_results">
            <div class="well search-result-item">
                <span class="postItem" data-userid="431">
                <a href="/directory-shopping-centres/mall">Mall Name</a>
                <!-- Missing closing tags -->
        """

        # Should not crash and return what it can extract
        malls = extractor.extract_mall_data(malformed_html)
        assert isinstance(malls, list)  # Should return a list even with malformed HTML

    @pytest.mark.asyncio
    async def test_get_mall_coordinates_from_detail_page(self, extractor):
        """Test extracting coordinates from individual mall detail pages"""
        # This would typically require visiting the individual mall page
        # For now, test the method exists and handles coordinates
        assert hasattr(extractor, 'get_mall_coordinates')

        # Mock coordinate data
        mock_coords = {"lat": 25.1234, "lng": 55.5678}
        coords = await extractor.get_mall_coordinates("/directory-shopping-centres/test-mall")

        # Should return coordinate structure (may be None if not implemented yet)
        assert isinstance(coords, (dict, type(None)))

    @pytest.mark.asyncio
    async def test_coordinate_extraction_from_detail_page(self, extractor):
        """Test coordinate extraction from individual mall detail pages"""
        # Test with Dalma Mall (we know it has coordinates)
        mall_url = "/directory-shopping-centres/dalma-mall"
        coordinates = await extractor.get_mall_coordinates(mall_url)

        assert coordinates is not None
        assert isinstance(coordinates, dict)
        assert "latitude" in coordinates
        assert "longitude" in coordinates

        # Validate coordinate ranges and values
        lat = coordinates["latitude"]
        lng = coordinates["longitude"]

        assert isinstance(lat, (int, float))
        assert isinstance(lng, (int, float))
        assert -90 <= lat <= 90
        assert -180 <= lng <= 180

        # Dalma Mall should be in Abu Dhabi, UAE
        # Approximate coordinates: ~24.3N, ~54.5E
        assert 24.0 <= lat <= 25.0  # UAE latitude range
        assert 54.0 <= lng <= 55.0  # UAE longitude range

    @pytest.mark.asyncio
    async def test_coordinate_extraction_invalid_url(self, extractor):
        """Test coordinate extraction handles invalid URLs gracefully"""
        coordinates = await extractor.get_mall_coordinates("invalid-url")
        assert coordinates is None

    @pytest.mark.asyncio
    async def test_coordinate_extraction_nonexistent_mall(self, extractor):
        """Test coordinate extraction handles non-existent mall pages"""
        coordinates = await extractor.get_mall_coordinates("/directory-shopping-centres/nonexistent-mall")
        assert coordinates is None

    @pytest.mark.asyncio
    async def test_mall_data_structure_validation(self, extractor, sample_mall_html):
        """Test that extracted mall data has required fields"""
        malls = extractor.extract_mall_data(sample_mall_html)

        for mall in malls:
            # Required fields
            assert "name" in mall
            assert "url" in mall

            # Should be strings
            assert isinstance(mall.get("name"), str)
            assert isinstance(mall.get("url"), str)

            # Optional fields should be present if available
            if "property_type" in mall:
                assert isinstance(mall["property_type"], str)
            if "status" in mall:
                assert isinstance(mall["status"], str)

            # Coordinate fields should be numeric if present
            if "latitude" in mall:
                assert isinstance(mall["latitude"], (int, float))
                assert -90 <= mall["latitude"] <= 90
            if "longitude" in mall:
                assert isinstance(mall["longitude"], (int, float))
                assert -180 <= mall["longitude"] <= 180
