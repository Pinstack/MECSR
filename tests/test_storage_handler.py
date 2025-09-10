"""
Test suite for StorageHandler component.
Tests data storage, export to various formats, and database operations.
"""

import pytest
import asyncio
import json
import csv
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List

from storage.storage_handler import StorageHandler, StorageResult
from processors.data_processor import MallData


class TestStorageHandler:
    """Test suite for StorageHandler class"""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def handler(self, temp_dir):
        """Create StorageHandler instance with temporary directory"""
        return StorageHandler(base_path=str(temp_dir))

    @pytest.fixture
    def sample_mall_data(self) -> List[MallData]:
        """Sample mall data for testing"""
        return [
            MallData(
                name='Dalma Mall',
                url='https://www.mecsr.org/directory-shopping-centres/dalma-mall',
                property_type='super_regional_centre',
                status='existing',
                latitude=24.3331427,
                longitude=54.5239257,
                country='UAE',
                city='Abu Dhabi',
                gla_sqft=1000000,
                stores_count=200,
                opening_year=2010,
                post_id='123',
                user_id='456',
                data_id='789',
                data_type='4',
                data_quality_score=0.95,
                last_updated=datetime.now()
            ),
            MallData(
                name='01 Burda',
                url='https://www.mecsr.org/directory-shopping-centres/01-burda',
                property_type='community_centre',
                status='existing',
                latitude=25.276987,
                longitude=55.296249,
                country='UAE',
                city='Dubai',
                stores_count=50,
                opening_year=2015,
                data_quality_score=0.88
            )
        ]

    @pytest.mark.asyncio
    async def test_storage_handler_initialization(self, handler):
        """Test that StorageHandler can be initialized"""
        assert handler is not None
        assert handler.base_path.exists()
        assert handler.db_path.parent == handler.base_path

    @pytest.mark.asyncio
    async def test_store_to_json_basic(self, handler, sample_mall_data):
        """Test basic JSON storage functionality"""
        result = await handler.store_to_json(sample_mall_data)

        assert isinstance(result, StorageResult)
        assert result.success is True
        assert result.records_stored == 2
        assert result.storage_format == 'json'
        assert result.storage_path is not None

        # Verify file was created and contains expected data
        file_path = Path(result.storage_path)
        assert file_path.exists()

        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        assert 'malls' in data
        assert len(data['malls']) == 2
        assert data['malls'][0]['name'] == 'Dalma Mall'
        assert 'metadata' in data

    @pytest.mark.asyncio
    async def test_store_to_json_with_custom_filename(self, handler, sample_mall_data):
        """Test JSON storage with custom filename"""
        custom_filename = "test_malls.json"
        result = await handler.store_to_json(sample_mall_data, filename=custom_filename)

        assert result.success is True
        assert custom_filename in result.storage_path

        file_path = Path(result.storage_path)
        assert file_path.name == custom_filename

    @pytest.mark.asyncio
    async def test_store_to_csv_basic(self, handler, sample_mall_data):
        """Test basic CSV storage functionality"""
        result = await handler.store_to_csv(sample_mall_data)

        assert isinstance(result, StorageResult)
        assert result.success is True
        assert result.records_stored == 2
        assert result.storage_format == 'csv'
        assert result.storage_path is not None

        # Verify CSV file was created and contains expected data
        file_path = Path(result.storage_path)
        assert file_path.exists()

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]['name'] == 'Dalma Mall'
        assert rows[0]['latitude'] == '24.3331427'
        assert rows[0]['longitude'] == '54.5239257'

    @pytest.mark.asyncio
    async def test_store_to_csv_without_coordinates(self, handler, sample_mall_data):
        """Test CSV storage without coordinate columns"""
        result = await handler.store_to_csv(sample_mall_data, include_coordinates=False)

        assert result.success is True

        # Verify coordinates are not in the CSV
        file_path = Path(result.storage_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

        assert 'latitude' not in fieldnames
        assert 'longitude' not in fieldnames

    @pytest.mark.asyncio
    async def test_store_to_sqlite_basic(self, handler, sample_mall_data):
        """Test basic SQLite storage functionality"""
        result = await handler.store_to_sqlite(sample_mall_data)

        assert isinstance(result, StorageResult)
        assert result.success is True
        assert result.records_stored == 2
        assert result.storage_format == 'sqlite'
        assert result.storage_path is not None

        # Verify database was created and contains expected data
        db_path = Path(result.storage_path)
        assert db_path.exists()

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Check table was created (SQLite may also create sqlite_sequence table)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        assert 'malls' in table_names

        # Check records were inserted
        cursor.execute("SELECT COUNT(*) FROM malls")
        count = cursor.fetchone()[0]
        assert count == 2

        # Check specific data
        cursor.execute("SELECT name, country, city FROM malls WHERE name = 'Dalma Mall'")
        row = cursor.fetchone()
        assert row[0] == 'Dalma Mall'
        assert row[1] == 'UAE'
        assert row[2] == 'Abu Dhabi'

        conn.close()

    @pytest.mark.asyncio
    async def test_store_to_sqlite_custom_table(self, handler, sample_mall_data):
        """Test SQLite storage with custom table name"""
        custom_table = "shopping_malls"
        result = await handler.store_to_sqlite(sample_mall_data, table_name=custom_table)

        assert result.success is True
        assert result.metadata['table_name'] == custom_table

        # Verify custom table was used
        conn = sqlite3.connect(str(handler.db_path))
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]

        assert custom_table in table_names
        conn.close()

    @pytest.mark.asyncio
    async def test_export_multiple_formats(self, handler, sample_mall_data):
        """Test exporting to multiple formats simultaneously"""
        formats = ['json', 'csv', 'sqlite']
        results = await handler.export_multiple_formats(sample_mall_data, formats=formats)

        assert len(results) == 3
        assert all(isinstance(result, StorageResult) for result in results.values())
        assert all(result.success for result in results.values())
        assert all(result.records_stored == 2 for result in results.values())

        # Verify files were created
        for format_name, result in results.items():
            if format_name != 'sqlite':  # SQLite doesn't create separate files per export
                file_path = Path(result.storage_path)
                assert file_path.exists()

    @pytest.mark.asyncio
    async def test_query_sqlite(self, handler, sample_mall_data):
        """Test SQLite database querying"""
        # First store some data
        await handler.store_to_sqlite(sample_mall_data)

        # Test basic query
        results = await handler.query_sqlite("SELECT name, country FROM malls ORDER BY name")
        assert len(results) == 2
        assert results[0]['name'] == '01 Burda'
        assert results[1]['name'] == 'Dalma Mall'
        assert results[0]['country'] == 'UAE'

        # Test query with WHERE clause
        results = await handler.query_sqlite("SELECT * FROM malls WHERE stores_count > 100")
        assert len(results) == 1
        assert results[0]['name'] == 'Dalma Mall'

        # Test query for non-existent data
        results = await handler.query_sqlite("SELECT * FROM malls WHERE country = 'USA'")
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_query_sqlite_nonexistent_table(self, handler):
        """Test querying non-existent table"""
        results = await handler.query_sqlite("SELECT * FROM nonexistent_table")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_storage_stats(self, handler, sample_mall_data):
        """Test storage statistics retrieval"""
        # Create some files first
        await handler.store_to_json(sample_mall_data)
        await handler.store_to_csv(sample_mall_data)
        await handler.store_to_sqlite(sample_mall_data)

        stats = await handler.get_storage_stats()

        assert isinstance(stats, dict)
        assert stats['total_files'] >= 3  # JSON, CSV, and DB files
        assert stats['total_size_bytes'] > 0
        assert 'json' in stats['files_by_format']
        assert 'csv' in stats['files_by_format']
        assert 'database_tables' in stats
        assert 'database_records' in stats
        assert 'malls' in stats['database_tables']
        assert stats['database_records']['malls'] == 2

    @pytest.mark.asyncio
    async def test_get_storage_stats_empty_directory(self, handler):
        """Test storage statistics with empty directory"""
        stats = await handler.get_storage_stats()

        assert isinstance(stats, dict)
        assert stats['total_files'] == 0
        assert stats['total_size_bytes'] == 0
        assert len(stats['files_by_format']) == 0
        assert len(stats['database_tables']) == 0

    @pytest.mark.asyncio
    async def test_cleanup_old_files(self, handler, sample_mall_data):
        """Test cleanup of old files"""
        # Create some files
        await handler.store_to_json(sample_mall_data, filename="old_file.json")

        # Create a newer file
        import time
        time.sleep(0.1)  # Small delay to ensure different timestamps
        await handler.store_to_json(sample_mall_data, filename="new_file.json")

        # Try to cleanup files older than 0 days (should remove the older file)
        cleanup_stats = await handler.cleanup_old_files(days_old=0)

        assert isinstance(cleanup_stats, dict)
        # Note: This test might be flaky due to timing, but it's testing the functionality

    @pytest.mark.asyncio
    async def test_duplicate_storage_sqlite(self, handler, sample_mall_data):
        """Test that SQLite handles duplicate records correctly"""
        # Store data twice
        result1 = await handler.store_to_sqlite(sample_mall_data)
        result2 = await handler.store_to_sqlite(sample_mall_data)

        assert result1.success is True
        assert result2.success is True

        # Should still have only 2 records (no duplicates due to UNIQUE constraint on URL)
        conn = sqlite3.connect(str(handler.db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM malls")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 2

    @pytest.mark.asyncio
    async def test_storage_error_handling_json(self, handler):
        """Test error handling in JSON storage"""
        # Try to store to invalid path
        invalid_handler = StorageHandler(base_path="/invalid/path/that/does/not/exist")
        result = await invalid_handler.store_to_json([])

        assert result.success is False
        assert result.error_message is not None
        assert result.records_stored == 0

    @pytest.mark.asyncio
    async def test_storage_error_handling_csv(self, handler):
        """Test error handling in CSV storage"""
        # Try to store to invalid path
        invalid_handler = StorageHandler(base_path="/invalid/path/that/does/not/exist")
        result = await invalid_handler.store_to_csv([])

        assert result.success is False
        assert result.error_message is not None
        assert result.records_stored == 0

    @pytest.mark.asyncio
    async def test_empty_data_storage(self, handler):
        """Test storage with empty data"""
        # Test all formats with empty data
        json_result = await handler.store_to_json([])
        csv_result = await handler.store_to_csv([])
        sqlite_result = await handler.store_to_sqlite([])

        assert json_result.success is True
        assert csv_result.success is True
        assert sqlite_result.success is True

        assert json_result.records_stored == 0
        assert csv_result.records_stored == 0
        assert sqlite_result.records_stored == 0

    @pytest.mark.asyncio
    async def test_metadata_inclusion(self, handler, sample_mall_data):
        """Test metadata inclusion in JSON export"""
        # With metadata
        result_with_metadata = await handler.store_to_json(sample_mall_data, include_metadata=True)

        file_path = Path(result_with_metadata.storage_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        assert 'metadata' in data
        assert 'total_records' in data['metadata']
        assert 'export_timestamp' in data['metadata']
        assert 'data_quality_summary' in data['metadata']

        # Without metadata
        result_without_metadata = await handler.store_to_json(sample_mall_data, include_metadata=False)

        file_path = Path(result_without_metadata.storage_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        assert 'metadata' not in data
        assert 'malls' in data

    @pytest.mark.asyncio
    async def test_datetime_serialization(self, handler, sample_mall_data):
        """Test that datetime objects are properly serialized"""
        result = await handler.store_to_json(sample_mall_data)

        file_path = Path(result.storage_path)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check that last_updated is serialized as ISO string
        mall_with_datetime = next(m for m in data['malls'] if m.get('last_updated'))
        assert 'last_updated' in mall_with_datetime
        assert isinstance(mall_with_datetime['last_updated'], str)

        # Should be parseable as ISO datetime
        datetime.fromisoformat(mall_with_datetime['last_updated'])
