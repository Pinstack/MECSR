"""
StorageHandler component for MECSR directory scraping.
Handles data persistence, export to various formats, and database operations.
"""

import json
import csv
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import asyncio
from dataclasses import dataclass

from processors.data_processor import MallData


@dataclass
class StorageResult:
    """Data class for storage operation results"""
    success: bool
    records_stored: int
    storage_path: Optional[str] = None
    error_message: Optional[str] = None
    storage_format: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class StorageHandler:
    """Handler for storing and exporting mall data in various formats"""

    def __init__(self, base_path: str = "output"):
        """
        Initialize the StorageHandler

        Args:
            base_path: Base directory for storing output files
        """
        self.base_path = Path(base_path)
        try:
            self.base_path.mkdir(exist_ok=True)
        except Exception:
            # If directory creation fails, we'll handle it in individual methods
            pass
        self.db_path = self.base_path / "mecsr_malls.db"

    async def store_to_json(self, mall_data: List[MallData],
                           filename: Optional[str] = None,
                           include_metadata: bool = True) -> StorageResult:
        """
        Store mall data to JSON format

        Args:
            mall_data: List of mall data objects
            filename: Optional custom filename
            include_metadata: Whether to include processing metadata

        Returns:
            StorageResult with operation details
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"mecsr_malls_{timestamp}.json"

            file_path = self.base_path / filename

            # Convert MallData objects to dictionaries
            data_dicts = []
            for mall in mall_data:
                mall_dict = mall.model_dump()

                # Convert datetime objects to ISO format strings
                if mall_dict.get('last_updated') and isinstance(mall_dict['last_updated'], datetime):
                    mall_dict['last_updated'] = mall_dict['last_updated'].isoformat()

                data_dicts.append(mall_dict)

            # Prepare data structure
            data_structure = {
                'malls': data_dicts
            }

            if include_metadata:
                data_structure['metadata'] = {
                    'total_records': len(mall_data),
                    'export_timestamp': datetime.now().isoformat(),
                    'data_quality_summary': self._calculate_quality_summary(mall_data),
                    'source': 'MECSR Directory Scraper'
                }

            # Write to JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_structure, f, indent=2, ensure_ascii=False)

            return StorageResult(
                success=True,
                records_stored=len(mall_data),
                storage_path=str(file_path),
                storage_format='json',
                metadata={
                    'file_size_bytes': file_path.stat().st_size,
                    'filename': filename
                }
            )

        except Exception as e:
            return StorageResult(
                success=False,
                records_stored=0,
                error_message=str(e),
                storage_format='json'
            )

    async def store_to_csv(self, mall_data: List[MallData],
                          filename: Optional[str] = None,
                          include_coordinates: bool = True) -> StorageResult:
        """
        Store mall data to CSV format

        Args:
            mall_data: List of mall data objects
            filename: Optional custom filename
            include_coordinates: Whether to include coordinate columns

        Returns:
            StorageResult with operation details
        """
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"mecsr_malls_{timestamp}.csv"

            file_path = self.base_path / filename

            # Define CSV fieldnames
            fieldnames = [
                'name', 'url', 'property_type', 'status', 'country', 'city',
                'address', 'phone', 'email', 'website', 'gla_sqft', 'gla_sqm',
                'stores_count', 'parking_spaces', 'opening_year',
                'post_id', 'user_id', 'data_id', 'data_type',
                'last_updated', 'data_quality_score'
            ]

            if include_coordinates:
                fieldnames.insert(6, 'longitude')
                fieldnames.insert(6, 'latitude')

            # Write to CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for mall in mall_data:
                    mall_dict = mall.model_dump()

                    # Convert datetime to string
                    if mall_dict.get('last_updated') and isinstance(mall_dict['last_updated'], datetime):
                        mall_dict['last_updated'] = mall_dict['last_updated'].isoformat()

                    # Ensure all fields are present
                    row_data = {field: mall_dict.get(field, '') for field in fieldnames}
                    writer.writerow(row_data)

            return StorageResult(
                success=True,
                records_stored=len(mall_data),
                storage_path=str(file_path),
                storage_format='csv',
                metadata={
                    'file_size_bytes': file_path.stat().st_size,
                    'filename': filename,
                    'columns_count': len(fieldnames)
                }
            )

        except Exception as e:
            return StorageResult(
                success=False,
                records_stored=0,
                error_message=str(e),
                storage_format='csv'
            )

    async def store_to_sqlite(self, mall_data: List[MallData],
                             table_name: str = "malls",
                             create_table: bool = True) -> StorageResult:
        """
        Store mall data to SQLite database

        Args:
            mall_data: List of mall data objects
            table_name: Name of the database table
            create_table: Whether to create table if it doesn't exist

        Returns:
            StorageResult with operation details
        """
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()

            if create_table:
                # Create table with appropriate schema
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        url TEXT UNIQUE,
                        property_type TEXT,
                        status TEXT,
                        latitude REAL,
                        longitude REAL,
                        country TEXT,
                        city TEXT,
                        address TEXT,
                        phone TEXT,
                        email TEXT,
                        website TEXT,
                        gla_sqft INTEGER,
                        gla_sqm INTEGER,
                        stores_count INTEGER,
                        parking_spaces INTEGER,
                        opening_year INTEGER,
                        post_id TEXT,
                        user_id TEXT,
                        data_id TEXT,
                        data_type TEXT,
                        last_updated TEXT,
                        data_quality_score REAL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)

            # Insert or replace records
            records_inserted = 0
            for mall in mall_data:
                mall_dict = mall.model_dump()

                # Convert datetime to string
                if mall_dict.get('last_updated') and isinstance(mall_dict['last_updated'], datetime):
                    mall_dict['last_updated'] = mall_dict['last_updated'].isoformat()

                # Prepare data for insertion
                columns = [k for k in mall_dict.keys() if k != 'id']
                values = [mall_dict.get(col) for col in columns]
                placeholders = ', '.join(['?' for _ in columns])

                # Insert or replace
                cursor.execute(f"""
                    INSERT OR REPLACE INTO {table_name}
                    ({', '.join(columns)})
                    VALUES ({placeholders})
                """, values)

                records_inserted += 1

            conn.commit()
            conn.close()

            return StorageResult(
                success=True,
                records_stored=records_inserted,
                storage_path=str(self.db_path),
                storage_format='sqlite',
                metadata={
                    'table_name': table_name,
                    'database_size_bytes': self.db_path.stat().st_size
                }
            )

        except Exception as e:
            return StorageResult(
                success=False,
                records_stored=0,
                error_message=str(e),
                storage_format='sqlite'
            )

    async def export_multiple_formats(self, mall_data: List[MallData],
                                    formats: List[str] = None) -> Dict[str, StorageResult]:
        """
        Export mall data to multiple formats simultaneously

        Args:
            mall_data: List of mall data objects
            formats: List of formats to export to (json, csv, sqlite)

        Returns:
            Dictionary mapping format names to StorageResult objects
        """
        if formats is None:
            formats = ['json', 'csv', 'sqlite']

        results = {}
        tasks = []

        # Prepare export tasks
        if 'json' in formats:
            tasks.append(('json', self.store_to_json(mall_data)))
        if 'csv' in formats:
            tasks.append(('csv', self.store_to_csv(mall_data)))
        if 'sqlite' in formats:
            tasks.append(('sqlite', self.store_to_sqlite(mall_data)))

        # Execute all exports concurrently
        if tasks:
            format_names, task_coroutines = zip(*tasks)
            task_results = await asyncio.gather(*task_coroutines, return_exceptions=True)

            for format_name, result in zip(format_names, task_results):
                if isinstance(result, Exception):
                    results[format_name] = StorageResult(
                        success=False,
                        records_stored=0,
                        error_message=str(result),
                        storage_format=format_name
                    )
                else:
                    results[format_name] = result

        return results

    async def query_sqlite(self, query: str,
                          table_name: str = "malls") -> List[Dict[str, Any]]:
        """
        Query the SQLite database

        Args:
            query: SQL query string
            table_name: Name of the table to query

        Returns:
            List of query results as dictionaries
        """
        try:
            if not self.db_path.exists():
                return []

            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(query)
            rows = cursor.fetchall()

            # Convert rows to dictionaries
            results = []
            for row in rows:
                results.append(dict(row))

            conn.close()
            return results

        except Exception as e:
            print(f"Database query error: {e}")
            return []

    async def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get statistics about stored data and files

        Returns:
            Dictionary with storage statistics
        """
        stats = {
            'total_files': 0,
            'total_size_bytes': 0,
            'files_by_format': {},
            'database_tables': [],
            'database_records': {},
            'last_modified': None
        }

        try:
            # Count files and sizes
            for file_path in self.base_path.rglob('*'):
                if file_path.is_file():
                    stats['total_files'] += 1
                    stats['total_size_bytes'] += file_path.stat().st_size

                    # Track by format
                    suffix = file_path.suffix.lower()
                    if suffix:
                        format_key = suffix[1:]  # Remove the dot
                        if format_key not in stats['files_by_format']:
                            stats['files_by_format'][format_key] = 0
                        stats['files_by_format'][format_key] += 1

                    # Track last modified
                    mtime = file_path.stat().st_mtime
                    if stats['last_modified'] is None or mtime > stats['last_modified']:
                        stats['last_modified'] = mtime

            # Database statistics
            if self.db_path.exists():
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()

                # Get table names
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                stats['database_tables'] = [row[0] for row in cursor.fetchall()]

                # Get record counts for each table
                for table in stats['database_tables']:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    stats['database_records'][table] = count

                conn.close()

            # Convert last_modified timestamp to datetime
            if stats['last_modified']:
                stats['last_modified'] = datetime.fromtimestamp(stats['last_modified']).isoformat()

        except Exception as e:
            stats['error'] = str(e)

        return stats

    def _calculate_quality_summary(self, mall_data: List[MallData]) -> Dict[str, Any]:
        """Calculate quality summary statistics"""
        if not mall_data:
            return {}

        total_records = len(mall_data)
        quality_scores = [m.data_quality_score for m in mall_data if m.data_quality_score is not None]

        summary = {
            'total_records': total_records,
            'average_quality_score': sum(quality_scores) / len(quality_scores) if quality_scores else 0.0,
            'quality_distribution': {
                'excellent': len([s for s in quality_scores if s >= 0.9]),
                'good': len([s for s in quality_scores if 0.7 <= s < 0.9]),
                'fair': len([s for s in quality_scores if 0.5 <= s < 0.7]),
                'poor': len([s for s in quality_scores if s < 0.5])
            },
            'records_with_coordinates': len([m for m in mall_data if m.latitude and m.longitude]),
            'records_with_country': len([m for m in mall_data if m.country]),
            'records_with_city': len([m for m in mall_data if m.city])
        }

        return summary

    async def cleanup_old_files(self, days_old: int = 30,
                               formats: List[str] = None) -> Dict[str, Any]:
        """
        Clean up old files to save disk space

        Args:
            days_old: Remove files older than this many days
            formats: Specific formats to clean up (None for all)

        Returns:
            Dictionary with cleanup statistics
        """
        try:
            import time
            from datetime import datetime, timedelta

            cutoff_time = time.time() - (days_old * 24 * 60 * 60)
            cleanup_stats = {
                'files_removed': 0,
                'space_freed_bytes': 0,
                'errors': []
            }

            for file_path in self.base_path.rglob('*'):
                if file_path.is_file():
                    # Check file age
                    if file_path.stat().st_mtime < cutoff_time:
                        # Check format filter
                        if formats:
                            suffix = file_path.suffix.lower()
                            format_name = suffix[1:] if suffix else ''
                            if format_name not in formats:
                                continue

                        try:
                            size = file_path.stat().st_size
                            file_path.unlink()
                            cleanup_stats['files_removed'] += 1
                            cleanup_stats['space_freed_bytes'] += size
                        except Exception as e:
                            cleanup_stats['errors'].append(f"Failed to remove {file_path}: {e}")

            return cleanup_stats

        except Exception as e:
            return {
                'files_removed': 0,
                'space_freed_bytes': 0,
                'errors': [str(e)]
            }
