"""
Comprehensive tests for NOAA CSV discovery, cache refresh, and fallback scenarios.

Tests cover:
- CSV auto-discovery service
- Background cache refresh service
- Graceful fallback mechanisms
- DuckDB query service
- SPC Storm Reports service
"""

import pytest
import asyncio
import os
import tempfile
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import httpx

from app.services.noaa_csv_discovery_service import NOAACSVAutoDiscoveryService
from app.services.background_cache_refresh_service import BackgroundCacheRefreshService
from app.services.duckdb_query_service import DuckDBQueryService
from app.services.spc_storm_reports_service import SPCStormReportsService
from app.services.noaa_weather_service import NOAAWeatherService


class TestNOAACSVAutoDiscoveryService:
    """Test CSV auto-discovery service."""
    
    @pytest.fixture
    def discovery_service(self):
        """Create discovery service instance."""
        return NOAACSVAutoDiscoveryService()
    
    @pytest.mark.asyncio
    async def test_discover_latest_csv_files_success(self, discovery_service):
        """Test successful CSV file discovery."""
        # Mock FTP directory listing HTML
        mock_html = """
        <html>
        <body>
        <a href="StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz">StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz</a>
        <a href="StormEvents_details-ftp_v1.0_d2024_c20250818.csv.gz">StormEvents_details-ftp_v1.0_d2024_c20250818.csv.gz</a>
        </body>
        </html>
        """
        
        with patch.object(discovery_service, '_fetch_ftp_directory_listing', return_value=mock_html):
            result = await discovery_service.discover_latest_csv_files([2023, 2024])
            
            assert len(result) == 2
            assert 2023 in result
            assert 2024 in result
            assert "StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz" in result[2023]
            assert "StormEvents_details-ftp_v1.0_d2024_c20250818.csv.gz" in result[2024]
    
    @pytest.mark.asyncio
    async def test_discover_latest_csv_files_fallback(self, discovery_service):
        """Test fallback to hardcoded URLs when discovery fails."""
        with patch.object(discovery_service, '_fetch_ftp_directory_listing', return_value=None):
            result = await discovery_service.discover_latest_csv_files([2023, 2024])
            
            assert len(result) == 2
            assert 2023 in result
            assert 2024 in result
            # Should contain fallback URLs
            assert "StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz" in result[2023]
            assert "StormEvents_details-ftp_v1.0_d2024_c20250818.csv.gz" in result[2024]
    
    def test_parse_filename_info(self, discovery_service):
        """Test filename parsing."""
        filename = "StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz"
        result = discovery_service.parse_filename_info(filename)
        
        assert result is not None
        assert result['year'] == 2023
        assert result['compilation_date'].year == 2025
        assert result['compilation_date'].month == 7
        assert result['compilation_date'].day == 31
        assert result['filename'] == filename
    
    def test_parse_filename_info_invalid(self, discovery_service):
        """Test filename parsing with invalid filename."""
        filename = "invalid_filename.csv.gz"
        result = discovery_service.parse_filename_info(filename)
        
        assert result is None


class TestBackgroundCacheRefreshService:
    """Test background cache refresh service."""
    
    @pytest.fixture
    def cache_service(self):
        """Create cache refresh service instance."""
        return BackgroundCacheRefreshService()
    
    @pytest.mark.asyncio
    async def test_warmup_cache_success(self, cache_service):
        """Test successful cache warmup."""
        with patch.object(cache_service.noaa_service, '_download_and_cache_csv') as mock_download, \
             patch.object(cache_service.csv_discovery_service, 'discover_latest_csv_files') as mock_discover:
            
            mock_discover.return_value = {
                2023: "http://example.com/2023.csv.gz",
                2024: "http://example.com/2024.csv.gz"
            }
            mock_download.return_value = "/tmp/test.csv"
            
            await cache_service.warmup_cache([2023, 2024])
            
            assert mock_discover.called
            assert mock_download.call_count == 2
    
    @pytest.mark.asyncio
    async def test_refresh_cache_job(self, cache_service):
        """Test background refresh job."""
        with patch.object(cache_service, 'warmup_cache') as mock_warmup:
            await cache_service.refresh_cache_job()
            assert mock_warmup.called
    
    @pytest.mark.asyncio
    async def test_cleanup_cache_job(self, cache_service):
        """Test background cleanup job."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_service.noaa_service.cache_dir = temp_dir
            
            # Create test cache files
            old_file = os.path.join(temp_dir, "storm_events_2020.csv")
            new_file = os.path.join(temp_dir, "storm_events_2024.csv")
            
            with open(old_file, 'w') as f:
                f.write("test data")
            with open(new_file, 'w') as f:
                f.write("test data")
            
            # Make old file actually old
            old_time = datetime.now().timestamp() - (61 * 24 * 3600)  # 61 days ago
            os.utime(old_file, (old_time, old_time))
            
            await cache_service.cleanup_cache_job()
            
            # Old file should be removed, new file should remain
            assert not os.path.exists(old_file)
            assert os.path.exists(new_file)


class TestDuckDBQueryService:
    """Test DuckDB query service."""
    
    @pytest.fixture
    def duckdb_service(self):
        """Create DuckDB service instance."""
        return DuckDBQueryService()
    
    def test_query_storm_events_with_duckdb(self, duckdb_service):
        """Test querying with DuckDB available."""
        if not duckdb_service.available:
            pytest.skip("DuckDB not available")
        
        # Create test CSV file
        test_csv_content = """EVENT_ID,EVENT_TYPE,BEGIN_DATE_TIME,BEGIN_LAT,BEGIN_LON,MAGNITUDE,MAGNITUDE_TYPE,CZ_NAME,STATE,DAMAGE_PROPERTY,INJURIES_DIRECT,DEATHS_DIRECT,EVENT_NARRATIVE
1,Hail,2023-06-15 14:30:00,40.0,-74.0,1.5,inches,Test County,NJ,0,0,0,Test hail event
2,Wind,2023-06-16 15:00:00,40.1,-74.1,65,mph,Test County,NJ,1000,0,0,Test wind event
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_csv_content)
            f.flush()
            
            try:
                events = duckdb_service.query_storm_events(
                    csv_files=[f.name],
                    min_lat=39.9,
                    max_lat=40.2,
                    min_lon=-74.2,
                    max_lon=-73.9,
                    start_date='2023-06-15',
                    end_date='2023-06-16'
                )
                
                assert len(events) == 2
                assert events[0]['event_type'] == 'hail'
                assert events[1]['event_type'] == 'wind'
                
            finally:
                os.unlink(f.name)
    
    def test_query_storm_events_fallback(self, duckdb_service):
        """Test fallback to csv.DictReader when DuckDB unavailable."""
        # Mock DuckDB as unavailable
        duckdb_service.available = False
        
        # Create test CSV file
        test_csv_content = """EVENT_ID,EVENT_TYPE,BEGIN_DATE_TIME,BEGIN_LAT,BEGIN_LON,MAGNITUDE,MAGNITUDE_TYPE,CZ_NAME,STATE,DAMAGE_PROPERTY,INJURIES_DIRECT,DEATHS_DIRECT,EVENT_NARRATIVE
1,Hail,2023-06-15 14:30:00,40.0,-74.0,1.5,inches,Test County,NJ,0,0,0,Test hail event
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(test_csv_content)
            f.flush()
            
            try:
                events = duckdb_service.query_storm_events(
                    csv_files=[f.name],
                    min_lat=39.9,
                    max_lat=40.2,
                    min_lon=-74.2,
                    max_lon=-73.9,
                    start_date='2023-06-15',
                    end_date='2023-06-15'
                )
                
                assert len(events) == 1
                assert events[0]['event_type'] == 'hail'
                
            finally:
                os.unlink(f.name)


class TestSPCStormReportsService:
    """Test SPC Storm Reports service."""
    
    @pytest.fixture
    def spc_service(self):
        """Create SPC service instance."""
        return SPCStormReportsService()
    
    @pytest.mark.asyncio
    async def test_fetch_spc_reports_success(self, spc_service):
        """Test successful SPC reports fetching."""
        # Mock CSV content
        mock_csv_content = """Time,Type,Size,Location,State,Lat,Lon
1430,Hail,1.75,Test City,NJ,40.0,-74.0
1500,Wind,65,Test City,NJ,40.1,-74.1
"""
        
        with patch.object(spc_service, '_download_daily_spc_csv') as mock_download:
            mock_download.return_value = [
                {
                    'event_type': 'hail',
                    'severity': 'severe',
                    'description': 'Hail (1.75")',
                    'timestamp': '14:30',
                    'source': 'NWS-SPC-Preliminary',
                    'magnitude': 1.75,
                    'magnitude_type': 'inches',
                    'location': 'Test City',
                    'state': 'NJ',
                    'data_quality': 'preliminary'
                }
            ]
            
            events = await spc_service.fetch_spc_reports(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 1),
                latitude=40.0,
                longitude=-74.0,
                radius_km=50.0
            )
            
            assert len(events) == 1
            assert events[0]['event_type'] == 'hail'
            assert events[0]['data_quality'] == 'preliminary'
    
    @pytest.mark.asyncio
    async def test_fetch_spc_reports_no_data(self, spc_service):
        """Test SPC reports fetching when no data available."""
        with patch.object(spc_service, '_download_daily_spc_csv', return_value=[]):
            events = await spc_service.fetch_spc_reports(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 1),
                latitude=40.0,
                longitude=-74.0,
                radius_km=50.0
            )
            
            assert len(events) == 0


class TestNOAAWeatherServiceFallbacks:
    """Test NOAA Weather Service fallback mechanisms."""
    
    @pytest.fixture
    def noaa_service(self):
        """Create NOAA service instance."""
        return NOAAWeatherService()
    
    def test_is_cache_stale_but_usable(self, noaa_service):
        """Test stale cache detection."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            f.flush()
            
            # Make file 2 days old (stale but usable for current year)
            old_time = datetime.now().timestamp() - (2 * 24 * 3600)
            os.utime(f.name, (old_time, old_time))
            
            current_year = date.today().year
            is_stale = noaa_service._is_cache_stale_but_usable(f.name, current_year)
            
            assert is_stale
            
            os.unlink(f.name)
    
    def test_get_previous_month_fallback_url(self, noaa_service):
        """Test previous month fallback URL generation."""
        original_url = "https://example.com/StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz"
        fallback_url = noaa_service._get_previous_month_fallback_url(original_url, 2023)
        
        assert fallback_url is not None
        assert "20250631" in fallback_url  # Previous month
    
    @pytest.mark.asyncio
    async def test_download_and_cache_csv_with_fallbacks(self, noaa_service):
        """Test CSV download with fallback mechanisms."""
        with patch('httpx.AsyncClient') as mock_client:
            # Mock failed download
            mock_response = Mock()
            mock_response.status_code = 404
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            # Mock stale cache file
            with tempfile.NamedTemporaryFile(delete=False) as cache_file:
                cache_file.write(b"stale data")
                cache_file.flush()
                
                # Make file stale
                old_time = datetime.now().timestamp() - (2 * 24 * 3600)
                os.utime(cache_file.name, (old_time, old_time))
                
                with patch.object(noaa_service, '_get_cache_file_path', return_value=cache_file.name):
                    result = await noaa_service._download_and_cache_csv("http://example.com/test.csv.gz", 2024)
                    
                    # Should return stale cache as last resort
                    assert result == cache_file.name
                
                os.unlink(cache_file.name)


class TestIntegrationScenarios:
    """Test integration scenarios combining multiple services."""
    
    @pytest.mark.asyncio
    async def test_hybrid_data_strategy_integration(self):
        """Test the complete hybrid data strategy."""
        noaa_service = NOAAWeatherService()
        
        # Mock the services
        with patch.object(noaa_service, '_fetch_nws_storm_events') as mock_storm_events, \
             patch.object(noaa_service, '_fetch_nws_current_alerts') as mock_alerts:
            
            # Mock historical storm events (verified)
            mock_storm_events.return_value = [
                {
                    'event_type': 'hail',
                    'severity': 'severe',
                    'source': 'NWS-StormEvents',
                    'data_quality': 'verified',
                    'timestamp': '2023-06-15'
                }
            ]
            
            # Mock current alerts
            mock_alerts.return_value = [
                {
                    'event_type': 'tornado',
                    'severity': 'severe',
                    'source': 'NWS Active Alerts',
                    'data_quality': 'current',
                    'timestamp': '2024-01-15'
                }
            ]
            
            # Mock SPC service
            with patch('app.services.noaa_weather_service.get_spc_service') as mock_spc:
                mock_spc_service = Mock()
                mock_spc_service.fetch_spc_reports.return_value = [
                    {
                        'event_type': 'hail',
                        'severity': 'moderate',
                        'source': 'NWS SPC Preliminary Reports',
                        'data_quality': 'preliminary',
                        'timestamp': '2024-01-10'
                    }
                ]
                mock_spc.return_value = mock_spc_service
                
                # Test hybrid data strategy
                events = await noaa_service.get_weather_events(
                    latitude=40.0,
                    longitude=-74.0,
                    start_date=date(2023, 1, 1),
                    end_date=date(2024, 1, 15),
                    radius_km=50.0
                )
                
                # Should have events from all sources
                assert len(events) >= 3
                
                # Check data quality indicators
                verified_events = [e for e in events if e.get('data_quality') == 'verified']
                preliminary_events = [e for e in events if e.get('data_quality') == 'preliminary']
                current_events = [e for e in events if e.get('data_quality') == 'current']
                
                assert len(verified_events) > 0
                assert len(preliminary_events) > 0
                assert len(current_events) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
