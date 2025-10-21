"""
Tests for NOAA CDO API functionality.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import date, timedelta
import httpx

from app.services.noaa_weather_service import NOAAWeatherService
from app.services.rate_limiter import RateLimitExceededError


class TestNOAACDOAPI:
    """Test NOAA CDO API functionality."""
    
    @pytest.fixture
    def weather_service(self):
        """Create a weather service for testing."""
        with patch('app.core.config.settings') as mock_settings:
            mock_settings.noaa_cdo_api_token = "test_token"
            mock_settings.noaa_nws_base_url = "https://api.weather.gov"
            mock_settings.noaa_cdo_base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"
            mock_settings.noaa_timeout = 30
            mock_settings.noaa_max_retries = 3
            mock_settings.noaa_user_agent = "test-agent"
            mock_settings.noaa_cdo_requests_per_second = 5
            mock_settings.noaa_cdo_requests_per_day = 10000
            mock_settings.noaa_cdo_rate_limit_buffer = 0.8
            
            return NOAAWeatherService()
    
    @pytest.mark.asyncio
    async def test_find_nearest_cdo_station_success(self, weather_service):
        """Test successful station finding with correct extent parameter."""
        mock_response_data = {
            "results": [
                {
                    "id": "USC00012345",
                    "name": "Test Station",
                    "distance": 5.2
                }
            ]
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await weather_service._find_nearest_cdo_station(40.7128, -74.0060)
            
            assert result == "USC00012345"
            
            # Verify correct parameters were used
            call_args = mock_client.return_value.__aenter__.return_value.get.call_args
            assert "extent" in call_args[1]["params"]
            assert call_args[1]["params"]["extent"] == "40.7128,-74.0060,40.7128,-74.0060"
            assert call_args[1]["params"]["datasetid"] == "GHCND"
            assert "startdate" in call_args[1]["params"]
            assert "enddate" in call_args[1]["params"]
    
    @pytest.mark.asyncio
    async def test_find_nearest_cdo_station_no_results(self, weather_service):
        """Test station finding when no stations are found."""
        mock_response_data = {"results": []}
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await weather_service._find_nearest_cdo_station(40.7128, -74.0060)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_find_nearest_cdo_station_http_errors(self, weather_service):
        """Test station finding with various HTTP errors."""
        test_cases = [
            (401, "NOAA CDO API token is invalid or expired"),
            (429, "NOAA CDO API rate limit exceeded"),
            (500, "HTTP error finding CDO station: 500")
        ]
        
        for status_code, expected_log in test_cases:
            with patch('httpx.AsyncClient') as mock_client:
                mock_response = MagicMock()
                mock_response.status_code = status_code
                
                http_error = httpx.HTTPStatusError(
                    message="Test error",
                    request=MagicMock(),
                    response=mock_response
                )
                
                mock_client.return_value.__aenter__.return_value.get.side_effect = http_error
                
                result = await weather_service._find_nearest_cdo_station(40.7128, -74.0060)
                
                assert result is None
    
    @pytest.mark.asyncio
    async def test_find_nearest_cdo_station_rate_limit_error(self, weather_service):
        """Test station finding with rate limit error."""
        with patch.object(weather_service.rate_limiter, 'wait_if_needed') as mock_wait:
            mock_wait.side_effect = RateLimitExceededError("Rate limit exceeded")
            
            result = await weather_service._find_nearest_cdo_station(40.7128, -74.0060)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_fetch_cdo_historical_data_success(self, weather_service):
        """Test successful historical data fetching."""
        mock_response_data = {
            "results": [
                {
                    "date": "2023-01-01T00:00:00",
                    "datatype": "TMAX",
                    "value": 25.5,
                    "station": "USC00012345"
                },
                {
                    "date": "2023-01-01T00:00:00",
                    "datatype": "TMIN",
                    "value": 15.2,
                    "station": "USC00012345"
                }
            ],
            "metadata": {
                "resultset": {
                    "count": 2,
                    "limit": 1000
                }
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = mock_response_data
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await weather_service._fetch_cdo_historical_data(
                "USC00012345",
                date(2023, 1, 1),
                date(2023, 1, 31)
            )
            
            assert result["source"] == "NOAA-CDO"
            assert result["station_id"] == "USC00012345"
            assert result["total_records"] == 2
            assert len(result["observations"]) == 2
    
    @pytest.mark.asyncio
    async def test_fetch_cdo_historical_data_pagination(self, weather_service):
        """Test historical data fetching with pagination."""
        # First page
        first_page_data = {
            "results": [{"date": "2023-01-01T00:00:00", "datatype": "TMAX", "value": 25.5}] * 1000,
            "metadata": {
                "resultset": {
                    "count": 1500,
                    "limit": 1000
                }
            }
        }
        
        # Second page
        second_page_data = {
            "results": [{"date": "2023-01-02T00:00:00", "datatype": "TMAX", "value": 26.0}] * 500,
            "metadata": {
                "resultset": {
                    "count": 1500,
                    "limit": 1000
                }
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.side_effect = [first_page_data, second_page_data]
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await weather_service._fetch_cdo_historical_data(
                "USC00012345",
                date(2023, 1, 1),
                date(2023, 1, 31)
            )
            
            assert result["total_records"] == 1500
            assert result["pages_fetched"] == 2
    
    @pytest.mark.asyncio
    async def test_fetch_cdo_historical_data_http_errors(self, weather_service):
        """Test historical data fetching with various HTTP errors."""
        test_cases = [
            (401, "NOAA CDO API token is invalid or expired"),
            (429, "NOAA CDO API rate limit exceeded"),
            (400, "Bad request to CDO API"),
            (500, "HTTP error fetching CDO data: 500")
        ]
        
        for status_code, expected_log in test_cases:
            with patch('httpx.AsyncClient') as mock_client:
                mock_response = MagicMock()
                mock_response.status_code = status_code
                mock_response.text = "Test error"
                
                http_error = httpx.HTTPStatusError(
                    message="Test error",
                    request=MagicMock(),
                    response=mock_response
                )
                
                mock_client.return_value.__aenter__.return_value.get.side_effect = http_error
                
                result = await weather_service._fetch_cdo_historical_data(
                    "USC00012345",
                    date(2023, 1, 1),
                    date(2023, 1, 31)
                )
                
                assert result["total_records"] == 0
                assert "error" in result
    
    @pytest.mark.asyncio
    async def test_fetch_cdo_historical_data_rate_limit_error(self, weather_service):
        """Test historical data fetching with rate limit error."""
        with patch.object(weather_service.rate_limiter, 'wait_if_needed') as mock_wait:
            mock_wait.side_effect = RateLimitExceededError("Rate limit exceeded")
            
            result = await weather_service._fetch_cdo_historical_data(
                "USC00012345",
                date(2023, 1, 1),
                date(2023, 1, 31)
            )
            
            assert result["total_records"] == 0
            assert result["error"] == "Rate limit exceeded"
    
    @pytest.mark.asyncio
    async def test_get_historical_weather_integration(self, weather_service):
        """Test the main historical weather method integration."""
        with patch.object(weather_service, '_find_nearest_cdo_station') as mock_find_station, \
             patch.object(weather_service, '_fetch_cdo_historical_data') as mock_fetch_data:
            
            mock_find_station.return_value = "USC00012345"
            mock_fetch_data.return_value = {
                "observations": [],
                "source": "NOAA-CDO",
                "total_records": 0
            }
            
            result = await weather_service.get_historical_weather(
                40.7128, -74.0060,
                date(2023, 1, 1),
                date(2023, 1, 31)
            )
            
            assert "trend_analysis" in result
            assert "seasonal_analysis" in result
            assert "period_analysis" in result
            assert "analysis_context" in result
    
    def test_convert_cdo_record_to_observation(self, weather_service):
        """Test CDO record conversion to observation format."""
        record = {
            "date": "2023-01-01T00:00:00",
            "datatype": "TMAX",
            "value": 25.5,
            "station": "USC00012345"
        }
        
        result = weather_service._convert_cdo_record_to_observation(record)
        
        assert result is not None
        assert result["date"] == "2023-01-01"
        assert result["temperature_max"] == 78.0  # 25.5°C converted to °F
        assert result["station_id"] == "USC00012345"
