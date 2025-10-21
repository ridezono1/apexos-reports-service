"""
Tests for NOAA Weather Service
"""

import pytest
import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from app.services.noaa_weather_service import NOAAWeatherService, get_noaa_weather_service
from app.core.config import settings


class TestNOAAWeatherService:
    """Test NOAA Weather Service functionality."""
    
    @pytest.fixture
    def noaa_service(self):
        """Create NOAA service instance for testing."""
        return NOAAWeatherService()
    
    @pytest.fixture
    def houston_coordinates(self):
        """Houston, TX coordinates for testing."""
        return (29.7604, -95.3698)
    
    @pytest.fixture
    def sample_point_metadata(self):
        """Sample NWS Points API response."""
        return {
            "properties": {
                "cwa": "HGX",
                "gridX": 95,
                "gridY": 96,
                "forecast": "https://api.weather.gov/gridpoints/HGX/95,96/forecast",
                "forecastHourly": "https://api.weather.gov/gridpoints/HGX/95,96/forecast/hourly",
                "observationStations": "https://api.weather.gov/gridpoints/HGX/95,96/stations"
            }
        }
    
    @pytest.fixture
    def sample_stations_response(self):
        """Sample NWS stations response."""
        return {
            "features": [
                {
                    "properties": {
                        "stationIdentifier": "KHOU",
                        "name": "Houston Hobby Airport"
                    }
                }
            ]
        }
    
    @pytest.fixture
    def sample_observation(self):
        """Sample NWS observation response."""
        return {
            "properties": {
                "temperature": {"value": 25.0},  # Celsius
                "windSpeed": {"value": 5.0},    # m/s
                "windDirection": {"value": 270}, # degrees
                "precipitationLastHour": {"value": 2.5}, # mm
                "relativeHumidity": {"value": 65},
                "barometricPressure": {"value": 101325}, # Pa
                "visibility": {"value": 10000}, # meters
                "textDescription": "Partly Cloudy",
                "timestamp": "2024-01-15T12:00:00Z"
            }
        }
    
    @pytest.fixture
    def sample_forecast(self):
        """Sample NWS forecast response."""
        return {
            "properties": {
                "periods": [
                    {
                        "name": "Today",
                        "startTime": "2024-01-15T06:00:00-06:00",
                        "endTime": "2024-01-15T18:00:00-06:00",
                        "temperature": 75,
                        "windSpeed": "10 mph",
                        "windDirection": "NW",
                        "probabilityOfPrecipitation": {"value": 20},
                        "shortForecast": "Partly Cloudy",
                        "detailedForecast": "Partly cloudy with a high near 75.",
                        "isDaytime": True
                    },
                    {
                        "name": "Tonight",
                        "startTime": "2024-01-15T18:00:00-06:00",
                        "endTime": "2024-01-16T06:00:00-06:00",
                        "temperature": 55,
                        "windSpeed": "5 mph",
                        "windDirection": "N",
                        "probabilityOfPrecipitation": {"value": 10},
                        "shortForecast": "Clear",
                        "detailedForecast": "Clear with a low around 55.",
                        "isDaytime": False
                    }
                ]
            }
        }
    
    @pytest.fixture
    def sample_cdo_data(self):
        """Sample NOAA CDO API response."""
        return {
            "results": [
                {
                    "date": "2024-01-15T00:00:00",
                    "TMAX": 25.0,  # Celsius
                    "TMIN": 15.0,  # Celsius
                    "TAVG": 20.0,  # Celsius
                    "PRCP": 5.0,   # mm
                    "WSFG": 3.0    # m/s
                },
                {
                    "date": "2024-01-16T00:00:00",
                    "TMAX": 27.0,
                    "TMIN": 17.0,
                    "TAVG": 22.0,
                    "PRCP": 0.0,
                    "WSFG": 2.5
                }
            ]
        }
    
    @pytest.mark.asyncio
    async def test_get_point_metadata(self, noaa_service, houston_coordinates):
        """Test getting point metadata from NWS Points API."""
        lat, lon = houston_coordinates
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "properties": {
                    "cwa": "HGX",
                    "gridX": 95,
                    "gridY": 96,
                    "forecast": "https://api.weather.gov/gridpoints/HGX/95,96/forecast",
                    "observationStations": "https://api.weather.gov/gridpoints/HGX/95,96/stations"
                }
            }
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await noaa_service.get_point_metadata(lat, lon)
            
            assert result["properties"]["cwa"] == "HGX"
            assert result["properties"]["gridX"] == 95
            assert result["properties"]["gridY"] == 96
    
    @pytest.mark.asyncio
    async def test_get_current_weather(self, noaa_service, houston_coordinates, sample_point_metadata, sample_stations_response, sample_observation):
        """Test getting current weather from NOAA."""
        lat, lon = houston_coordinates
        
        with patch.object(noaa_service, 'get_point_metadata', return_value=sample_point_metadata):
            with patch('httpx.AsyncClient') as mock_client:
                # Mock stations response
                stations_response = MagicMock()
                stations_response.json.return_value = sample_stations_response
                stations_response.raise_for_status.return_value = None
                
                # Mock observation response
                obs_response = MagicMock()
                obs_response.json.return_value = sample_observation
                obs_response.raise_for_status.return_value = None
                
                mock_client.return_value.__aenter__.return_value.get.side_effect = [stations_response, obs_response]
                
                result = await noaa_service.get_current_weather(lat, lon, "Houston, TX")
                
                # Verify temperature conversion (25°C = 77°F)
                assert result["temperature"] == 77.0
                assert result["temperature_unit"] == "F"
                
                # Verify wind speed conversion (5 m/s = 11.2 mph)
                assert result["wind_speed"] == "11.2 mph"
                
                # Verify wind direction conversion (270° = W)
                assert result["wind_direction"] == "W"
                
                # Verify precipitation conversion (2.5 mm = 0.1 inches)
                assert result["precipitation_intensity"] == 0.1
                
                assert result["source"] == "NOAA-NWS"
    
    @pytest.mark.asyncio
    async def test_get_weather_forecast(self, noaa_service, houston_coordinates, sample_point_metadata, sample_forecast):
        """Test getting weather forecast from NOAA."""
        lat, lon = houston_coordinates
        
        with patch.object(noaa_service, 'get_point_metadata', return_value=sample_point_metadata):
            with patch('httpx.AsyncClient') as mock_client:
                forecast_response = MagicMock()
                forecast_response.json.return_value = sample_forecast
                forecast_response.raise_for_status.return_value = None
                
                mock_client.return_value.__aenter__.return_value.get.return_value = forecast_response
                
                result = await noaa_service.get_weather_forecast(lat, lon, days=2)
                
                assert len(result["forecasts"]) == 2
                assert result["source"] == "NOAA-NWS"
                assert result["total_periods"] == 2
                
                # Check first forecast period
                today_forecast = result["forecasts"][0]
                assert today_forecast["name"] == "Today"
                assert today_forecast["temperature"] == 75
                assert today_forecast["temperature_unit"] == "F"
                assert today_forecast["wind_speed"] == "10 mph"
                assert today_forecast["wind_direction"] == "NW"
                assert today_forecast["precipitation_probability"] == 20
                assert today_forecast["is_daytime"] == True
    
    @pytest.mark.asyncio
    async def test_get_historical_weather_24_months(self, noaa_service, houston_coordinates, sample_cdo_data):
        """Test getting 24-month historical weather data."""
        lat, lon = houston_coordinates
        end_date = date.today()
        start_date = end_date - timedelta(days=730)  # 24 months
        
        with patch.object(noaa_service, '_find_nearest_cdo_station', return_value="USW00012918"):
            with patch.object(noaa_service, '_fetch_cdo_historical_data', return_value={
                "observations": [
                    {"timestamp": "2024-01-15", "temperature": 68.0, "precipitation": 0.2, "wind_speed": 6.7},
                    {"timestamp": "2024-01-16", "temperature": 71.6, "precipitation": 0.0, "wind_speed": 5.6}
                ],
                "source": "NOAA-CDO",
                "station_id": "USW00012918",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_records": 2
            }):
                result = await noaa_service.get_historical_weather(lat, lon, start_date, end_date)
                
                assert result["source"] == "NOAA-CDO"
                assert result["station_id"] == "USW00012918"
                assert result["total_records"] == 2
                assert result["analysis_context"]["period_type"] == "24_month"
                assert result["analysis_context"]["period_length_days"] == 730
                
                # Check trend analysis
                assert "trend_analysis" in result
                assert "seasonal_analysis" in result
                assert "period_analysis" in result
    
    @pytest.mark.asyncio
    async def test_get_weather_events(self, noaa_service, houston_coordinates):
        """Test getting weather events from NOAA."""
        lat, lon = houston_coordinates
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)
        
        sample_alerts = {
            "features": [
                {
                    "properties": {
                        "event": "Severe Thunderstorm Warning",
                        "severity": "Moderate",
                        "urgency": "Immediate",
                        "description": "Wind gusts up to 60 mph expected",
                        "sent": "2024-01-15T14:00:00Z"
                    }
                }
            ]
        }
        
        with patch('httpx.AsyncClient') as mock_client:
            alerts_response = MagicMock()
            alerts_response.json.return_value = sample_alerts
            alerts_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = alerts_response
            
            result = await noaa_service.get_weather_events(lat, lon, start_date, end_date)
            
            assert len(result) == 1
            event = result[0]
            assert event["event_type"] == "severe thunderstorm warning"
            assert event["severity"] == "moderate"
            assert event["urgency"] == "immediate"
            assert event["source"] == "NOAA-NWS"
            assert event["magnitude"] == 60.0  # Extracted from description
    
    def test_convert_weather_text_to_code(self, noaa_service):
        """Test weather text to code conversion."""
        assert noaa_service._convert_weather_text_to_code("Clear") == 1000
        assert noaa_service._convert_weather_text_to_code("Partly Cloudy") == 1101
        assert noaa_service._convert_weather_text_to_code("Mostly Cloudy") == 1102
        assert noaa_service._convert_weather_text_to_code("Cloudy") == 1001
        assert noaa_service._convert_weather_text_to_code("Fog") == 2000
        assert noaa_service._convert_weather_text_to_code("Rain") == 4001
        assert noaa_service._convert_weather_text_to_code("Snow") == 5000
        assert noaa_service._convert_weather_text_to_code("Thunderstorm") == 8000
        assert noaa_service._convert_weather_text_to_code("Unknown") == 1000
    
    def test_convert_wind_direction_to_text(self, noaa_service):
        """Test wind direction conversion."""
        assert noaa_service._convert_wind_direction_to_text(0) == "N"
        assert noaa_service._convert_wind_direction_to_text(90) == "E"
        assert noaa_service._convert_wind_direction_to_text(180) == "S"
        assert noaa_service._convert_wind_direction_to_text(270) == "W"
        assert noaa_service._convert_wind_direction_to_text(45) == "NE"
        assert noaa_service._convert_wind_direction_to_text(225) == "SW"
        assert noaa_service._convert_wind_direction_to_text(None) == "Unknown"
    
    def test_extract_magnitude_from_description(self, noaa_service):
        """Test magnitude extraction from weather descriptions."""
        # Wind speed patterns
        assert noaa_service._extract_magnitude_from_description("Wind gusts up to 60 mph", "wind") == 60.0
        assert noaa_service._extract_magnitude_from_description("Winds 45 miles per hour", "wind") == 45.0
        assert noaa_service._extract_magnitude_from_description("Wind speed 25 knots", "wind") == pytest.approx(28.8, rel=1e-1)
        
        # Hail size patterns
        assert noaa_service._extract_magnitude_from_description("Hail up to 1.5 inch", "hail") == 1.5
        assert noaa_service._extract_magnitude_from_description("Hail 2\" diameter", "hail") == 2.0
        
        # No match
        assert noaa_service._extract_magnitude_from_description("No specific magnitude", "wind") is None
    
    def test_calculate_trend_slope(self, noaa_service):
        """Test trend slope calculation."""
        # Increasing trend
        increasing_values = [10, 12, 14, 16, 18]
        slope = noaa_service._calculate_trend_slope(increasing_values)
        assert slope > 0
        
        # Decreasing trend
        decreasing_values = [20, 18, 16, 14, 12]
        slope = noaa_service._calculate_trend_slope(decreasing_values)
        assert slope < 0
        
        # No trend
        constant_values = [15, 15, 15, 15, 15]
        slope = noaa_service._calculate_trend_slope(constant_values)
        assert slope == 0
    
    def test_calculate_variability(self, noaa_service):
        """Test variability calculation."""
        # High variability
        high_var_values = [10, 20, 5, 25, 15]
        variability = noaa_service._calculate_variability(high_var_values)
        assert variability > 0
        
        # Low variability
        low_var_values = [14, 15, 16, 15, 14]
        variability = noaa_service._calculate_variability(low_var_values)
        assert variability < 1.0
        
        # No variability
        constant_values = [15, 15, 15, 15, 15]
        variability = noaa_service._calculate_variability(constant_values)
        assert variability == 0
    
    def test_identify_drought_periods(self, noaa_service):
        """Test drought period identification."""
        # Normal precipitation pattern
        normal_precip = [5.0, 3.0, 0.0, 2.0, 8.0, 1.0, 4.0]
        droughts = noaa_service._identify_drought_periods(normal_precip)
        assert len(droughts) == 0
        
        # Drought period (7+ consecutive days < 1mm)
        drought_precip = [5.0, 3.0, 0.5, 0.2, 0.0, 0.1, 0.3, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0]
        droughts = noaa_service._identify_drought_periods(drought_precip)
        assert len(droughts) == 1
        assert droughts[0]["length_days"] == 8
        assert droughts[0]["severity"] == "moderate"
        
        # Severe drought (14+ consecutive days)
        severe_drought_precip = [0.0] * 20 + [5.0]
        droughts = noaa_service._identify_drought_periods(severe_drought_precip)
        assert len(droughts) == 1
        assert droughts[0]["length_days"] == 20
        assert droughts[0]["severity"] == "severe"
    
    @pytest.mark.asyncio
    async def test_fallback_weather_data(self, noaa_service):
        """Test fallback weather data generation."""
        result = noaa_service._get_fallback_weather_data(29.7604, -95.3698, "Houston, TX")
        
        assert result["temperature"] is not None
        assert result["temperature_unit"] == "F"
        assert result["wind_speed"] == "5 mph"
        assert result["wind_direction"] == "NW"
        assert result["source"] == "NOAA-Fallback"
        assert "Houston, TX" in result["detailed_forecast"]
    
    def test_singleton_pattern(self):
        """Test that NOAA service follows singleton pattern."""
        service1 = get_noaa_weather_service()
        service2 = get_noaa_weather_service()
        
        assert service1 is service2
        assert isinstance(service1, NOAAWeatherService)


@pytest.mark.asyncio
async def test_integration_houston_weather():
    """Integration test with real NOAA API for Houston, TX."""
    # This test requires internet connection and may be slow
    # Skip in CI/CD environments
    import os
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.skip("Skipping integration test in CI environment")
    
    service = NOAAWeatherService()
    lat, lon = 29.7604, -95.3698  # Houston, TX
    
    try:
        # Test point metadata
        point_metadata = await service.get_point_metadata(lat, lon)
        assert "properties" in point_metadata
        assert "cwa" in point_metadata["properties"]
        
        # Test current weather
        current_weather = await service.get_current_weather(lat, lon, "Houston, TX")
        assert current_weather["source"] == "NOAA-NWS"
        assert current_weather["temperature"] is not None
        assert current_weather["temperature_unit"] == "F"
        
        # Test forecast
        forecast = await service.get_weather_forecast(lat, lon, days=3)
        assert forecast["source"] == "NOAA-NWS"
        assert len(forecast["forecasts"]) <= 3
        
        print(f"\nHouston, TX Weather (NOAA):")
        print(f"Current: {current_weather['temperature']}°F, {current_weather['short_forecast']}")
        print(f"Forecast periods: {forecast['total_periods']}")
        
    except Exception as e:
        pytest.fail(f"Integration test failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
