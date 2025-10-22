"""
Tests for Tomorrow.io Weather API integration.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from datetime import date, datetime, timedelta

from app.services.tomorrow_io_service import TomorrowIOService, get_tomorrow_io_service
from app.services.weather_data_service import WeatherDataService, get_weather_data_service
from app.core.weather_thresholds import WeatherThresholds


class TestTomorrowIOService:
    """Test Tomorrow.io service integration."""

    @pytest.fixture
    def tomorrow_io_service(self):
        """Create Tomorrow.io service instance."""
        with patch('app.services.tomorrow_io_service.settings') as mock_settings:
            mock_settings.tomorrow_io_api_key = "test_api_key"
            mock_settings.tomorrow_io_base_url = "https://api.tomorrow.io/v4"
            mock_settings.tomorrow_io_timeout = 30
            mock_settings.tomorrow_io_max_retries = 3
            return TomorrowIOService()

    @pytest.fixture
    def mock_timeline_response(self):
        """Mock Timeline API response."""
        return {
            "data": {
                "timelines": [
                    {
                        "intervals": [
                            {
                                "startTime": "2025-01-07T12:00:00Z",
                                "values": {
                                    "temperature": 72.5,
                                    "humidity": 65.0,
                                    "windSpeed": 15.0,
                                    "precipitationIntensity": 0.0,
                                    "weatherCode": 1000,
                                    "hailProbability": 5.0,
                                    "fireIndex": 25.0
                                }
                            }
                        ]
                    }
                ]
            }
        }

    @pytest.fixture
    def mock_historical_response(self):
        """Mock Historical API response."""
        return {
            "data": {
                "timelines": [
                    {
                        "intervals": [
                            {
                                "startTime": "2024-07-07T00:00:00Z",
                                "values": {
                                    "temperature": 85.0,
                                    "humidity": 70.0,
                                    "windSpeed": 20.0,
                                    "precipitationIntensity": 0.5,
                                    "weatherCode": 4001,
                                    "hailProbability": 0.0,
                                    "fireIndex": 45.0
                                }
                            }
                        ]
                    }
                ]
            }
        }

    @pytest.fixture
    def mock_events_response(self):
        """Mock Events API response."""
        return {
            "data": {
                "events": [
                    {
                        "eventType": "hail",
                        "severity": "moderate",
                        "startTime": "2024-07-15T14:30:00Z",
                        "endTime": "2024-07-15T15:00:00Z",
                        "location": {
                            "lat": 29.7604,
                            "lon": -95.3698
                        },
                        "description": "Quarter size hail reported",
                        "impact": "moderate",
                        "certainty": "observed",
                        "urgency": "immediate"
                    }
                ]
            }
        }

    @pytest.mark.asyncio
    async def test_get_current_and_forecast(self, tomorrow_io_service, mock_timeline_response):
        """Test current weather and forecast retrieval."""
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_response = AsyncMock()
            mock_response.json.return_value = mock_timeline_response
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response

            result = await tomorrow_io_service.get_current_and_forecast(29.7604, -95.3698)

            assert result["source"] == "Tomorrow.io"
            assert "current_weather" in result
            assert "forecast" in result

    @pytest.mark.asyncio
    async def test_get_historical_weather(self, tomorrow_io_service, mock_historical_response):
        """Test historical weather retrieval."""
        with patch('httpx.AsyncClient') as mock_client:
            mock_response = AsyncMock()
            mock_response.json = AsyncMock(return_value=mock_historical_response)
            mock_response.raise_for_status = AsyncMock()
            
            mock_client_instance = AsyncMock()
            mock_client_instance.post = AsyncMock(return_value=mock_response)
            mock_client.return_value.__aenter__.return_value = mock_client_instance

            start_date = date(2024, 7, 1)
            end_date = date(2024, 7, 31)
            result = await tomorrow_io_service.get_historical_weather(29.7604, -95.3698, start_date, end_date)

            assert result["source"] == "Tomorrow.io"
            assert "observations" in result
            assert len(result["observations"]) > 0

    @pytest.mark.asyncio
    async def test_get_severe_weather_events(self, tomorrow_io_service, mock_events_response):
        """Test severe weather events retrieval."""
        with patch.object(tomorrow_io_service, '_parse_events_data') as mock_parse:
            mock_parse.return_value = [
                {
                    "event_type": "hail",
                    "severity": "moderate",
                    "start_time": "2024-07-15T14:30:00Z",
                    "end_time": "2024-07-15T15:00:00Z",
                    "latitude": 29.7604,
                    "longitude": -95.3698,
                    "description": "Quarter size hail reported",
                    "impact": "moderate",
                    "certainty": "observed",
                    "urgency": "immediate"
                }
            ]

            start_date = date(2024, 7, 1)
            end_date = date(2024, 7, 31)
            result = await tomorrow_io_service.get_severe_weather_events(29.7604, -95.3698, start_date, end_date)

            assert len(result) > 0
            assert result[0]["event_type"] == "hail"

    @pytest.mark.asyncio
    async def test_get_hail_probability_forecast(self, tomorrow_io_service):
        """Test hail probability forecast retrieval."""
        with patch.object(tomorrow_io_service, '_parse_hail_probability_data') as mock_parse:
            mock_parse.return_value = {
                "hail_probability": [
                    {
                        "timestamp": "2025-01-07T12:00:00Z",
                        "hail_probability": 45.0,
                        "hail_binary": 1,
                        "precipitation_intensity": 2.5
                    }
                ],
                "source": "Tomorrow.io"
            }

            result = await tomorrow_io_service.get_hail_probability_forecast(29.7604, -95.3698)

            assert result["source"] == "Tomorrow.io"
            assert "hail_probability" in result
            assert len(result["hail_probability"]) > 0

    @pytest.mark.asyncio
    async def test_get_fire_risk_data(self, tomorrow_io_service):
        """Test fire risk data retrieval."""
        with patch.object(tomorrow_io_service, '_parse_fire_risk_data') as mock_parse:
            mock_parse.return_value = {
                "fire_index": [
                    {
                        "timestamp": "2025-01-07T00:00:00Z",
                        "fire_index": 75.0,
                        "temperature": 95.0,
                        "humidity": 25.0,
                        "wind_speed": 25.0
                    }
                ],
                "source": "Tomorrow.io"
            }

            result = await tomorrow_io_service.get_fire_risk_data(29.7604, -95.3698)

            assert result["source"] == "Tomorrow.io"
            assert "fire_index" in result
            assert len(result["fire_index"]) > 0

    def test_singleton_pattern(self):
        """Test Tomorrow.io service singleton pattern."""
        service1 = get_tomorrow_io_service()
        service2 = get_tomorrow_io_service()
        assert service1 is service2


class TestWeatherDataServiceIntegration:
    """Test WeatherDataService integration with Tomorrow.io."""

    @pytest.fixture
    def weather_service(self):
        """Create weather data service instance."""
        return WeatherDataService()

    @pytest.mark.asyncio
    async def test_get_current_weather_integration(self, weather_service):
        """Test current weather integration with Tomorrow.io."""
        with patch.object(weather_service.tomorrow_io, 'get_current_and_forecast') as mock_tomorrow:
            mock_tomorrow.return_value = {
                "current_weather": {
                    "temperature": 75.0,
                    "humidity": 60.0,
                    "wind_speed": 12.0,
                    "weather_code": 1000,
                    "timestamp": "2025-01-07T12:00:00Z"
                },
                "forecast": [],
                "source": "Tomorrow.io"
            }

            result = await weather_service.get_current_weather(29.7604, -95.3698, "Houston, TX")

            assert result["source"] == "Tomorrow.io"
            assert result["temperature"] == 75.0

    @pytest.mark.asyncio
    async def test_get_hail_probability(self, weather_service):
        """Test hail probability method."""
        with patch.object(weather_service.tomorrow_io, 'get_hail_probability_forecast') as mock_hail:
            mock_hail.return_value = {
                "hail_probability": [
                    {
                        "timestamp": "2025-01-07T12:00:00Z",
                        "hail_probability": 35.0,
                        "hail_binary": 0,
                        "precipitation_intensity": 1.0
                    }
                ],
                "source": "Tomorrow.io"
            }

            result = await weather_service.get_hail_probability(29.7604, -95.3698)

            assert result["source"] == "Tomorrow.io"
            assert "hail_probability" in result

    @pytest.mark.asyncio
    async def test_get_fire_risk_assessment(self, weather_service):
        """Test fire risk assessment method."""
        with patch.object(weather_service.tomorrow_io, 'get_fire_risk_data') as mock_fire:
            mock_fire.return_value = {
                "fire_index": [
                    {
                        "timestamp": "2025-01-07T00:00:00Z",
                        "fire_index": 65.0,
                        "temperature": 90.0,
                        "humidity": 30.0,
                        "wind_speed": 20.0
                    }
                ],
                "source": "Tomorrow.io"
            }

            result = await weather_service.get_fire_risk_assessment(29.7604, -95.3698)

            assert result["source"] == "Tomorrow.io"
            assert "fire_index" in result

    @pytest.mark.asyncio
    async def test_24_month_historical_analysis(self, weather_service):
        """Test 24-month historical analysis support."""
        with patch.object(weather_service.tomorrow_io, 'get_historical_weather') as mock_historical:
            mock_historical.return_value = {
                "observations": [
                    {
                        "datetime": "2023-01-07T00:00:00Z",
                        "temperature": 45.0,
                        "humidity": 80.0,
                        "wind_speed": 8.0,
                        "precipitation_intensity": 0.0
                    }
                ],
                "source": "Tomorrow.io"
            }

            start_date = date.today() - timedelta(days=730)  # 24 months ago
            end_date = date.today()

            result = await weather_service.get_historical_weather(29.7604, -95.3698, start_date, end_date)

            assert result["source"] == "Tomorrow.io"
            assert "observations" in result
            assert result["analysis_context"]["period_type"] == "24_month"


class TestWeatherThresholdsEnhancement:
    """Test enhanced weather thresholds with Tomorrow.io features."""

    def test_hail_probability_classification(self):
        """Test hail probability risk classification."""
        assert WeatherThresholds.classify_hail_probability(70.0) == "high"
        assert WeatherThresholds.classify_hail_probability(45.0) == "moderate"
        assert WeatherThresholds.classify_hail_probability(15.0) == "low"
        assert WeatherThresholds.classify_hail_probability(5.0) == "minimal"

    def test_fire_index_classification(self):
        """Test fire index danger classification."""
        assert WeatherThresholds.classify_fire_index(85.0) == "extreme"
        assert WeatherThresholds.classify_fire_index(65.0) == "high"
        assert WeatherThresholds.classify_fire_index(45.0) == "moderate"
        assert WeatherThresholds.classify_fire_index(25.0) == "low"
        assert WeatherThresholds.classify_fire_index(10.0) == "minimal"

    def test_hail_probability_actionable(self):
        """Test hail probability actionable threshold."""
        assert WeatherThresholds.is_hail_probability_actionable(35.0) is True
        assert WeatherThresholds.is_hail_probability_actionable(25.0) is False
        assert WeatherThresholds.is_hail_probability_actionable(50.0) is True

    def test_fire_index_actionable(self):
        """Test fire index actionable threshold."""
        assert WeatherThresholds.is_fire_index_actionable(45.0) is True
        assert WeatherThresholds.is_fire_index_actionable(35.0) is False
        assert WeatherThresholds.is_fire_index_actionable(60.0) is True

    def test_enhanced_business_impact_scoring(self):
        """Test enhanced business impact scoring with hail probability and fire index."""
        # Test with hail probability
        score_with_hail_prob = WeatherThresholds.calculate_business_impact_score(
            hail_probability=65.0
        )
        assert score_with_hail_prob > 0

        # Test with fire index
        score_with_fire = WeatherThresholds.calculate_business_impact_score(
            fire_index=75.0
        )
        assert score_with_fire > 0

        # Test combined scoring
        score_combined = WeatherThresholds.calculate_business_impact_score(
            hail_size=1.5,
            wind_speed=65.0,
            hail_probability=40.0,
            fire_index=50.0
        )
        assert score_combined > 50  # Should be significant with multiple factors


class TestDataConversion:
    """Test Tomorrow.io data format conversion."""

    def test_weather_code_conversion(self):
        """Test weather code to description conversion."""
        service = WeatherDataService()
        
        assert service._get_weather_description(1000) == "Clear"
        assert service._get_weather_description(4001) == "Rain"
        assert service._get_weather_description(8000) == "Thunderstorm"
        assert service._get_weather_description(9999) == "Unknown"

    def test_forecast_period_naming(self):
        """Test forecast period name generation."""
        service = WeatherDataService()
        
        # Test with valid timestamp
        timestamp = "2025-01-07T12:00:00Z"
        period_name = service._get_forecast_period_name(timestamp)
        assert period_name == "Tuesday"  # January 7, 2025 is a Tuesday

        # Test with invalid timestamp
        invalid_name = service._get_forecast_period_name(None)
        assert invalid_name == "Unknown"

    def test_detailed_forecast_generation(self):
        """Test detailed forecast description generation."""
        service = WeatherDataService()
        
        period_data = {
            "temperature": 75.0,
            "precipitation_probability": 30.0,
            "wind_speed": 15.0,
            "weather_code": 1000
        }
        
        forecast = service._get_detailed_forecast(period_data)
        assert "Clear" in forecast
        assert "75.0°F" in forecast
        assert "30.0% chance of precipitation" in forecast


if __name__ == "__main__":
    pytest.main([__file__])
