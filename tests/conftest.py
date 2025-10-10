"""
Pytest configuration and shared fixtures.
"""

import pytest
import asyncio
from typing import Generator


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def cypress_tx_coordinates():
    """Coordinates for Cypress, TX (10211 Peytons Grace Ln)."""
    return {
        "latitude": 29.9924,
        "longitude": -95.6981,
        "address": "10211 Peytons Grace Ln, Cypress, TX 77433"
    }


@pytest.fixture
def houston_coordinates():
    """Coordinates for Houston, TX downtown."""
    return {
        "latitude": 29.7604,
        "longitude": -95.3698,
        "address": "Houston, TX"
    }


@pytest.fixture
def dallas_coordinates():
    """Coordinates for Dallas, TX."""
    return {
        "latitude": 32.7767,
        "longitude": -96.7970,
        "address": "Dallas, TX"
    }


@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing."""
    return {
        "current_weather": {
            "temperature": 75.0,
            "temperature_unit": "F",
            "humidity": 65.0,
            "pressure": 1013.25,
            "wind_speed": 10.0,
            "wind_direction": "SSW",
            "weather_condition": "Partly Cloudy",
            "timestamp": "2024-01-15T12:00:00Z"
        },
        "forecast": {
            "forecasts": [
                {
                    "name": "This Afternoon",
                    "temperature": 78,
                    "temperature_unit": "F",
                    "wind_speed": "10 mph",
                    "short_forecast": "Partly Cloudy",
                    "detailed_forecast": "Partly cloudy skies with a high near 78."
                },
                {
                    "name": "Tonight",
                    "temperature": 62,
                    "temperature_unit": "F",
                    "wind_speed": "5 mph",
                    "short_forecast": "Mostly Clear",
                    "detailed_forecast": "Mostly clear skies with a low around 62."
                }
            ]
        },
        "historical_weather": {
            "observations": [
                {"temperature": 70, "precipitation": 0.5, "wind_speed": 8},
                {"temperature": 72, "precipitation": 1.0, "wind_speed": 10},
                {"temperature": 68, "precipitation": 0.0, "wind_speed": 6}
            ]
        },
        "weather_events": [
            {
                "id": "event1",
                "event": "Thunderstorm Warning",
                "severity": "Moderate",
                "areas": "Harris County",
                "description": "Thunderstorms expected"
            }
        ]
    }


@pytest.fixture
def sample_geocode_result():
    """Sample geocode result for testing."""
    return {
        "formatted_address": "10211 Peytons Grace Ln, Cypress, TX 77433, USA",
        "latitude": 29.9924,
        "longitude": -95.6981,
        "place_id": "ChIJTest123",
        "location_type": "ROOFTOP",
        "street_number": "10211",
        "route": "Peytons Grace Ln",
        "locality": "Cypress",
        "administrative_area_level_1": "Texas",
        "state_code": "TX",
        "postal_code": "77433",
        "country": "United States",
        "country_code": "US"
    }


@pytest.fixture
def analysis_options_6_month():
    """Standard analysis options for 6-month period."""
    return {
        "risk_factors": ["hail", "wind", "flooding", "tornado"],
        "include_business_impact": True,
        "include_lead_qualification": True,
        "include_charts": True,
        "include_forecast": True,
        "include_storm_events": True
    }


@pytest.fixture
def analysis_options_9_month():
    """Standard analysis options for 9-month period."""
    return {
        "risk_factors": ["hail", "wind", "flooding", "tornado"],
        "include_business_impact": True,
        "include_lead_qualification": True,
        "include_charts": True,
        "include_forecast": True,
        "include_storm_events": True
    }


@pytest.fixture
def spatial_analysis_options():
    """Standard options for spatial analysis."""
    return {
        "risk_factors": ["hail", "wind", "tornado"],
        "storm_types": ["thunderstorm", "tornado", "hail"],
        "include_risk_assessment": True,
        "include_route_optimization": True,
        "include_heat_maps": True
    }


@pytest.fixture
def mock_boundary_radius():
    """Mock radius boundary for testing."""
    return {
        "center_lat": 29.9924,
        "center_lon": -95.6981,
        "radius_km": 5.0
    }


@pytest.fixture
def mock_boundary_polygon():
    """Mock polygon boundary for testing."""
    return {
        "coordinates": [
            (29.99, -95.69),
            (30.00, -95.69),
            (30.00, -95.70),
            (29.99, -95.70),
            (29.99, -95.69)
        ]
    }


# pytest-asyncio configuration
pytest_plugins = ('pytest_asyncio',)
