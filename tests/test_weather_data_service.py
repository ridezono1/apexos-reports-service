"""
Tests for the weather data service.
"""

import pytest
from datetime import date, timedelta
from app.services.weather_data_service import WeatherDataService


@pytest.fixture
def weather_service():
    """Create weather data service instance."""
    return WeatherDataService()


@pytest.fixture
def cypress_coordinates():
    """Coordinates for Cypress, TX (10211 Peytons Grace Ln)."""
    return {
        "latitude": 29.9924,
        "longitude": -95.6981
    }


@pytest.mark.asyncio
async def test_get_current_weather_cypress(weather_service, cypress_coordinates):
    """Test getting current weather for Cypress, TX."""
    result = await weather_service.get_current_weather(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"],
        location_name="Cypress, TX"
    )

    # Verify we got data back
    assert result is not None
    assert "source" in result
    assert result["source"] == "NOAA"

    # Verify basic weather fields are present
    assert "temperature" in result or "detailed_forecast" in result

    print(f"\nCurrent weather for Cypress, TX:")
    print(f"Temperature: {result.get('temperature')} {result.get('temperature_unit', '')}")
    print(f"Forecast: {result.get('short_forecast', 'N/A')}")
    print(f"Wind: {result.get('wind_speed', 'N/A')} from {result.get('wind_direction', 'N/A')}")


@pytest.mark.asyncio
async def test_get_weather_forecast_cypress(weather_service, cypress_coordinates):
    """Test getting weather forecast for Cypress, TX."""
    result = await weather_service.get_weather_forecast(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"],
        days=7
    )

    # Verify we got forecast data
    assert result is not None
    assert "forecasts" in result
    assert "source" in result
    assert result["source"] == "NOAA"

    # Should have multiple forecast periods
    forecasts = result["forecasts"]
    assert len(forecasts) > 0
    assert len(forecasts) <= 14  # 7 days * 2 periods per day

    # Verify forecast structure
    first_forecast = forecasts[0]
    assert "name" in first_forecast
    assert "temperature" in first_forecast
    assert "detailed_forecast" in first_forecast

    print(f"\n7-day forecast for Cypress, TX:")
    for forecast in forecasts[:3]:  # Print first 3 periods
        print(f"{forecast['name']}: {forecast['temperature']}°{forecast.get('temperature_unit', 'F')} - {forecast.get('short_forecast')}")


@pytest.mark.asyncio
async def test_get_historical_weather_6_months(weather_service, cypress_coordinates):
    """Test getting 6-month historical weather data."""
    end_date = date.today()
    start_date = end_date - timedelta(days=182)  # 6 months

    result = await weather_service.get_historical_weather(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"],
        start_date=start_date,
        end_date=end_date
    )

    # Verify we got historical data
    assert result is not None
    assert "observations" in result
    assert "source" in result

    # Verify analysis fields
    assert "trend_analysis" in result
    assert "seasonal_analysis" in result
    assert "period_analysis" in result
    assert "analysis_context" in result

    # Verify period analysis
    period_analysis = result["period_analysis"]
    assert period_analysis["period_type"] == "6_month"
    assert 180 <= period_analysis["period_days"] <= 185

    # Verify analysis context
    context = result["analysis_context"]
    assert context["period_type"] == "6_month"
    assert context["analysis_date"] == end_date.isoformat()

    print(f"\n6-month historical weather for Cypress, TX:")
    print(f"Observations: {len(result['observations'])}")
    print(f"Period: {start_date} to {end_date}")
    print(f"Period type: {period_analysis['period_type']}")

    if "temperature" in period_analysis:
        print(f"Temperature avg: {period_analysis['temperature']['average']:.1f}°")
        print(f"Temperature range: {period_analysis['temperature']['min']:.1f}° to {period_analysis['temperature']['max']:.1f}°")


@pytest.mark.asyncio
async def test_get_historical_weather_9_months(weather_service, cypress_coordinates):
    """Test getting 9-month historical weather data."""
    end_date = date.today()
    start_date = end_date - timedelta(days=274)  # 9 months

    result = await weather_service.get_historical_weather(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"],
        start_date=start_date,
        end_date=end_date
    )

    # Verify we got historical data
    assert result is not None
    assert "observations" in result

    # Verify period analysis
    period_analysis = result["period_analysis"]
    assert period_analysis["period_type"] == "9_month"
    assert 270 <= period_analysis["period_days"] <= 275

    # Verify analysis context
    context = result["analysis_context"]
    assert context["period_type"] == "9_month"

    print(f"\n9-month historical weather for Cypress, TX:")
    print(f"Period type: {period_analysis['period_type']}")
    print(f"Total observations: {period_analysis.get('total_observations', 0)}")


@pytest.mark.asyncio
async def test_historical_weather_invalid_period(weather_service, cypress_coordinates):
    """Test that invalid period raises error."""
    end_date = date.today()
    start_date = end_date - timedelta(days=90)  # 3 months - invalid

    with pytest.raises(ValueError, match="Analysis period must be exactly 6 months or 9 months"):
        await weather_service.get_historical_weather(
            latitude=cypress_coordinates["latitude"],
            longitude=cypress_coordinates["longitude"],
            start_date=start_date,
            end_date=end_date
        )


@pytest.mark.asyncio
async def test_get_weather_events_cypress(weather_service, cypress_coordinates):
    """Test getting weather events for Cypress, TX."""
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    result = await weather_service.get_weather_events(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"],
        start_date=start_date,
        end_date=end_date,
        radius_km=50.0
    )

    # Verify we got a list of events (may be empty)
    assert isinstance(result, list)

    print(f"\nWeather events for Cypress, TX (last 30 days):")
    print(f"Total events: {len(result)}")

    if result:
        for event in result[:3]:  # Print first 3 events
            print(f"- {event.get('event')}: {event.get('severity')} ({event.get('areas')})")


@pytest.mark.asyncio
async def test_find_nearest_noaa_station(weather_service, cypress_coordinates):
    """Test finding nearest NOAA weather station."""
    station_id = await weather_service._find_nearest_noaa_station(
        latitude=cypress_coordinates["latitude"],
        longitude=cypress_coordinates["longitude"]
    )

    # Should find a station
    assert station_id is not None
    assert isinstance(station_id, str)
    assert len(station_id) > 0

    print(f"\nNearest NOAA station to Cypress, TX: {station_id}")


def test_calculate_trend_slope(weather_service):
    """Test trend slope calculation."""
    # Increasing trend
    increasing_values = [1.0, 2.0, 3.0, 4.0, 5.0]
    slope = weather_service._calculate_trend_slope(increasing_values)
    assert slope > 0

    # Decreasing trend
    decreasing_values = [5.0, 4.0, 3.0, 2.0, 1.0]
    slope = weather_service._calculate_trend_slope(decreasing_values)
    assert slope < 0

    # Stable trend
    stable_values = [3.0, 3.0, 3.0, 3.0, 3.0]
    slope = weather_service._calculate_trend_slope(stable_values)
    assert abs(slope) < 0.1


def test_calculate_variability(weather_service):
    """Test variability calculation."""
    # Low variability
    low_var = [10.0, 10.1, 9.9, 10.0, 10.1]
    variability = weather_service._calculate_variability(low_var)
    assert variability < 1.0

    # High variability
    high_var = [1.0, 10.0, 5.0, 20.0, 3.0]
    variability = weather_service._calculate_variability(high_var)
    assert variability > 1.0


def test_identify_drought_periods(weather_service):
    """Test drought period identification."""
    # Create precipitation data with a drought period
    precipitation = [
        5.0, 3.0, 2.0,  # Normal
        0.5, 0.2, 0.1, 0.3, 0.4, 0.2, 0.1, 0.5,  # 8-day drought
        10.0, 5.0, 3.0  # Normal again
    ]

    drought_periods = weather_service._identify_drought_periods(precipitation)

    # Should identify one drought period
    assert len(drought_periods) >= 1

    first_drought = drought_periods[0]
    assert first_drought["length_days"] >= 7
    assert first_drought["severity"] in ["moderate", "severe"]

    print(f"\nDrought periods identified: {len(drought_periods)}")
    for i, drought in enumerate(drought_periods):
        print(f"Drought {i+1}: {drought['length_days']} days ({drought['severity']})")
