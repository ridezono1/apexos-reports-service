# Weather Reports Service Tests

Comprehensive test suite for the weather reports service.

## Test Coverage

### Service Tests
- **test_geocoding_service.py** - Geocoding and address resolution
  - Address geocoding (10211 Peytons Grace Ln, Cypress, TX)
  - Reverse geocoding
  - Address autocomplete

- **test_weather_data_service.py** - Weather data fetching and analysis
  - Current weather conditions
  - Weather forecasts
  - Historical weather (6-month and 9-month periods)
  - Weather events and alerts
  - Trend analysis and drought detection

- **test_spatial_analysis_service.py** - Spatial weather analysis
  - Radius boundary analysis
  - City/county boundary analysis
  - Risk assessment
  - Route optimization
  - Point-in-polygon calculations

- **test_address_analysis_service.py** - Property-specific analysis
  - Property risk assessment
  - Business impact analysis
  - Lead qualification
  - Historical context analysis

### API Tests
- **test_api_geocoding.py** - Geocoding API endpoints
  - `/api/v1/geocoding/autocomplete`
  - `/api/v1/geocoding/geocode`
  - `/api/v1/geocoding/reverse-geocode`

- **test_api_reports.py** - Report generation API endpoints
  - `/api/v1/reports/generate/weather`
  - `/api/v1/reports/generate/spatial`
  - `/api/v1/reports/{id}/status`
  - `/api/v1/reports/{id}/download`
  - `/api/v1/reports/templates`

## Running Tests

### All Tests
```bash
pytest
```

### Specific Test File
```bash
pytest tests/test_geocoding_service.py
```

### Specific Test Function
```bash
pytest tests/test_geocoding_service.py::test_geocode_cypress_tx_address
```

### With Verbose Output
```bash
pytest -v
```

### With Output Capture Disabled (see print statements)
```bash
pytest -s
```

### By Marker
```bash
# Run only API tests
pytest -m api

# Run only async tests
pytest -m asyncio

# Run only weather-related tests
pytest -m weather
```

### With Coverage Report
```bash
pytest --cov=app --cov-report=html
```

## Test Markers

Tests are marked with the following markers for easy filtering:

- `asyncio` - Async tests
- `unit` - Unit tests
- `integration` - Integration tests
- `api` - API endpoint tests
- `slow` - Slow-running tests
- `geocoding` - Geocoding-related tests
- `weather` - Weather data tests
- `spatial` - Spatial analysis tests
- `reports` - Report generation tests

## Test Data

All tests use real coordinates for Cypress, TX:
- **Address**: 10211 Peytons Grace Ln, Cypress, TX 77433
- **Latitude**: 29.9924
- **Longitude**: -95.6981

## Requirements

Ensure you have the test dependencies installed:

```bash
pip install pytest pytest-asyncio httpx
```

## Environment Variables

Some tests require API keys:
- `GOOGLE_MAPS_API_KEY` - For geocoding tests

If not set, tests will use fallback services or skip gracefully.

## Continuous Integration

Tests are designed to run in CI/CD pipelines:

```bash
# Quick test run (excludes slow tests)
pytest -m "not slow"

# Full test suite
pytest
```

## Writing New Tests

1. Create test file in `tests/` directory with prefix `test_`
2. Use fixtures from `conftest.py` for common test data
3. Mark tests appropriately with pytest markers
4. Follow naming convention: `test_<functionality>`
5. Use descriptive assertions and error messages
6. Add docstrings explaining what the test validates

Example:
```python
import pytest

@pytest.mark.asyncio
@pytest.mark.weather
async def test_get_weather_for_location(weather_service, cypress_tx_coordinates):
    \"\"\"Test fetching current weather for a specific location.\"\"\"
    result = await weather_service.get_current_weather(
        latitude=cypress_tx_coordinates["latitude"],
        longitude=cypress_tx_coordinates["longitude"]
    )

    assert result is not None
    assert "temperature" in result
    assert result["source"] == "NOAA"
```

## Troubleshooting

### Tests timing out
Increase timeout in pytest.ini or use `@pytest.mark.timeout(seconds)`

### API rate limits
Tests use free APIs (NOAA, OpenStreetMap) which have rate limits. Add delays between tests if needed.

### Missing dependencies
```bash
pip install -r requirements.txt
```

### Async event loop errors
Ensure pytest-asyncio is installed and configured properly in pytest.ini
