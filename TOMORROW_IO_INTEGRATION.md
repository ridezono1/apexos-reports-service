# Tomorrow.io Weather API Integration

This document describes the integration of Tomorrow.io Weather API into the Reports Service, providing comprehensive weather data for address and spatial reports.

## Overview

The Reports Service now uses Tomorrow.io Weather API as the primary weather data source, replacing NOAA APIs. This integration provides:

- **Current Weather**: Real-time conditions with detailed metrics
- **Forecasts**: Up to 15-day weather forecasts
- **Historical Data**: Up to 24 months of historical weather observations
- **Severe Weather Events**: Comprehensive severe weather event tracking
- **Hail Probability**: Probabilistic hail forecasting
- **Fire Risk Assessment**: Fire danger index and risk assessment
- **Weather Maps**: Professional weather map visualizations

## API Endpoints Used

### Timeline API
- **Endpoint**: `POST /v4/timelines`
- **Purpose**: Current weather and forecasts
- **Data Layers**: 
  - Core: temperature, precipitation, windSpeed, windGust, weatherCode
  - Advanced: precipitationType, hailBinary, hailProbability
  - Fire: fireIndex
- **Timesteps**: 1h (hourly), 1d (daily)
- **Coverage**: Current + 15 days forward, 6 hours back

### Historical API
- **Endpoint**: `POST /v4/timelines`
- **Purpose**: Historical weather observations
- **Data Layers**: Same as Timeline API
- **Timesteps**: 1d (daily)
- **Coverage**: Up to 24 months of historical data

### Events API
- **Endpoint**: `POST /v4/events`
- **Purpose**: Severe weather events
- **Event Types**: hail, wind, tornado, fire, flood, winter, thunderstorm
- **Coverage**: Historical severe weather events with location and severity data

### Weather Maps API
- **Endpoint**: `GET /v4/map`
- **Purpose**: Weather map tiles
- **Layers**: precipitation, temperature, wind, fire
- **Coverage**: Professional weather visualizations

## Configuration

### Environment Variables

```bash
# Required
TOMORROW_IO_API_KEY=FcKkOlKqwCs8nHtx7InegJVWS8wJuem3

# Optional (with defaults)
TOMORROW_IO_BASE_URL=https://api.tomorrow.io/v4
TOMORROW_IO_TIMEOUT=30
TOMORROW_IO_MAX_RETRIES=3
```

### Settings Configuration

The Tomorrow.io API configuration is managed in `app/core/config.py`:

```python
# Tomorrow.io Weather API configuration
tomorrow_io_api_key: Optional[str] = None
tomorrow_io_base_url: str = "https://api.tomorrow.io/v4"
tomorrow_io_timeout: int = 30
tomorrow_io_max_retries: int = 3
```

## Data Layer Selection

### Core Weather Data
- `temperature` - Air temperature
- `temperatureApparent` - Feels-like temperature
- `humidity` - Relative humidity
- `precipitationIntensity` - Precipitation rate
- `precipitationProbability` - Chance of precipitation
- `precipitationType` - Type of precipitation
- `windSpeed` - Wind speed
- `windGust` - Wind gust speed
- `windDirection` - Wind direction
- `weatherCode` - Weather condition code
- `weatherCodeFull` - Detailed weather description
- `visibility` - Visibility distance
- `pressureSurfaceLevel` - Atmospheric pressure
- `cloudCover` - Cloud coverage percentage
- `uvIndex` - UV index

### Advanced Features
- `hailBinary` - Binary hail indicator
- `hailProbability` - Hail probability percentage
- `fireIndex` - Fire danger index (0-100)

## New Features

### Hail Probability Forecasting
- **Thresholds**:
  - High Risk: ≥60% probability
  - Moderate Risk: ≥30% probability
  - Low Risk: ≥10% probability
- **Business Impact**: Probabilistic risk assessment for property damage
- **Forecast Period**: Up to 15 days ahead

### Fire Risk Assessment
- **Index Scale**: 0-100 (0 = minimal, 100 = extreme)
- **Thresholds**:
  - Extreme: ≥80
  - High: ≥60
  - Moderate: ≥40
  - Low: ≥20
- **Business Impact**: Fire danger assessment for property protection

### 24-Month Historical Analysis
- **Coverage**: Extended historical analysis periods
- **Data**: Daily weather observations
- **Analysis**: Multi-year weather pattern assessment
- **Use Cases**: Long-term trend analysis, seasonal pattern recognition

## Service Architecture

### TomorrowIOService
- **Location**: `app/services/tomorrow_io_service.py`
- **Purpose**: Direct API client for Tomorrow.io endpoints
- **Methods**:
  - `get_current_and_forecast()` - Timeline API integration
  - `get_historical_weather()` - Historical API integration
  - `get_severe_weather_events()` - Events API integration
  - `get_hail_probability_forecast()` - Hail probability data
  - `get_fire_risk_data()` - Fire risk assessment
  - `get_weather_map_tiles()` - Weather maps integration

### WeatherDataService Integration
- **Location**: `app/services/weather_data_service.py`
- **Changes**: Refactored to use Tomorrow.io instead of NOAA
- **New Methods**:
  - `get_hail_probability()` - Hail probability forecasting
  - `get_fire_risk_assessment()` - Fire risk assessment
- **Enhanced Methods**:
  - `get_historical_weather()` - Now supports 24-month periods
  - `get_weather_events()` - Uses Tomorrow.io Events API

### Weather Thresholds Enhancement
- **Location**: `app/core/weather_thresholds.py`
- **New Thresholds**:
  - Hail probability thresholds (10%, 30%, 60%)
  - Fire index thresholds (20, 40, 60, 80)
- **New Methods**:
  - `classify_hail_probability()` - Risk level classification
  - `classify_fire_index()` - Fire danger classification
  - `is_hail_probability_actionable()` - Actionable risk check
  - `is_fire_index_actionable()` - Actionable fire danger check
- **Enhanced Business Impact Scoring**:
  - Includes hail probability and fire index factors
  - Probabilistic risk assessment

## Report Integration

### Address Reports
- **Enhanced Data**: Hail probability forecasts, fire risk assessment
- **24-Month Analysis**: Extended historical trend analysis
- **Business Impact**: Enhanced scoring with probabilistic factors

### Spatial Reports
- **Fire Risk Heat Maps**: Fire danger visualization
- **Severe Weather Events**: Tomorrow.io event data integration
- **24-Month Patterns**: Multi-year weather pattern analysis

## Error Handling

### API Resilience
- **Timeout**: 30-second timeout for API requests
- **Retries**: Up to 3 retry attempts with exponential backoff
- **Fallback**: Graceful degradation to empty data structures
- **Logging**: Comprehensive error logging with Sentry integration

### Data Validation
- **Response Parsing**: Robust parsing of Tomorrow.io API responses
- **Data Conversion**: Conversion to expected internal data formats
- **Error Recovery**: Fallback data when API requests fail

## Rate Limiting

### Tomorrow.io Limits
- **Free Tier**: 100 calls/day, 10 calls/minute
- **Paid Tiers**: Higher limits available
- **Best Practices**:
  - Batch requests when possible
  - Cache data appropriately
  - Monitor usage patterns

### Implementation
- **Request Batching**: Multiple data layers in single requests
- **Caching**: Weather map tiles cached locally
- **Monitoring**: API usage tracking and alerting

## Testing

### Unit Tests
- **Location**: `tests/test_tomorrow_io_service.py`
- **Coverage**: All Tomorrow.io service methods
- **Mocking**: API response mocking for reliable testing

### Integration Tests
- **Real API Calls**: Tests with actual Tomorrow.io API
- **Data Validation**: Verify data parsing and conversion
- **Error Handling**: Test failure scenarios and fallbacks

### Test Data
- **Sample Responses**: Mock Tomorrow.io API responses
- **Edge Cases**: Empty responses, malformed data
- **Error Scenarios**: Network failures, API errors

## Troubleshooting

### Common Issues

#### API Key Issues
- **Error**: "Tomorrow.io API key not configured"
- **Solution**: Set `TOMORROW_IO_API_KEY` environment variable
- **Verification**: Check API key validity in Tomorrow.io dashboard

#### Rate Limiting
- **Error**: HTTP 429 (Too Many Requests)
- **Solution**: Implement request throttling or upgrade API plan
- **Monitoring**: Track API usage patterns

#### Data Parsing Errors
- **Error**: "Error parsing Tomorrow.io data"
- **Solution**: Check API response format changes
- **Debugging**: Enable debug logging for response inspection

#### Network Timeouts
- **Error**: "Timeout fetching Tomorrow.io data"
- **Solution**: Increase timeout settings or check network connectivity
- **Fallback**: Service gracefully falls back to empty data

### Debugging

#### Enable Debug Logging
```python
import logging
logging.getLogger("app.services.tomorrow_io_service").setLevel(logging.DEBUG)
```

#### API Response Inspection
- Log raw API responses for debugging
- Verify data layer availability
- Check request parameters

#### Performance Monitoring
- Track API response times
- Monitor error rates
- Analyze usage patterns

## Migration from NOAA

### Data Source Changes
- **From**: NOAA Weather API (free)
- **To**: Tomorrow.io Weather API (paid)
- **Benefits**: More comprehensive data, better reliability, advanced features

### Data Format Changes
- **Weather Codes**: Tomorrow.io uses different weather code system
- **Units**: Imperial units maintained for consistency
- **Timestamps**: ISO 8601 format with timezone support

### Feature Enhancements
- **Hail Probability**: New probabilistic forecasting
- **Fire Risk**: New fire danger assessment
- **24-Month Analysis**: Extended historical coverage
- **Professional Maps**: Weather map visualizations

## Best Practices

### API Usage
- **Batch Requests**: Request multiple data layers together
- **Cache Data**: Cache weather map tiles and static data
- **Monitor Usage**: Track API calls and costs
- **Error Handling**: Implement robust error handling

### Data Processing
- **Validate Data**: Check data quality and completeness
- **Handle Missing Data**: Graceful handling of missing fields
- **Convert Units**: Ensure consistent unit usage
- **Parse Timestamps**: Handle timezone conversions properly

### Performance
- **Async Operations**: Use async/await for API calls
- **Connection Pooling**: Reuse HTTP connections
- **Timeout Management**: Set appropriate timeouts
- **Retry Logic**: Implement exponential backoff

## Future Enhancements

### Planned Features
- **Weather Maps Integration**: Full weather map tile integration
- **Real-time Alerts**: Webhook-based weather alerts
- **Advanced Analytics**: Machine learning weather insights
- **Custom Thresholds**: User-configurable risk thresholds

### API Improvements
- **Caching Layer**: Redis-based response caching
- **Batch Processing**: Bulk weather data requests
- **Webhook Support**: Real-time weather event notifications
- **Custom Layers**: Additional data layer support

---

**Last Updated**: 2025-01-07
**API Version**: Tomorrow.io v4
**Integration Status**: Complete
