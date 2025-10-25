# Reports Service

A scalable FastAPI microservice for generating professional reports. Currently supports weather and spatial reports, with an extensible architecture for additional report types.

## Features

- **Weather Reports**: Generate comprehensive weather reports with current conditions, forecasts, and historical data
- **Spatial Reports**: Generate area-based reports for cities, counties, and regions with heat maps and risk zones
- **Geocoding Services**: Address autocomplete, geocoding, and reverse geocoding with Google Maps API
- **Multiple Formats**: PDF (ReportLab), Excel, CSV, and JSON output formats
- **Map Integration**: Satellite maps, street maps, weather events maps, and risk assessment maps
- **Template Engine**: Customizable HTML templates with Jinja2
- **Professional Styling**: Branded reports with charts, tables, and visualizations
- **File Storage**: S3 integration for generated report storage
- **Status Tracking**: Real-time report generation status
- **Industry-Standard Thresholds**: Industry-based hail (≥1") and wind (≥60 mph) thresholds for actionable events

## Report Types

### Weather Reports
- **Weather Summary**: Comprehensive weather analysis with current conditions and forecasts
- **Storm Analysis**: Storm-focused analysis with impact assessment and severity scoring
- **Alert Report**: Weather alerts and warnings analysis
- **Weather Trends**: Historical weather trends and pattern analysis
- **Impact Assessment**: Property and area impact analysis

### Spatial Reports
- **Spatial Analysis**: Geographic boundary-based reports with heat maps
- **Risk Assessment**: Risk zone mapping and analysis
- **Boundary Visualization**: Administrative boundary analysis

## Deployment

This reports service is part of the ApexOS monorepo (converted from git submodule to regular directory on Oct 9, 2025) and deployed as a standalone microservice.

### Automated Deployment (CI/CD) - Recommended

The service uses **container-based deployment** via GitHub Actions:

- **Triggers**: Automatically deploys when changes are pushed to `reports-service/**` files on the `main` branch
- **Method**: Docker container built and pushed to Heroku Container Registry
- **Stack**: `container` (platform-independent)
- **URL**: https://apexos-reports-service-ce3abe785e88.herokuapp.com

The GitHub Actions workflow (`.github/workflows/deploy-reports-service.yml`) handles:
1. Docker image build with multi-stage optimization
2. Push to Heroku Container Registry
3. Container release
4. Health check verification with retry logic

### Manual Deployment Options

**Option 1: Container-based (recommended for consistency with CI/CD)**
```bash
# From the main ApexOS repository root
cd reports-service

# Deploy via Heroku CLI (container stack)
heroku container:push web --app apexos-reports-service
heroku container:release web --app apexos-reports-service
```

**Option 2: Trigger GitHub Actions workflow manually**
```bash
gh workflow run deploy-reports-service.yml
```

**Option 3: Legacy deployment script (uses git push, not recommended)**
```bash
# From the main ApexOS repository root
./deploy-reports-service.sh
```
Note: This script creates a temporary git repo and pushes directly. Since the stack is now `container`, it's better to use container-based deployment methods above.

## Quick Start

### Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --port 8000
```

### Docker

```bash
# Build and run
docker-compose up --build
```

## Cache Management

The Reports Service implements intelligent caching strategies for optimal performance and data freshness.

### Cache Architecture

#### 1. **DuckDB Performance Optimization**
- **Technology**: DuckDB columnar analytics engine
- **Performance**: 10-30x faster than csv.DictReader
- **Query Time**: 8-15s → 0.5-1.5s for 24-month reports
- **Fallback**: Automatic fallback to csv.DictReader if DuckDB unavailable

#### 2. **Age-Based Cache Duration**
- **Historical Years (>2 years old)**: 30 days cache
- **Previous Year (1-2 years old)**: 7 days cache  
- **Current Year (last 12 months)**: 24 hours cache
- **Configuration**: Adjustable via environment variables

#### 3. **CSV Auto-Discovery**
- **Source**: NOAA Storm Events Database FTP directory
- **URL**: `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`
- **Fallback**: Hardcoded URLs when auto-discovery fails
- **Pattern**: `StormEvents_details-ftp_v1.0_d{year}_c{compilation_date}.csv.gz`

#### 4. **Background Cache Refresh**
- **Schedule**: Daily at 2:00 AM UTC (configurable)
- **Cleanup**: Weekly cleanup of files older than 60 days
- **Warmup**: Optional cache warmup on service startup
- **Technology**: APScheduler with graceful fallbacks

#### 5. **Graceful Fallback Mechanisms**
- **Primary**: Auto-discovered latest CSV files
- **Secondary**: Previous month's compilation date
- **Tertiary**: Stale cache with warnings
- **Last Resort**: Hardcoded URLs

### Hybrid Data Strategy

#### **100% Data Coverage for 24-Month Reports**

| Period | Data Source | Quality | Coverage |
|--------|-------------|---------|----------|
| **Historical (>120 days)** | NOAA Storm Events Database | ✅ Verified | 100% |
| **Recent (last 120 days)** | NWS SPC Preliminary Reports | ⚠️ Preliminary | 100% |
| **Current** | NWS Active Alerts | 🔄 Real-time | 100% |

#### **Data Quality Indicators**
- **Verified** (green): Official NOAA data, fully verified
- **Preliminary** (orange, italic): Same-day preliminary reports
- **Current** (blue): Real-time active alerts

### Cache Configuration

#### Environment Variables
```bash
# Cache duration settings
CACHE_DURATION_HISTORICAL_DAYS=30
CACHE_DURATION_PREVIOUS_YEAR_DAYS=7
CACHE_DURATION_CURRENT_YEAR_HOURS=24

# Background refresh settings
CACHE_WARMUP_ON_STARTUP=true
```

#### Cache Directory Structure
```
/tmp/reports/
├── storm_events_cache/
│   ├── storm_events_2022.csv
│   ├── storm_events_2023.csv
│   └── storm_events_2024.csv
└── spc_reports_cache/
    ├── spc_reports_20240101.csv
    └── spc_reports_20240102.csv
```

### Performance Metrics

#### Before Optimization
- **Query Time**: 8-15 seconds
- **Data Coverage**: 83.6% (610/730 days)
- **Cache Strategy**: 24-hour fixed TTL
- **Data Sources**: Storm Events only

#### After Optimization
- **Query Time**: 0.5-1.5 seconds (**10-30x faster**)
- **Data Coverage**: 100% (730/730 days) (**+16.4%**)
- **Cache Strategy**: Age-based intelligent TTL
- **Data Sources**: Storm Events + SPC + Alerts

### Monitoring and Maintenance

#### Health Checks
- **Cache Status**: `/health` endpoint includes cache metrics
- **Background Jobs**: Scheduler status monitoring
- **Data Freshness**: Automatic freshness date calculation

#### Troubleshooting
- **Cache Misses**: Check network connectivity to NOAA FTP
- **Stale Data**: Verify background refresh job execution
- **Performance Issues**: Monitor DuckDB availability and fallback usage

#### Manual Cache Management
```python
# Force cache refresh
from app.services.background_cache_refresh_service import get_cache_refresh_service
cache_service = get_cache_refresh_service()
await cache_service.warmup_cache([2023, 2024])

# Check scheduler status
status = cache_service.get_scheduler_status()
print(f"Scheduler running: {status['running']}")
```

## API Endpoints

### Reports
- `POST /api/v1/reports/generate/weather` - Generate weather report
- `POST /api/v1/reports/generate/spatial` - Generate spatial report
- `GET /api/v1/reports/{report_id}/status` - Check report status
- `GET /api/v1/reports/{report_id}/download` - Download generated report
- `GET /api/v1/templates` - List available templates
- `DELETE /api/v1/reports/{report_id}` - Delete report

### Geocoding
- `GET /api/v1/geocoding/autocomplete` - Address autocomplete suggestions
- `POST /api/v1/geocoding/geocode` - Geocode address or place ID
- `POST /api/v1/geocoding/reverse-geocode` - Reverse geocode coordinates

## Weather Thresholds

The service uses industry-standard thresholds based on industry standards for identifying actionable weather events:

### Hail Thresholds
- **Actionable**: ≥ 1.0 inch (quarter size)
- **Severe**: ≥ 1.0 inch
- **Extreme**: ≥ 2.0 inches (tennis ball or larger)

### Wind Thresholds
- **Actionable**: ≥ 60 mph (damaging winds)
- **Severe**: ≥ 60 mph
- **Extreme**: ≥ 80 mph

These thresholds identify weather events that represent business opportunities for property damage assessment and roof inspections. See [WEATHER_THRESHOLDS.md](WEATHER_THRESHOLDS.md) for complete documentation.

## Environment Variables

- `GOOGLE_MAPS_API_KEY` - Google Maps API key for geocoding services
- `TOMORROW_IO_API_KEY` - Tomorrow.io Weather API key for weather data
- `ALERT_HAIL_MIN_SIZE_INCHES` - Minimum actionable hail size (default: 1.0)
- `ALERT_WIND_MIN_SPEED_MPH` - Minimum actionable wind speed (default: 60.0)

## Architecture

This microservice is designed to be stateless and focused solely on report generation. It uses real APIs for data:

- **Google Maps API**: For geocoding and address resolution
- **Tomorrow.io Weather API**: For comprehensive weather data including current conditions, forecasts, historical data, severe weather events, hail probability, and fire risk assessment
- **Local File Storage**: Reports are stored temporarily in `/tmp/reports`

The service fetches real-time data from Tomorrow.io's professional weather API and generates comprehensive reports with advanced features like probabilistic hail forecasting and fire danger assessment. Files are stored locally and can be downloaded directly from the service.

## Extensibility

This microservice uses a modular architecture that makes it easy to add new report types:

```
reports-service/
├── app/
│   ├── services/
│   │   ├── weather_data_service.py      # Weather-specific service
│   │   ├── spatial_analysis_service.py  # Spatial-specific service
│   │   ├── address_analysis_service.py  # Address-specific service
│   │   └── [future report services]     # Add new report types here
│   └── api/
│       └── routes/
│           ├── reports.py               # Main report endpoints
│           └── [future route modules]   # Add new endpoints here
└── templates/
    ├── weather/                         # Weather report templates
    ├── spatial/                         # Spatial report templates
    └── [future template folders]        # Add new templates here
```

To add a new report type:
1. Create a new service in `app/services/`
2. Add templates in `templates/[report_type]/`
3. Add routes in `app/api/routes/`
4. Register in `app/main.py`# Trigger deployment Sat Oct 25 11:19:42 CDT 2025
