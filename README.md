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
- **Industry-Standard Thresholds**: SkyLink-based hail (≥1") and wind (≥60 mph) thresholds for actionable events

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

The service uses industry-standard thresholds based on SkyLink standards for identifying actionable weather events:

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
- `ALERT_HAIL_MIN_SIZE_INCHES` - Minimum actionable hail size (default: 1.0)
- `ALERT_WIND_MIN_SPEED_MPH` - Minimum actionable wind speed (default: 60.0)

## Architecture

This microservice is designed to be stateless and focused solely on report generation. It uses real APIs for data:

- **Google Maps API**: For geocoding and address resolution
- **NOAA Weather API**: For all weather data - current conditions, forecasts, historical data, and weather events (free, no API key required)
- **Local File Storage**: Reports are stored temporarily in `/tmp/reports`

The service fetches real-time data from NOAA's official weather API and generates professional reports. Files are stored locally and can be downloaded directly from the service.

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
4. Register in `app/main.py`