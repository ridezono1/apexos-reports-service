# Reports Service Documentation

This directory contains comprehensive documentation for the ApexOS Reports Service, a FastAPI microservice that generates weather and spatial analysis reports.

## 📋 Documentation Index

### 🚀 Getting Started
- **[API Key Setup](API_KEY_SETUP.md)** - Configure API keys for external services (Google Maps, NOAA, Tomorrow.io)
- **[Microservice Guide](MICROSERVICE.md)** - Service architecture, deployment, and configuration

### 📊 Data & Analysis
- **[NOAA Data Freshness](NOAA_DATA_FRESHNESS.md)** - Data sources, freshness strategy, and hybrid data approach
- **[Weather Thresholds](WEATHER_THRESHOLDS.md)** - Weather alert thresholds, criteria, and risk assessment based on industry standards

### 🔧 Operations
- **[Sentry Setup](SENTRY_SETUP.md)** - Error monitoring, logging, and performance tracking

## 🏗️ Service Architecture

The Reports Service is a FastAPI microservice that provides:

- **Weather Reports**: Address-based weather analysis with 24-month historical data
- **Spatial Reports**: Geographic area analysis with risk assessment
- **Data Sources**: NOAA Storm Events Database + SPC Storm Reports (hybrid approach)
- **Performance**: DuckDB integration for 10-30x faster data processing
- **Caching**: Smart age-based caching with background refresh

## 🔗 Quick Links

- **Main README**: [../../README.md](../../README.md)
- **Service Root**: [../README.md](../README.md)
- **API Endpoints**: See service startup logs for interactive API documentation

## 📈 Recent Updates

- ✅ **DuckDB Integration**: 10-30x performance improvement for CSV processing
- ✅ **SPC Storm Reports**: Real-time preliminary data to fill 120-day NOAA lag
- ✅ **Smart Caching**: Age-based cache duration (30d historical, 7d previous year, 24h current)
- ✅ **Background Refresh**: Automated cache management with APScheduler
- ✅ **Template Engine**: Fixed template context for proper report rendering

## 🆘 Support

For issues or questions:
1. Check the relevant documentation above
2. Review service logs for error details
3. See main project README for general troubleshooting
