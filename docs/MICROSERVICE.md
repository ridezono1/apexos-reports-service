# Reports Service Microservice

A scalable microservice for generating professional reports (weather, spatial, and more) within the ApexOS monorepo.

## 🏗️ Architecture

This microservice is designed to be:
- **Independent**: Can be developed, tested, and deployed separately
- **Scalable**: Supports horizontal scaling and load balancing
- **Resilient**: Includes health checks, monitoring, and error tracking
- **Maintainable**: Clean separation of concerns and modular design

## 🚀 Quick Start

### Development Mode
```bash
# Install dependencies
./manage.sh install

# Start development server
./manage.sh dev
```

### Docker Mode
```bash
# Start with Docker
./manage.sh docker

# Stop Docker services
./manage.sh stop
```

### Production Deployment
```bash
# Deploy to Heroku
./manage.sh deploy
```

## 📋 API Endpoints

### Reports API (`/api/v1/reports`)
- `POST /api/v1/reports/generate/weather` - Generate weather reports
- `POST /api/v1/reports/generate/spatial` - Generate spatial reports  
- `POST /api/v1/reports/generate/address` - Generate address reports
- `GET /api/v1/reports/{report_id}/status` - Check report status

### Geocoding API (`/api/v1/geocoding`)
- `GET /api/v1/geocoding/autocomplete` - Address autocomplete
- `POST /api/v1/geocoding/geocode` - Geocode address/place ID
- `POST /api/v1/geocoding/reverse-geocode` - Reverse geocode coordinates

### Health & Monitoring
- `GET /health` - Health check endpoint
- `GET /docs` - API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation

## 🔧 Configuration

### Environment Variables
```bash
# Required
ENVIRONMENT=development|staging|production
DEBUG=true|false
LOG_LEVEL=debug|info|warning|error

# Optional
SENTRY_DSN=your_sentry_dsn_here
API_KEY=your_api_key_here
```

### Service Configuration
See `microservice.yml` for detailed configuration options including:
- Service ports and protocols
- Health check intervals
- Resource limits
- Scaling policies

## 🧪 Testing

```bash
# Run all tests
./manage.sh test

# Run specific test file
python -m pytest tests/test_api_reports.py -v

# Run with coverage
python -m pytest tests/ --cov=app --cov-report=html
```

## 🐳 Docker

### Development
```bash
# Build and run
docker-compose -f docker-compose.microservice.yml up --build

# Run in background
docker-compose -f docker-compose.microservice.yml up -d
```

### Production
```bash
# Build production image
docker build -t reports-service:latest .

# Run production container
docker run -p 8000:8000 -e ENVIRONMENT=production reports-service:latest
```

## 📊 Monitoring

### Health Checks
- **Endpoint**: `/health`
- **Interval**: 30 seconds
- **Timeout**: 5 seconds
- **Retries**: 3

### Metrics
- **Endpoint**: `/metrics` (if enabled)
- **Format**: Prometheus-compatible
- **Sampling**: 10% of transactions

### Error Tracking
- **Service**: Sentry integration
- **Coverage**: All API endpoints
- **Environment**: Tagged by ENVIRONMENT variable

## 🔄 Integration with Monorepo

### Dependencies
This microservice depends on:
- **Backend Service**: For weather and spatial data
- **Shared Libraries**: Common utilities and configurations

### Deployment
- **Heroku**: Automatic deployment via git subtree
- **Docker**: Containerized deployment
- **Local**: Development server with hot reload

### Development Workflow
1. Make changes in `reports-service/` directory
2. Test locally with `./manage.sh dev`
3. Run tests with `./manage.sh test`
4. Deploy with `./manage.sh deploy`

## 📁 Project Structure

```
reports-service/
├── app/                    # Application code
│   ├── api/v1/            # API endpoints
│   ├── core/              # Core configuration
│   ├── services/          # Business logic
│   └── schemas/           # Data models
├── tests/                 # Test suite
├── templates/             # Report templates
├── static/               # Static assets
├── microservice.yml      # Service configuration
├── docker-compose.microservice.yml  # Docker setup
├── manage.sh             # Management script
└── requirements.txt      # Python dependencies
```

## 🚨 Troubleshooting

### Common Issues

**Service won't start**
```bash
# Check logs
./manage.sh status
docker logs reports-service

# Check health
./manage.sh health
```

**API errors**
```bash
# Check API documentation
curl http://localhost:8001/docs

# Test endpoints
curl http://localhost:8001/health
```

**Docker issues**
```bash
# Rebuild image
./manage.sh build

# Clean up containers
docker-compose -f docker-compose.microservice.yml down -v
```

## 📚 Documentation

- **API Docs**: http://localhost:8001/docs
- **Service Config**: `microservice.yml`
- **Docker Setup**: `docker-compose.microservice.yml`
- **Management**: `./manage.sh help`

## 🤝 Contributing

1. Make changes in the `reports-service/` directory
2. Test your changes with `./manage.sh test`
3. Start development server with `./manage.sh dev`
4. Submit changes to the monorepo

## 📄 License

Part of the ApexOS monorepo. See main repository for license information.
