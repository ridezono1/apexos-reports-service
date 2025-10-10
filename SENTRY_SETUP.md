# Sentry Integration Setup for Reports Service

The Reports Service is integrated with Sentry for comprehensive error tracking and monitoring.

## Features

✅ **Automatic Error Capture** - All unhandled exceptions are automatically reported to Sentry
✅ **FastAPI Integration** - Deep integration with FastAPI for request context
✅ **Performance Monitoring** - 10% transaction sampling for performance insights
✅ **Environment Tagging** - Errors tagged by environment (development, staging, production)
✅ **Manual Exception Tracking** - All API endpoints have explicit Sentry error tracking

## Configuration

### 1. Environment Variables

Add to your `.env` file:

```bash
# Sentry Error Tracking
SENTRY_DSN=https://your-sentry-dsn@sentry.io/your-project-id
ENVIRONMENT=production  # or development, staging
```

### 2. Sentry DSN

Get your Sentry DSN from:
1. Log into [Sentry.io](https://sentry.io)
2. Create or select your project
3. Go to Settings > Client Keys (DSN)
4. Copy the DSN value

### 3. Environment Settings

The `ENVIRONMENT` variable determines how errors are tagged in Sentry:
- `development` - Local development
- `staging` - Staging/testing environment
- `production` - Production environment

## Error Tracking Coverage

### Automatic Capture (via FastAPI Integration)

All unhandled exceptions in the FastAPI application are automatically captured.

### Manual Capture (Explicit Error Tracking)

The following endpoints have explicit Sentry error tracking:

#### Reports Endpoints
- `POST /api/v1/reports/generate/weather` - Weather report generation
- `POST /api/v1/reports/generate/spatial` - Spatial report generation
- `POST /api/v1/reports/generate/address` - Address report generation

#### Geocoding Endpoints
- `GET /api/v1/geocoding/autocomplete` - Address autocomplete
- `POST /api/v1/geocoding/geocode` - Geocode address/place ID
- `POST /api/v1/geocoding/reverse-geocode` - Reverse geocode coordinates

## Testing Sentry Integration

### Test Error Capture

You can verify Sentry is working by triggering a test error:

```bash
# Add a test endpoint temporarily
curl -X GET "http://localhost:8000/test-sentry-error"
```

Or check the Sentry dashboard after any API error occurs.

### What Gets Captured

Sentry captures:
- **Exception Type** - Full exception class name
- **Stack Trace** - Complete call stack
- **Request Context** - HTTP method, URL, headers
- **User Context** - If available
- **Environment** - Tagged by ENVIRONMENT variable
- **Timestamp** - When the error occurred

## Integration Code

### Main Application (main.py)

```python
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

# Initialize Sentry
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[FastApiIntegration()],
        traces_sample_rate=0.1,  # 10% of transactions
        environment=os.getenv("ENVIRONMENT", "development"),
    )
```

### API Endpoints

```python
import sentry_sdk

try:
    # ... endpoint logic ...
except Exception as e:
    logger.error(f"Error message: {e}")
    sentry_sdk.capture_exception(e)  # Explicitly capture
    raise HTTPException(status_code=500, detail=str(e))
```

## Performance Monitoring

Sentry's performance monitoring is configured with 10% transaction sampling:
- Tracks request duration
- Identifies slow endpoints
- Database query performance (if configured)

Adjust `traces_sample_rate` in main.py to change sampling:
- `1.0` = 100% sampling (development)
- `0.1` = 10% sampling (production - recommended)
- `0.0` = Disabled

## Production Deployment

### Heroku

Set environment variables in Heroku:

```bash
heroku config:set SENTRY_DSN=https://your-dsn@sentry.io/project-id
heroku config:set ENVIRONMENT=production
```

### Docker

Add to your docker-compose.yml or Dockerfile:

```yaml
environment:
  - SENTRY_DSN=${SENTRY_DSN}
  - ENVIRONMENT=${ENVIRONMENT}
```

## Monitoring Dashboard

Access your Sentry dashboard at: https://sentry.io

Key metrics to monitor:
- **Error Rate** - Errors per minute/hour
- **Affected Users** - If user context is available
- **Release Health** - Error rates by deployment
- **Performance** - Slow transactions and endpoints

## Best Practices

1. **Don't Log Sensitive Data** - Sentry captures request data; avoid logging PII
2. **Tag Errors** - Use tags for better filtering
3. **Set Release** - Tag errors with release version
4. **Monitor Regularly** - Set up alerts for critical errors
5. **Fix High-Impact Errors First** - Prioritize by frequency and user impact

## Troubleshooting

### Errors Not Appearing in Sentry

1. Check `SENTRY_DSN` is set correctly
2. Verify internet connection (Sentry requires outbound HTTPS)
3. Check logs for Sentry initialization messages
4. Ensure errors are actually being raised (not caught and suppressed)

### Too Many Errors

1. Reduce `traces_sample_rate` for less transaction tracking
2. Filter out known/expected errors
3. Use Sentry's rate limiting features

### Missing Context

1. Add custom tags: `sentry_sdk.set_tag('key', 'value')`
2. Add user context: `sentry_sdk.set_user({'id': user_id})`
3. Add breadcrumbs: `sentry_sdk.add_breadcrumb({'message': 'User action'})`

## Support

For issues with Sentry integration:
- Sentry Documentation: https://docs.sentry.io/platforms/python/
- FastAPI Integration: https://docs.sentry.io/platforms/python/guides/fastapi/
