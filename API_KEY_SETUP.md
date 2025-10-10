# API Key Authentication Setup

## Overview
The Reports Service now requires API key authentication for all report endpoints. This ensures only authorized clients (like the mobile app) can generate and access reports.

## Configuration

### Heroku Environment Variables

Add these environment variables to the Reports Service on Heroku:

```bash
# Enable API key authentication
API_KEYS_ENABLED=true

# Comma-separated list of valid API keys
API_KEY_LIST=apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY

# Google Maps API Key (already added)
GOOGLE_MAPS_API_KEY=your_actual_google_maps_key
```

### To Add to Heroku:

1. Go to Heroku Dashboard: https://dashboard.heroku.com/apps/apexos-reports-service-ce3abe785e88
2. Click **Settings** tab
3. Click **Reveal Config Vars**
4. Add these config vars:
   - Key: `API_KEYS_ENABLED` Value: `true`
   - Key: `API_KEY_LIST` Value: `apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY`
5. Click **Save**
6. Restart the dyno (More → Restart all dynos)

## Mobile App Configuration

The mobile app has been configured with the API key in `.env`:

```bash
EXPO_PUBLIC_REPORTS_SERVICE_API_KEY=apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY
```

**After updating `.env`, restart Metro bundler:**
```bash
npx expo start -c
```

## How It Works

### Request Flow:
1. Mobile app sends request with `X-API-Key` header
2. FastAPI validates the API key using the `get_api_key()` dependency
3. If valid, request proceeds
4. If invalid/missing, returns 401 Unauthorized

### Protected Endpoints:
- ✅ `POST /api/v1/reports/generate/address` - Create address report
- ✅ `GET /api/v1/reports/{id}/status` - Check report status
- ✅ `GET /api/v1/reports/{id}/download` - Download PDF

### Header Format:
```
X-API-Key: apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY
```

## API Key Management

### Generate New API Keys:
```python
import secrets
new_key = f"apexos_reports_{secrets.token_urlsafe(32)}"
print(new_key)
```

### Add Multiple Keys:
Separate with commas in `API_KEY_LIST`:
```
API_KEY_LIST=key1,key2,key3
```

### Disable Authentication (for testing):
```
API_KEYS_ENABLED=false
```

## Security Notes

1. **Never commit API keys** to version control
2. **Rotate keys periodically** for security
3. **Use different keys** for different clients (mobile, web, etc.)
4. **Monitor usage** via application logs

## Testing

### Test with curl:
```bash
# Without API key (should fail)
curl -X POST https://apexos-reports-service-ce3abe785e88.herokuapp.com/api/v1/reports/generate/address \
  -H "Content-Type: application/json" \
  -d '{"location": "123 Main St", "analysis_period": "6_months", ...}'

# With API key (should work)
curl -X POST https://apexos-reports-service-ce3abe785e88.herokuapp.com/api/v1/reports/generate/address \
  -H "Content-Type: application/json" \
  -H "X-API-Key: apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY" \
  -d '{"location": "123 Main St", "analysis_period": "6_months", ...}'
```

## Troubleshooting

### 401 Unauthorized Error:
- Check API key is correct in mobile `.env`
- Verify Heroku config var `API_KEY_LIST` contains the key
- Restart Metro bundler after changing `.env`
- Check Reports Service logs on Heroku

### API key not being sent:
- Make sure mobile app's `ReportsServiceConfig` is using the env var
- Check headers in network logs
- Verify axios interceptor is adding `X-API-Key` header

## Current API Key

**Mobile App API Key:**
```
apexos_reports_Hj2oI_0KBvjudZcLWGEy-I6dmKOY0EyFJH7HcejM8wY
```

**Status:** ✅ Configured in mobile `.env`
**Next Step:** 🔄 Add to Heroku config vars
