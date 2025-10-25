#!/bin/bash
set -e

# Wait for the application to be ready
echo "Starting Weather Reports Service..."

# Check if NOAA is disabled
if [ "$DISABLE_NOAA" = "true" ]; then
    echo "NOAA data fetching is disabled via DISABLE_NOAA environment variable"
    echo "Service will start but NOAA data will not be fetched"
    # Set environment variable to disable NOAA in the application
    export WEATHER_PROVIDER="disabled"
fi

# Use PORT environment variable if available, otherwise default to 8000
PORT=${PORT:-8000}

# Start the application
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1
