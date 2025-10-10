#!/bin/bash
set -e

# Wait for the application to be ready
echo "Starting Weather Reports Service..."

# Use PORT environment variable if available, otherwise default to 8000
PORT=${PORT:-8000}

# Start the application
exec uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1
