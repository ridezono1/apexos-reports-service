# Multi-stage build for smaller image size
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies including Chrome for Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 \
    libxslt1.1 \
    libjpeg62-turbo \
    libpng16-16 \
    libgobject-2.0-0 \
    libgdk-pixbuf-2.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libpangoft2-1.0-0 \
    shared-mime-info \
    curl \
    wget \
    gnupg \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome and ChromeDriver for Selenium map rendering (supports both AMD64 and ARM64)
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        # Install Google Chrome
        wget -q -O /tmp/google-chrome-key.pub https://dl-ssl.google.com/linux/linux_signing_key.pub && \
        gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg /tmp/google-chrome-key.pub && \
        rm /tmp/google-chrome-key.pub && \
        echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
        apt-get update && \
        apt-get install -y --no-install-recommends google-chrome-stable && \
        # Install ChromeDriver (must match Chrome version)
        CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+') && \
        CHROME_MAJOR_VERSION=$(echo $CHROME_VERSION | cut -d. -f1) && \
        echo "Chrome version: $CHROME_VERSION (major: $CHROME_MAJOR_VERSION)" && \
        # Download matching ChromeDriver version
        CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_MAJOR_VERSION") && \
        echo "ChromeDriver version: $CHROMEDRIVER_VERSION" && \
        wget -q "https://storage.googleapis.com/chrome-for-testing-public/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip" && \
        unzip chromedriver-linux64.zip && \
        mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
        chmod +x /usr/local/bin/chromedriver && \
        rm -rf chromedriver-linux64* && \
        echo "ChromeDriver installed at /usr/local/bin/chromedriver"; \
    elif [ "$ARCH" = "arm64" ]; then \
        # For ARM64, use Chromium instead of Chrome (Chrome doesn't have ARM64 packages)
        apt-get update && \
        apt-get install -y --no-install-recommends chromium chromium-driver; \
    fi && \
    rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder to /usr/local for system-wide access
COPY --from=builder /root/.local /usr/local

# Copy application code
COPY app/ ./app/
COPY templates/ ./templates/
COPY start.sh ./

# Copy static directory if it exists (may be empty, used for future assets)
COPY static ./static/

# Make start script executable
RUN chmod +x ./start.sh

# Create temp directory for report generation
RUN mkdir -p /tmp/reports

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash --uid 1000 app && \
    chown -R app:app /app /tmp/reports

# Switch to non-root user
USER app

# Expose port (Heroku will override with $PORT)
EXPOSE 8000

# Add labels for metadata
LABEL maintainer="ApexOS <apexos@herokumanager.com>"
LABEL description="Weather Reports Microservice - Standalone FastAPI service for generating weather and spatial reports"
LABEL version="1.0"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Run the application
CMD ["./start.sh"]