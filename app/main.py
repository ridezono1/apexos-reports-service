"""
Reports Service - Main FastAPI Application

A scalable microservice for generating professional reports (weather, spatial, and more).
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import uvicorn
import os
from dotenv import load_dotenv
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
import logging

# Check if NOAA is disabled via environment variable
if os.getenv("DISABLE_NOAA") == "true":
    print("NOAA data fetching is disabled via DISABLE_NOAA environment variable")
    # Set environment variable to disable NOAA in the application
    os.environ["WEATHER_PROVIDER"] = "disabled"
    # Also disable NOAA data fetching completely
    os.environ["NOAA_DATA_FETCHING_ENABLED"] = "false"

from app.api.v1 import reports, geocoding
from app.core.config import settings

# Apply emergency error handling fixes
try:
    from app.services.emergency_fix import add_emergency_error_handling
    add_emergency_error_handling()
except Exception as e:
    logging.warning(f"Could not apply emergency fixes: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Sentry for error tracking
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[FastApiIntegration()],
        traces_sample_rate=0.1,  # 10% of transactions
        environment=os.getenv("ENVIRONMENT", "development"),
    )

# Create FastAPI application
app = FastAPI(
    title="Reports Service",
    description="Scalable microservice for generating professional reports (weather, spatial, and more)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handlers to ensure JSON responses
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions and return JSON responses"""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "status_code": exc.status_code,
            "message": exc.detail,
            "path": str(request.url)
        }
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors and return JSON responses"""
    logger.error(f"Validation Error: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={
            "error": True,
            "status_code": 422,
            "message": "Validation error",
            "details": exc.errors(),
            "path": str(request.url)
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle all other exceptions and return JSON responses"""
    logger.error(f"Unhandled Exception: {type(exc).__name__}: {str(exc)}")
    sentry_sdk.capture_exception(exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "status_code": 500,
            "message": "Internal server error",
            "path": str(request.url)
        }
    )

# Include API routes
app.include_router(reports.router, prefix="/api/v1", tags=["reports"])
app.include_router(geocoding.router, prefix="/api/v1/geocoding", tags=["geocoding"])

# Startup and shutdown event handlers
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    try:
        from app.services.background_cache_refresh_service import get_cache_refresh_service
        
        # Start background cache refresh service
        cache_service = get_cache_refresh_service()
        if cache_service.start():
            logger.info("Background cache refresh service started")
            
            # Optional: Warmup cache on startup (last 3 years)
            # This can be disabled in production if startup time is critical
            if os.getenv("CACHE_WARMUP_ON_STARTUP", "true").lower() == "true":
                logger.info("Starting cache warmup...")
                await cache_service.warmup_cache()
                logger.info("Cache warmup completed")
        else:
            logger.warning("Failed to start background cache refresh service")
            
    except Exception as e:
        logger.error(f"Error during startup: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    try:
        from app.services.background_cache_refresh_service import get_cache_refresh_service
        
        # Stop background cache refresh service
        cache_service = get_cache_refresh_service()
        cache_service.stop()
        logger.info("Background cache refresh service stopped")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "Reports Service",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "reports-service",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )