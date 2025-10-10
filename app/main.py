"""
Reports Service - Main FastAPI Application

A scalable microservice for generating professional reports (weather, spatial, and more).
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
from dotenv import load_dotenv
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

from app.api.v1 import reports, geocoding
from app.core.config import settings

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

# Include API routes
app.include_router(reports.router, prefix="/api/v1", tags=["reports"])
app.include_router(geocoding.router, prefix="/api/v1/geocoding", tags=["geocoding"])

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