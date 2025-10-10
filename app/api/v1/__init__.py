"""
API v1 router
"""

from fastapi import APIRouter
from . import reports

# Create v1 router
router = APIRouter()

# Include all v1 endpoints
router.include_router(reports.router, prefix="/reports", tags=["reports"])