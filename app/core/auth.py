"""
API Key authentication for Reports Service
"""

from fastapi import Security, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
from app.core.config import settings
from app.core.logging import get_logger
from typing import Optional

logger = get_logger(__name__)

# API Key header scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Validate API key from request header.

    Args:
        api_key: API key from X-API-Key header

    Returns:
        Validated API key

    Raises:
        HTTPException: If API key is missing or invalid
    """
    # Check if API key authentication is enabled
    if not settings.api_keys_enabled:
        # API key authentication disabled - allow all requests
        logger.debug("API key authentication is disabled")
        return "disabled"

    if not api_key:
        logger.warning("API key missing from request")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Include X-API-Key header."
        )

    # Validate against configured API keys
    if api_key not in settings.valid_api_keys:
        logger.warning(f"Invalid API key attempted: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )

    logger.debug(f"Valid API key authenticated: {api_key[:8]}...")
    return api_key


def generate_api_key() -> str:
    """
    Generate a new API key.

    Returns:
        New API key string
    """
    import secrets
    return f"apexos_reports_{secrets.token_urlsafe(32)}"
