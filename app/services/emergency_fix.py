"""
Emergency Fix: Add Robust Error Handling for Production

This script adds additional error handling to prevent 500 errors
in the production environment.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import date

logger = logging.getLogger(__name__)

def add_emergency_error_handling():
    """
    Add emergency error handling to prevent 500 errors.
    This should be called during service initialization.
    """
    
    # Patch the NOAA weather service with additional error handling
    try:
        from app.services.noaa_weather_service import NOAAWeatherService
        
        # Store original method
        original_get_weather_events = NOAAWeatherService.get_weather_events
        
        async def safe_get_weather_events(
            self,
            latitude: float,
            longitude: float,
            start_date: date,
            end_date: date,
            radius_km: float = 50.0
        ) -> List[Dict[str, Any]]:
            """
            Safe wrapper for get_weather_events with additional error handling.
            """
            try:
                # Validate inputs
                if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
                    logger.error(f"Invalid coordinates: {latitude}, {longitude}")
                    return []
                
                if latitude < -90 or latitude > 90 or longitude < -180 or longitude > 180:
                    logger.error(f"Coordinates out of range: {latitude}, {longitude}")
                    return []
                
                if start_date > end_date:
                    logger.error(f"Invalid date range: {start_date} to {end_date}")
                    return []
                
                # Call original method with timeout protection
                import asyncio
                try:
                    result = await asyncio.wait_for(
                        original_get_weather_events(self, latitude, longitude, start_date, end_date, radius_km),
                        timeout=60.0  # 60 second timeout
                    )
                    return result or []
                except asyncio.TimeoutError:
                    logger.error(f"Timeout fetching weather events for {latitude}, {longitude}")
                    return []
                
            except Exception as e:
                logger.error(f"Critical error in weather events fetch: {e}")
                import traceback
                traceback.print_exc()
                return []
        
        # Replace the method
        NOAAWeatherService.get_weather_events = safe_get_weather_events
        logger.info("Emergency error handling applied to NOAAWeatherService")
        
    except Exception as e:
        logger.error(f"Failed to apply emergency error handling: {e}")

# Apply the fix immediately
add_emergency_error_handling()
