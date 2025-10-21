"""
Weather data service for fetching real weather data from Tomorrow.io API.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging
import sentry_sdk

from app.core.config import settings
from app.core.logging import get_logger
from app.services.tomorrow_io_service import get_tomorrow_io_service
from app.services.noaa_weather_service import get_noaa_weather_service

logger = get_logger(__name__)

# Weather thresholds from SkyLink standards
WIND_MIN_ACTIONABLE = settings.alert_wind_min_speed_mph
WIND_SEVERE = settings.wind_severe_threshold
WIND_MODERATE = settings.wind_moderate_threshold


class WeatherDataService:
    """Service for fetching real weather data from Tomorrow.io API."""
    
    def __init__(self):
        """Initialize weather data service."""
        self.tomorrow_io = get_tomorrow_io_service()
        self.noaa_service = get_noaa_weather_service()
        self.weather_provider = settings.weather_provider
        
        logger.info(f"Weather data service initialized - using {self.weather_provider} as primary provider")
    
    async def get_current_weather(
        self,
        latitude: float,
        longitude: float,
        location_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get current weather conditions for a location.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            location_name: Optional location name for context
            
        Returns:
            Current weather data
        """
        try:
            if self.weather_provider == "noaa":
                # Use NOAA as primary provider
                return await self.noaa_service.get_current_weather(latitude, longitude, location_name)
            elif self.weather_provider == "tomorrow_io":
                # Use Tomorrow.io as primary provider with NOAA fallback
                try:
                    timeline_data = await self.tomorrow_io.get_current_and_forecast(
                        latitude, longitude, timesteps=['1h']
                    )
                    
                    current_weather = timeline_data.get("current_weather", {})
                    if current_weather:
                        return self._convert_tomorrow_io_current_weather(current_weather, location_name)
                    
                    # Fallback to NOAA if Tomorrow.io returns empty data
                    logger.warning(f"Tomorrow.io returned empty data for {latitude}, {longitude}, using NOAA backup")
                    return await self.noaa_service.get_current_weather(latitude, longitude, location_name)
                    
                except Exception as e:
                    logger.warning(f"Tomorrow.io failed for {latitude}, {longitude}, using NOAA backup: {e}")
                    sentry_sdk.capture_message(f"Tomorrow.io failure, NOAA backup active: {e}")
                    return await self.noaa_service.get_current_weather(latitude, longitude, location_name)
            elif self.weather_provider == "auto":
                # Auto-select: try Tomorrow.io first, fallback to NOAA
                try:
                    timeline_data = await self.tomorrow_io.get_current_and_forecast(
                        latitude, longitude, timesteps=['1h']
                    )
                    
                    current_weather = timeline_data.get("current_weather", {})
                    if current_weather:
                        return self._convert_tomorrow_io_current_weather(current_weather, location_name)
                    
                    # Fallback to NOAA if Tomorrow.io returns empty data
                    logger.info(f"Tomorrow.io returned empty data for {latitude}, {longitude}, using NOAA")
                    return await self.noaa_service.get_current_weather(latitude, longitude, location_name)
                    
                except Exception as e:
                    logger.info(f"Tomorrow.io failed for {latitude}, {longitude}, using NOAA: {e}")
                    return await self.noaa_service.get_current_weather(latitude, longitude, location_name)
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching current weather for {latitude}, {longitude}: {e}")
            # Return fallback data instead of raising exception
            return self._get_fallback_weather_data(latitude, longitude, location_name)
    
    async def get_weather_forecast(
        self,
        latitude: float,
        longitude: float,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get weather forecast for a location.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of forecast days (max 15)
            
        Returns:
            Weather forecast data
        """
        try:
            if self.weather_provider == "noaa":
                # Use NOAA as primary provider
                return await self.noaa_service.get_weather_forecast(latitude, longitude, days)
            elif self.weather_provider == "tomorrow_io":
                # Use Tomorrow.io as primary provider with NOAA fallback
                try:
                    timeline_data = await self.tomorrow_io.get_current_and_forecast(
                        latitude, longitude, timesteps=['1d']
                    )
                    
                    forecast = timeline_data.get("forecast", [])
                    if forecast:
                        limited_forecast = forecast[:days]
                        return self._convert_tomorrow_io_forecast(limited_forecast)
                    
                    # Fallback to NOAA if Tomorrow.io returns empty data
                    logger.warning(f"Tomorrow.io returned empty forecast for {latitude}, {longitude}, using NOAA backup")
                    return await self.noaa_service.get_weather_forecast(latitude, longitude, days)
                    
                except Exception as e:
                    logger.warning(f"Tomorrow.io forecast failed for {latitude}, {longitude}, using NOAA backup: {e}")
                    return await self.noaa_service.get_weather_forecast(latitude, longitude, days)
            elif self.weather_provider == "auto":
                # Auto-select: try Tomorrow.io first, fallback to NOAA
                try:
                    timeline_data = await self.tomorrow_io.get_current_and_forecast(
                        latitude, longitude, timesteps=['1d']
                    )
                    
                    forecast = timeline_data.get("forecast", [])
                    if forecast:
                        limited_forecast = forecast[:days]
                        return self._convert_tomorrow_io_forecast(limited_forecast)
                    
                    # Fallback to NOAA if Tomorrow.io returns empty data
                    logger.info(f"Tomorrow.io returned empty forecast for {latitude}, {longitude}, using NOAA")
                    return await self.noaa_service.get_weather_forecast(latitude, longitude, days)
                    
                except Exception as e:
                    logger.info(f"Tomorrow.io forecast failed for {latitude}, {longitude}, using NOAA: {e}")
                    return await self.noaa_service.get_weather_forecast(latitude, longitude, days)
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching weather forecast for {latitude}, {longitude}: {e}")
            raise
    
    async def get_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Get historical weather data for a location and date range.
        Supports 6-month, 9-month, or 24-month analysis periods ending at current date.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for historical data
            end_date: End date (defaults to today if not provided)
            
        Returns:
            Historical weather data with trend analysis
        """
        try:
            # Always use current date as end date for 24-month rolling window
            if end_date is None:
                end_date = date.today()
            
            # Calculate period length
            period_days = (end_date - start_date).days
            
            # Validate period is 6, 9, or 24 months
            if not (180 <= period_days <= 185 or 270 <= period_days <= 275 or 720 <= period_days <= 735):
                raise ValueError("Analysis period must be exactly 6 months, 9 months, or 24 months")
            
            if self.weather_provider == "noaa":
                # Use NOAA as primary provider
                return await self.noaa_service.get_historical_weather(latitude, longitude, start_date, end_date)
            elif self.weather_provider == "tomorrow_io":
                # Use Tomorrow.io as primary provider with NOAA fallback
                try:
                    historical_data = await self.tomorrow_io.get_historical_weather(
                        latitude, longitude, start_date, end_date
                    )
                    
                    # Add trend analysis for all periods
                    historical_data["trend_analysis"] = self._analyze_weather_trends(
                        historical_data.get("observations", [])
                    )
                    historical_data["seasonal_analysis"] = self._analyze_seasonal_patterns(
                        historical_data.get("observations", []), start_date, end_date
                    )
                    historical_data["period_analysis"] = self._analyze_period_patterns(
                        historical_data.get("observations", []), period_days
                    )
                    
                    # Add current date context
                    historical_data["analysis_context"] = {
                        "analysis_date": end_date.isoformat(),
                        "period_length_days": period_days,
                        "period_type": "6_month" if 180 <= period_days <= 185 else "9_month" if 270 <= period_days <= 275 else "24_month"
                    }
                    
                    return historical_data
                    
                except Exception as e:
                    logger.warning(f"Tomorrow.io historical data failed for {latitude}, {longitude}, using NOAA backup: {e}")
                    return await self.noaa_service.get_historical_weather(latitude, longitude, start_date, end_date)
            elif self.weather_provider == "auto":
                # Auto-select: try Tomorrow.io first, fallback to NOAA
                try:
                    historical_data = await self.tomorrow_io.get_historical_weather(
                        latitude, longitude, start_date, end_date
                    )
                    
                    # Add trend analysis for all periods
                    historical_data["trend_analysis"] = self._analyze_weather_trends(
                        historical_data.get("observations", [])
                    )
                    historical_data["seasonal_analysis"] = self._analyze_seasonal_patterns(
                        historical_data.get("observations", []), start_date, end_date
                    )
                    historical_data["period_analysis"] = self._analyze_period_patterns(
                        historical_data.get("observations", []), period_days
                    )
                    
                    # Add current date context
                    historical_data["analysis_context"] = {
                        "analysis_date": end_date.isoformat(),
                        "period_length_days": period_days,
                        "period_type": "6_month" if 180 <= period_days <= 185 else "9_month" if 270 <= period_days <= 275 else "24_month"
                    }
                    
                    return historical_data
                    
                except Exception as e:
                    logger.info(f"Tomorrow.io historical data failed for {latitude}, {longitude}, using NOAA: {e}")
                    return await self.noaa_service.get_historical_weather(latitude, longitude, start_date, end_date)
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching historical weather for {latitude}, {longitude}: {e}")
            raise
    
    async def get_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Get weather events (storms, severe weather) for a location and date range.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for events
            end_date: End date for events
            radius_km: Search radius in kilometers
            
        Returns:
            List of weather events
        """
        try:
            if self.weather_provider == "noaa":
                # Use NOAA as primary provider
                return await self.noaa_service.get_weather_events(latitude, longitude, start_date, end_date, radius_km)
            elif self.weather_provider == "tomorrow_io":
                # Use Tomorrow.io as primary provider with NOAA fallback
                try:
                    return await self.tomorrow_io.get_severe_weather_events(
                        latitude, longitude, start_date, end_date, radius_km
                    )
                except Exception as e:
                    logger.warning(f"Tomorrow.io weather events failed for {latitude}, {longitude}, using NOAA backup: {e}")
                    return await self.noaa_service.get_weather_events(latitude, longitude, start_date, end_date, radius_km)
            elif self.weather_provider == "auto":
                # Auto-select: try Tomorrow.io first, fallback to NOAA
                try:
                    return await self.tomorrow_io.get_severe_weather_events(
                        latitude, longitude, start_date, end_date, radius_km
                    )
                except Exception as e:
                    logger.info(f"Tomorrow.io weather events failed for {latitude}, {longitude}, using NOAA: {e}")
                    return await self.noaa_service.get_weather_events(latitude, longitude, start_date, end_date, radius_km)
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching weather events for {latitude}, {longitude}: {e}")
            raise
    
    async def get_hail_probability(
        self,
        latitude: float,
        longitude: float,
        days: int = 15
    ) -> Dict[str, Any]:
        """
        Get hail probability forecast for the next N days.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of forecast days
            
        Returns:
            Hail probability forecast data
        """
        try:
            if self.weather_provider == "noaa":
                # NOAA doesn't have hail probability API, return empty data
                logger.info(f"NOAA doesn't support hail probability forecasting for {latitude}, {longitude}")
                return {"hail_probability": [], "source": "NOAA-NWS", "note": "Hail probability not available from NOAA"}
            elif self.weather_provider in ["tomorrow_io", "auto"]:
                # Use Tomorrow.io for hail probability
                try:
                    return await self.tomorrow_io.get_hail_probability_forecast(
                        latitude, longitude, days
                    )
                except Exception as e:
                    logger.warning(f"Tomorrow.io hail probability failed for {latitude}, {longitude}: {e}")
                    return {"hail_probability": [], "source": "Tomorrow.io", "note": "Hail probability unavailable"}
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching hail probability for {latitude}, {longitude}: {e}")
            return {"hail_probability": [], "source": "Error", "note": "Hail probability unavailable"}
    
    async def get_fire_risk_assessment(
        self,
        latitude: float,
        longitude: float,
        days: int = 15
    ) -> Dict[str, Any]:
        """
        Get fire risk assessment for the next N days.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of forecast days
            
        Returns:
            Fire risk assessment data
        """
        try:
            if self.weather_provider == "noaa":
                # NOAA doesn't have fire risk API, return empty data
                logger.info(f"NOAA doesn't support fire risk assessment for {latitude}, {longitude}")
                return {"fire_index": [], "source": "NOAA-NWS", "note": "Fire risk assessment not available from NOAA"}
            elif self.weather_provider in ["tomorrow_io", "auto"]:
                # Use Tomorrow.io for fire risk
                try:
                    return await self.tomorrow_io.get_fire_risk_data(
                        latitude, longitude, days
                    )
                except Exception as e:
                    logger.warning(f"Tomorrow.io fire risk failed for {latitude}, {longitude}: {e}")
                    return {"fire_index": [], "source": "Tomorrow.io", "note": "Fire risk assessment unavailable"}
            else:
                raise ValueError(f"Unknown weather provider: {self.weather_provider}")
                
        except Exception as e:
            logger.error(f"Error fetching fire risk assessment for {latitude}, {longitude}: {e}")
            return {"fire_index": [], "source": "Error", "note": "Fire risk assessment unavailable"}
    
    def _convert_tomorrow_io_current_weather(self, weather_data: Dict[str, Any], location_name: Optional[str] = None) -> Dict[str, Any]:
        """Convert Tomorrow.io current weather format to expected format."""
        try:
            return {
                "temperature": weather_data.get("temperature"),
                "temperature_unit": "F",
                "temperature_apparent": weather_data.get("temperature_apparent"),
                "humidity": weather_data.get("humidity"),
                "wind_speed": weather_data.get("wind_speed"),
                "wind_gust": weather_data.get("wind_gust"),
                "wind_direction": weather_data.get("wind_direction"),
                "precipitation_intensity": weather_data.get("precipitation_intensity"),
                "precipitation_probability": weather_data.get("precipitation_probability"),
                "precipitation_type": weather_data.get("precipitation_type"),
                "weather_code": weather_data.get("weather_code"),
                "weather_code_full": weather_data.get("weather_code_full"),
                "visibility": weather_data.get("visibility"),
                "pressure": weather_data.get("pressure"),
                "cloud_cover": weather_data.get("cloud_cover"),
                "uv_index": weather_data.get("uv_index"),
                "hail_binary": weather_data.get("hail_binary"),
                "hail_probability": weather_data.get("hail_probability"),
                "fire_index": weather_data.get("fire_index"),
                "detailed_forecast": f"Weather conditions for {location_name or 'this location'}",
                "short_forecast": self._get_weather_description(weather_data.get("weather_code")),
                "is_daytime": True,  # Default to daytime
                "timestamp": weather_data.get("timestamp"),
                "source": "Tomorrow.io"
            }
        except Exception as e:
            logger.error(f"Error converting Tomorrow.io current weather: {e}")
            return self._get_fallback_weather_data(0, 0, location_name)
    
    def _convert_tomorrow_io_forecast(self, forecast_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Convert Tomorrow.io forecast format to expected format."""
        try:
            forecasts = []
            for period in forecast_data:
                forecast = {
                    "name": self._get_forecast_period_name(period.get("timestamp")),
                    "start_time": period.get("timestamp"),
                    "end_time": period.get("timestamp"),  # Will be calculated if needed
                    "temperature": period.get("temperature"),
                    "temperature_unit": "F",
                    "temperature_apparent": period.get("temperature_apparent"),
                    "humidity": period.get("humidity"),
                    "wind_speed": period.get("wind_speed"),
                    "wind_gust": period.get("wind_gust"),
                    "wind_direction": period.get("wind_direction"),
                    "precipitation_intensity": period.get("precipitation_intensity"),
                    "precipitation_probability": period.get("precipitation_probability"),
                    "precipitation_type": period.get("precipitation_type"),
                    "weather_code": period.get("weather_code"),
                    "weather_code_full": period.get("weather_code_full"),
                    "visibility": period.get("visibility"),
                    "pressure": period.get("pressure"),
                    "cloud_cover": period.get("cloud_cover"),
                    "uv_index": period.get("uv_index"),
                    "hail_binary": period.get("hail_binary"),
                    "hail_probability": period.get("hail_probability"),
                    "fire_index": period.get("fire_index"),
                    "detailed_forecast": self._get_detailed_forecast(period),
                    "short_forecast": self._get_weather_description(period.get("weather_code")),
                    "is_daytime": True  # Default to daytime
                }
                forecasts.append(forecast)
            
            return {
                "forecasts": forecasts,
                "source": "Tomorrow.io",
                "total_periods": len(forecasts)
            }
        except Exception as e:
            logger.error(f"Error converting Tomorrow.io forecast: {e}")
            return {"forecasts": [], "source": "Tomorrow.io", "total_periods": 0}
    
    def _get_weather_description(self, weather_code: Optional[int]) -> str:
        """Convert weather code to description."""
        if weather_code is None:
            return "Unknown"
        
        # Basic weather code mapping (can be expanded)
        weather_descriptions = {
            1000: "Clear",
            1100: "Mostly Clear",
            1101: "Partly Cloudy",
            1102: "Mostly Cloudy",
            1001: "Cloudy",
            2000: "Fog",
            2100: "Light Fog",
            4000: "Drizzle",
            4001: "Rain",
            4200: "Light Rain",
            4201: "Heavy Rain",
            5000: "Snow",
            5001: "Flurries",
            5100: "Light Snow",
            5101: "Heavy Snow",
            6000: "Freezing Drizzle",
            6001: "Freezing Rain",
            6200: "Light Freezing Rain",
            6201: "Heavy Freezing Rain",
            7000: "Ice Pellets",
            7101: "Heavy Ice Pellets",
            7102: "Light Ice Pellets",
            8000: "Thunderstorm"
        }
        
        return weather_descriptions.get(weather_code, "Unknown")
    
    def _get_forecast_period_name(self, timestamp: Optional[str]) -> str:
        """Get forecast period name from timestamp."""
        if not timestamp:
            return "Unknown"
        
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return dt.strftime("%A")
        except Exception:
            return "Unknown"
    
    def _get_detailed_forecast(self, period: Dict[str, Any]) -> str:
        """Generate detailed forecast description."""
        try:
            temp = period.get("temperature")
            precip_prob = period.get("precipitation_probability")
            wind_speed = period.get("wind_speed")
            weather_desc = self._get_weather_description(period.get("weather_code"))
            
            details = [weather_desc]
            
            if temp is not None:
                details.append(f"Temperature around {temp}°F")
            
            if precip_prob is not None and precip_prob > 0:
                details.append(f"{precip_prob}% chance of precipitation")
            
            if wind_speed is not None and wind_speed > 10:
                details.append(f"Winds {wind_speed} mph")
            
            return ". ".join(details) + "."
        except Exception:
            return "Weather forecast available"
    
    def _get_fallback_weather_data(self, latitude: float, longitude: float, location_name: Optional[str] = None) -> Dict[str, Any]:
        """Get fallback weather data when Tomorrow.io API fails."""
        try:
            # Generate reasonable fallback data based on location
            # This is a simplified fallback for when Tomorrow.io API is unavailable
            
            # Basic temperature estimation based on latitude
            base_temp = 70 - (abs(latitude) - 30) * 1.5  # Rough temperature estimation
            
            fallback_data = {
                "temperature": round(base_temp, 1),
                "temperature_unit": "F",
                "wind_speed": "5 mph",
                "wind_direction": "NW",
                "detailed_forecast": f"Weather data temporarily unavailable. Estimated conditions for {location_name or 'this location'}.",
                "short_forecast": "Partly Cloudy",
                "is_daytime": True,
                "timestamp": datetime.now().isoformat(),
                "source": "Fallback",
                "note": "Using estimated data due to Tomorrow.io API unavailability"
            }
            
            logger.info(f"Using fallback weather data for {latitude}, {longitude}")
            return fallback_data
            
        except Exception as e:
            logger.error(f"Error generating fallback weather data: {e}")
            # Return minimal fallback data
            return {
                "temperature": 72,
                "temperature_unit": "F",
                "wind_speed": "5 mph",
                "wind_direction": "NW",
                "detailed_forecast": "Weather data unavailable",
                "short_forecast": "Unknown",
                "is_daytime": True,
                "timestamp": datetime.now().isoformat(),
                "source": "Emergency Fallback",
                "note": "Minimal fallback data due to service error"
            }
    
    def _analyze_weather_trends(self, observations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze weather trends from historical observations."""
        if not observations:
            return {}
        
        try:
            # Extract temperature data
            temperatures = []
            precipitations = []
            wind_speeds = []
            
            for obs in observations:
                # Handle temperature - could be dict with "value" or direct value
                temp_data = obs.get("temperature")
                if isinstance(temp_data, dict):
                    temp_val = temp_data.get("value")
                else:
                    temp_val = temp_data
                if temp_val is not None:
                    temperatures.append(temp_val)
                
                # Handle precipitation - could be dict with "value" or direct value
                precip_data = obs.get("precipitation")
                if isinstance(precip_data, dict):
                    precip_val = precip_data.get("value")
                else:
                    precip_val = precip_data
                if precip_val is not None:
                    precipitations.append(precip_val)
                
                # Handle wind speed - could be dict with "value" or direct value
                wind_data = obs.get("wind_speed")
                if isinstance(wind_data, dict):
                    wind_val = wind_data.get("value")
                else:
                    wind_val = wind_data
                if wind_val is not None:
                    wind_speeds.append(wind_val)
            
            trends = {}
            
            # Temperature trends
            if temperatures:
                trends["temperature"] = {
                    "average": sum(temperatures) / len(temperatures),
                    "min": min(temperatures),
                    "max": max(temperatures),
                    "range": max(temperatures) - min(temperatures),
                    "trend_slope": self._calculate_trend_slope(temperatures),
                    "variability": self._calculate_variability(temperatures)
                }
            
            # Precipitation trends
            if precipitations:
                trends["precipitation"] = {
                    "total": sum(precipitations),
                    "average": sum(precipitations) / len(precipitations),
                    "max_daily": max(precipitations),
                    "rainy_days": len([p for p in precipitations if p > 0]),
                    "trend_slope": self._calculate_trend_slope(precipitations)
                }
            
            # Wind trends using SkyLink thresholds
            if wind_speeds:
                trends["wind"] = {
                    "average": sum(wind_speeds) / len(wind_speeds),
                    "max": max(wind_speeds),
                    "actionable_wind_days": len([w for w in wind_speeds if w >= WIND_MIN_ACTIONABLE]),  # ≥60 mph
                    "severe_wind_days": len([w for w in wind_speeds if w >= WIND_SEVERE]),  # ≥60 mph
                    "trend_slope": self._calculate_trend_slope(wind_speeds)
                }
            
            return trends
            
        except Exception as e:
            logger.error(f"Error analyzing weather trends: {e}")
            return {}
    
    def _analyze_seasonal_patterns(self, observations: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, Any]:
        """Analyze seasonal weather patterns."""
        if not observations:
            return {}
        
        try:
            # Group observations by month
            monthly_data = {}
            
            for obs in observations:
                timestamp_str = obs.get("timestamp")
                if timestamp_str:
                    try:
                        obs_date = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).date()
                        month = obs_date.month
                        
                        if month not in monthly_data:
                            monthly_data[month] = {"temperatures": [], "precipitations": [], "wind_speeds": []}
                        
                        # Handle temperature - could be dict with "value" or direct value
                        temp_data = obs.get("temperature")
                        if isinstance(temp_data, dict):
                            temp_val = temp_data.get("value")
                        else:
                            temp_val = temp_data
                        if temp_val is not None:
                            monthly_data[month]["temperatures"].append(temp_val)
                        
                        # Handle precipitation - could be dict with "value" or direct value
                        precip_data = obs.get("precipitation")
                        if isinstance(precip_data, dict):
                            precip_val = precip_data.get("value")
                        else:
                            precip_val = precip_data
                        if precip_val is not None:
                            monthly_data[month]["precipitations"].append(precip_val)
                        
                        # Handle wind speed - could be dict with "value" or direct value
                        wind_data = obs.get("wind_speed")
                        if isinstance(wind_data, dict):
                            wind_val = wind_data.get("value")
                        else:
                            wind_val = wind_data
                        if wind_val is not None:
                            monthly_data[month]["wind_speeds"].append(wind_val)
                    except Exception as e:
                        logger.warning(f"Error parsing observation timestamp: {e}")
            
            # Calculate monthly statistics
            seasonal_patterns = {}
            for month, data in monthly_data.items():
                month_name = date(1900, month, 1).strftime('%B')
                patterns = {}
                
                if data["temperatures"]:
                    patterns["temperature"] = {
                        "average": sum(data["temperatures"]) / len(data["temperatures"]),
                        "min": min(data["temperatures"]),
                        "max": max(data["temperatures"])
                    }
                
                if data["precipitations"]:
                    patterns["precipitation"] = {
                        "total": sum(data["precipitations"]),
                        "average": sum(data["precipitations"]) / len(data["precipitations"]),
                        "rainy_days": len([p for p in data["precipitations"] if p > 0])
                    }
                
                if data["wind_speeds"]:
                    patterns["wind"] = {
                        "average": sum(data["wind_speeds"]) / len(data["wind_speeds"]),
                        "max": max(data["wind_speeds"])
                    }
                
                seasonal_patterns[month_name] = patterns
            
            return seasonal_patterns
            
        except Exception as e:
            logger.error(f"Error analyzing seasonal patterns: {e}")
            return {}
    
    def _analyze_period_patterns(self, observations: List[Dict[str, Any]], period_days: int) -> Dict[str, Any]:
        """Analyze weather patterns specific to 6, 9, or 24 month periods."""
        if not observations:
            return {}
        
        try:
            # Determine period type
            if 180 <= period_days <= 185:
                period_type = "6_month"
            elif 270 <= period_days <= 275:
                period_type = "9_month"
            elif 720 <= period_days <= 735:
                period_type = "24_month"
            else:
                period_type = "unknown"
            
            # Extract key metrics
            temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
            precipitation = [obs.get("precipitation_intensity", 0) for obs in observations]
            wind_speeds = [obs.get("wind_speed") for obs in observations if obs.get("wind_speed")]
            
            # Calculate period-specific statistics
            analysis = {
                "period_type": period_type,
                "period_days": period_days,
                "total_observations": len(observations)
            }
            
            # Temperature analysis
            if temperatures:
                analysis["temperature"] = {
                    "average": sum(temperatures) / len(temperatures),
                    "min": min(temperatures),
                    "max": max(temperatures),
                    "range": max(temperatures) - min(temperatures),
                    "extreme_days": len([t for t in temperatures if t < 32 or t > 90])  # Freezing or hot days
                }
            
            # Precipitation analysis
            analysis["precipitation"] = {
                "total": sum(precipitation),
                "average_daily": sum(precipitation) / len(precipitation) if precipitation else 0,
                "rainy_days": len([p for p in precipitation if p > 0]),
                "heavy_rain_days": len([p for p in precipitation if p > 10]),
                "drought_periods": self._identify_drought_periods(precipitation)
            }
            
            # Wind analysis using SkyLink thresholds
            if wind_speeds:
                analysis["wind"] = {
                    "average": sum(wind_speeds) / len(wind_speeds),
                    "max": max(wind_speeds),
                    "actionable_wind_days": len([w for w in wind_speeds if w >= WIND_MIN_ACTIONABLE]),  # ≥60 mph
                    "severe_wind_days": len([w for w in wind_speeds if w >= WIND_SEVERE]),  # ≥60 mph
                    "moderate_wind_days": len([w for w in wind_speeds if w >= WIND_MODERATE]),  # ≥40 mph
                    "calm_days": len([w for w in wind_speeds if w < 10])
                }
            
            # Period-specific insights
            if period_type == "6_month":
                analysis["insights"] = [
                    "6-month analysis provides seasonal transition patterns",
                    "Captures half-year weather variability",
                    "Suitable for mid-term weather trend assessment"
                ]
            elif period_type == "9_month":
                analysis["insights"] = [
                    "9-month analysis provides comprehensive seasonal coverage",
                    "Captures nearly full annual weather cycle",
                    "Suitable for long-term weather pattern assessment"
                ]
            elif period_type == "24_month":
                analysis["insights"] = [
                    "24-month analysis provides multi-year weather patterns",
                    "Captures full seasonal cycles and year-over-year trends",
                    "Suitable for comprehensive historical weather assessment"
                ]
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing period patterns: {e}")
            return {"period_type": "unknown", "error": str(e)}
    
    def _identify_drought_periods(self, precipitation: List[float]) -> List[Dict[str, Any]]:
        """Identify drought periods (7+ consecutive days with <1mm precipitation)."""
        drought_periods = []
        current_drought_start = None
        drought_length = 0
        
        for i, precip in enumerate(precipitation):
            if precip < 1.0:  # Less than 1mm precipitation
                if current_drought_start is None:
                    current_drought_start = i
                drought_length += 1
            else:
                if drought_length >= 7:  # 7+ consecutive dry days
                    drought_periods.append({
                        "start_day": current_drought_start,
                        "length_days": drought_length,
                        "severity": "moderate" if drought_length < 14 else "severe"
                    })
                current_drought_start = None
                drought_length = 0
        
        # Check for drought period at end of data
        if drought_length >= 7:
            drought_periods.append({
                "start_day": current_drought_start,
                "length_days": drought_length,
                "severity": "moderate" if drought_length < 14 else "severe"
            })
        
        return drought_periods
    
    def _calculate_trend_slope(self, values: List[float]) -> float:
        """Calculate the slope of a trend line for given values."""
        if len(values) < 2:
            return 0
        
        n = len(values)
        x = list(range(n))
        y = values
        
        # Simple linear regression slope calculation
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(x[i] ** 2 for i in range(n))
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
        return slope
    
    def _calculate_variability(self, values: List[float]) -> float:
        """Calculate the variability (standard deviation) of values."""
        if len(values) < 2:
            return 0

        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5


# Singleton instance
_weather_data_service: Optional[WeatherDataService] = None


def get_weather_data_service() -> WeatherDataService:
    """Get weather data service instance."""
    global _weather_data_service
    if _weather_data_service is None:
        _weather_data_service = WeatherDataService()
    return _weather_data_service