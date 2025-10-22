"""
Tomorrow.io Weather API service for fetching comprehensive weather data.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging
import sentry_sdk
from urllib.parse import urlencode

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class TomorrowIOService:
    """Service for fetching weather data from Tomorrow.io API."""
    
    def __init__(self):
        """Initialize Tomorrow.io service."""
        self.api_key = settings.tomorrow_io_api_key
        self.base_url = settings.tomorrow_io_base_url
        self.timeout = settings.tomorrow_io_timeout
        self.max_retries = settings.tomorrow_io_max_retries
        
        if not self.api_key:
            logger.warning("Tomorrow.io API key not configured")
        
        logger.info("Tomorrow.io Weather API service initialized")
    
    async def get_current_and_forecast(
        self,
        latitude: float,
        longitude: float,
        timesteps: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get current weather and forecast using Timeline API.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            timesteps: List of timesteps ['1h', '1d'] for hourly and daily data
            
        Returns:
            Current weather and forecast data
        """
        if timesteps is None:
            timesteps = ['1h', '1d']
        
        try:
            # Core weather data layers - comprehensive fields for address reports
            fields = [
                # Temperature
                'temperature',
                'temperatureApparent',
                'dewPoint',

                # Humidity
                'humidity',

                # Precipitation - comprehensive coverage
                'precipitationIntensity',
                'precipitationProbability',
                'precipitationType',
                'rainIntensity',
                'rainAccumulation',
                'snowIntensity',
                'snowAccumulation',
                'freezingRainIntensity',
                'iceAccumulation',
                'sleetIntensity',

                # Wind - critical for damage assessment
                'windSpeed',
                'windGust',
                'windDirection',

                # Severe weather indicators
                'weatherCode',
                'weatherCodeFull',
                'thunderstormProbability',

                # Visibility & Pressure
                'visibility',
                'pressureSurfaceLevel',

                # Cloud data
                'cloudCover',
                'cloudBase',
                'cloudCeiling',

                # UV & Environmental
                'uvIndex',

                # Hail - critical for roofing
                'hailBinary',
                'hailProbability',

                # Fire risk
                'fireIndex'
            ]
            
            # Build Timeline API request
            url = f"{self.base_url}/timelines"
            
            # Timeline API uses POST with JSON body
            payload = {
                "location": f"{latitude},{longitude}",
                "fields": fields,
                "timesteps": timesteps,
                "units": "imperial",
                "timezone": "America/Chicago"  # Default to Central Time
            }
            
            # Add time range for forecast (next 15 days)
            now = datetime.utcnow()
            payload["startTime"] = now.isoformat() + "Z"
            payload["endTime"] = (now + timedelta(days=15)).isoformat() + "Z"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "apikey": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_timeline_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching current weather and forecast: {e}")
            sentry_sdk.capture_exception(e)
            return self._get_empty_weather_data()
    
    async def get_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """
        Get historical weather data using Historical API.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for historical data
            end_date: End date for historical data
            
        Returns:
            Historical weather data
        """
        try:
            # Historical API fields - comprehensive data for trend analysis
            fields = [
                # Temperature
                'temperature',
                'temperatureApparent',
                'dewPoint',

                # Humidity
                'humidity',

                # Precipitation - comprehensive historical data
                'precipitationIntensity',
                'precipitationProbability',
                'precipitationType',
                'rainIntensity',
                'rainAccumulation',
                'snowIntensity',
                'snowAccumulation',
                'freezingRainIntensity',
                'iceAccumulation',
                'sleetIntensity',

                # Wind - historical wind patterns
                'windSpeed',
                'windGust',
                'windDirection',

                # Weather conditions
                'weatherCode',
                'thunderstormProbability',

                # Visibility & Pressure
                'visibility',
                'pressureSurfaceLevel',

                # Cloud data
                'cloudCover',

                # UV
                'uvIndex',

                # Hail - historical hail events
                'hailBinary',
                'hailProbability',

                # Fire risk
                'fireIndex'
            ]
            
            # Build Historical API request
            url = f"{self.base_url}/timelines"
            
            payload = {
                "location": f"{latitude},{longitude}",
                "fields": fields,
                "timesteps": ["1d"],  # Daily historical data
                "units": "imperial",
                "timezone": "America/Chicago"
            }
            
            # Add time range
            payload["startTime"] = start_date.isoformat() + "T00:00:00Z"
            payload["endTime"] = end_date.isoformat() + "T23:59:59Z"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "apikey": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_historical_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching historical weather: {e}")
            sentry_sdk.capture_exception(e)
            return {"observations": [], "source": "Tomorrow.io"}
    
    async def get_severe_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Get severe weather events using Events API.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for events
            end_date: End date for events
            radius_km: Search radius in kilometers
            
        Returns:
            List of severe weather events
        """
        try:
            # Events API endpoint
            url = f"{self.base_url}/events"
            
            payload = {
                "location": f"{latitude},{longitude}",
                "startTime": start_date.isoformat() + "T00:00:00Z",
                "endTime": end_date.isoformat() + "T23:59:59Z",
                "radius": f"{radius_km}km",
                "eventTypes": [
                    "hail",              # Hail events
                    "wind",              # High wind events
                    "tornado",           # Tornado events
                    "tropical",          # Hurricane & tropical storm events
                    "fire",              # Fire weather events
                    "flood",             # Flooding events
                    "winter",            # Winter storm events
                    "thunderstorm"       # Thunderstorm events
                ]
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "apikey": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_events_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching severe weather events: {e}")
            sentry_sdk.capture_exception(e)
            return []
    
    async def get_hail_probability_forecast(
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
            url = f"{self.base_url}/timelines"
            
            payload = {
                "location": f"{latitude},{longitude}",
                "fields": ["hailProbability", "hailBinary", "precipitationIntensity"],
                "timesteps": ["1h"],
                "units": "imperial",
                "timezone": "America/Chicago"
            }
            
            # Add time range
            now = datetime.utcnow()
            payload["startTime"] = now.isoformat() + "Z"
            payload["endTime"] = (now + timedelta(days=days)).isoformat() + "Z"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "apikey": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_hail_probability_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching hail probability forecast: {e}")
            sentry_sdk.capture_exception(e)
            return {"hail_probability": [], "source": "Tomorrow.io"}
    
    async def get_fire_risk_data(
        self,
        latitude: float,
        longitude: float,
        days: int = 15
    ) -> Dict[str, Any]:
        """
        Get fire risk data for the next N days.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of forecast days
            
        Returns:
            Fire risk data
        """
        try:
            url = f"{self.base_url}/timelines"
            
            payload = {
                "location": f"{latitude},{longitude}",
                "fields": ["fireIndex", "temperature", "humidity", "windSpeed"],
                "timesteps": ["1d"],
                "units": "imperial",
                "timezone": "America/Chicago"
            }
            
            # Add time range
            now = datetime.utcnow()
            payload["startTime"] = now.isoformat() + "Z"
            payload["endTime"] = (now + timedelta(days=days)).isoformat() + "Z"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "apikey": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_fire_risk_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching fire risk data: {e}")
            sentry_sdk.capture_exception(e)
            return {"fire_index": [], "source": "Tomorrow.io"}
    
    async def get_weather_map_tiles(
        self,
        latitude: float,
        longitude: float,
        layer: str,
        zoom: int = 10
    ) -> Dict[str, Any]:
        """
        Get weather map tile URLs for visualization.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            layer: Map layer type (precipitation, temperature, wind, fire)
            zoom: Map zoom level
            
        Returns:
            Map tile information
        """
        try:
            # Weather Maps API endpoint
            url = f"{self.base_url}/map"
            
            params = {
                "location": f"{latitude},{longitude}",
                "fields": layer,
                "zoom": zoom,
                "apikey": self.api_key
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                return self._parse_map_tiles_data(data)
                
        except Exception as e:
            logger.error(f"Error fetching weather map tiles: {e}")
            sentry_sdk.capture_exception(e)
            return {"tiles": [], "source": "Tomorrow.io"}
    
    def _parse_timeline_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Timeline API response data."""
        try:
            timelines = data.get("data", {}).get("timelines", [])
            if not timelines:
                return self._get_empty_weather_data()
            
            timeline = timelines[0]  # Get first timeline
            intervals = timeline.get("intervals", [])
            
            current_weather = {}
            forecast = []
            
            for interval in intervals:
                values = interval.get("values", {})
                start_time = interval.get("startTime")
                
                weather_data = {
                    "timestamp": start_time,
                    "temperature": values.get("temperature"),
                    "temperature_apparent": values.get("temperatureApparent"),
                    "humidity": values.get("humidity"),
                    "precipitation_intensity": values.get("precipitationIntensity"),
                    "precipitation_probability": values.get("precipitationProbability"),
                    "precipitation_type": values.get("precipitationType"),
                    "wind_speed": values.get("windSpeed"),
                    "wind_gust": values.get("windGust"),
                    "wind_direction": values.get("windDirection"),
                    "weather_code": values.get("weatherCode"),
                    "weather_code_full": values.get("weatherCodeFull"),
                    "visibility": values.get("visibility"),
                    "pressure": values.get("pressureSurfaceLevel"),
                    "cloud_cover": values.get("cloudCover"),
                    "uv_index": values.get("uvIndex"),
                    "hail_binary": values.get("hailBinary"),
                    "hail_probability": values.get("hailProbability"),
                    "fire_index": values.get("fireIndex")
                }
                
                # First interval is current weather
                if not current_weather:
                    current_weather = weather_data
                else:
                    forecast.append(weather_data)
            
            return {
                "current_weather": current_weather,
                "forecast": forecast,
                "source": "Tomorrow.io"
            }
            
        except Exception as e:
            logger.error(f"Error parsing timeline data: {e}")
            return self._get_empty_weather_data()
    
    def _parse_historical_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Historical API response data."""
        try:
            timelines = data.get("data", {}).get("timelines", [])
            if not timelines:
                return {"observations": [], "source": "Tomorrow.io"}
            
            timeline = timelines[0]
            intervals = timeline.get("intervals", [])
            
            observations = []
            for interval in intervals:
                values = interval.get("values", {})
                start_time = interval.get("startTime")
                
                observation = {
                    "datetime": start_time,
                    "temperature": values.get("temperature"),
                    "temperature_apparent": values.get("temperatureApparent"),
                    "humidity": values.get("humidity"),
                    "precipitation_intensity": values.get("precipitationIntensity"),
                    "precipitation_probability": values.get("precipitationProbability"),
                    "precipitation_type": values.get("precipitationType"),
                    "wind_speed": values.get("windSpeed"),
                    "wind_gust": values.get("windGust"),
                    "wind_direction": values.get("windDirection"),
                    "weather_code": values.get("weatherCode"),
                    "visibility": values.get("visibility"),
                    "pressure": values.get("pressureSurfaceLevel"),
                    "cloud_cover": values.get("cloudCover"),
                    "uv_index": values.get("uvIndex"),
                    "hail_binary": values.get("hailBinary"),
                    "hail_probability": values.get("hailProbability"),
                    "fire_index": values.get("fireIndex")
                }
                observations.append(observation)
            
            return {
                "observations": observations,
                "source": "Tomorrow.io"
            }
            
        except Exception as e:
            logger.error(f"Error parsing historical data: {e}")
            return {"observations": [], "source": "Tomorrow.io"}
    
    def _parse_events_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse Events API response data."""
        try:
            events = []
            for event in data.get("data", {}).get("events", []):
                event_data = {
                    "event_type": event.get("eventType"),
                    "severity": event.get("severity"),
                    "start_time": event.get("startTime"),
                    "end_time": event.get("endTime"),
                    "latitude": event.get("location", {}).get("lat"),
                    "longitude": event.get("location", {}).get("lon"),
                    "description": event.get("description"),
                    "impact": event.get("impact"),
                    "certainty": event.get("certainty"),
                    "urgency": event.get("urgency")
                }
                events.append(event_data)
            
            return events
            
        except Exception as e:
            logger.error(f"Error parsing events data: {e}")
            return []
    
    def _parse_hail_probability_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse hail probability forecast data."""
        try:
            timelines = data.get("data", {}).get("timelines", [])
            if not timelines:
                return {"hail_probability": [], "source": "Tomorrow.io"}
            
            timeline = timelines[0]
            intervals = timeline.get("intervals", [])
            
            hail_data = []
            for interval in intervals:
                values = interval.get("values", {})
                hail_data.append({
                    "timestamp": interval.get("startTime"),
                    "hail_probability": values.get("hailProbability"),
                    "hail_binary": values.get("hailBinary"),
                    "precipitation_intensity": values.get("precipitationIntensity")
                })
            
            return {
                "hail_probability": hail_data,
                "source": "Tomorrow.io"
            }
            
        except Exception as e:
            logger.error(f"Error parsing hail probability data: {e}")
            return {"hail_probability": [], "source": "Tomorrow.io"}
    
    def _parse_fire_risk_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse fire risk data."""
        try:
            timelines = data.get("data", {}).get("timelines", [])
            if not timelines:
                return {"fire_index": [], "source": "Tomorrow.io"}
            
            timeline = timelines[0]
            intervals = timeline.get("intervals", [])
            
            fire_data = []
            for interval in intervals:
                values = interval.get("values", {})
                fire_data.append({
                    "timestamp": interval.get("startTime"),
                    "fire_index": values.get("fireIndex"),
                    "temperature": values.get("temperature"),
                    "humidity": values.get("humidity"),
                    "wind_speed": values.get("windSpeed")
                })
            
            return {
                "fire_index": fire_data,
                "source": "Tomorrow.io"
            }
            
        except Exception as e:
            logger.error(f"Error parsing fire risk data: {e}")
            return {"fire_index": [], "source": "Tomorrow.io"}
    
    def _parse_map_tiles_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse weather map tiles data."""
        try:
            return {
                "tiles": data.get("data", {}).get("tiles", []),
                "source": "Tomorrow.io"
            }
            
        except Exception as e:
            logger.error(f"Error parsing map tiles data: {e}")
            return {"tiles": [], "source": "Tomorrow.io"}
    
    def _get_empty_weather_data(self) -> Dict[str, Any]:
        """Return empty weather data structure when API fails."""
        return {
            "current_weather": {},
            "forecast": [],
            "source": "Tomorrow.io",
            "error": "API request failed"
        }


# Singleton instance
_tomorrow_io_service: Optional[TomorrowIOService] = None


def get_tomorrow_io_service() -> TomorrowIOService:
    """Get Tomorrow.io service instance."""
    global _tomorrow_io_service
    if _tomorrow_io_service is None:
        _tomorrow_io_service = TomorrowIOService()
    return _tomorrow_io_service
