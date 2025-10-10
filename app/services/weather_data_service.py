"""
Weather data service for fetching real weather data from NOAA and other APIs.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging
import sentry_sdk

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Weather thresholds from SkyLink standards
WIND_MIN_ACTIONABLE = settings.alert_wind_min_speed_mph
WIND_SEVERE = settings.wind_severe_threshold
WIND_MODERATE = settings.wind_moderate_threshold


class WeatherDataService:
    """Service for fetching real weather data from various APIs."""
    
    def __init__(self):
        """Initialize weather data service."""
        logger.info("Weather data service initialized - using NOAA Weather API (free)")
    
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
            # Try NOAA Weather API first (free, no API key required)
            weather_data = await self._get_noaa_current_weather(latitude, longitude)
            if weather_data:
                return weather_data
            
            # Fallback to mock data if NOAA fails
            logger.warning(f"NOAA API failed for {latitude}, {longitude}, using fallback data")
            return self._get_fallback_weather_data(latitude, longitude, location_name)
            
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
            days: Number of forecast days (max 16)
            
        Returns:
            Weather forecast data
        """
        try:
            # Use NOAA Weather API for forecasts
            return await self._get_noaa_forecast(latitude, longitude, days)
            
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
        Supports 6-month or 9-month analysis periods ending at current date.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for historical data
            end_date: End date (defaults to today if not provided)
            
        Returns:
            Historical weather data with trend analysis
        """
        try:
            # Use current date as end date if not provided
            if end_date is None:
                end_date = date.today()
            
            # Calculate period length
            period_days = (end_date - start_date).days
            
            # Validate period is 6, 9, or 24 months
            if not (180 <= period_days <= 185 or 270 <= period_days <= 275 or 720 <= period_days <= 735):
                raise ValueError("Analysis period must be exactly 6 months, 9 months, or 24 months")
            
            # For 6 or 9 month periods, fetch data in chunks
            historical_data = await self._get_extended_historical_weather(
                latitude, longitude, start_date, end_date
            )
            
            # Add trend analysis for both 6 and 9 month periods
            historical_data["trend_analysis"] = self._analyze_weather_trends(
                historical_data.get("observations", [])
            )
            historical_data["seasonal_analysis"] = self._analyze_seasonal_patterns(
                historical_data.get("observations", []), start_date, end_date
            )
            
            # Add period-specific analysis
            historical_data["period_analysis"] = self._analyze_period_patterns(
                historical_data.get("observations", []), period_days
            )
            
            # Add current date context
            historical_data["analysis_context"] = {
                "analysis_date": end_date.isoformat(),
                "period_length_days": period_days,
                "period_type": "6_month" if 180 <= period_days <= 185 else "9_month"
            }
            
            return historical_data
            
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
            return await self._get_noaa_weather_events(latitude, longitude, start_date, end_date, radius_km)
            
        except Exception as e:
            logger.error(f"Error fetching weather events for {latitude}, {longitude}: {e}")
            raise
    
    async def _get_noaa_current_weather(self, latitude: float, longitude: float) -> Optional[Dict[str, Any]]:
        """Get current weather from NOAA API."""
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Get current conditions for the point
                url = f"https://api.weather.gov/points/{latitude},{longitude}"
                response = await client.get(
                    url, 
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                response.raise_for_status()
                
                points_data = response.json()
                forecast_url = points_data["properties"]["forecast"]
                
                # Get current forecast
                forecast_response = await client.get(
                    forecast_url,
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                forecast_response.raise_for_status()
                
                forecast_data = forecast_response.json()
                return self._parse_noaa_forecast(forecast_data)
                
        except Exception as e:
            logger.error(f"Error fetching NOAA current weather: {e}")
            return None
    
    async def _get_extended_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """
        Fetch historical weather data for extended periods (e.g., 6-9 months) in chunks.
        NOAA API has limits on observation fetches, so this will paginate or chunk requests.
        """
        all_observations = []
        current_date = start_date
        
        while current_date <= end_date:
            chunk_end_date = min(current_date + timedelta(days=30), end_date) # Fetch in 30-day chunks
            try:
                chunk_data = await self._get_noaa_historical_weather(
                    latitude, longitude, current_date, chunk_end_date
                )
                all_observations.extend(chunk_data.get("observations", []))
            except Exception as e:
                logger.warning(f"Could not fetch historical data for {current_date} to {chunk_end_date}: {e}")
            current_date = chunk_end_date + timedelta(days=1)
        
        return {"observations": all_observations, "source": "NOAA"}
    
    async def _get_noaa_forecast(self, latitude: float, longitude: float, days: int) -> Dict[str, Any]:
        """Get weather forecast from NOAA API."""
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Get forecast URL from points API
                url = f"https://api.weather.gov/points/{latitude},{longitude}"
                response = await client.get(
                    url, 
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                response.raise_for_status()
                
                points_data = response.json()
                forecast_url = points_data["properties"]["forecast"]
                
                # Get extended forecast
                forecast_response = await client.get(
                    forecast_url,
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                forecast_response.raise_for_status()
                
                forecast_data = forecast_response.json()
                return self._parse_noaa_extended_forecast(forecast_data, days)
                
        except Exception as e:
            logger.error(f"Error fetching NOAA forecast: {e}")
            raise
    
    def _parse_noaa_extended_forecast(self, data: Dict[str, Any], days: int) -> Dict[str, Any]:
        """Parse NOAA extended forecast data."""
        try:
            properties = data.get("properties", {})
            periods = properties.get("periods", [])
            
            # Limit to requested number of days (each period is 12 hours)
            max_periods = min(days * 2, len(periods))
            forecast_periods = periods[:max_periods]
            
            forecasts = []
            for period in forecast_periods:
                forecasts.append({
                    "name": period.get("name"),
                    "start_time": period.get("startTime"),
                    "end_time": period.get("endTime"),
                    "temperature": period.get("temperature"),
                    "temperature_unit": period.get("temperatureUnit"),
                    "wind_speed": period.get("windSpeed"),
                    "wind_direction": period.get("windDirection"),
                    "detailed_forecast": period.get("detailedForecast"),
                    "short_forecast": period.get("shortForecast"),
                    "is_daytime": period.get("isDaytime"),
                    "icon": period.get("icon")
                })
            
            return {
                "forecasts": forecasts,
                "source": "NOAA",
                "total_periods": len(forecasts)
            }
        except Exception as e:
            logger.error(f"Error parsing NOAA extended forecast: {e}")
            return {"forecasts": [], "source": "NOAA", "total_periods": 0}
    
    async def _get_noaa_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Get historical weather data from NOAA API."""
        try:
            # Find nearest weather station
            station_id = await self._find_nearest_noaa_station(latitude, longitude)
            if not station_id:
                raise Exception("No NOAA weather station found for location")
            
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Get historical observations for date range
                # NOAA API requires ISO 8601 format with timezone (RFC 3339)
                url = f"https://api.weather.gov/stations/{station_id}/observations"

                # Convert dates to datetime with timezone (UTC)
                from datetime import datetime as dt
                start_datetime = dt.combine(start_date, dt.min.time()).replace(tzinfo=None).isoformat() + 'Z'
                end_datetime = dt.combine(end_date, dt.max.time()).replace(tzinfo=None).isoformat() + 'Z'

                params = {
                    "start": start_datetime,
                    "end": end_datetime,
                    "limit": 500
                }

                response = await client.get(url, params=params, timeout=30.0)
                response.raise_for_status()
                
                data = response.json()
                return self._parse_noaa_historical(data)
                
        except Exception as e:
            logger.error(f"Error fetching NOAA historical weather: {e}")
            raise
    
    async def _get_noaa_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_km: float
    ) -> List[Dict[str, Any]]:
        """
        Get historical weather events from NOAA Storm Events Database.

        This uses the NOAA NCEI Storm Events Database which provides gzipped CSV files
        organized by year at: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
        """
        try:
            import gzip
            from io import BytesIO

            events = []

            # NOAA NCEI Storm Events Database CSV files (organized by year)
            base_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"

            # Get all years in the date range
            start_year = start_date.year
            end_year = end_date.year

            async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
                # Fetch data for each year in the range
                for year in range(start_year, end_year + 1):
                    # NOAA file naming convention - try multiple compile dates
                    # Files are updated periodically with new compile dates
                    # Start with most recent compile dates first
                    compile_dates = ["20251001", "20250901", "20250818", "20250801", "20250701", "20241201", "20240916"]

                    year_found = False
                    for compile_date in compile_dates:
                        if year_found:
                            break

                        filename = f"StormEvents_details-ftp_v1.0_d{year}_c{compile_date}.csv.gz"
                        file_url = f"{base_url}{filename}"

                        try:
                            logger.info(f"Trying storm events for year {year} from {file_url}")

                            response = await client.get(
                                file_url,
                                headers={"User-Agent": "Weather-Reports-Service/1.0"}
                            )

                            if response.status_code == 200:
                                # Decompress gzip file
                                try:
                                    csv_data = gzip.decompress(response.content).decode('utf-8')

                                    # Parse CSV and filter by location and date
                                    year_events = await self._parse_storm_events_csv(
                                        csv_data, latitude, longitude, radius_km, start_date, end_date
                                    )
                                    events.extend(year_events)
                                    logger.info(f"Found {len(year_events)} events for year {year}")
                                    year_found = True

                                except Exception as decompress_error:
                                    logger.error(f"Failed to decompress storm events file for {year}: {decompress_error}")
                                    sentry_sdk.capture_exception(decompress_error)
                            else:
                                logger.debug(f"Storm events file not found with compile date {compile_date} (HTTP {response.status_code})")

                        except Exception as year_error:
                            logger.debug(f"Failed to fetch storm events for year {year} with compile date {compile_date}: {year_error}")
                            # Try next compile date
                            continue

                    if not year_found:
                        logger.warning(f"No storm events file found for year {year} (tried all compile dates)")

            # Sort events by date (most recent first)
            events.sort(key=lambda x: x.get('date', ''), reverse=True)

            logger.info(f"Total storm events found: {len(events)}")
            return events

        except Exception as e:
            logger.error(f"Error fetching NOAA weather events: {e}")
            sentry_sdk.capture_exception(e)
            raise
    
    async def _get_noaa_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Get historical weather data from NOAA API."""
        try:
            # Find nearest weather station
            station_id = await self._find_nearest_noaa_station(latitude, longitude)
            if not station_id:
                raise Exception("No NOAA weather station found for location")
            
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Get historical observations for date range
                # NOAA API requires ISO 8601 format with timezone (RFC 3339)
                url = f"https://api.weather.gov/stations/{station_id}/observations"

                # Convert dates to datetime with timezone (UTC)
                from datetime import datetime as dt
                start_datetime = dt.combine(start_date, dt.min.time()).replace(tzinfo=None).isoformat() + 'Z'
                end_datetime = dt.combine(end_date, dt.max.time()).replace(tzinfo=None).isoformat() + 'Z'

                params = {
                    "start": start_datetime,
                    "end": end_datetime,
                    "limit": 500
                }

                response = await client.get(
                    url,
                    params=params,
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=30.0
                )
                response.raise_for_status()
                
                data = response.json()
                return self._parse_noaa_historical(data)
                
        except Exception as e:
            logger.error(f"Error fetching NOAA historical weather: {e}")
            raise
    
    async def _find_nearest_noaa_station(self, latitude: float, longitude: float) -> Optional[str]:
        """Find the nearest NOAA weather station."""
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                # Get the point data first
                url = f"https://api.weather.gov/points/{latitude},{longitude}"
                response = await client.get(
                    url,
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                response.raise_for_status()
                
                points_data = response.json()
                station_url = points_data.get("properties", {}).get("observationStations")
                
                if station_url:
                    # Get the nearest station
                    stations_response = await client.get(
                        station_url,
                        headers={"User-Agent": "Weather-Reports-Service/1.0"},
                        timeout=10.0
                    )
                    stations_response.raise_for_status()
                    
                    stations_data = stations_response.json()
                    stations = stations_data.get("features", [])
                    
                    if stations:
                        # Extract station ID from the first (nearest) station
                        station_id = stations[0].get("properties", {}).get("stationIdentifier")
                        return station_id
                
                return None
                
        except Exception as e:
            logger.error(f"Error finding nearest NOAA station: {e}")
            return None
    
    def _parse_noaa_forecast(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse NOAA forecast data."""
        try:
            properties = data.get("properties", {})
            periods = properties.get("periods", [])
            
            if not periods:
                return {}
            
            # Get current period (first period)
            current_period = periods[0]
            
            return {
                "temperature": current_period.get("temperature"),
                "temperature_unit": current_period.get("temperatureUnit"),
                "wind_speed": current_period.get("windSpeed"),
                "wind_direction": current_period.get("windDirection"),
                "detailed_forecast": current_period.get("detailedForecast"),
                "short_forecast": current_period.get("shortForecast"),
                "is_daytime": current_period.get("isDaytime"),
                "timestamp": current_period.get("startTime"),
                "source": "NOAA"
            }
        except Exception as e:
            logger.error(f"Error parsing NOAA forecast: {e}")
            return {}
    
    def _parse_noaa_observation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse NOAA observation data."""
        try:
            properties = data.get("properties", {})
            
            return {
                "temperature": properties.get("temperature", {}).get("value"),
                "humidity": properties.get("relativeHumidity", {}).get("value"),
                "pressure": properties.get("barometricPressure", {}).get("value"),
                "wind_speed": properties.get("windSpeed", {}).get("value"),
                "wind_direction": properties.get("windDirection", {}).get("value"),
                "visibility": properties.get("visibility", {}).get("value"),
                "weather_condition": properties.get("textDescription"),
                "timestamp": properties.get("timestamp"),
                "source": "NOAA"
            }
        except Exception as e:
            logger.error(f"Error parsing NOAA observation: {e}")
            return {}
    
    
    def _parse_noaa_historical(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse NOAA historical weather data."""
        try:
            observations = []
            for item in data.get("features", []):
                properties = item.get("properties", {})
                
                observations.append({
                    "datetime": properties.get("timestamp"),
                    "temperature": properties.get("temperature", {}).get("value"),
                    "humidity": properties.get("relativeHumidity", {}).get("value"),
                    "pressure": properties.get("barometricPressure", {}).get("value"),
                    "wind_speed": properties.get("windSpeed", {}).get("value"),
                    "wind_direction": properties.get("windDirection", {}).get("value"),
                    "visibility": properties.get("visibility", {}).get("value"),
                    "weather_condition": properties.get("textDescription")
                })
            
            return {
                "observations": observations,
                "source": "NOAA"
            }
        except Exception as e:
            logger.error(f"Error parsing NOAA historical data: {e}")
            return {"observations": [], "source": "NOAA"}
    
    def _parse_noaa_alerts(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse NOAA weather alerts."""
        try:
            alerts = []
            for item in data.get("features", []):
                properties = item.get("properties", {})
                
                alerts.append({
                    "id": properties.get("id"),
                    "event": properties.get("event"),
                    "severity": properties.get("severity"),
                    "urgency": properties.get("urgency"),
                    "areas": properties.get("areaDesc"),
                    "description": properties.get("description"),
                    "instruction": properties.get("instruction"),
                    "effective": properties.get("effective"),
                    "expires": properties.get("expires"),
                    "status": properties.get("status")
                })
            
            return alerts
        except Exception as e:
            logger.error(f"Error parsing NOAA alerts: {e}")
            return []


        return drought_periods
    
    def _analyze_period_patterns(self, observations: List[Dict[str, Any]], period_days: int) -> Dict[str, Any]:
        """Analyze weather patterns specific to 6 or 9 month periods."""
        if not observations:
            return {}
        
        try:
            # Determine period type
            period_type = "6_month" if 180 <= period_days <= 185 else "9_month"
            
            # Extract key metrics
            temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
            precipitation = [obs.get("precipitation", 0) for obs in observations]
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
                "average_daily": sum(precipitation) / len(precipitation),
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
            else:  # 9_month
                analysis["insights"] = [
                    "9-month analysis provides comprehensive seasonal coverage",
                    "Captures nearly full annual weather cycle",
                    "Suitable for long-term weather pattern assessment"
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

    async def _get_state_from_coordinates(self, latitude: float, longitude: float) -> str:
        """Get state abbreviation from coordinates using reverse geocoding."""
        # Simple mapping for Texas (Cypress, TX area)
        # In production, use proper reverse geocoding
        if 25 <= latitude <= 37 and -107 <= longitude <= -93:
            return "TEXAS"
        return "TEXAS"  # Default for now

    async def _parse_storm_events_csv(
        self,
        csv_text: str,
        latitude: float,
        longitude: float,
        radius_km: float,
        start_date: date,
        end_date: date
    ) -> List[Dict[str, Any]]:
        """Parse NOAA Storm Events CSV data and filter by distance and date range."""
        import csv
        from io import StringIO
        from datetime import datetime

        events = []

        try:
            reader = csv.DictReader(StringIO(csv_text))
            for row in reader:
                try:
                    # Parse event date
                    begin_date_str = row.get('BEGIN_DATE_TIME', '') or row.get('BEGIN_DATE', '')
                    if not begin_date_str:
                        continue

                    # NOAA date format: "dd-MMM-yy HH:MM:SS" or "dd-MMM-yy"
                    try:
                        if ' ' in begin_date_str:
                            event_date = datetime.strptime(begin_date_str, '%d-%b-%y %H:%M:%S').date()
                        else:
                            event_date = datetime.strptime(begin_date_str, '%d-%b-%y').date()
                    except ValueError:
                        # Try alternate formats
                        continue

                    # Filter by date range
                    if not (start_date <= event_date <= end_date):
                        continue

                    # Parse coordinates
                    event_lat = float(row.get('BEGIN_LAT', 0))
                    event_lon = float(row.get('BEGIN_LON', 0))

                    # Skip events without valid coordinates
                    if event_lat == 0 and event_lon == 0:
                        continue

                    # Calculate distance
                    distance = self._calculate_distance(
                        latitude, longitude, event_lat, event_lon
                    )

                    # Filter by radius
                    if distance <= radius_km:
                        # Extract magnitude and type
                        magnitude = row.get('MAGNITUDE', '')
                        magnitude_type = row.get('MAGNITUDE_TYPE', '')

                        # For hail, use TOR_F_SCALE, for others use MAGNITUDE
                        event_type = row.get('EVENT_TYPE', '').strip()

                        events.append({
                            'event': event_type,
                            'date': event_date.strftime('%Y-%m-%d'),
                            'magnitude': magnitude,
                            'magnitude_type': magnitude_type,
                            'severity': self._determine_severity(row),
                            'latitude': event_lat,
                            'longitude': event_lon,
                            'distance_km': round(distance, 2),
                            'description': row.get('EVENT_NARRATIVE', '') or f"{event_type} event",
                            'injuries_direct': row.get('INJURIES_DIRECT', 0),
                            'injuries_indirect': row.get('INJURIES_INDIRECT', 0),
                            'deaths_direct': row.get('DEATHS_DIRECT', 0),
                            'deaths_indirect': row.get('DEATHS_INDIRECT', 0),
                            'damage_property': row.get('DAMAGE_PROPERTY', ''),
                            'damage_crops': row.get('DAMAGE_CROPS', '')
                        })
                except (ValueError, KeyError) as e:
                    # Skip malformed rows
                    continue
        except Exception as e:
            logger.error(f"Error parsing storm events CSV: {e}")
            sentry_sdk.capture_exception(e)

        return events

    def _calculate_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float
    ) -> float:
        """Calculate distance between two coordinates in km (Haversine formula)."""
        import math

        R = 6371  # Earth's radius in km

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) *
             math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def _determine_severity(self, storm_event_row: Dict[str, Any]) -> str:
        """Determine severity from storm event data."""
        event_type = storm_event_row.get('EVENT_TYPE', '').lower()
        magnitude = storm_event_row.get('MAGNITUDE', '')

        try:
            mag_value = float(magnitude) if magnitude else 0
        except ValueError:
            mag_value = 0

        # Hail severity
        if 'hail' in event_type:
            if mag_value >= 2.0:
                return "Severe"
            elif mag_value >= 1.0:
                return "Moderate"
            else:
                return "Minor"

        # Wind severity
        if 'wind' in event_type or 'thunderstorm' in event_type:
            if mag_value >= 75:
                return "Severe"
            elif mag_value >= 50:
                return "Moderate"
            else:
                return "Minor"

        # Tornado severity
        if 'tornado' in event_type:
            return "Severe"

        return "Moderate"

    def _get_fallback_weather_data(self, latitude: float, longitude: float, location_name: Optional[str] = None) -> Dict[str, Any]:
        """Get fallback weather data when NOAA API fails."""
        try:
            # Generate reasonable fallback data based on location
            # This is a simplified fallback for when NOAA API is unavailable
            
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
                "note": "Using estimated data due to NOAA API unavailability"
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


# Singleton instance
_weather_data_service: Optional[WeatherDataService] = None


def get_weather_data_service() -> WeatherDataService:
    """Get weather data service instance."""
    global _weather_data_service
    if _weather_data_service is None:
        _weather_data_service = WeatherDataService()
    return _weather_data_service