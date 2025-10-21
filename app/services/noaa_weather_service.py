"""
NOAA Weather API service for fetching comprehensive weather data.
Primary weather provider for ApexOS Reports Service.
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
from app.services.rate_limiter import CDORateLimiter, RateLimitExceededError

logger = get_logger(__name__)


class NOAAWeatherService:
    """Primary service for fetching weather data from NOAA/NWS APIs."""
    
    def __init__(self):
        """Initialize NOAA weather service."""
        self.cdo_api_token = settings.noaa_cdo_api_token
        self.nws_base_url = settings.noaa_nws_base_url
        self.cdo_base_url = settings.noaa_cdo_base_url
        self.timeout = settings.noaa_timeout
        self.max_retries = settings.noaa_max_retries
        self.user_agent = settings.noaa_user_agent
        
        # Initialize rate limiter for CDO API
        self.rate_limiter = CDORateLimiter(
            requests_per_second=settings.noaa_cdo_requests_per_second,
            requests_per_day=settings.noaa_cdo_requests_per_day,
            buffer_factor=settings.noaa_cdo_rate_limit_buffer
        )
        
        if not self.cdo_api_token:
            logger.warning("NOAA CDO API token not configured - historical data will be limited")
        
        logger.info("NOAA Weather API service initialized as primary provider")
    
    async def get_current_weather(
        self,
        latitude: float,
        longitude: float,
        location_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get current weather conditions from nearest NWS observation station.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            location_name: Optional location name for context
            
        Returns:
            Current weather data
        """
        try:
            # Step 1: Get point metadata to find forecast office
            point_metadata = await self.get_point_metadata(latitude, longitude)
            
            # Step 2: Get nearest observation station
            station_url = point_metadata.get("observationStations")
            if not station_url:
                logger.warning(f"No observation stations URL for {latitude}, {longitude}")
                return self._get_fallback_weather_data(latitude, longitude, location_name)
            
            # Step 3: Get latest observation from nearest station
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Get station list
                stations_response = await client.get(
                    station_url,
                    headers={"User-Agent": self.user_agent}
                )
                stations_response.raise_for_status()
                stations_data = stations_response.json()
                
                # Get nearest station
                stations = stations_data.get("features", [])
                if not stations:
                    logger.warning(f"No stations found for {latitude}, {longitude}")
                    return self._get_fallback_weather_data(latitude, longitude, location_name)
                
                nearest_station = stations[0]
                station_id = nearest_station.get("properties", {}).get("stationIdentifier")
                
                if not station_id:
                    logger.warning(f"No station ID found for nearest station")
                    return self._get_fallback_weather_data(latitude, longitude, location_name)
                
                # Get latest observation
                obs_url = f"{self.nws_base_url}/stations/{station_id}/observations/latest"
                obs_response = await client.get(
                    obs_url,
                    headers={"User-Agent": self.user_agent}
                )
                obs_response.raise_for_status()
                obs_data = obs_response.json()
                
                # Convert to standard format
                return self._convert_nws_observation_to_weather_data(obs_data, location_name)
                
        except Exception as e:
            logger.error(f"Error fetching current weather from NOAA for {latitude}, {longitude}: {e}")
            sentry_sdk.capture_exception(e)
            return self._get_fallback_weather_data(latitude, longitude, location_name)
    
    async def get_weather_forecast(
        self,
        latitude: float,
        longitude: float,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get weather forecast from NWS Gridpoints API.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            days: Number of forecast days (max 7 for NWS)
            
        Returns:
            Weather forecast data
        """
        try:
            # Step 1: Get point metadata
            point_metadata = await self.get_point_metadata(latitude, longitude)
            
            # Step 2: Get forecast URL
            forecast_url = point_metadata.get("forecast")
            if not forecast_url:
                logger.warning(f"No forecast URL for {latitude}, {longitude}")
                return {"forecasts": [], "source": "NOAA-NWS", "total_periods": 0}
            
            # Step 3: Get forecast data
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                forecast_response = await client.get(
                    forecast_url,
                    headers={"User-Agent": self.user_agent}
                )
                forecast_response.raise_for_status()
                forecast_data = forecast_response.json()
                
                # Convert to standard format
                return self._convert_nws_forecast_to_forecast_data(forecast_data, days)
                
        except Exception as e:
            logger.error(f"Error fetching weather forecast from NOAA for {latitude}, {longitude}: {e}")
            sentry_sdk.capture_exception(e)
            return {"forecasts": [], "source": "NOAA-NWS", "total_periods": 0}
    
    async def get_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: Optional[date] = None
    ) -> Dict[str, Any]:
        """
        Get historical weather data from NOAA CDO API.
        Always calculates 24-month period from current date.
        
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
            
            if not self.cdo_api_token:
                logger.warning("NOAA CDO API token not configured - returning empty historical data")
                return self._get_empty_historical_data(start_date, end_date)
            
            # Find nearest weather station
            station_id = await self._find_nearest_cdo_station(latitude, longitude)
            if not station_id:
                logger.warning(f"No CDO station found near {latitude}, {longitude}")
                return self._get_empty_historical_data(start_date, end_date)
            
            # Fetch historical data from CDO API
            historical_data = await self._fetch_cdo_historical_data(
                station_id, start_date, end_date
            )
            
            # Add trend analysis
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
            logger.error(f"Error fetching historical weather from NOAA for {latitude}, {longitude}: {e}")
            sentry_sdk.capture_exception(e)
            return self._get_empty_historical_data(start_date, end_date)
    
    async def get_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Get severe weather events from NOAA CDO API and NWS alerts API.
        
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
            events = []
            
            # First, try to get historical severe weather events from NOAA CDO API
            if self.cdo_api_token:
                try:
                    historical_events = await self._fetch_cdo_severe_weather_events(
                        latitude, longitude, start_date, end_date, radius_km
                    )
                    events.extend(historical_events)
                    logger.info(f"Fetched {len(historical_events)} historical severe weather events from NOAA CDO")
                except Exception as e:
                    logger.warning(f"Failed to fetch historical severe weather events from NOAA CDO: {e}")
            
            # Also get current alerts from NWS alerts API
            try:
                current_alerts = await self._fetch_nws_current_alerts(
                    latitude, longitude, radius_km
                )
                events.extend(current_alerts)
                logger.info(f"Fetched {len(current_alerts)} current weather alerts from NWS")
            except Exception as e:
                logger.warning(f"Failed to fetch current alerts from NWS: {e}")
            
            # Sort events by timestamp (most recent first)
            events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            logger.info(f"Total weather events fetched: {len(events)}")
            return events
                
        except Exception as e:
            logger.error(f"Error fetching weather events from NOAA for {latitude}, {longitude}: {e}")
            sentry_sdk.capture_exception(e)
            return []
    
    async def _fetch_cdo_severe_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """Fetch historical severe weather events from NOAA CDO API."""
        try:
            events = []
            
            # Calculate bounding box for the search area
            lat_offset = radius_km / 111.0  # Rough conversion km to degrees
            lon_offset = radius_km / (111.0 * abs(latitude / 90.0))  # Adjust for latitude
            
            min_lat = latitude - lat_offset
            max_lat = latitude + lat_offset
            min_lon = longitude - lon_offset
            max_lon = longitude + lon_offset
            
            # Try multiple severe weather datasets
            # Note: NEXRAD2, NEXRAD3, and SWDI may require different parameters or be restricted
            severe_weather_datasets = [
                "GHCND"         # Global Historical Climatology Network Daily (primary)
                # "NEXRAD2",      # NEXRAD Level II data (severe weather) - may require special access
                # "NEXRAD3",      # NEXRAD Level III data (severe weather) - may require special access  
                # "SWDI",         # Severe Weather Data Inventory - may require special access
            ]
            
            for dataset_id in severe_weather_datasets:
                try:
                    # Apply rate limiting before CDO API request
                    await self.rate_limiter.wait_if_needed()
                    
                    data_url = f"{self.cdo_base_url}/data"
                    
                    # Break date range into 1-year chunks (NOAA CDO API limit)
                    current_start = start_date
                    all_results = []
                    
                    while current_start < end_date:
                        # Calculate end date for this chunk (max 1 year)
                        chunk_end = min(
                            date(current_start.year + 1, current_start.month, current_start.day),
                            end_date
                        )
                        
                        # Use different parameters based on dataset
                        if dataset_id == "GHCND":
                            # For GHCND, use location-based queries with Texas FIPS code
                            # This is more reliable than extent-based queries
                            params = {
                                "datasetid": dataset_id,
                                "locationid": "FIPS:48",  # Texas FIPS code
                                "startdate": current_start.isoformat(),
                                "enddate": chunk_end.isoformat(),
                                "limit": 1000,
                                "includemetadata": "false",
                                # Focus on severe weather datatypes
                                "datatypeid": "PRCP,TMAX,TMIN,SNOW,SNWD,WSFG,WSF1,WSF2,WSF5,WSF6,WSF7,WSF8,WSF9,WSFA,WSFB,WSFC,WSFD,WSFE,WSFF,WSFG,WSFH,WSFI,WSFJ,WSFK,WSFL,WSFM,WSFN,WSFO,WSFP,WSFQ,WSFR,WSFS,WSFT,WSFU,WSFV,WSFW,WSFX,WSFY,WSFZ"
                            }
                        else:
                            # For other datasets, use extent parameter
                            params = {
                                "datasetid": dataset_id,
                                "extent": f"{min_lat},{min_lon},{max_lat},{max_lon}",
                                "startdate": current_start.isoformat(),
                                "enddate": chunk_end.isoformat(),
                                "limit": 1000,
                                "includemetadata": "false"
                            }
                        
                        logger.debug(f"Fetching {dataset_id} data for {current_start} to {chunk_end}")
                        
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(
                                data_url,
                                params=params,
                                headers={"token": self.cdo_api_token}
                            )
                            response.raise_for_status()
                            data = response.json()
                            
                            results = data.get("results", [])
                            all_results.extend(results)
                            
                            # Apply rate limiting between chunks
                            if chunk_end < end_date:
                                await self.rate_limiter.wait_if_needed()
                        
                        # Move to next chunk
                        current_start = chunk_end
                    
                    if all_results:
                        logger.info(f"Found {len(all_results)} total records in {dataset_id} dataset")
                        
                        # Convert CDO records to weather events
                        for record in all_results:
                            event = self._convert_cdo_record_to_severe_weather_event(record, dataset_id)
                            if event:
                                events.append(event)
                    else:
                        logger.debug(f"No severe weather data found in {dataset_id} dataset")
                    
                    # If we found data in this dataset, we can stop searching
                    if all_results:
                        break
                        
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401:
                        logger.error("NOAA CDO API token is invalid or expired")
                        break
                    elif e.response.status_code == 429:
                        logger.error("NOAA CDO API rate limit exceeded")
                        break
                    else:
                        logger.warning(f"HTTP error fetching {dataset_id} data: {e.response.status_code}")
                        continue
                except Exception as e:
                    logger.warning(f"Error fetching {dataset_id} severe weather data: {e}")
                    continue
            
            # If we didn't get enough severe weather events from CDO, supplement with NWS Storm Events
            if len(events) < 10:  # Threshold for comprehensive data
                logger.info("Supplementing with NWS Storm Events data")
                storm_events = await self._fetch_nws_storm_events(latitude, longitude, start_date, end_date)
                events.extend(storm_events)
                
            logger.info(f"Total severe weather events found: {len(events)}")
            return events
            
        except Exception as e:
            logger.error(f"Error fetching CDO severe weather events: {e}")
            return []
    
    async def _fetch_nws_storm_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date
    ) -> List[Dict[str, Any]]:
        """Fetch severe weather events from NWS Storm Events Database."""
        try:
            events = []
            
            # NWS Storm Events API endpoint
            base_url = "https://www.ncei.noaa.gov/stormevents/csv"
            
            # Calculate bounding box (50km radius around location)
            lat_offset = 0.45  # ~50km offset
            lon_offset = 0.45  # ~50km offset
            
            min_lat = latitude - lat_offset
            max_lat = latitude + lat_offset
            min_lon = longitude - lon_offset
            max_lon = longitude + lon_offset
            
            # Generate URLs for each year in the date range
            current_year = start_date.year
            end_year = end_date.year
            
            while current_year <= end_year:
                # NWS Storm Events CSV files are organized by year
                url = f"{base_url}/StormEvents_details-ftp_v1.0_d{current_year}0101_c{current_year}1231.csv"
                
                try:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        response = await client.get(url)
                        
                        if response.status_code == 200:
                            # Parse CSV data
                            csv_content = response.text
                            lines = csv_content.split('\n')
                            
                            if len(lines) > 1:  # Has header and data
                                headers = lines[0].split(',')
                                
                                for line in lines[1:]:
                                    if not line.strip():
                                        continue
                                        
                                    values = line.split(',')
                                    if len(values) >= len(headers):
                                        # Create record dictionary
                                        record = dict(zip(headers, values))
                                        
                                        # Check if event is within our date range and geographic bounds
                                        try:
                                            event_date = datetime.strptime(record.get('BEGIN_DATE_TIME', ''), '%Y-%m-%d %H:%M:%S').date()
                                            event_lat = float(record.get('BEGIN_LAT', 0))
                                            event_lon = float(record.get('BEGIN_LON', 0))
                                            
                                            if (start_date <= event_date <= end_date and
                                                min_lat <= event_lat <= max_lat and
                                                min_lon <= event_lon <= max_lon):
                                                
                                                # Convert to our event format
                                                event = self._convert_storm_event_to_severe_weather_event(record)
                                                if event:
                                                    events.append(event)
                                                    
                                        except (ValueError, TypeError):
                                            continue
                                            
                        else:
                            logger.warning(f"HTTP error fetching NWS Storm Events for {current_year}: {response.status_code}")
                            
                except Exception as e:
                    logger.warning(f"Error fetching NWS Storm Events for {current_year}: {e}")
                    
                current_year += 1
                
            logger.info(f"Fetched {len(events)} events from NWS Storm Events Database")
            return events
            
        except Exception as e:
            logger.error(f"Error fetching NWS Storm Events: {e}")
            return []
    
    def _convert_storm_event_to_severe_weather_event(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert NWS Storm Event record to severe weather event format."""
        try:
            event_type = record.get('EVENT_TYPE', '').lower()
            magnitude = record.get('MAGNITUDE', '')
            magnitude_type = record.get('MAGNITUDE_TYPE', '')
            
            # Parse magnitude
            try:
                mag_value = float(magnitude) if magnitude else 0
            except (ValueError, TypeError):
                mag_value = 0
                
            # Determine severity based on event type and magnitude
            severity = "moderate"
            if event_type in ["tornado", "hail"] and mag_value > 0:
                severity = "severe"
            elif event_type in ["thunderstorm wind", "high wind"] and mag_value >= 60:
                severity = "severe"
            elif event_type in ["flash flood", "flood"] and mag_value > 0:
                severity = "severe"
                
            # Format description
            description = f"{event_type.title()} event"
            if magnitude and magnitude_type:
                description += f" ({magnitude} {magnitude_type})"
                
            # Format date
            try:
                date_str = record.get('BEGIN_DATE_TIME', '')
                if date_str:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    date_str = date_obj.strftime('%Y-%m-%d')
            except:
                date_str = record.get('BEGIN_DATE_TIME', '')
                
            return {
                "event_type": event_type,
                "severity": severity,
                "urgency": "immediate" if severity == "severe" else "expected",
                "description": description,
                "timestamp": date_str,
                "source": "NWS-StormEvents",
                "magnitude": mag_value,
                "magnitude_type": magnitude_type,
                "location": record.get('CZ_NAME', ''),
                "state": record.get('STATE', ''),
                "county": record.get('CZ_FIPS', '')
            }
            
        except Exception as e:
            logger.error(f"Error converting storm event record: {e}")
            return None
    
    async def _fetch_nws_current_alerts(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """Fetch current weather alerts from NWS alerts API."""
        try:
            # Get alerts for the area
            alerts_url = f"{self.nws_base_url}/alerts"
            
            params = {
                "point": f"{latitude},{longitude}",
                "status": "actual",
                "message_type": "alert"
            }
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                alerts_response = await client.get(
                    alerts_url,
                    params=params,
                    headers={"User-Agent": self.user_agent}
                )
                alerts_response.raise_for_status()
                alerts_data = alerts_response.json()
                
                # Convert alerts to weather events format
                events = []
                for alert in alerts_data.get("features", []):
                    event = self._convert_nws_alert_to_weather_event(alert)
                    if event:
                        events.append(event)
                
                return events
                
        except Exception as e:
            logger.error(f"Error fetching NWS current alerts: {e}")
            return []
    
    def _convert_cdo_record_to_severe_weather_event(
        self,
        record: Dict[str, Any],
        dataset_id: str
    ) -> Optional[Dict[str, Any]]:
        """Convert CDO record to severe weather event format."""
        try:
            # Extract basic information
            date_str = record.get("date")
            station_id = record.get("station")
            
            if not date_str:
                return None
            
            # Determine event type based on dataset and data values
            event_type = "unknown"
            severity = "moderate"
            magnitude = None
            description = f"Weather event recorded on {date_str}"
            
            if dataset_id == "NEXRAD2" or dataset_id == "NEXRAD3":
                # NEXRAD data - look for severe weather indicators
                if record.get("REFL") and record.get("REFL") > 50:  # High reflectivity (hail)
                    event_type = "hail"
                    severity = "severe"
                    magnitude = record.get("REFL")
                    description = f"Hail event detected (reflectivity: {magnitude} dBZ)"
                elif record.get("VEL") and abs(record.get("VEL")) > 30:  # High velocity (wind)
                    event_type = "wind"
                    severity = "severe"
                    magnitude = abs(record.get("VEL"))
                    description = f"High wind event detected (velocity: {magnitude} m/s)"
                elif record.get("REFL") and record.get("REFL") > 40:  # Moderate reflectivity
                    event_type = "thunderstorm"
                    severity = "moderate"
                    description = f"Thunderstorm activity detected"
            
            elif dataset_id == "SWDI":
                # Severe Weather Data Inventory
                event_type = record.get("event_type", "unknown").lower()
                severity = record.get("severity", "moderate").lower()
                magnitude = record.get("magnitude")
                description = record.get("description", f"Severe weather event: {event_type}")
            
            elif dataset_id == "GHCND":
                # Look for severe weather indicators in GHCND data
                # Note: Temperature values are in tenths of degrees Celsius
                # Note: Precipitation values are in tenths of millimeters
                datatype = record.get("datatype", "")
                value = record.get("value", 0)
                
                # Much more lenient thresholds to capture Houston-area weather events
                if datatype == "PRCP" and value >= 10:  # Any precipitation (>=1mm)
                    event_type = "precipitation"
                    severity = "severe" if value >= 100 else "moderate" if value >= 50 else "light"
                    magnitude = value / 10.0  # Convert to mm
                    description = f"Precipitation event ({magnitude:.1f} mm)"
                elif datatype == "TMAX" and value >= 300:  # Hot weather (>=30°C / 86°F)
                    event_type = "heat"
                    severity = "severe" if value >= 350 else "moderate"
                    magnitude = value / 10.0  # Convert to °C
                    description = f"Hot weather event ({magnitude:.1f}°C)"
                elif datatype == "TMIN" and value <= 0:  # Cold weather (<=0°C / 32°F)
                    event_type = "cold"
                    severity = "severe" if value <= -50 else "moderate"
                    magnitude = value / 10.0  # Convert to °C
                    description = f"Cold weather event ({magnitude:.1f}°C)"
                elif datatype == "SNOW" and value > 0:  # Any snow
                    event_type = "winter"
                    severity = "severe" if value >= 100 else "moderate"
                    magnitude = value / 10.0  # Convert to mm
                    description = f"Snow event ({magnitude:.1f} mm)"
                elif datatype == "SNWD" and value > 0:  # Any snow depth
                    event_type = "winter"
                    severity = "severe" if value >= 100 else "moderate"
                    magnitude = value / 10.0  # Convert to mm
                    description = f"Snow depth event ({magnitude:.1f} mm)"
                elif datatype in ["WSFG", "WSF1", "WSF2", "WSF5", "WSF6", "WSF7", "WSF8", "WSF9", "WSFA", "WSFB", "WSFC", "WSFD", "WSFE", "WSFF", "WSFG", "WSFH", "WSFI", "WSFJ", "WSFK", "WSFL", "WSFM", "WSFN", "WSFO", "WSFP", "WSFQ", "WSFR", "WSFS", "WSFT", "WSFU", "WSFV", "WSFW", "WSFX", "WSFY", "WSFZ"]:
                    # Wind speed data - much lower thresholds for Houston
                    wind_speed_mph = value * 0.621371  # Convert m/s to mph
                    if wind_speed_mph >= 20:  # Moderate wind (>=20 mph)
                        event_type = "wind"
                        severity = "severe" if wind_speed_mph >= 40 else "moderate"
                        magnitude = wind_speed_mph
                        description = f"High wind event ({magnitude:.1f} mph)"
            
            # Only return events that are actually severe weather
            if event_type in ["hail", "wind", "tornado", "thunderstorm", "flood", "heat", "cold", "winter", "precipitation"]:
                return {
                    "event_type": event_type,
                    "severity": severity,
                    "urgency": "immediate" if severity == "severe" else "expected",
                    "description": description,
                    "timestamp": date_str,
                    "source": f"NOAA-CDO-{dataset_id}",
                    "magnitude": magnitude,
                    "station_id": station_id
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error converting CDO record to severe weather event: {e}")
            return None

    async def get_point_metadata(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """Get point metadata from NWS Points API."""
        try:
            url = f"{self.nws_base_url}/points/{latitude},{longitude}"
            
            async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
                response = await client.get(
                    url,
                    headers={"User-Agent": self.user_agent}
                )
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 301:
                # Handle redirect manually if needed
                logger.warning(f"Redirect encountered for {latitude}, {longitude}, attempting to follow")
                try:
                    redirect_url = e.response.headers.get("location")
                    if redirect_url:
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(
                                redirect_url,
                                headers={"User-Agent": self.user_agent}
                            )
                            response.raise_for_status()
                            return response.json()
                except Exception as redirect_error:
                    logger.error(f"Failed to follow redirect for {latitude}, {longitude}: {redirect_error}")
                    raise
            else:
                logger.error(f"HTTP error fetching point metadata for {latitude}, {longitude}: {e}")
                raise
        except Exception as e:
            logger.error(f"Error fetching point metadata for {latitude}, {longitude}: {e}")
            raise
    
    async def _find_nearest_cdo_station(self, latitude: float, longitude: float) -> Optional[str]:
        """Find nearest CDO weather station using geographic extent."""
        try:
            if not self.cdo_api_token:
                logger.warning("NOAA CDO API token not configured")
                return None
            
            # Search for stations near the location using extent parameter
            stations_url = f"{self.cdo_base_url}/stations"
            
            # Calculate date range to ensure station has recent data (last 2 years)
            end_date = date.today()
            start_date = end_date - timedelta(days=730)
            
            # Calculate proper bounding box (NOAA requires different min/max values)
            # Use a larger offset to ensure we find stations
            lat_offset = 0.1  # ~11km offset
            lon_offset = 0.1  # ~11km offset
            
            min_lat = latitude - lat_offset
            max_lat = latitude + lat_offset
            min_lon = longitude - lon_offset
            max_lon = longitude + lon_offset
            
            params = {
                "extent": f"{min_lat},{min_lon},{max_lat},{max_lon}",  # Valid geographic bounding box
                "datasetid": "GHCND",  # Ensure station supports Global Historical Climatology Network Daily
                "startdate": start_date.isoformat(),
                "enddate": end_date.isoformat(),
                "limit": 5,  # Get more stations to choose from
                "sortfield": "distance",
                "sortorder": "asc",
                "includemetadata": "false"
            }
            
            logger.debug(f"Searching for CDO stations near {latitude}, {longitude} with params: {params}")
            
            # Apply rate limiting before CDO API request
            await self.rate_limiter.wait_if_needed()
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    stations_url,
                    params=params,
                    headers={"token": self.cdo_api_token}
                )
                response.raise_for_status()
                data = response.json()
                
                stations = data.get("results", [])
                if not stations:
                    logger.warning(f"No CDO stations found near {latitude}, {longitude}")
                    return None
                
                station = stations[0]
                station_id = station.get("id")
                station_name = station.get("name", "Unknown")
                station_distance = station.get("distance", "Unknown")
                
                logger.info(
                    f"Found nearest CDO station: {station_name} (ID: {station_id}, "
                    f"Distance: {station_distance})"
                )
                
                return station_id
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error("NOAA CDO API token is invalid or expired")
            elif e.response.status_code == 429:
                logger.error("NOAA CDO API rate limit exceeded")
            else:
                logger.error(f"HTTP error finding CDO station: {e.response.status_code}")
            return None
        except RateLimitExceededError as e:
            logger.error(f"Rate limit exceeded: {e}")
            return None
        except Exception as e:
            logger.error(f"Error finding nearest CDO station: {e}")
            return None
    
    async def _fetch_cdo_historical_data(
        self,
        station_id: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """Fetch historical data from CDO API with rate limiting and pagination."""
        try:
            data_url = f"{self.cdo_base_url}/data"
            params = {
                "datasetid": "GHCND",  # Global Historical Climatology Network Daily
                "stationid": f"GHCND:{station_id}",
                "startdate": start_date.isoformat(),
                "enddate": end_date.isoformat(),
                "units": "metric",
                "limit": 1000,
                "includemetadata": "false"  # Improve response time
            }
            
            logger.debug(f"Fetching CDO historical data for station {station_id} from {start_date} to {end_date}")
            
            # Apply rate limiting before CDO API request
            await self.rate_limiter.wait_if_needed()
            
            all_observations = []
            offset = 0
            
            while True:
                # Add pagination parameters
                current_params = params.copy()
                current_params["offset"] = offset
                
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(
                        data_url,
                        params=current_params,
                        headers={"token": self.cdo_api_token}
                    )
                    response.raise_for_status()
                    data = response.json()
                    
                    results = data.get("results", [])
                    if not results:
                        break
                    
                    # Convert CDO data to standard format
                    for record in results:
                        obs = self._convert_cdo_record_to_observation(record)
                        if obs:
                            all_observations.append(obs)
                    
                    # Check if we have more data to fetch
                    metadata = data.get("metadata", {})
                    resultset = metadata.get("resultset", {})
                    count = resultset.get("count", 0)
                    limit = resultset.get("limit", 1000)
                    
                    logger.debug(f"Fetched {len(results)} records (offset: {offset}, total: {count})")
                    
                    # If we got fewer records than the limit, we've reached the end
                    if len(results) < limit:
                        break
                    
                    offset += limit
                    
                    # Apply rate limiting for next page request
                    if offset < count:
                        await self.rate_limiter.wait_if_needed()
            
            return {
                "observations": all_observations,
                "source": "NOAA-CDO",
                "station_id": station_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "total_records": len(all_observations),
                "pages_fetched": (offset // 1000) + 1
            }
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.error("NOAA CDO API token is invalid or expired")
            elif e.response.status_code == 429:
                logger.error("NOAA CDO API rate limit exceeded")
            elif e.response.status_code == 400:
                logger.error(f"Bad request to CDO API: {e.response.text}")
            else:
                logger.error(f"HTTP error fetching CDO data: {e.response.status_code}")
            return {"observations": [], "source": "NOAA-CDO", "total_records": 0, "error": f"HTTP {e.response.status_code}"}
        except RateLimitExceededError as e:
            logger.error(f"Rate limit exceeded: {e}")
            return {"observations": [], "source": "NOAA-CDO", "total_records": 0, "error": "Rate limit exceeded"}
        except Exception as e:
            logger.error(f"Error fetching CDO historical data: {e}")
            return {"observations": [], "source": "NOAA-CDO", "total_records": 0, "error": str(e)}
    
    def _convert_nws_observation_to_weather_data(
        self,
        obs_data: Dict[str, Any],
        location_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Convert NWS observation to standard weather data format."""
        try:
            properties = obs_data.get("properties", {})
            
            # Extract temperature (convert from Celsius to Fahrenheit)
            temp_c = properties.get("temperature", {}).get("value")
            temp_f = round((temp_c * 9/5) + 32, 1) if temp_c is not None else None
            
            # Extract wind speed (convert from m/s to mph)
            wind_speed_ms = properties.get("windSpeed", {}).get("value")
            wind_speed_mph = round(wind_speed_ms * 2.237, 1) if wind_speed_ms is not None else None
            
            # Extract wind direction
            wind_direction = properties.get("windDirection", {}).get("value")
            wind_direction_text = self._convert_wind_direction_to_text(wind_direction) if wind_direction is not None else None
            
            # Extract precipitation (convert from mm to inches)
            precip_mm = properties.get("precipitationLastHour", {}).get("value")
            precip_inches = round(precip_mm * 0.0393701, 2) if precip_mm is not None else 0
            
            # Extract humidity
            humidity = properties.get("relativeHumidity", {}).get("value")
            
            # Extract pressure (convert from Pa to inHg)
            pressure_pa = properties.get("barometricPressure", {}).get("value")
            pressure_inhg = round(pressure_pa * 0.0002953, 2) if pressure_pa is not None else None
            
            # Extract visibility (convert from m to miles)
            visibility_m = properties.get("visibility", {}).get("value")
            visibility_miles = round(visibility_m * 0.000621371, 1) if visibility_m is not None else None
            
            # Extract weather condition
            weather_condition = properties.get("textDescription", "Unknown")
            
            return {
                "temperature": temp_f,
                "temperature_unit": "F",
                "wind_speed": f"{wind_speed_mph} mph" if wind_speed_mph is not None else "0 mph",
                "wind_direction": wind_direction_text or "Unknown",
                "precipitation_intensity": precip_inches,
                "humidity": humidity,
                "pressure": pressure_inhg,
                "visibility": visibility_miles,
                "weather_code": self._convert_weather_text_to_code(weather_condition),
                "weather_code_full": weather_condition,
                "detailed_forecast": f"Current conditions: {weather_condition}",
                "short_forecast": weather_condition,
                "is_daytime": True,  # Default to daytime
                "timestamp": properties.get("timestamp"),
                "source": "NOAA-NWS"
            }
            
        except Exception as e:
            logger.error(f"Error converting NWS observation: {e}")
            return self._get_fallback_weather_data(0, 0, location_name)
    
    def _convert_nws_forecast_to_forecast_data(
        self,
        forecast_data: Dict[str, Any],
        days: int
    ) -> Dict[str, Any]:
        """Convert NWS forecast to standard forecast format."""
        try:
            periods = forecast_data.get("properties", {}).get("periods", [])
            
            forecasts = []
            for period in periods[:days]:  # Limit to requested days
                # Extract temperature
                temp = period.get("temperature")
                
                # Extract wind information
                wind = period.get("windSpeed", "Unknown")
                wind_direction = period.get("windDirection", "Unknown")
                
                # Extract precipitation probability
                precip_prob = period.get("probabilityOfPrecipitation", {}).get("value")
                
                # Extract weather condition
                weather_condition = period.get("shortForecast", "Unknown")
                
                forecast = {
                    "name": period.get("name", "Unknown"),
                    "start_time": period.get("startTime"),
                    "end_time": period.get("endTime"),
                    "temperature": temp,
                    "temperature_unit": "F",
                    "wind_speed": wind,
                    "wind_direction": wind_direction,
                    "precipitation_probability": precip_prob,
                    "weather_code": self._convert_weather_text_to_code(weather_condition),
                    "weather_code_full": weather_condition,
                    "detailed_forecast": period.get("detailedForecast", weather_condition),
                    "short_forecast": weather_condition,
                    "is_daytime": period.get("isDaytime", True)
                }
                forecasts.append(forecast)
            
            return {
                "forecasts": forecasts,
                "source": "NOAA-NWS",
                "total_periods": len(forecasts)
            }
            
        except Exception as e:
            logger.error(f"Error converting NWS forecast: {e}")
            return {"forecasts": [], "source": "NOAA-NWS", "total_periods": 0}
    
    def _convert_cdo_record_to_observation(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert CDO record to observation format."""
        try:
            # Extract key weather parameters
            date_str = record.get("date")
            if not date_str:
                return None
            
            # Extract temperature (convert from Celsius to Fahrenheit)
            tmax_c = record.get("TMAX")  # Maximum temperature
            tmin_c = record.get("TMIN")  # Minimum temperature
            tavg_c = record.get("TAVG")  # Average temperature
            
            temp_max_f = round((tmax_c * 9/5) + 32, 1) if tmax_c is not None else None
            temp_min_f = round((tmin_c * 9/5) + 32, 1) if tmin_c is not None else None
            temp_avg_f = round((tavg_c * 9/5) + 32, 1) if tavg_c is not None else None
            
            # Extract precipitation (convert from mm to inches)
            precip_mm = record.get("PRCP")  # Precipitation
            precip_inches = round(precip_mm * 0.0393701, 2) if precip_mm is not None else 0
            
            # Extract wind speed (convert from m/s to mph)
            wind_speed_ms = record.get("WSFG")  # Wind speed
            wind_speed_mph = round(wind_speed_ms * 2.237, 1) if wind_speed_ms is not None else None
            
            return {
                "timestamp": date_str,
                "temperature": temp_avg_f,
                "temperature_max": temp_max_f,
                "temperature_min": temp_min_f,
                "precipitation": precip_inches,
                "wind_speed": wind_speed_mph,
                "source": "NOAA-CDO"
            }
            
        except Exception as e:
            logger.error(f"Error converting CDO record: {e}")
            return None
    
    def _convert_nws_alert_to_weather_event(self, alert: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert NWS alert to weather event format."""
        try:
            properties = alert.get("properties", {})
            
            # Extract event information
            event_type = properties.get("event", "Unknown")
            severity = properties.get("severity", "Unknown")
            urgency = properties.get("urgency", "Unknown")
            description = properties.get("description", "")
            
            # Convert to standard event format
            return {
                "event_type": event_type.lower(),
                "severity": severity.lower(),
                "urgency": urgency.lower(),
                "description": description,
                "timestamp": properties.get("sent"),
                "source": "NOAA-NWS",
                "magnitude": self._extract_magnitude_from_description(description, event_type)
            }
            
        except Exception as e:
            logger.error(f"Error converting NWS alert to weather event: {e}")
            return None
    
    def _convert_weather_text_to_code(self, weather_text: str) -> int:
        """Convert weather text description to numeric code."""
        weather_text_lower = weather_text.lower()
        
        # Map common weather conditions to codes
        if "clear" in weather_text_lower or "sunny" in weather_text_lower:
            return 1000
        elif "partly cloudy" in weather_text_lower:
            return 1101
        elif "mostly cloudy" in weather_text_lower:
            return 1102
        elif "cloudy" in weather_text_lower or "overcast" in weather_text_lower:
            return 1001
        elif "fog" in weather_text_lower:
            return 2000
        elif "rain" in weather_text_lower:
            return 4001
        elif "snow" in weather_text_lower:
            return 5000
        elif "thunderstorm" in weather_text_lower:
            return 8000
        else:
            return 1000  # Default to clear
    
    def _convert_wind_direction_to_text(self, degrees: float) -> str:
        """Convert wind direction degrees to text."""
        if degrees is None:
            return "Unknown"
        
        directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        
        index = round(degrees / 22.5) % 16
        return directions[index]
    
    def _extract_magnitude_from_description(self, description: str, event_type: str) -> Optional[float]:
        """Extract magnitude from weather event description."""
        try:
            # Look for wind speed patterns
            if "wind" in event_type.lower():
                import re
                wind_patterns = [
                    r"(\d+)\s*mph",
                    r"(\d+)\s*miles?\s*per\s*hour",
                    r"(\d+)\s*knots?"
                ]
                
                for pattern in wind_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        speed = float(match.group(1))
                        # Convert knots to mph if needed
                        if "knot" in match.group(0).lower():
                            speed *= 1.15078
                        return speed
            
            # Look for hail size patterns
            elif "hail" in event_type.lower():
                import re
                hail_patterns = [
                    r"(\d+(?:\.\d+)?)\s*inch",
                    r"(\d+(?:\.\d+)?)\s*\""
                ]
                
                for pattern in hail_patterns:
                    match = re.search(pattern, description, re.IGNORECASE)
                    if match:
                        return float(match.group(1))
            
            return None
            
        except Exception:
            return None
    
    def _analyze_weather_trends(self, observations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze weather trends from historical observations."""
        if not observations:
            return {}
        
        try:
            # Extract temperature data
            temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
            precipitations = [obs.get("precipitation", 0) for obs in observations]
            wind_speeds = [obs.get("wind_speed") for obs in observations if obs.get("wind_speed")]
            
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
            
            # Wind trends
            if wind_speeds:
                trends["wind"] = {
                    "average": sum(wind_speeds) / len(wind_speeds),
                    "max": max(wind_speeds),
                    "actionable_wind_days": len([w for w in wind_speeds if w >= 60]),  # ≥60 mph
                    "severe_wind_days": len([w for w in wind_speeds if w >= 60]),  # ≥60 mph
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
                        
                        if obs.get("temperature"):
                            monthly_data[month]["temperatures"].append(obs["temperature"])
                        if obs.get("precipitation"):
                            monthly_data[month]["precipitations"].append(obs["precipitation"])
                        if obs.get("wind_speed"):
                            monthly_data[month]["wind_speeds"].append(obs["wind_speed"])
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
                "average_daily": sum(precipitation) / len(precipitation) if precipitation else 0,
                "rainy_days": len([p for p in precipitation if p > 0]),
                "heavy_rain_days": len([p for p in precipitation if p > 10]),
                "drought_periods": self._identify_drought_periods(precipitation)
            }
            
            # Wind analysis
            if wind_speeds:
                analysis["wind"] = {
                    "average": sum(wind_speeds) / len(wind_speeds),
                    "max": max(wind_speeds),
                    "actionable_wind_days": len([w for w in wind_speeds if w >= 60]),  # ≥60 mph
                    "severe_wind_days": len([w for w in wind_speeds if w >= 60]),  # ≥60 mph
                    "moderate_wind_days": len([w for w in wind_speeds if w >= 40]),  # ≥40 mph
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
    
    def _get_fallback_weather_data(self, latitude: float, longitude: float, location_name: Optional[str] = None) -> Dict[str, Any]:
        """Get fallback weather data when NOAA APIs fail."""
        try:
            # Generate reasonable fallback data based on location
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
                "source": "NOAA-Fallback",
                "note": "Using estimated data due to NOAA API unavailability"
            }
            
            logger.info(f"Using NOAA fallback weather data for {latitude}, {longitude}")
            return fallback_data
            
        except Exception as e:
            logger.error(f"Error generating NOAA fallback weather data: {e}")
            return {
                "temperature": 72,
                "temperature_unit": "F",
                "wind_speed": "5 mph",
                "wind_direction": "NW",
                "detailed_forecast": "Weather data unavailable",
                "short_forecast": "Unknown",
                "is_daytime": True,
                "timestamp": datetime.now().isoformat(),
                "source": "NOAA-Emergency-Fallback",
                "note": "Minimal fallback data due to service error"
            }
    
    def _get_empty_historical_data(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Get empty historical data structure."""
        return {
            "observations": [],
            "source": "NOAA-CDO",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_records": 0,
            "trend_analysis": {},
            "seasonal_analysis": {},
            "period_analysis": {},
            "analysis_context": {
                "analysis_date": end_date.isoformat(),
                "period_length_days": (end_date - start_date).days,
                "period_type": "unknown"
            }
        }


# Singleton instance
_noaa_weather_service: Optional[NOAAWeatherService] = None


def get_noaa_weather_service() -> NOAAWeatherService:
    """Get NOAA weather service instance."""
    global _noaa_weather_service
    if _noaa_weather_service is None:
        _noaa_weather_service = NOAAWeatherService()
    return _noaa_weather_service
