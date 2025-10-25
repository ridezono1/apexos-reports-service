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
import math
import os
import hashlib
import json

import csv
import gzip
import tempfile
from app.core.config import settings
from app.core.logging import get_logger
from app.services.rate_limiter import CDORateLimiter, RateLimitExceededError
from app.core.noaa_data_freshness import get_data_freshness_info
from app.services.noaa_csv_discovery_service import get_csv_discovery_service
from app.services.duckdb_query_service import get_duckdb_query_service
from app.services.spc_storm_reports_service import get_spc_service

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
        
        # CSV cache directory
        self.cache_dir = os.path.join(settings.temp_dir, "storm_events_cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        if not self.cdo_api_token:
            logger.warning("NOAA CDO API token not configured - historical data will be limited")
        
        logger.info("NOAA Weather API service initialized as primary provider")
    
    def _get_cache_file_path(self, year: int) -> str:
        """Get cache file path for a given year."""
        return os.path.join(self.cache_dir, f"storm_events_{year}.csv")
    
    def _is_cache_valid(self, cache_file: str, year: int) -> bool:
        """Check if cache file is valid based on year-specific cache duration."""
        if not os.path.exists(cache_file):
            return False
        
        # Determine cache duration based on year age
        current_year = date.today().year
        year_age = current_year - year
        
        if year_age > 2:
            # Historical years (>2 years old): 30 days cache
            max_age_hours = settings.cache_duration_historical_days * 24
        elif year_age == 1:
            # Previous year (1-2 years old): 7 days cache
            max_age_hours = settings.cache_duration_previous_year_days * 24
        else:
            # Current year (last 12 months): 24 hours cache
            max_age_hours = settings.cache_duration_current_year_hours
        
        # Check file age
        file_age = datetime.now().timestamp() - os.path.getmtime(cache_file)
        is_valid = file_age < (max_age_hours * 3600)  # Convert hours to seconds
        
        logger.debug(f"Cache for {year} (age {year_age} years): {'valid' if is_valid else 'expired'} (age: {file_age/3600:.1f}h, max: {max_age_hours}h)")
        return is_valid
    
    def _is_cache_stale_but_usable(self, cache_file: str, year: int) -> bool:
        """Check if cache is stale but still usable with warnings."""
        if not os.path.exists(cache_file):
            return False
        
        # Determine cache duration based on year age
        current_year = date.today().year
        year_age = current_year - year
        
        if year_age > 2:
            # Historical years: 30 days normal, 60 days stale but usable
            normal_age_hours = settings.cache_duration_historical_days * 24
            stale_age_hours = normal_age_hours * 2
        elif year_age == 1:
            # Previous year: 7 days normal, 14 days stale but usable
            normal_age_hours = settings.cache_duration_previous_year_days * 24
            stale_age_hours = normal_age_hours * 2
        else:
            # Current year: 24 hours normal, 72 hours stale but usable
            normal_age_hours = settings.cache_duration_current_year_hours
            stale_age_hours = normal_age_hours * 3
        
        # Check file age
        file_age = datetime.now().timestamp() - os.path.getmtime(cache_file)
        file_age_hours = file_age / 3600
        
        is_stale_but_usable = normal_age_hours <= file_age_hours < stale_age_hours
        
        if is_stale_but_usable:
            logger.warning(f"Using stale cache for {year} (age: {file_age_hours:.1f}h, normal: {normal_age_hours}h)")
        
        return is_stale_but_usable
    
    async def _download_and_cache_csv(self, url: str, year: int) -> Optional[str]:
        """Download compressed CSV file and cache it locally with graceful fallbacks."""
        cache_file = self._get_cache_file_path(year)
        
        # Check if we have a valid cached version
        if self._is_cache_valid(cache_file, year):
            logger.debug(f"Using cached Storm Events data for {year}")
            return cache_file
        
        # Check if we have a stale but usable cache
        if self._is_cache_stale_but_usable(cache_file, year):
            logger.warning(f"Using stale cache for {year} - will attempt refresh in background")
            return cache_file
        
        try:
            logger.debug(f"Downloading Storm Events data for {year}")
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
                response = await client.get(url)
                
                if response.status_code == 200:
                    # Decompress the gzipped content
                    import gzip
                    import io
                    
                    # Read the compressed content
                    compressed_content = response.content
                    
                    # Decompress it
                    decompressed_content = gzip.decompress(compressed_content)
                    
                    # Write to cache file
                    with open(cache_file, 'wb') as f:
                        f.write(decompressed_content)
                    
                    logger.info(f"Cached Storm Events data for {year}")
                    return cache_file
                else:
                    logger.warning(f"Failed to download Storm Events data for {year}: HTTP {response.status_code}")
                    
                    # Try fallback: use previous month's URL if current fails
                    fallback_url = self._get_previous_month_fallback_url(url, year)
                    if fallback_url and fallback_url != url:
                        logger.info(f"Trying fallback URL for {year}: {fallback_url}")
                        return await self._download_and_cache_csv(fallback_url, year)
                    
                    # Last resort: use stale cache if available
                    if os.path.exists(cache_file):
                        logger.warning(f"Using stale cache as last resort for {year}")
                        return cache_file
                    
                    return None
                    
        except Exception as e:
            logger.error(f"Error downloading Storm Events data for {year}: {e}")
            
            # Last resort: use stale cache if available
            if os.path.exists(cache_file):
                logger.warning(f"Using stale cache as last resort for {year}")
                return cache_file
            
            return None
    
    def _get_previous_month_fallback_url(self, original_url: str, year: int) -> Optional[str]:
        """Generate fallback URL using previous month's compilation date."""
        try:
            # Extract compilation date from URL
            import re
            match = re.search(r'_c(\d{8})\.csv\.gz', original_url)
            if not match:
                return None
            
            compilation_date_str = match.group(1)
            compilation_date = datetime.strptime(compilation_date_str, '%Y%m%d').date()
            
            # Go back one month
            from dateutil.relativedelta import relativedelta
            prev_month_date = compilation_date - relativedelta(months=1)
            prev_month_str = prev_month_date.strftime('%Y%m%d')
            
            # Replace compilation date in URL
            fallback_url = original_url.replace(f'_c{compilation_date_str}', f'_c{prev_month_str}')
            
            logger.debug(f"Generated fallback URL for {year}: {fallback_url}")
            return fallback_url
            
        except Exception as e:
            logger.error(f"Error generating fallback URL for {year}: {e}")
            return None
    
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
        Get weather events with hybrid data strategy (Storm Events + SPC).
        
        Uses Storm Events Database for historical data (>120 days ago) and
        SPC Storm Reports for recent data (last 120 days) to achieve 100% coverage.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            start_date: Start date for events
            end_date: End date for events
            radius_km: Search radius in kilometers
            
        Returns:
            List of weather events with data quality indicators
        """
        try:
            from app.core.noaa_data_freshness import NOAA_UPDATE_LAG_DAYS
            
            events = []
            gap_cutoff = date.today() - timedelta(days=NOAA_UPDATE_LAG_DAYS)
            
            # Historical (>120 days): Storm Events Database (verified)
            if start_date < gap_cutoff:
                historical_end = min(end_date, gap_cutoff)
                logger.info(f"Fetching historical Storm Events from {start_date} to {historical_end}")
                
                storm_events = await self._fetch_nws_storm_events(
                    latitude, longitude, start_date, historical_end
                )
                
                # Add data quality indicators
                for event in storm_events:
                    event['source'] = 'NOAA Storm Events (verified)'
                    event['data_quality'] = 'verified'
                
                events.extend(storm_events)
                logger.info(f"Fetched {len(storm_events)} verified Storm Events")
            
            # Recent (last 120 days): SPC Storm Reports (preliminary)
            if end_date > gap_cutoff:
                recent_start = max(start_date, gap_cutoff)
                logger.info(f"Fetching preliminary SPC reports from {recent_start} to {end_date}")
                
                spc_service = get_spc_service()
                spc_events = await spc_service.fetch_spc_reports(
                    recent_start, end_date, latitude, longitude, radius_km
                )
                
                # Add data quality indicators
                for event in spc_events:
                    event['source'] = 'NWS SPC Preliminary Reports'
                    event['data_quality'] = 'preliminary'
                
                events.extend(spc_events)
                logger.info(f"Fetched {len(spc_events)} preliminary SPC reports")
            
            # Also get current alerts from NWS alerts API (always current)
            try:
                current_alerts = await self._fetch_nws_current_alerts(
                    latitude, longitude, radius_km
                )
                
                # Add data quality indicators
                for event in current_alerts:
                    event['source'] = 'NWS Active Alerts'
                    event['data_quality'] = 'current'
                
                events.extend(current_alerts)
                logger.info(f"Fetched {len(current_alerts)} current NWS alerts")
                
            except Exception as e:
                logger.warning(f"Failed to fetch current alerts from NWS: {e}")
            
            # Sort events by timestamp (most recent first)
            events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            # Consolidate events by date for address and spatial reports
            consolidated_events = self._consolidate_events_by_date(events)
            
            logger.info(f"Total weather events fetched: {len(events)} (verified: {len([e for e in events if e.get('data_quality') == 'verified'])}, preliminary: {len([e for e in events if e.get('data_quality') == 'preliminary'])}, current: {len([e for e in events if e.get('data_quality') == 'current'])})")
            logger.info(f"Consolidated into {len(consolidated_events)} daily entries for reports")
            return consolidated_events
                
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
            lat_offset = radius_km / 111.0  # Rough conversion km to degrees latitude
            cos_lat = math.cos(math.radians(latitude))
            if abs(cos_lat) < 0.001:
                cos_lat = 0.001  # Avoid division by zero near the poles
            lon_offset = radius_km / (111.320 * cos_lat)

            min_lat = max(-90.0, latitude - lat_offset)
            max_lat = min(90.0, latitude + lat_offset)
            min_lon = max(-180.0, longitude - lon_offset)
            max_lon = min(180.0, longitude + lon_offset)
            
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
                        
                        params = {
                            "datasetid": dataset_id,
                            "extent": f"{min_lat},{min_lon},{max_lat},{max_lon}",
                            "startdate": current_start.isoformat(),
                            "enddate": chunk_end.isoformat(),
                            "limit": 1000,
                            "includemetadata": "false"
                        }

                        if dataset_id == "GHCND":
                            # Focus on severe weather datatypes for daily summaries
                            params["datatypeid"] = (
                                "PRCP,TMAX,TMIN,SNOW,SNWD,WSFG,WSF1,WSF2,WSF5,WSF6,WSF7,"
                                "WSF8,WSF9,WSFA,WSFB,WSFC,WSFD,WSFE,WSFF,WSFG,WSFH,WSFI,"
                                "WSFJ,WSFK,WSFL,WSFM,WSFN,WSFO,WSFP,WSFQ,WSFR,WSFS,WSFT,"
                                "WSFU,WSFV,WSFW,WSFX,WSFY,WSFZ"
                            )
                        
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
        """Fetch severe weather events from NWS Storm Events Database via CSV files."""
        try:
            events = []
            
            # Calculate bounding box (50km radius around location)
            lat_offset = 0.45  # ~50km offset
            lon_offset = 0.45  # ~50km offset

            min_lat = latitude - lat_offset
            max_lat = latitude + lat_offset
            min_lon = longitude - lon_offset
            max_lon = longitude + lon_offset
            
            logger.debug(f"Geographic bounds: lat {min_lat:.3f} to {max_lat:.3f}, lon {min_lon:.3f} to {max_lon:.3f}")
            
            # Generate URLs for each year in the date range
            current_year = start_date.year
            end_year = end_date.year
            
            # Only fetch years that have data available
            # Storm Events Database typically has data up to previous month
            available_years = []
            current_year_today = date.today().year
            
            # First, check which years have cached data
            cached_years = []
            for year in range(current_year, end_year + 1):
                cache_file = self._get_cache_file_path(year)
                if os.path.exists(cache_file):
                    cached_years.append(year)
            
            logger.info(f"Cached years available: {cached_years}")
            
            for year in range(current_year, end_year + 1):
                # Prioritize cached data
                if year in cached_years:
                    available_years.append(year)
                elif year < current_year_today:
                    # Previous years should have complete data (but not cached)
                    # Skip for now to avoid 404 errors
                    logger.warning(f"Skipping year {year} - not cached and may not be available")
                    continue
                elif year == current_year_today:
                    # Current year data is typically not available until late in the year
                    # Skip current year data to avoid 404 errors
                    logger.warning(f"Skipping current year {year} - data not yet available")
                    continue
                # Skip future years
            
            logger.debug(f"Date range: {start_date} to {end_date}")
            logger.debug(f"Current year today: {current_year_today}")
            logger.debug(f"Year range: {current_year} to {end_year}")
            logger.debug(f"Fetching Storm Events data for years: {available_years}")
            
            # Ensure we have at least some data to work with
            if not available_years:
                logger.warning(f"No Storm Events data available for years {current_year}-{end_year}")
                logger.info(f"Using cached data only: {cached_years}")
                available_years = cached_years  # Use whatever cached data we have
            
            if not available_years:
                logger.error("No Storm Events data available at all")
                return []
            
            # Auto-discover latest CSV files for all years
            csv_discovery_service = get_csv_discovery_service()
            latest_csv_urls = await csv_discovery_service.discover_latest_csv_files(available_years)
            
            for year in available_years:
                # Get the latest CSV URL for this year
                url = latest_csv_urls.get(year)
                if not url:
                    logger.warning(f"No CSV URL found for year {year}")
                    continue
                
                # Use caching for CSV downloads with year-specific cache duration
                cache_file = await self._download_and_cache_csv(url, year)
                if not cache_file:
                    logger.warning(f"Failed to get Storm Events data for {year}")
                    continue
                
                try:
                    # Use DuckDB for fast CSV querying instead of csv.DictReader loop
                    duckdb_service = get_duckdb_query_service()
                    
                    # Convert dates to ISO format for DuckDB
                    start_date_iso = start_date.strftime('%Y-%m-%d')
                    end_date_iso = end_date.strftime('%Y-%m-%d')
                    
                    # Query with DuckDB (10-30x faster than csv.DictReader)
                    year_events = duckdb_service.query_storm_events(
                        csv_files=[cache_file],
                        min_lat=min_lat,
                        max_lat=max_lat,
                        min_lon=min_lon,
                        max_lon=max_lon,
                        start_date=start_date_iso,
                        end_date=end_date_iso
                    )
                    
                    # Add events to our list
                    events.extend(year_events)
                    
                    logger.debug(f"DuckDB query returned {len(year_events)} Storm Events for {year}")
                    
                except Exception as e:
                    logger.error(f"Error processing Storm Events data for {year}: {e}")
                
            logger.info(f"Fetched {len(events)} total events from NWS Storm Events Database")
            return events
            
        except Exception as e:
            logger.error(f"Error fetching NWS Storm Events: {e}")
            return []
    
    def _consolidate_events_by_date(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Consolidate multiple storm events per day into single entries with counts.
        Used for address and spatial reports to prevent duplicate rows.
        """
        daily_events = {}
        
        for event in events:
            # Extract date from timestamp (YYYY-MM-DD format)
            timestamp = event.get('timestamp', event.get('date', ''))
            if timestamp:
                date_key = timestamp[:10] if len(timestamp) >= 10 else timestamp
            else:
                continue
                
            event_type = event.get('event_type', 'unknown')
            severity = event.get('severity', 'unknown')
            
            # Create unique key for date + event_type + severity combination
            event_key = f"{date_key}_{event_type}_{severity}"
            
            if event_key not in daily_events:
                daily_events[event_key] = {
                    'date': date_key,
                    'event_type': event_type,
                    'severity': severity,
                    'count': 0,
                    'total_damage': 0.0,
                    'max_magnitude': 0.0,
                    'locations': set(),
                    'sample_event': event  # Keep one sample for reference
                }
            
            daily_events[event_key]['count'] += 1
            
            # Aggregate damage if available
            damage = event.get('damage', 0)
            if isinstance(damage, (int, float)) and damage > 0:
                daily_events[event_key]['total_damage'] += damage
            
            # Track maximum magnitude
            magnitude = event.get('magnitude', 0)
            if isinstance(magnitude, (int, float)) and magnitude > daily_events[event_key]['max_magnitude']:
                daily_events[event_key]['max_magnitude'] = magnitude
            
            # Track locations
            location = event.get('location', '')
            if location:
                daily_events[event_key]['locations'].add(location)
        
        # Convert back to list format for reports
        consolidated = []
        for event_data in daily_events.values():
            # Create description based on count
            if event_data['count'] == 1:
                description = f"{event_data['event_type'].title()} event"
            else:
                description = f"{event_data['count']} {event_data['event_type']} events"
            
            # Add magnitude info if available
            if event_data['max_magnitude'] > 0:
                magnitude_type = event_data['sample_event'].get('magnitude_type', '')
                if magnitude_type:
                    description += f" (max {event_data['max_magnitude']} {magnitude_type})"
            
            # Add damage info if available
            if event_data['total_damage'] > 0:
                description += f" - ${event_data['total_damage']:,.0f} damage"
            
            consolidated_event = {
                'date': event_data['date'],
                'event_type': event_data['event_type'],
                'severity': event_data['severity'],
                'count': event_data['count'],
                'description': description,
                'magnitude': event_data['max_magnitude'] if event_data['max_magnitude'] > 0 else None,
                'damage': event_data['total_damage'] if event_data['total_damage'] > 0 else None,
                'location': ', '.join(sorted(event_data['locations'])) if event_data['locations'] else None,
                'timestamp': event_data['date']  # For compatibility
            }
            
            consolidated.append(consolidated_event)
        
        # Sort by date (most recent first)
        consolidated.sort(key=lambda x: x['date'], reverse=True)
        
        logger.info(f"Consolidated {len(events)} storm events into {len(consolidated)} daily entries")
        return consolidated

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
            
            # Only process actual severe weather datasets, not GHCND daily measurements
            if dataset_id == "NEXRAD2" or dataset_id == "NEXRAD3":
                # NEXRAD data - look for severe weather indicators
                event_type = "unknown"
                severity = "moderate"
                magnitude = None
                description = f"Weather event recorded on {date_str}"
                
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
                
                if event_type != "unknown":
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
            
            elif dataset_id == "SWDI":
                # Severe Weather Data Inventory
                event_type = record.get("event_type", "unknown").lower()
                severity = record.get("severity", "moderate").lower()
                magnitude = record.get("magnitude")
                description = record.get("description", f"Severe weather event: {event_type}")
                
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
            
            # GHCND dataset contains daily weather measurements, not discrete severe weather events
            # We should not infer severe weather from basic temperature/precipitation/wind measurements
            # Use Storm Events Database for actual severe weather events instead
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
# Force rebuild Sat Oct 25 12:00:22 CDT 2025
# Force rebuild Sat Oct 25 12:01:19 CDT 2025
