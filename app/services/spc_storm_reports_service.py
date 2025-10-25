"""
SPC Storm Reports Service

Fetches preliminary storm reports from NOAA Storm Prediction Center (SPC)
to fill the 90-120 day gap in NOAA Storm Events Database.

SPC provides same-day preliminary reports for:
- Tornadoes
- Hail ≥1.0 inch
- Wind ≥58 mph

Data is available within hours of events occurring.
"""

import os
import csv
import tempfile
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
import httpx
import logging

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class SPCStormReportsService:
    """Service for fetching preliminary storm reports from SPC."""
    
    def __init__(self):
        """Initialize SPC service."""
        self.base_url = "https://www.spc.noaa.gov/climo/reports"
        self.cache_dir = os.path.join(settings.temp_dir, "spc_reports_cache")
        self.timeout = 30.0
        self.user_agent = "ApexOS-Reports-Service/1.0"
        
        # Ensure cache directory exists
        os.makedirs(self.cache_dir, exist_ok=True)
    
    async def fetch_spc_reports(
        self,
        start_date: date,
        end_date: date,
        latitude: float,
        longitude: float,
        radius_km: float = 50.0
    ) -> List[Dict[str, Any]]:
        """
        Fetch SPC storm reports for a date range and geographic area.
        
        Args:
            start_date: Start date for reports
            end_date: End date for reports
            latitude: Center latitude
            longitude: Center longitude
            radius_km: Search radius in kilometers
            
        Returns:
            List of storm report events
        """
        try:
            events = []
            current_date = start_date
            
            while current_date <= end_date:
                # Download daily SPC report
                daily_events = await self._download_daily_spc_csv(
                    current_date, latitude, longitude, radius_km
                )
                events.extend(daily_events)
                current_date += timedelta(days=1)
            
            logger.info(f"Fetched {len(events)} SPC storm reports from {start_date} to {end_date}")
            return events
            
        except Exception as e:
            logger.error(f"Error fetching SPC storm reports: {e}")
            return []
    
    async def _download_daily_spc_csv(
        self,
        report_date: date,
        latitude: float,
        longitude: float,
        radius_km: float
    ) -> List[Dict[str, Any]]:
        """Download and parse a single day's SPC storm reports."""
        try:
            # Format date as YYMMDD
            date_str = report_date.strftime('%y%m%d')
            url = f"{self.base_url}/{date_str}_rpts_filtered.csv"
            
            # Check cache first
            cache_file = self._get_cache_file_path(report_date)
            if self._is_cache_valid(cache_file, max_age_hours=168):  # 7 days cache
                logger.debug(f"Using cached SPC report for {report_date}")
                return self._parse_spc_csv_to_events(cache_file, latitude, longitude, radius_km)
            
            # Download the CSV file
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    url,
                    headers={"User-Agent": self.user_agent}
                )
                
                if response.status_code == 200:
                    # Save to cache
                    with open(cache_file, 'wb') as f:
                        f.write(response.content)
                    
                    logger.debug(f"Downloaded SPC report for {report_date}")
                    return self._parse_spc_csv_to_events(cache_file, latitude, longitude, radius_km)
                    
                elif response.status_code == 404:
                    # No reports for this date (common)
                    logger.debug(f"No SPC reports available for {report_date}")
                    return []
                    
                else:
                    logger.warning(f"Failed to download SPC report for {report_date}: HTTP {response.status_code}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error downloading SPC report for {report_date}: {e}")
            return []
    
    def _get_cache_file_path(self, report_date: date) -> str:
        """Get cache file path for a given date."""
        return os.path.join(self.cache_dir, f"spc_reports_{report_date.strftime('%Y%m%d')}.csv")
    
    def _is_cache_valid(self, cache_file: str, max_age_hours: int = 168) -> bool:
        """Check if cache file is valid and not too old."""
        if not os.path.exists(cache_file):
            return False
        
        # Check file age
        file_age = datetime.now().timestamp() - os.path.getmtime(cache_file)
        return file_age < (max_age_hours * 3600)  # Convert hours to seconds
    
    def _parse_spc_csv_to_events(
        self,
        csv_file: str,
        latitude: float,
        longitude: float,
        radius_km: float
    ) -> List[Dict[str, Any]]:
        """Parse SPC CSV file and filter by geographic area."""
        events = []
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for record in reader:
                    try:
                        # Parse coordinates
                        lat_str = record.get('Lat', '').strip()
                        lon_str = record.get('Lon', '').strip()
                        
                        if not lat_str or not lon_str:
                            continue
                        
                        try:
                            event_lat = float(lat_str)
                            event_lon = float(lon_str)
                        except (ValueError, TypeError):
                            continue
                        
                        # Check if within radius
                        distance_km = self._calculate_distance(
                            latitude, longitude, event_lat, event_lon
                        )
                        
                        if distance_km > radius_km:
                            continue
                        
                        # Convert to our event format
                        event = self._convert_spc_record_to_event(record, distance_km)
                        if event:
                            events.append(event)
                            
                    except Exception as e:
                        logger.debug(f"Error parsing SPC record: {e}")
                        continue
            
            logger.debug(f"Parsed {len(events)} SPC events from {csv_file}")
            return events
            
        except Exception as e:
            logger.error(f"Error parsing SPC CSV file {csv_file}: {e}")
            return []
    
    def _convert_spc_record_to_event(self, record: Dict[str, Any], distance_km: float) -> Optional[Dict[str, Any]]:
        """Convert SPC record to our standardized event format."""
        try:
            # Extract basic information
            time_str = record.get('Time', '').strip()
            event_type_raw = record.get('Type', '').strip()
            magnitude_str = record.get('Size', '').strip()
            location = record.get('Location', '').strip()
            state = record.get('State', '').strip()
            
            # Parse event type
            event_type_mapping = {
                'hail': 'hail',
                'wind': 'wind',
                'tornado': 'tornado',
                'funnel cloud': 'tornado',
                'waterspout': 'tornado'
            }
            
            event_type = event_type_mapping.get(event_type_raw.lower(), event_type_raw.lower())
            
            # Parse magnitude
            mag_value = None
            if magnitude_str and magnitude_str != '':
                try:
                    # SPC reports magnitude as string like "1.75" or "1.75 INCH"
                    mag_value = float(magnitude_str.split()[0])
                except (ValueError, TypeError, IndexError):
                    mag_value = None
            
            # Determine severity based on thresholds
            severity = "minor"
            insurance_relevant = False
            
            if event_type == "hail":
                if mag_value and mag_value >= 2.0:  # ≥2.0 inches
                    severity = "extreme"
                    insurance_relevant = True
                elif mag_value and mag_value >= 1.0:  # ≥1.0 inch
                    severity = "severe"
                    insurance_relevant = True
                elif mag_value and mag_value >= 0.5:  # ≥0.5 inch
                    severity = "moderate"
                    insurance_relevant = True
                else:
                    severity = "minor"
                    
            elif event_type == "wind":
                if mag_value and mag_value >= 80:  # ≥80 mph
                    severity = "extreme"
                    insurance_relevant = True
                elif mag_value and mag_value >= 60:  # ≥60 mph
                    severity = "severe"
                    insurance_relevant = True
                elif mag_value and mag_value >= 40:  # ≥40 mph
                    severity = "moderate"
                    insurance_relevant = True
                else:
                    severity = "minor"
                    
            elif event_type == "tornado":
                # All tornadoes are severe and insurance-relevant
                severity = "severe"
                insurance_relevant = True
                mag_value = None  # Tornadoes don't have magnitude in SPC reports
            
            # Format description
            description = event_type_raw.title()
            if mag_value is not None:
                if event_type == "hail":
                    description += f" ({mag_value:.2f}\")"
                elif event_type == "wind":
                    description += f" ({mag_value:.0f} mph)"
            
            # Format timestamp
            formatted_date = time_str
            if time_str:
                try:
                    # SPC format is typically "HHMM" or "HH:MM"
                    if ':' in time_str:
                        time_obj = datetime.strptime(time_str, '%H:%M')
                    else:
                        time_obj = datetime.strptime(time_str, '%H%M')
                    formatted_date = time_obj.strftime('%H:%M')
                except ValueError:
                    formatted_date = time_str
            
            return {
                "event_type": event_type,
                "severity": severity,
                "urgency": "immediate" if severity in ["severe", "extreme"] else "expected",
                "description": description,
                "timestamp": formatted_date,
                "source": "NWS-SPC-Preliminary",
                "magnitude": mag_value,
                "magnitude_type": "inches" if event_type == "hail" else "mph" if event_type == "wind" else None,
                "location": location,
                "state": state,
                "county": "",  # SPC doesn't provide county
                "insurance_relevant": insurance_relevant,
                "roofing_damage_risk": "high" if event_type in ["hail", "tornado"] else "medium" if event_type == "wind" else "low",
                "data_quality": "preliminary",
                "distance_km": round(distance_km, 1)
            }
            
        except Exception as e:
            logger.error(f"Error converting SPC record: {e}")
            return None
    
    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers using Haversine formula."""
        import math
        
        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        
        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth's radius in kilometers
        earth_radius_km = 6371.0
        
        return earth_radius_km * c


# Singleton instance
_spc_service: Optional[SPCStormReportsService] = None


def get_spc_service() -> SPCStormReportsService:
    """Get SPC storm reports service instance."""
    global _spc_service
    if _spc_service is None:
        _spc_service = SPCStormReportsService()
    return _spc_service
