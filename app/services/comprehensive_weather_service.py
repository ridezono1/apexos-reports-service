"""
Comprehensive Weather Service for Roofing Marketing Use Cases

This service combines multiple NOAA data sources to provide comprehensive
severe weather data for insurance claim analysis and roofing marketing.

Key Use Cases:
- Identify properties that experienced severe weather damage
- Provide 24 months of historical severe weather data
- Support insurance claim validation
- Enable targeted roofing marketing campaigns
"""

import asyncio
import httpx
import csv
import io
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class SevereWeatherEvent:
    """Represents a severe weather event for insurance/roofing analysis"""

    event_type: str  # hail, tornado, wind, thunderstorm, flood, heat, cold, winter
    severity: str  # minor, moderate, severe, extreme
    urgency: str  # immediate, expected, future, past
    description: str
    timestamp: str
    source: str
    magnitude: Optional[float] = None
    station_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    damage_potential: str = "unknown"  # low, medium, high, extreme
    insurance_relevant: bool = False


class ComprehensiveWeatherService:
    """
    Comprehensive weather service that combines multiple NOAA data sources
    to provide complete severe weather analysis for roofing marketing.
    """

    def __init__(self, cdo_api_token: str):
        self.cdo_api_token = cdo_api_token
        self.cdo_base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"
        self.swdi_base_url = "https://www.ncei.noaa.gov/swdiws/json"
        self.weather_gov_base_url = "https://api.weather.gov"
        self.timeout = httpx.Timeout(30.0)

        # Storm Prediction Center CSV URLs
        self.spc_storm_reports_url = "https://www.spc.noaa.gov/climo/reports"

    async def get_comprehensive_weather_events(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_miles: float = 25.0,
    ) -> List[SevereWeatherEvent]:
        """
        Get comprehensive severe weather events from multiple sources.

        Args:
            latitude: Property latitude
            longitude: Property longitude
            start_date: Analysis start date
            end_date: Analysis end date
            radius_miles: Search radius in miles

        Returns:
            List of severe weather events sorted by timestamp
        """
        events = []

        # Calculate bounding box for area search
        bbox = self._calculate_bounding_box(latitude, longitude, radius_miles)

        logger.info(
            f"Fetching comprehensive weather data for {start_date} to {end_date}"
        )
        logger.info(f"Search area: {bbox}")

        # 1. Get Storm Prediction Center storm reports (most comprehensive)
        spc_events = await self._fetch_spc_storm_reports(
            latitude, longitude, start_date, end_date, radius_miles
        )
        events.extend(spc_events)

        # 2. Get NOAA SWDI data (recent severe weather)
        swdi_events = await self._fetch_swdi_data(bbox, start_date, end_date)
        events.extend(swdi_events)

        # 3. Get enhanced GHCND analysis (basic weather patterns)
        ghcnd_events = await self._fetch_enhanced_ghcnd_data(
            latitude, longitude, start_date, end_date
        )
        events.extend(ghcnd_events)

        # 4. Get current weather alerts
        alert_events = await self._fetch_current_alerts(latitude, longitude)
        if alert_events:
            events.extend(alert_events)

        # Sort by timestamp and remove duplicates
        events = self._deduplicate_and_sort_events(events)

        # Mark insurance relevance
        events = self._mark_insurance_relevance(events)

        logger.info(f"Found {len(events)} total severe weather events")
        return events

    async def _fetch_spc_storm_reports(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        radius_miles: float,
    ) -> List[SevereWeatherEvent]:
        """
        Fetch Storm Prediction Center storm reports.
        These are the most comprehensive severe weather records.
        """
        events = []

        try:
            logger.info("Fetching SPC storm reports...")

            # Calculate bounding box for area search
            bbox = self._calculate_bounding_box(latitude, longitude, radius_miles)

            # Try to access Storm Events Database through NCEI
            # This is the most comprehensive source for severe weather events
            current_start = start_date

            while current_start < end_date:
                chunk_end = min(
                    date(
                        current_start.year + 1, current_start.month, current_start.day
                    ),
                    end_date,
                )

                # Try different Storm Events dataset IDs
                storm_datasets = [
                    "STORMEVENTS",  # Storm Events Database
                    "STORM_EVENTS",  # Alternative ID
                    "SEVERE_WEATHER",  # Another possible ID
                ]

                for dataset_id in storm_datasets:
                    try:
                        params = {
                            "datasetid": dataset_id,
                            "extent": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                            "startdate": current_start.isoformat(),
                            "enddate": chunk_end.isoformat(),
                            "limit": 1000,
                            "includemetadata": "false",
                        }

                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(
                                f"{self.cdo_base_url}/data",
                                params=params,
                                headers={"token": self.cdo_api_token},
                            )
                            response.raise_for_status()
                            data = response.json()

                            results = data.get("results", [])
                            if results:
                                logger.info(
                                    f"Found {len(results)} storm events in {dataset_id}"
                                )

                                for record in results:
                                    event = self._convert_storm_event_to_event(record)
                                    if event:
                                        events.append(event)

                                # If we found data, we can stop trying other datasets
                                break

                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 400:
                            logger.debug(
                                f"Dataset {dataset_id} not available or invalid parameters"
                            )
                        else:
                            logger.debug(f"Error accessing {dataset_id}: {e}")
                        continue
                    except Exception as e:
                        logger.debug(f"Error fetching {dataset_id}: {e}")
                        continue

                current_start = chunk_end

        except Exception as e:
            logger.error(f"Error fetching SPC storm reports: {e}")

        return events

    async def _fetch_swdi_data(
        self, bbox: Tuple[float, float, float, float], start_date: date, end_date: date
    ) -> List[SevereWeatherEvent]:
        """
        Fetch NOAA SWDI (Severe Weather Data Inventory) data.
        Limited to 31-day chunks due to API restrictions.
        """
        events = []

        try:
            # SWDI has 744-hour (31-day) limit, so we need to chunk requests
            current_start = start_date
            bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

            while current_start < end_date:
                # Calculate end date for this chunk (max 31 days)
                chunk_end = min(current_start + timedelta(days=31), end_date)

                # Try different SWDI endpoints
                swdi_endpoints = [
                    "nx3hail",  # Hail signatures
                    "nx3tvs",  # Tornado vortex signatures
                    "nx3meso",  # Mesocyclone signatures
                    "nx3vil",  # Vertically integrated liquid
                ]

                for endpoint in swdi_endpoints:
                    try:
                        date_range = f"{current_start.strftime('%Y%m%d')}:{chunk_end.strftime('%Y%m%d')}"
                        url = f"{self.swdi_base_url}/{endpoint}/{date_range}?bbox={bbox_str}"

                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(url)
                            response.raise_for_status()
                            data = response.json()

                            if data.get("result"):
                                for feature in data["result"]:
                                    event = self._convert_swdi_to_event(
                                        feature, endpoint
                                    )
                                    if event:
                                        events.append(event)

                    except Exception as e:
                        logger.debug(f"SWDI endpoint {endpoint} failed: {e}")
                        continue

                current_start = chunk_end

        except Exception as e:
            logger.error(f"Error fetching SWDI data: {e}")

        return events

    async def _fetch_enhanced_ghcnd_data(
        self, latitude: float, longitude: float, start_date: date, end_date: date
    ) -> List[SevereWeatherEvent]:
        """
        Fetch enhanced GHCND data with improved severe weather detection.
        """
        events = []

        try:
            # Use existing NOAA CDO API with enhanced thresholds
            current_start = start_date

            while current_start < end_date:
                chunk_end = min(
                    date(
                        current_start.year + 1, current_start.month, current_start.day
                    ),
                    end_date,
                )

                params = {
                    "datasetid": "GHCND",
                    "locationid": f"FIPS:48",  # Texas
                    "startdate": current_start.isoformat(),
                    "enddate": chunk_end.isoformat(),
                    "limit": 1000,
                    "includemetadata": "false",
                }

                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(
                        f"{self.cdo_base_url}/data",
                        params=params,
                        headers={"token": self.cdo_api_token},
                    )
                    response.raise_for_status()
                    data = response.json()

                    for record in data.get("results", []):
                        event = self._convert_enhanced_ghcnd_to_event(record)
                        if event:
                            events.append(event)

                current_start = chunk_end

        except Exception as e:
            logger.error(f"Error fetching enhanced GHCND data: {e}")

        return events

    async def _fetch_current_alerts(
        self, latitude: float, longitude: float
    ) -> List[SevereWeatherEvent]:
        """
        Fetch current weather alerts from weather.gov API.
        """
        events = []

        try:
            # Get weather alerts for the area
            url = f"{self.weather_gov_base_url}/alerts/active"
            params = {"point": f"{latitude},{longitude}"}

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                for feature in data.get("features", []):
                    event = self._convert_alert_to_event(feature)
                    if event:
                        events.append(event)

        except Exception as e:
            logger.error(f"Error fetching current alerts: {e}")

        return events

    def _convert_storm_event_to_event(
        self, record: Dict
    ) -> Optional[SevereWeatherEvent]:
        """Convert Storm Events Database record to SevereWeatherEvent"""
        try:
            # Map storm event types to our event types
            event_type_map = {
                "Hail": "hail",
                "Tornado": "tornado",
                "Thunderstorm Wind": "wind",
                "Thunderstorm Winds": "wind",
                "High Wind": "wind",
                "Strong Wind": "wind",
                "Heavy Rain": "flood",
                "Flood": "flood",
                "Flash Flood": "flood",
                "Heat": "heat",
                "Cold": "cold",
                "Winter Storm": "winter",
                "Ice Storm": "winter",
                "Blizzard": "winter",
            }

            event_type_raw = record.get("event_type", "")
            event_type = event_type_map.get(event_type_raw, event_type_raw.lower())

            # Extract magnitude and severity
            magnitude = record.get("magnitude", 0)
            severity = "moderate"

            # Determine severity based on event type and magnitude
            if event_type == "hail":
                if magnitude >= 2.0:
                    severity = "severe"
                elif magnitude >= 1.0:
                    severity = "moderate"
                else:
                    severity = "minor"
            elif event_type == "tornado":
                # Tornadoes are always severe
                severity = "severe"
            elif event_type == "wind":
                if magnitude >= 75:
                    severity = "severe"
                elif magnitude >= 58:
                    severity = "moderate"
                else:
                    severity = "minor"
            elif event_type in ["flood", "heat", "cold", "winter"]:
                if magnitude > 50:  # Threshold varies by type
                    severity = "severe"
                elif magnitude > 25:
                    severity = "moderate"
                else:
                    severity = "minor"

            return SevereWeatherEvent(
                event_type=event_type,
                severity=severity,
                urgency="past",
                description=f"{event_type_raw} event (magnitude: {magnitude})",
                timestamp=record.get("date", ""),
                source="NOAA-Storm-Events-Database",
                magnitude=magnitude,
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
                damage_potential="high"
                if severity in ["severe", "extreme"]
                else "medium",
                insurance_relevant=True,
            )

        except Exception as e:
            logger.error(f"Error converting storm event: {e}")
            return None

    def _convert_swdi_to_event(
        self, feature: Dict, endpoint: str
    ) -> Optional[SevereWeatherEvent]:
        """Convert SWDI feature to SevereWeatherEvent"""
        try:
            props = feature.get("properties", {})

            # Map endpoint to event type
            event_type_map = {
                "nx3hail": "hail",
                "nx3tvs": "tornado",
                "nx3meso": "thunderstorm",
                "nx3vil": "thunderstorm",
            }

            event_type = event_type_map.get(endpoint, "unknown")

            # Extract magnitude and severity
            magnitude = props.get("max", 0)
            severity = "moderate"

            if endpoint == "nx3hail":
                if magnitude > 1.0:
                    severity = "severe"
                elif magnitude > 0.5:
                    severity = "moderate"
                else:
                    severity = "minor"

            return SevereWeatherEvent(
                event_type=event_type,
                severity=severity,
                urgency="past",
                description=f"{event_type.title()} signature detected (magnitude: {magnitude})",
                timestamp=props.get("time", ""),
                source=f"NOAA-SWDI-{endpoint}",
                magnitude=magnitude,
                latitude=props.get("lat"),
                longitude=props.get("lon"),
                damage_potential="high"
                if severity in ["severe", "extreme"]
                else "medium",
                insurance_relevant=True,
            )

        except Exception as e:
            logger.error(f"Error converting SWDI event: {e}")
            return None

    def _convert_enhanced_ghcnd_to_event(
        self, record: Dict
    ) -> Optional[SevereWeatherEvent]:
        """Convert GHCND record to SevereWeatherEvent with enhanced thresholds"""
        try:
            datatype = record.get("datatype", "")
            value = record.get("value", 0)
            date_str = record.get("date", "")
            station_id = record.get("station", "")

            # Enhanced thresholds for roofing/insurance relevance
            if datatype == "PRCP" and value > 25:  # >2.5mm precipitation
                event_type = "flood"
                severity = "moderate" if value > 50 else "minor"
                magnitude = value / 10.0
                description = f"Precipitation event ({magnitude:.1f} mm)"

            elif datatype == "TMAX" and value > 280:  # >28°C
                event_type = "heat"
                severity = "moderate" if value > 320 else "minor"
                magnitude = value / 10.0
                description = f"High temperature ({magnitude:.1f}°C)"

            elif datatype == "TMIN" and value < -50:  # <-5°C
                event_type = "cold"
                severity = "moderate" if value < -100 else "minor"
                magnitude = value / 10.0
                description = f"Low temperature ({magnitude:.1f}°C)"

            elif datatype == "SNOW" and value > 50:  # >5mm snow
                event_type = "winter"
                severity = "moderate" if value > 200 else "minor"
                magnitude = value / 10.0
                description = f"Snow event ({magnitude:.1f} mm)"

            else:
                return None

            return SevereWeatherEvent(
                event_type=event_type,
                severity=severity,
                urgency="past",
                description=description,
                timestamp=date_str,
                source="NOAA-GHCND-Enhanced",
                magnitude=magnitude,
                station_id=station_id,
                damage_potential="medium" if severity == "moderate" else "low",
                insurance_relevant=severity in ["moderate", "severe"],
            )

        except Exception as e:
            logger.error(f"Error converting enhanced GHCND event: {e}")
            return None

    def _convert_alert_to_event(self, feature: Dict) -> Optional[SevereWeatherEvent]:
        """Convert weather.gov alert to SevereWeatherEvent"""
        try:
            props = feature.get("properties", {})

            event_type_map = {
                "Severe Thunderstorm Warning": "thunderstorm",
                "Tornado Warning": "tornado",
                "Hail Warning": "hail",
                "Wind Advisory": "wind",
                "Flood Warning": "flood",
                "Heat Advisory": "heat",
                "Winter Storm Warning": "winter",
            }

            event_type = event_type_map.get(props.get("event", ""), "unknown")
            severity = props.get("severity", "unknown").lower()

            return SevereWeatherEvent(
                event_type=event_type,
                severity=severity,
                urgency=props.get("urgency", "expected").lower(),
                description=props.get("description", ""),
                timestamp=props.get("effective", ""),
                source="NOAA-Weather-Gov",
                damage_potential="high"
                if severity in ["severe", "extreme"]
                else "medium",
                insurance_relevant=True,
            )

        except Exception as e:
            logger.error(f"Error converting alert event: {e}")
            return None

    def _calculate_bounding_box(
        self, latitude: float, longitude: float, radius_miles: float
    ) -> Tuple[float, float, float, float]:
        """Calculate bounding box for area search"""
        # Approximate conversion: 1 degree ≈ 69 miles
        lat_offset = radius_miles / 69.0
        lon_offset = radius_miles / (69.0 * abs(latitude) / 90.0)

        return (
            longitude - lon_offset,  # min_lon
            latitude - lat_offset,  # min_lat
            longitude + lon_offset,  # max_lon
            latitude + lat_offset,  # max_lat
        )

    def _deduplicate_and_sort_events(
        self, events: List[SevereWeatherEvent]
    ) -> List[SevereWeatherEvent]:
        """Remove duplicate events and sort by timestamp"""
        # Simple deduplication by timestamp and event type
        seen = set()
        unique_events = []

        for event in events:
            key = (event.timestamp, event.event_type, event.source)
            if key not in seen:
                seen.add(key)
                unique_events.append(event)

        # Sort by timestamp
        unique_events.sort(key=lambda x: x.timestamp)
        return unique_events

    def _mark_insurance_relevance(
        self, events: List[SevereWeatherEvent]
    ) -> List[SevereWeatherEvent]:
        """Mark events for insurance relevance based on damage potential"""
        for event in events:
            # High damage potential events are always insurance relevant
            if event.damage_potential in ["high", "extreme"]:
                event.insurance_relevant = True
            # Moderate events with certain types are relevant
            elif event.damage_potential == "medium" and event.event_type in [
                "hail",
                "tornado",
                "wind",
                "thunderstorm",
            ]:
                event.insurance_relevant = True
            # Severe events are always relevant
            elif event.severity in ["severe", "extreme"]:
                event.insurance_relevant = True

        return events


# Factory function
def get_comprehensive_weather_service(
    cdo_api_token: str,
) -> ComprehensiveWeatherService:
    """Get comprehensive weather service instance"""
    return ComprehensiveWeatherService(cdo_api_token)
