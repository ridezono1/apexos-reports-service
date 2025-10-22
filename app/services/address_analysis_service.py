"""
Property-specific address analysis service for location-based weather intelligence.
Implements SkyLink's address report capabilities.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging
import math

from app.core.config import settings
from app.core.logging import get_logger
from app.services.weather_data_service import get_weather_data_service
from app.services.geocoding_service import get_geocoding_service

logger = get_logger(__name__)

# Weather thresholds from SkyLink standards
HAIL_MIN_ACTIONABLE = settings.alert_hail_min_size_inches
HAIL_SEVERE = settings.hail_severe_threshold
HAIL_EXTREME = settings.hail_extreme_threshold
HAIL_MODERATE = settings.hail_moderate_threshold

WIND_MIN_ACTIONABLE = settings.alert_wind_min_speed_mph
WIND_SEVERE = settings.wind_severe_threshold
WIND_EXTREME = settings.wind_extreme_threshold
WIND_MODERATE = settings.wind_moderate_threshold


class AddressAnalysisService:
    """Service for property-specific weather analysis and risk assessment."""
    
    def __init__(self):
        """Initialize address analysis service."""
        self.weather_service = get_weather_data_service()
        self.geocoding_service = get_geocoding_service()
    
    async def analyze_property_address(
        self,
        address: str,
        analysis_period: Dict[str, str],
        analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze weather data and risks for a specific property address.
        
        Args:
            address: Property address to analyze
            analysis_period: Start and end dates for analysis
            analysis_options: Additional analysis options
            
        Returns:
            Comprehensive property weather analysis
        """
        try:
            # Geocode the address to get precise coordinates
            geocode_result = await self.geocoding_service.geocode_address(address)
            if not geocode_result.result:
                raise Exception(f"Could not geocode address: {address}")
            
            coordinates = geocode_result.result
            lat, lon = coordinates.latitude, coordinates.longitude
            
            # Fetch comprehensive weather data for the property
            weather_data = await self._fetch_property_weather_data(
                lat, lon, analysis_period, analysis_options
            )
            
            # Perform property-specific risk assessment
            risk_assessment = await self._assess_property_risks(
                weather_data, coordinates, analysis_options
            )
            
            # Analyze historical weather context
            historical_context = await self._analyze_historical_context(
                weather_data, analysis_period
            )
            
            # Assess business impact for the property
            business_impact = await self._assess_property_business_impact(
                risk_assessment, weather_data, analysis_options
            )
            
            # Determine lead qualification
            lead_qualification = await self._qualify_property_lead(
                risk_assessment, weather_data, analysis_options
            )
            
            # Generate location-based alerts
            location_alerts = await self._generate_location_alerts(
                weather_data, risk_assessment, analysis_options
            )
            
            return {
                "property_info": {
                    "address": address,
                    "coordinates": (lat, lon),
                    "geocoded_address": coordinates.formatted_address,
                    "address_components": {
                        "street_number": coordinates.street_number,
                        "route": coordinates.street,
                        "locality": coordinates.city,
                        "administrative_area_level_1": coordinates.state,
                        "postal_code": coordinates.postal_code,
                        "country": coordinates.country
                    }
                },
                "analysis_period": analysis_period,
                "weather_summary": weather_data["summary"],
                "risk_assessment": risk_assessment,
                "historical_context": historical_context,
                "business_impact": business_impact,
                "lead_qualification": lead_qualification,
                "location_alerts": location_alerts,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in property address analysis: {e}")
            raise

    @staticmethod
    def _get_event_field(event: Dict[str, Any], *keys: str) -> Optional[str]:
        """Return the first non-empty field value from the event."""
        for key in keys:
            value = event.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    def _get_event_type(self, event: Dict[str, Any]) -> str:
        """Return normalized event type (lowercase)."""
        value = self._get_event_field(event, "event", "event_type", "eventType", "type")
        return value.lower() if value else ""

    def _get_event_type_display(self, event: Dict[str, Any]) -> str:
        """Return event type formatted for display."""
        value = self._get_event_field(event, "event", "event_type", "eventType", "type")
        return value.title() if value else "Unknown"

    def _get_event_severity(self, event: Dict[str, Any]) -> str:
        """Return normalized severity (lowercase)."""
        value = self._get_event_field(event, "severity", "severity_level", "severityType")
        return value.lower() if value else ""

    def _get_event_severity_display(self, event: Dict[str, Any]) -> str:
        """Return severity formatted for display."""
        value = self._get_event_field(event, "severity", "severity_level", "severityType")
        return value.title() if value else "Unknown"

    def _get_event_start(self, event: Dict[str, Any]) -> str:
        """Return the best available start timestamp for the event."""
        value = self._get_event_field(
            event,
            "date",
            "timestamp",
            "start_time",
            "startTime",
            "begin_date",
            "beginDate",
            "BEGIN_DATE_TIME"
        )
        return value if value else "Unknown"

    def _get_event_end(self, event: Dict[str, Any]) -> Optional[str]:
        """Return the best available end timestamp for the event."""
        return self._get_event_field(
            event,
            "end_time",
            "endTime",
            "end_date",
            "endDate",
            "END_DATE_TIME"
        )
    
    async def _fetch_property_weather_data(
        self,
        latitude: float,
        longitude: float,
        analysis_period: Dict[str, str],
        analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fetch comprehensive weather data for the property."""
        try:
            start_date = datetime.fromisoformat(analysis_period["start"])
            end_date = datetime.fromisoformat(analysis_period["end"])

            # IMPORTANT: Always fetch 24 months of severe weather events for address reports
            # This provides comprehensive storm history regardless of selected analysis period
            severe_weather_start = end_date - timedelta(days=730)  # 24 months = ~730 days

            logger.info(f"Fetching weather data for address report:")
            logger.info(f"  - Analysis period: {start_date.date()} to {end_date.date()}")
            logger.info(f"  - Severe weather events period: {severe_weather_start.date()} to {end_date.date()} (24 months)")

            # Fetch all weather data types
            current_weather = await self.weather_service.get_current_weather(latitude, longitude)
            forecast = await self.weather_service.get_weather_forecast(latitude, longitude, days=7)
            historical_weather = await self.weather_service.get_historical_weather(
                latitude, longitude, start_date.date(), end_date.date()
            )

            # Always fetch 24 months of severe weather events (tornadoes, hurricanes, hail, strong winds, etc.)
            weather_events = await self.weather_service.get_weather_events(
                latitude, longitude, severe_weather_start.date(), end_date.date(), radius_km=50.0
            )
            
            # Calculate property-specific weather summary
            summary = self._calculate_property_weather_summary(
                current_weather, forecast, historical_weather, weather_events
            )
            
            return {
                "current_weather": current_weather,
                "forecast": forecast,
                "historical_weather": historical_weather,
                "weather_events": weather_events,
                "summary": summary
            }
            
        except Exception as e:
            logger.error(f"Error fetching property weather data: {e}")
            raise
    
    def _calculate_property_weather_summary(
        self,
        current_weather: Dict[str, Any],
        forecast: Dict[str, Any],
        historical_weather: Dict[str, Any],
        weather_events: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate weather summary specific to the property."""
        
        # Calculate weather metrics for the summary table
        max_wind_speed = self._get_max_wind_speed(weather_events, historical_weather)
        total_events = len(weather_events)
        severe_events = len([e for e in weather_events if self._get_event_severity(e) in {"severe", "extreme"}])
        hail_events = len([e for e in weather_events if "hail" in self._get_event_type(e)])
        max_hail_size = self._get_max_hail_size(weather_events)
        temp_range = self._get_temperature_range_from_historical(historical_weather)
        
        # Format severe weather events for the table
        severe_weather_events = self._format_severe_weather_events(weather_events)
        
        summary = {
            "current_conditions": {
                "temperature": current_weather.get("temperature"),  # Already in Fahrenheit from NOAA
                "condition": current_weather.get("weather_condition"),
                "wind_speed": self._convert_ms_to_mph(current_weather.get("wind_speed")),
                "humidity": current_weather.get("humidity")
            },
            "forecast_summary": {
                "next_7_days": len(forecast.get("forecasts", [])),
                "temperature_range": self._get_temperature_range(forecast.get("forecasts", [])),
                "precipitation_chance": self._get_precipitation_chance(forecast.get("forecasts", []))
            },
            "historical_summary": {
                "observations_count": len(historical_weather.get("observations", [])),
                "average_temperature": self._get_average_temperature(historical_weather.get("observations", [])),
                "total_precipitation": self._get_total_precipitation(historical_weather.get("observations", []))
            },
            "weather_events_summary": {
                "total_events": total_events,
                "severe_events": severe_events,
                "event_types": sorted({
                    self._get_event_type_display(event) for event in weather_events
                })
            },
            # New fields for the weather summary table
            "max_wind_speed": max_wind_speed,
            "total_events": total_events,
            "severe_events": severe_events,
            "hail_events": hail_events,
            "max_hail_size": max_hail_size,
            "temp_range": temp_range,
            "severe_weather_events": severe_weather_events
        }
        
        return summary
    
    def _get_max_wind_speed(self, weather_events: List[Dict[str, Any]], historical_weather: Dict[str, Any]) -> float:
        """Get maximum wind speed from real NOAA weather events and historical data."""
        max_wind = 0.0

        # Check NOAA storm events for wind data
        for event in weather_events:
            event_type = self._get_event_type(event)
            if "wind" in event_type or "thunderstorm" in event_type:
                magnitude_value = event.get("magnitude") or event.get("magnitude_value")
                try:
                    wind_speed = float(magnitude_value) if magnitude_value not in (None, "") else 0
                except (ValueError, TypeError):
                    wind_speed = 0

                if wind_speed == 0:
                    severity_level = self._get_event_severity(event)
                    if severity_level == "extreme":
                        wind_speed = WIND_EXTREME
                    elif severity_level == "severe":
                        wind_speed = WIND_SEVERE

                max_wind = max(max_wind, wind_speed)

        # Check historical weather observations for wind data
        observations = historical_weather.get("observations", [])
        for obs in observations:
            wind_speed = obs.get("wind_speed", 0)
            if isinstance(wind_speed, (int, float)):
                # Convert from m/s to mph (NOAA provides wind speed in m/s)
                wind_mph = wind_speed * 2.237
                max_wind = max(max_wind, wind_mph)

        return max_wind
    
    def _get_max_hail_size(self, weather_events: List[Dict[str, Any]]) -> float:
        """Get maximum hail size from weather events (from real NOAA data)."""
        max_hail = 0.0

        for event in weather_events:
            if "hail" in self._get_event_type(event):
                magnitude_value = event.get("magnitude") or event.get("magnitude_value")
                try:
                    hail_size = float(magnitude_value) if magnitude_value not in (None, "") else 0
                except (ValueError, TypeError):
                    hail_size = 0

                if hail_size == 0:
                    severity_level = self._get_event_severity(event)
                    if severity_level == "extreme":
                        hail_size = HAIL_EXTREME
                    elif severity_level == "severe":
                        hail_size = HAIL_SEVERE
                    elif severity_level == "moderate":
                        hail_size = max(hail_size, HAIL_MODERATE)

                max_hail = max(max_hail, hail_size)

        return max_hail
    
    def _convert_celsius_to_fahrenheit(self, temp_celsius: float) -> float:
        """Convert temperature from Celsius to Fahrenheit."""
        if temp_celsius is None:
            return None
        if isinstance(temp_celsius, str):
            return None  # Handle string temperatures
        return temp_celsius * 9/5 + 32
    
    def _convert_ms_to_mph(self, wind_ms: float) -> float:
        """Convert wind speed from m/s to mph."""
        if wind_ms is None:
            return None
        if isinstance(wind_ms, str):
            # Handle string wind speeds like "5 to 10 mph"
            try:
                # Extract first number from string
                import re
                numbers = re.findall(r'\d+', wind_ms)
                if numbers:
                    return float(numbers[0])  # Return first number as mph
                return None
            except:
                return None
        return wind_ms * 2.237
    
    def _get_temperature_range_from_historical(self, historical_weather: Dict[str, Any]) -> str:
        """Get temperature range as a formatted string."""
        observations = historical_weather.get("observations", [])
        if not observations:
            return "N/A"
        
        temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
        if not temperatures:
            return "N/A"
        
        # Convert from Celsius to Fahrenheit (NOAA provides temperature in Celsius)
        min_temp_f = min(temperatures) * 9/5 + 32
        max_temp_f = max(temperatures) * 9/5 + 32
        return f"{min_temp_f:.0f}°F - {max_temp_f:.0f}°F"
    
    def _format_severe_weather_events(self, weather_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format severe weather events for the table.

        Applies SkyLink business thresholds:
        - Wind: ≥60 mph (damaging winds threshold)
        - Hail: ≥1 inch (property damage threshold)
        - Tornadoes: All tornadoes included regardless of EF rating
        """
        severe_events = []

        for event in weather_events:
            event_type = self._get_event_type(event)
            if not event_type:
                continue

            magnitude_raw = self._get_event_field(
                event, "magnitude", "magnitude_value", "mag", "value"
            )
            magnitude_type = self._get_event_field(
                event, "magnitude_type", "magnitudeType", "magnitude_unit"
            ) or ""
            severity_value = self._get_event_severity(event)
            severity = severity_value.title() if severity_value else "Unknown"

            try:
                mag_value = float(magnitude_raw) if magnitude_raw else 0
            except (ValueError, TypeError):
                mag_value = 0

            # Determine if event meets business thresholds
            is_actionable = False
            algorithm_magnitude = "N/A"

            # HAIL THRESHOLD: ≥1 inch
            if "hail" in event_type:
                if mag_value >= HAIL_MIN_ACTIONABLE or severity_value in {"moderate", "severe", "extreme"}:
                    is_actionable = True
                    if mag_value >= HAIL_EXTREME or severity_value == "extreme":
                        algorithm_magnitude = "Extreme"
                        severity = "Severe"
                        if mag_value == 0:
                            mag_value = HAIL_EXTREME
                    elif mag_value >= HAIL_SEVERE or severity_value == "severe":
                        algorithm_magnitude = "Severe"
                        severity = "Severe"
                        if mag_value == 0:
                            mag_value = HAIL_SEVERE
                    elif mag_value >= HAIL_MODERATE or severity_value == "moderate":
                        algorithm_magnitude = "Moderate"
                        if mag_value == 0:
                            mag_value = HAIL_MODERATE
                    else:
                        algorithm_magnitude = "Minor"

            # WIND THRESHOLD: ≥60 mph (damaging winds)
            elif "wind" in event_type or "thunderstorm" in event_type:
                if mag_value >= WIND_MIN_ACTIONABLE or severity_value in {"severe", "extreme"}:
                    is_actionable = True
                    if mag_value >= WIND_EXTREME or severity_value == "extreme":
                        algorithm_magnitude = "Extreme"
                        severity = "Severe"
                        if mag_value == 0:
                            mag_value = WIND_EXTREME
                    elif mag_value >= WIND_SEVERE or severity_value == "severe":
                        algorithm_magnitude = "Severe"
                        severity = "Severe"
                        if mag_value == 0:
                            mag_value = WIND_SEVERE
                    elif mag_value >= WIND_MODERATE:
                        algorithm_magnitude = "Moderate"
                    else:
                        algorithm_magnitude = "Minor"

            # TORNADO THRESHOLD: All tornadoes included
            elif "tornado" in event_type:
                is_actionable = True
                # Use EF rating from magnitude_type if available
                magnitude_type_upper = magnitude_type.upper()
                ef_rating = magnitude_type_upper if "EF" in magnitude_type_upper else f"EF{int(mag_value)}" if mag_value > 0 else "EF0"

                if mag_value >= 4:
                    algorithm_magnitude = f"Violent ({ef_rating})"
                    severity = "Severe"
                elif mag_value >= 2:
                    algorithm_magnitude = f"Strong ({ef_rating})"
                    severity = "Severe"
                else:
                    algorithm_magnitude = f"Weak ({ef_rating})"
                    severity = "Moderate"

            # TROPICAL STORMS & HURRICANES: All included (Category 1-5)
            elif "tropical" in event_type or "hurricane" in event_type or "cyclone" in event_type:
                is_actionable = True
                # Category based on wind speed or explicit category
                magnitude_type_lower = magnitude_type.lower()
                if "category" in magnitude_type_lower:
                    algorithm_magnitude = magnitude_type
                    severity = "Severe" if mag_value >= 3 else "Moderate"
                else:
                    # Estimate category from wind speed
                    if mag_value >= 157:  # Cat 5: ≥157 mph
                        algorithm_magnitude = "Category 5"
                        severity = "Severe"
                    elif mag_value >= 130:  # Cat 4: 130-156 mph
                        algorithm_magnitude = "Category 4"
                        severity = "Severe"
                    elif mag_value >= 111:  # Cat 3: 111-129 mph
                        algorithm_magnitude = "Category 3"
                        severity = "Severe"
                    elif mag_value >= 96:  # Cat 2: 96-110 mph
                        algorithm_magnitude = "Category 2"
                        severity = "Moderate"
                    elif mag_value >= 74:  # Cat 1: 74-95 mph
                        algorithm_magnitude = "Category 1"
                        severity = "Moderate"
                    else:  # Tropical Storm: 39-73 mph
                        algorithm_magnitude = "Tropical Storm"
                        severity = "Moderate"

            # FLOOD EVENTS: All included
            elif "flood" in event_type:
                is_actionable = True
                if "flash" in event_type or mag_value > 3:
                    algorithm_magnitude = "Major Flooding"
                    severity = "Severe"
                elif mag_value > 1:
                    algorithm_magnitude = "Moderate Flooding"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Minor Flooding"
                    severity = "Moderate"

            # WINTER STORM EVENTS: All included
            elif "winter" in event_type or "blizzard" in event_type or "ice" in event_type:
                is_actionable = True
                if "blizzard" in event_type.lower() or mag_value > 12:  # Blizzard or heavy snow
                    algorithm_magnitude = "Severe Winter Storm"
                    severity = "Severe"
                elif mag_value > 6:
                    algorithm_magnitude = "Moderate Winter Storm"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Light Winter Storm"
                    severity = "Moderate"

            # FIRE WEATHER EVENTS: High risk events included
            elif "fire" in event_type:
                if mag_value >= 50 or severity_value in {"severe", "extreme"}:
                    is_actionable = True
                    if mag_value >= 75:
                        algorithm_magnitude = "Extreme Fire Risk"
                        severity = "Severe"
                    else:
                        algorithm_magnitude = "High Fire Risk"
                        severity = "Moderate"

            # NOAA WEATHER EVENTS: Include all weather events from NOAA
            elif "precipitation" in event_type or "rain" in event_type:
                is_actionable = True
                if mag_value >= 50:  # Heavy precipitation (≥50mm)
                    algorithm_magnitude = "Heavy Precipitation"
                    severity = "Severe"
                elif mag_value >= 25:  # Moderate precipitation (≥25mm)
                    algorithm_magnitude = "Moderate Precipitation"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Light Precipitation"
                    severity = "Moderate"

            elif "heat" in event_type:
                is_actionable = True
                if mag_value >= 35:  # Very hot (≥35°C / 95°F)
                    algorithm_magnitude = "Extreme Heat"
                    severity = "Severe"
                elif mag_value >= 30:  # Hot (≥30°C / 86°F)
                    algorithm_magnitude = "High Temperature"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Elevated Temperature"
                    severity = "Moderate"

            elif "cold" in event_type:
                is_actionable = True
                if mag_value <= -10:  # Very cold (≤-10°C / 14°F)
                    algorithm_magnitude = "Extreme Cold"
                    severity = "Severe"
                elif mag_value <= 0:  # Freezing (≤0°C / 32°F)
                    algorithm_magnitude = "Freezing Temperature"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Low Temperature"
                    severity = "Moderate"

            elif "winter" in event_type or "snow" in event_type:
                is_actionable = True
                if mag_value >= 100:  # Heavy snow (≥100mm)
                    algorithm_magnitude = "Heavy Snow"
                    severity = "Severe"
                elif mag_value >= 25:  # Moderate snow (≥25mm)
                    algorithm_magnitude = "Moderate Snow"
                    severity = "Moderate"
                else:
                    algorithm_magnitude = "Light Snow"
                    severity = "Moderate"

            # Only include events that meet business thresholds
            if is_actionable:
                # Format date to match HailTrace style (e.g., "August 18, 2025")
                date_str = self._get_event_start(event)
                date_formatted = date_str
                if date_str != "Unknown":
                    try:
                        if "T" in date_str:
                            date_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        else:
                            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                        date_formatted = date_obj.strftime("%B %d, %Y")
                    except Exception:
                        date_formatted = date_str

                formatted_event = {
                    "date": date_str,  # Keep original for sorting
                    "date_formatted": date_formatted,  # Human-readable format
                    "type": self._get_event_type_display(event),
                    "duration": self._calculate_event_duration(event),
                    "severity": severity,
                    "magnitude": self._format_magnitude(event_type, mag_value, magnitude_type),
                    "algorithm_magnitude": algorithm_magnitude
                }
                severe_events.append(formatted_event)

        # Sort by date (most recent first)
        severe_events.sort(key=lambda x: x["date"], reverse=True)

        return severe_events

    def _calculate_event_duration(self, event: Dict[str, Any]) -> str:
        """Calculate event duration from begin and end times."""
        try:
            begin_raw = self._get_event_field(
                event,
                "begin_date",
                "beginDate",
                "BEGIN_DATE_TIME",
                "start_time",
                "startTime",
                "timestamp"
            )
            end_raw = self._get_event_field(
                event,
                "end_date",
                "endDate",
                "END_DATE_TIME",
                "end_time",
                "endTime"
            )

            def _parse(value: str) -> Optional[datetime]:
                try:
                    if "T" in value:
                        return datetime.fromisoformat(value.replace("Z", "+00:00"))
                    try:
                        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        return datetime.strptime(value, "%Y-%m-%d")
                except Exception:
                    return None

            if begin_raw and end_raw:
                begin = _parse(begin_raw)
                end = _parse(end_raw)
                if begin and end:
                    duration = (end - begin).total_seconds() / 60  # minutes

                    if duration < 60:
                        return f"{int(duration)} min"
                    hours = int(duration / 60)
                    minutes = int(duration % 60)
                    return f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"

            # Default duration estimates
            event_type = self._get_event_type(event)
            if "tornado" in event_type:
                return "~15 min"
            elif "hail" in event_type:
                return "~30 min"
            elif "wind" in event_type:
                return "~1-2 hr"
            else:
                return "Variable"
        except:
            return "Unknown"

    def _format_magnitude(self, event_type: str, magnitude: float, magnitude_type: str) -> str:
        """Format magnitude display based on event type."""
        if magnitude == 0:
            return "N/A"

        if "hail" in event_type:
            return f"{magnitude}\" diameter"
        elif "wind" in event_type or "thunderstorm" in event_type:
            return f"{int(magnitude)} mph"
        elif "tornado" in event_type:
            return magnitude_type if magnitude_type else f"EF{int(magnitude)}"
        else:
            return str(magnitude)
    
    def _get_temperature_range(self, forecasts: List[Dict[str, Any]]) -> Dict[str, float]:
        """Get temperature range from forecasts."""
        if not forecasts:
            return {"min": None, "max": None}
        
        temperatures = [f.get("temperature") for f in forecasts if f.get("temperature")]
        if not temperatures:
            return {"min": None, "max": None}
        
        return {"min": min(temperatures), "max": max(temperatures)}
    
    def _get_precipitation_chance(self, forecasts: List[Dict[str, Any]]) -> float:
        """Get average precipitation chance from forecasts."""
        if not forecasts:
            return 0
        
        precip_chances = [f.get("precipitation_probability", 0) for f in forecasts]
        return sum(precip_chances) / len(precip_chances) if precip_chances else 0
    
    def _get_average_temperature(self, observations: List[Dict[str, Any]]) -> Optional[float]:
        """Get average temperature from historical observations."""
        if not observations:
            return None
        
        temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
        return sum(temperatures) / len(temperatures) if temperatures else None
    
    def _get_total_precipitation(self, observations: List[Dict[str, Any]]) -> float:
        """Get total precipitation from historical observations."""
        if not observations:
            return 0
        
        precipitation = [obs.get("precipitation", 0) for obs in observations]
        return sum(precipitation)
    
    async def _assess_property_risks(
        self,
        weather_data: Dict[str, Any],
        coordinates: Any,
        analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess weather-related risks specific to the property."""
        try:
            risk_factors = analysis_options.get("risk_factors", ["hail", "wind", "flooding", "tornado"])
            weather_events = weather_data["weather_events"]
            historical_weather = weather_data["historical_weather"]
            
            risk_scores = {}
            risk_details = {}
            
            # Assess hail risk
            if "hail" in risk_factors:
                hail_risk = self._assess_hail_risk(weather_events, historical_weather)
                risk_scores["hail"] = hail_risk["score"]
                risk_details["hail"] = hail_risk
            
            # Assess wind risk
            if "wind" in risk_factors:
                wind_risk = self._assess_wind_risk(weather_events, historical_weather, weather_data["current_weather"])
                risk_scores["wind"] = wind_risk["score"]
                risk_details["wind"] = wind_risk
            
            # Assess flooding risk
            if "flooding" in risk_factors:
                flood_risk = self._assess_flooding_risk(historical_weather, coordinates)
                risk_scores["flooding"] = flood_risk["score"]
                risk_details["flooding"] = flood_risk
            
            # Assess tornado risk
            if "tornado" in risk_factors:
                tornado_risk = self._assess_tornado_risk(weather_events)
                risk_scores["tornado"] = tornado_risk["score"]
                risk_details["tornado"] = tornado_risk
            
            # Calculate overall risk score
            overall_risk = sum(risk_scores.values()) / len(risk_scores) if risk_scores else 0
            
            return {
                "overall_risk_score": overall_risk,
                "risk_level": "high" if overall_risk >= 0.7 else "medium" if overall_risk >= 0.4 else "low",
                "risk_factors": risk_scores,
                "risk_details": risk_details,
                "risk_recommendations": self._generate_risk_recommendations(risk_scores, risk_details)
            }
            
        except Exception as e:
            logger.error(f"Error assessing property risks: {e}")
            return {"overall_risk_score": 0, "risk_level": "low", "risk_factors": {}, "risk_details": {}, "risk_recommendations": []}
    
    def _assess_hail_risk(self, weather_events: List[Dict[str, Any]], historical_weather: Dict[str, Any]) -> Dict[str, Any]:
        """Assess hail damage risk for the property."""
        hail_events = [e for e in weather_events if "hail" in self._get_event_type(e)]
        
        risk_score = 0
        risk_factors = []
        
        if hail_events:
            risk_score += 0.6
            risk_factors.append(f"{len(hail_events)} hail events detected")
            
            severe_hail = [e for e in hail_events if self._get_event_severity(e) in {"severe", "extreme"}]
            if severe_hail:
                risk_score += 0.3
                risk_factors.append(f"{len(severe_hail)} severe hail events")
        
        # Check historical precipitation patterns
        observations = historical_weather.get("observations", [])
        high_precip_days = len([obs for obs in observations if obs.get("precipitation", 0) > 10])
        if high_precip_days > 5:
            risk_score += 0.1
            risk_factors.append("High precipitation days indicate storm activity")
        
        return {
            "score": min(risk_score, 1.0),
            "level": "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.4 else "low",
            "factors": risk_factors,
            "events_count": len(hail_events)
        }
    
    def _assess_wind_risk(self, weather_events: List[Dict[str, Any]], historical_weather: Dict[str, Any], current_weather: Dict[str, Any]) -> Dict[str, Any]:
        """Assess wind damage risk for the property."""
        wind_events = [e for e in weather_events if "wind" in self._get_event_type(e)]
        
        risk_score = 0
        risk_factors = []
        
        if wind_events:
            risk_score += 0.5
            risk_factors.append(f"{len(wind_events)} wind events detected")
            
            severe_wind = [e for e in wind_events if self._get_event_severity(e) in {"severe", "extreme"}]
            if severe_wind:
                risk_score += 0.3
                risk_factors.append(f"{len(severe_wind)} severe wind events")
        
        # Check current wind conditions using SkyLink thresholds
        current_wind = current_weather.get("wind_speed", 0)
        # Ensure current_wind is a number
        try:
            current_wind = float(current_wind) if current_wind else 0
        except (ValueError, TypeError):
            current_wind = 0

        if current_wind >= WIND_EXTREME:  # ≥80 mph - Extreme
            risk_score += 0.5
            risk_factors.append(f"Extreme current wind speed: {current_wind} mph")
        elif current_wind >= WIND_SEVERE:  # ≥60 mph - Severe/Damaging
            risk_score += 0.3
            risk_factors.append(f"Severe current wind speed: {current_wind} mph (damaging winds)")
        elif current_wind >= WIND_MODERATE:  # ≥40 mph - Moderate
            risk_score += 0.2
            risk_factors.append(f"Moderate current wind speed: {current_wind} mph")

        # Check historical wind patterns using actionable threshold
        observations = historical_weather.get("observations", [])
        high_wind_days = len([obs for obs in observations if obs.get("wind_speed") is not None and obs.get("wind_speed", 0) >= WIND_MIN_ACTIONABLE])
        if high_wind_days > 0:
            risk_score += min(0.2, high_wind_days * 0.05)  # Cap at 0.2
            risk_factors.append(f"{high_wind_days} days with actionable wind speeds (≥{WIND_MIN_ACTIONABLE} mph)")
        
        return {
            "score": min(risk_score, 1.0),
            "level": "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.4 else "low",
            "factors": risk_factors,
            "events_count": len(wind_events),
            "current_wind_speed": current_wind
        }
    
    def _assess_flooding_risk(self, historical_weather: Dict[str, Any], coordinates: Any) -> Dict[str, Any]:
        """Assess flooding risk for the property."""
        risk_score = 0
        risk_factors = []
        
        observations = historical_weather.get("observations", [])
        
        # Check precipitation patterns
        total_precipitation = sum(obs.get("precipitation", 0) for obs in observations)
        if total_precipitation > 50:  # mm
            risk_score += 0.4
            risk_factors.append(f"High total precipitation: {total_precipitation:.1f}mm")
        
        # Check for heavy rain days
        heavy_rain_days = len([obs for obs in observations if obs.get("precipitation", 0) > 20])
        if heavy_rain_days > 2:
            risk_score += 0.3
            risk_factors.append(f"{heavy_rain_days} heavy rain days")
        
        # Check elevation (if available from geocoding)
        elevation = getattr(coordinates, 'elevation', None)
        if elevation is not None and elevation < 100:  # feet
            risk_score += 0.2
            risk_factors.append(f"Low elevation: {elevation} feet")
        
        return {
            "score": min(risk_score, 1.0),
            "level": "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.4 else "low",
            "factors": risk_factors,
            "total_precipitation": total_precipitation,
            "heavy_rain_days": heavy_rain_days
        }
    
    def _assess_tornado_risk(self, weather_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess tornado risk for the property."""
        tornado_events = [e for e in weather_events if "tornado" in self._get_event_type(e)]
        
        risk_score = 0
        risk_factors = []
        
        if tornado_events:
            risk_score += 0.8
            risk_factors.append(f"{len(tornado_events)} tornado events detected")
            
            severe_tornado = [e for e in tornado_events if self._get_event_severity(e) in {"severe", "extreme"}]
            if severe_tornado:
                risk_score += 0.2
                risk_factors.append(f"{len(severe_tornado)} severe tornado events")
        
        return {
            "score": min(risk_score, 1.0),
            "level": "high" if risk_score >= 0.7 else "medium" if risk_score >= 0.4 else "low",
            "factors": risk_factors,
            "events_count": len(tornado_events)
        }
    
    def _generate_risk_recommendations(self, risk_scores: Dict[str, float], risk_details: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on risk assessment."""
        recommendations = []
        
        for risk_type, score in risk_scores.items():
            if score >= 0.7:
                if risk_type == "hail":
                    recommendations.append("High hail risk - consider protective measures for vehicles and outdoor equipment")
                elif risk_type == "wind":
                    recommendations.append("High wind risk - secure loose objects and inspect roof condition")
                elif risk_type == "flooding":
                    recommendations.append("High flooding risk - ensure proper drainage and consider flood barriers")
                elif risk_type == "tornado":
                    recommendations.append("High tornado risk - identify safe shelter areas and prepare emergency kit")
            elif score >= 0.4:
                if risk_type == "hail":
                    recommendations.append("Moderate hail risk - monitor weather conditions")
                elif risk_type == "wind":
                    recommendations.append("Moderate wind risk - check for loose items")
                elif risk_type == "flooding":
                    recommendations.append("Moderate flooding risk - check drainage systems")
                elif risk_type == "tornado":
                    recommendations.append("Moderate tornado risk - review emergency procedures")
        
        return recommendations
    
    async def _analyze_historical_context(
        self,
        weather_data: Dict[str, Any],
        analysis_period: Dict[str, str]
    ) -> Dict[str, Any]:
        """Analyze historical weather context for the property."""
        try:
            historical_weather = weather_data["historical_weather"]
            weather_events = weather_data["weather_events"]
            
            observations = historical_weather.get("observations", [])
            
            # Analyze temperature trends
            temperatures = [obs.get("temperature") for obs in observations if obs.get("temperature")]
            temp_trend = self._calculate_temperature_trend(temperatures)
            
            # Analyze precipitation patterns
            precipitation = [obs.get("precipitation", 0) for obs in observations]
            precip_pattern = self._analyze_precipitation_pattern(precipitation)
            
            # Analyze weather event frequency
            event_frequency = self._analyze_event_frequency(weather_events)
            
            return {
                "temperature_analysis": temp_trend,
                "precipitation_analysis": precip_pattern,
                "event_frequency": event_frequency,
                "analysis_period": analysis_period,
                "data_points": len(observations)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing historical context: {e}")
            return {"temperature_analysis": {}, "precipitation_analysis": {}, "event_frequency": {}}
    
    def _calculate_temperature_trend(self, temperatures: List[float]) -> Dict[str, Any]:
        """Calculate temperature trend from historical data."""
        if len(temperatures) < 2:
            return {"trend": "insufficient_data"}
        
        # Simple linear trend calculation
        n = len(temperatures)
        x_values = list(range(n))
        
        # Calculate slope
        sum_x = sum(x_values)
        sum_y = sum(temperatures)
        sum_xy = sum(x * y for x, y in zip(x_values, temperatures))
        sum_x2 = sum(x * x for x in x_values)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        
        if slope > 0.1:
            trend = "increasing"
        elif slope < -0.1:
            trend = "decreasing"
        else:
            trend = "stable"
        
        return {
            "trend": trend,
            "slope": slope,
            "average": sum(temperatures) / len(temperatures),
            "range": {"min": min(temperatures), "max": max(temperatures)}
        }
    
    def _analyze_precipitation_pattern(self, precipitation: List[float]) -> Dict[str, Any]:
        """Analyze precipitation patterns."""
        if not precipitation:
            return {"pattern": "no_data"}
        
        total_precip = sum(precipitation)
        rainy_days = len([p for p in precipitation if p > 0])
        heavy_rain_days = len([p for p in precipitation if p > 10])
        
        return {
            "total_precipitation": total_precip,
            "rainy_days": rainy_days,
            "heavy_rain_days": heavy_rain_days,
            "average_daily": total_precip / len(precipitation),
            "pattern": "wet" if rainy_days > len(precipitation) * 0.5 else "dry"
        }
    
    def _analyze_event_frequency(self, weather_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze weather event frequency."""
        if not weather_events:
            return {"frequency": "low", "events_by_type": {}}
        
        events_by_type = {}
        for event in weather_events:
            event_type = self._get_event_type_display(event)
            events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
        
        total_events = len(weather_events)
        if total_events > 10:
            frequency = "high"
        elif total_events > 5:
            frequency = "medium"
        else:
            frequency = "low"
        
        return {
            "frequency": frequency,
            "total_events": total_events,
            "events_by_type": events_by_type
        }
    
    async def _assess_property_business_impact(
        self,
        risk_assessment: Dict[str, Any],
        weather_data: Dict[str, Any],
        analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess business impact for the specific property."""
        try:
            overall_risk = risk_assessment["overall_risk_score"]
            weather_events = weather_data["weather_events"]
            
            impact_score = 0
            impact_factors = []
            
            # High risk properties have higher business impact
            if overall_risk >= 0.7:
                impact_score += 0.4
                impact_factors.append("High weather risk property")
            elif overall_risk >= 0.4:
                impact_score += 0.2
                impact_factors.append("Moderate weather risk property")
            
            # Severe weather events indicate immediate business impact
            severe_events = len([e for e in weather_events if self._get_event_severity(e) in {"severe", "extreme"}])
            if severe_events > 0:
                impact_score += 0.3
                impact_factors.append(f"{severe_events} severe weather events")
            
            # Multiple event types indicate complex weather patterns
            unique_events = len({
                self._get_event_type_display(e) for e in weather_events if self._get_event_type(e)
            })
            if unique_events > 2:
                impact_score += 0.2
                impact_factors.append("Multiple weather event types")
            
            # Current weather conditions affect immediate operations
            current_weather = weather_data["current_weather"]
            current_wind = current_weather.get("wind_speed", 0)
            # Ensure current_wind is a number
            try:
                current_wind = float(current_wind) if current_wind else 0
            except (ValueError, TypeError):
                current_wind = 0

            if current_wind >= WIND_SEVERE:
                impact_score += 0.3
                impact_factors.append(f"Severe wind conditions ({current_wind} mph - damaging winds)")
            elif current_wind >= WIND_MODERATE:
                impact_score += 0.1
                impact_factors.append(f"Moderate wind conditions ({current_wind} mph)")
            
            return {
                "impact_score": min(impact_score, 1.0),
                "impact_level": "high" if impact_score >= 0.7 else "medium" if impact_score >= 0.4 else "low",
                "impact_factors": impact_factors,
                "business_recommendations": self._generate_business_recommendations(impact_score, impact_factors)
            }
            
        except Exception as e:
            logger.error(f"Error assessing business impact: {e}")
            return {"impact_score": 0, "impact_level": "low", "impact_factors": [], "business_recommendations": []}
    
    def _generate_business_recommendations(self, impact_score: float, impact_factors: List[str]) -> List[str]:
        """Generate business recommendations based on impact assessment."""
        recommendations = []
        
        if impact_score >= 0.7:
            recommendations.extend([
                "High business impact detected - prioritize this property for immediate attention",
                "Schedule emergency property inspection",
                "Prepare for potential service disruptions",
                "Notify property owner of weather-related risks"
            ])
        elif impact_score >= 0.4:
            recommendations.extend([
                "Moderate business impact - monitor property conditions closely",
                "Schedule routine property inspection",
                "Prepare contingency plans for service delivery"
            ])
        else:
            recommendations.extend([
                "Low business impact - normal operations can continue",
                "Include in regular property maintenance schedule"
            ])
        
        return recommendations
    
    async def _qualify_property_lead(
        self,
        risk_assessment: Dict[str, Any],
        weather_data: Dict[str, Any],
        analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Qualify the property as a potential lead."""
        try:
            overall_risk = risk_assessment["overall_risk_score"]
            weather_events = weather_data["weather_events"]
            risk_details = risk_assessment["risk_details"]
            
            lead_score = 0
            lead_factors = []
            lead_type = "maintenance"
            
            # High-risk properties are high-value leads
            if overall_risk >= 0.7:
                lead_score += 0.4
                lead_factors.append("High weather risk property")
                lead_type = "emergency_response"
            elif overall_risk >= 0.4:
                lead_score += 0.2
                lead_factors.append("Moderate weather risk property")
                lead_type = "preventive_maintenance"
            
            # Specific risk factors indicate service needs
            if risk_details.get("hail", {}).get("score", 0) >= 0.5:
                lead_score += 0.3
                lead_factors.append("Hail damage assessment needed")
                lead_type = "property_damage_assessment"
            
            if risk_details.get("wind", {}).get("score", 0) >= 0.5:
                lead_score += 0.2
                lead_factors.append("Wind damage inspection recommended")
            
            if risk_details.get("flooding", {}).get("score", 0) >= 0.5:
                lead_score += 0.2
                lead_factors.append("Flooding risk assessment needed")
            
            # Severe weather events indicate immediate service needs
            severe_events = len([e for e in weather_events if e.get("severity") == "Severe"])
            if severe_events > 0:
                lead_score += 0.3
                lead_factors.append(f"{severe_events} severe weather events")
                lead_type = "emergency_response"
            
            return {
                "lead_score": min(lead_score, 1.0),
                "lead_level": "high" if lead_score >= 0.7 else "medium" if lead_score >= 0.4 else "low",
                "lead_type": lead_type,
                "lead_factors": lead_factors,
                "qualification_reason": self._generate_qualification_reason(lead_score, lead_factors)
            }
            
        except Exception as e:
            logger.error(f"Error qualifying property lead: {e}")
            return {"lead_score": 0, "lead_level": "low", "lead_type": "maintenance", "lead_factors": [], "qualification_reason": ""}
    
    def _generate_qualification_reason(self, lead_score: float, lead_factors: List[str]) -> str:
        """Generate qualification reason for the lead."""
        if lead_score >= 0.7:
            return f"High-priority lead due to: {', '.join(lead_factors)}"
        elif lead_score >= 0.4:
            return f"Medium-priority lead due to: {', '.join(lead_factors)}"
        else:
            return "Low-priority lead - routine maintenance opportunity"
    
    async def _generate_location_alerts(
        self,
        weather_data: Dict[str, Any],
        risk_assessment: Dict[str, Any],
        analysis_options: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate location-based alerts for the property."""
        try:
            alerts = []
            current_weather = weather_data["current_weather"]
            weather_events = weather_data["weather_events"]
            risk_details = risk_assessment["risk_details"]
            
            # Current weather alerts using SkyLink thresholds
            current_wind = current_weather.get("wind_speed", 0)
            # Ensure current_wind is a number
            try:
                current_wind = float(current_wind) if current_wind else 0
            except (ValueError, TypeError):
                current_wind = 0

            if current_wind >= WIND_EXTREME:
                alerts.append({
                    "type": "wind_warning",
                    "severity": "extreme",
                    "message": f"Extreme wind conditions: {current_wind} mph",
                    "action": "Seek shelter immediately. Significant structural damage possible."
                })
            elif current_wind >= WIND_SEVERE:
                alerts.append({
                    "type": "wind_warning",
                    "severity": "high",
                    "message": f"Damaging wind conditions: {current_wind} mph",
                    "action": "Secure all loose objects. Potential for roof damage and downed trees."
                })
            elif current_wind >= WIND_MODERATE:
                alerts.append({
                    "type": "wind_advisory",
                    "severity": "medium",
                    "message": f"Moderate wind conditions: {current_wind} mph",
                    "action": "Secure loose objects and monitor conditions"
                })
            
            if current_weather.get("temperature", 0) < 32:
                alerts.append({
                    "type": "freeze_warning",
                    "severity": "medium",
                    "message": f"Freezing temperatures: {current_weather.get('temperature')}°F",
                    "action": "Protect exposed pipes and equipment"
                })
            
            # Risk-based alerts
            if risk_details.get("hail", {}).get("score", 0) >= 0.7:
                alerts.append({
                    "type": "hail_risk",
                    "severity": "high",
                    "message": "High hail risk detected",
                    "action": "Consider protective measures for vehicles and equipment"
                })
            
            if risk_details.get("flooding", {}).get("score", 0) >= 0.7:
                alerts.append({
                    "type": "flooding_risk",
                    "severity": "high",
                    "message": "High flooding risk detected",
                    "action": "Check drainage systems and prepare flood barriers"
                })
            
            # Weather event alerts
            active_severe_events = [e for e in weather_events if e.get("status") == "actual"]
            for event in active_severe_events:
                alerts.append({
                    "type": "severe_weather",
                    "severity": "high",
                    "message": f"Active severe weather: {event.get('event')}",
                    "action": "Monitor conditions and prepare for potential impact"
                })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error generating location alerts: {e}")
            return []


# Singleton instance
_address_analysis_service: Optional[AddressAnalysisService] = None


def get_address_analysis_service() -> AddressAnalysisService:
    """Get address analysis service instance."""
    global _address_analysis_service
    if _address_analysis_service is None:
        _address_analysis_service = AddressAnalysisService()
    return _address_analysis_service
