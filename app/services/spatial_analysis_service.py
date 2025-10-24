"""
Spatial analysis service for boundary-based weather intelligence reports.
Implements SkyLink's spatial report capabilities.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, timedelta
import logging
import math

from app.core.config import settings
from app.core.logging import get_logger
from app.core.noaa_data_freshness import get_data_freshness_info, format_data_disclaimer
from app.services.weather_data_service import get_weather_data_service
from app.services.geocoding_service import get_geocoding_service

logger = get_logger(__name__)


class SpatialAnalysisService:
    """Service for spatial weather analysis within geographic boundaries."""

    def __init__(self):
        """Initialize spatial analysis service."""
        self.weather_service = get_weather_data_service()
        self.geocoding_service = get_geocoding_service()

    async def analyze_spatial_area(
        self,
        boundary_type: str,
        boundary_data: Dict[str, Any],
        analysis_period: Dict[str, str],
        analysis_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Analyze weather data within a spatial boundary.

        Args:
            boundary_type: Type of boundary (county, city, neighborhood, radius, polygon)
            boundary_data: Boundary definition data
            analysis_period: Start and end dates for analysis
            analysis_options: Additional analysis options

        Returns:
            Comprehensive spatial weather analysis
        """
        try:
            # Get boundary coordinates
            boundary_coords = await self._get_boundary_coordinates(
                boundary_type, boundary_data
            )

            # Generate analysis grid points within boundary
            grid_points = await self._generate_analysis_grid(
                boundary_coords, boundary_type
            )

            # Fetch weather data for all grid points
            weather_data = await self._fetch_spatial_weather_data(
                grid_points, analysis_period, analysis_options
            )

            # Perform spatial analysis
            analysis_results = await self._perform_spatial_analysis(
                weather_data, grid_points, analysis_options
            )

            # Generate business impact assessment
            business_impact = await self._assess_business_impact(
                analysis_results, boundary_data, analysis_options
            )

            # Identify potential leads
            lead_opportunities = await self._identify_lead_opportunities(
                analysis_results, boundary_data, analysis_options
            )

            # Calculate center point of boundary for maps
            lats = [coord[0] for coord in boundary_coords]
            lons = [coord[1] for coord in boundary_coords]
            center_lat = sum(lats) / len(lats) if lats else 0
            center_lon = sum(lons) / len(lons) if lons else 0

            # Calculate data freshness information
            start_date_obj = datetime.fromisoformat(analysis_period["start"]).date()
            end_date_obj = datetime.fromisoformat(analysis_period["end"]).date()
            freshness_info = get_data_freshness_info(start_date_obj, end_date_obj)

            return {
                "boundary_info": {
                    "type": boundary_type,
                    "coordinates": boundary_coords,
                    "area_sq_km": self._calculate_area(boundary_coords),
                    "grid_points": len(grid_points),
                },
                "center_latitude": center_lat,
                "center_longitude": center_lon,
                "analysis_period": analysis_period,
                "data_freshness": {
                    "freshness_date": freshness_info["freshness_date"].isoformat(),
                    "freshness_date_formatted": freshness_info[
                        "freshness_date_formatted"
                    ],
                    "is_complete": freshness_info["is_complete"],
                    "coverage_percent": freshness_info["coverage_percent"],
                    "warning_message": freshness_info["warning_message"],
                    "disclaimer": format_data_disclaimer(freshness_info),
                },
                "weather_summary": analysis_results["summary"],
                "risk_assessment": analysis_results["risk_assessment"],
                "weather_events": analysis_results["weather_events"],
                "grid_data": analysis_results.get("grid_data", []),
                "business_impact": business_impact,
                "lead_opportunities": lead_opportunities,
                "route_optimization": await self._optimize_routes(
                    grid_points, analysis_options
                ),
                "generated_at": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error in spatial analysis: {e}")
            logger.error(f"Error type: {type(e)}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def _get_boundary_coordinates(
        self, boundary_type: str, boundary_data: Dict[str, Any]
    ) -> List[Tuple[float, float]]:
        """Get coordinates for the specified boundary."""

        if boundary_type == "county":
            # Support both 'name' and 'county_name' for flexibility
            county_name = boundary_data.get("county_name") or boundary_data.get("name")
            if not county_name:
                raise ValueError("County name is required for county boundary type")
            return await self._get_county_boundary(
                county_name, boundary_data.get("state_code")
            )
        elif boundary_type == "city":
            city_name = boundary_data.get("city_name") or boundary_data.get("name")
            if not city_name:
                raise ValueError("City name is required for city boundary type")
            return await self._get_city_boundary(
                city_name, boundary_data.get("state_code")
            )
        elif boundary_type == "neighborhood":
            neighborhood_name = boundary_data.get(
                "neighborhood_name"
            ) or boundary_data.get("name")
            if not neighborhood_name:
                raise ValueError(
                    "Neighborhood name is required for neighborhood boundary type"
                )
            return await self._get_neighborhood_boundary(
                neighborhood_name, boundary_data.get("city_name")
            )
        elif boundary_type == "radius":
            return await self._get_radius_boundary(
                boundary_data["center_lat"],
                boundary_data["center_lon"],
                boundary_data["radius_km"],
            )
        elif boundary_type == "polygon":
            return boundary_data["coordinates"]
        else:
            raise ValueError(f"Unsupported boundary type: {boundary_type}")

    async def _get_county_boundary(
        self, county_name: str, state_code: Optional[str] = None
    ) -> List[Tuple[float, float]]:
        """Get county boundary coordinates using OpenStreetMap Nominatim API."""
        try:
            async with httpx.AsyncClient() as client:
                # Search for county boundary
                # Don't add "county" if it's already in the name
                search_query = (
                    county_name
                    if "county" in county_name.lower()
                    else f"{county_name} county"
                )
                if state_code:
                    search_query += f", {state_code}"

                logger.info(f"Searching OSM for county: {search_query}")

                url = "https://nominatim.openstreetmap.org/search"
                params = {
                    "q": search_query,
                    "format": "json",
                    "polygon_geojson": "1",
                    "limit": 1,
                }

                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "User-Agent": "WeatherReportsService/1.0 (contact@example.com)"
                    },
                    timeout=10.0,
                )
                response.raise_for_status()

                data = response.json()
                logger.info(f"OSM response: {len(data)} results found")

                if data and len(data) > 0:
                    # Extract boundary coordinates from GeoJSON
                    geometry = data[0].get("geojson", {})
                    logger.info(f"Geometry type: {geometry.get('type')}")

                    if geometry.get("type") == "Polygon":
                        coordinates = geometry["coordinates"][0]  # First ring
                        logger.info(
                            f"Found polygon with {len(coordinates)} coordinates"
                        )
                        return [
                            (coord[1], coord[0]) for coord in coordinates
                        ]  # Convert to (lat, lon)
                    elif geometry.get("type") == "MultiPolygon":
                        # Use the first polygon from MultiPolygon
                        coordinates = geometry["coordinates"][0][0]
                        logger.info(
                            f"Found multipolygon, using first polygon with {len(coordinates)} coordinates"
                        )
                        return [(coord[1], coord[0]) for coord in coordinates]

                # Fallback: create approximate boundary using bounding box
                bbox = data[0].get("boundingbox", [])
                logger.info(f"Using bounding box fallback: {bbox}")
                if len(bbox) >= 4:
                    return self._create_bbox_polygon(bbox)

                raise Exception(f"Could not find boundary for county: {county_name}")

        except Exception as e:
            logger.error(f"Error getting county boundary: {e}")
            raise

    async def _get_city_boundary(
        self, city_name: str, state_code: Optional[str] = None
    ) -> List[Tuple[float, float]]:
        """Get city boundary coordinates."""
        try:
            async with httpx.AsyncClient() as client:
                search_query = f"{city_name}"
                if state_code:
                    search_query += f", {state_code}"

                url = "https://nominatim.openstreetmap.org/search"
                params = {
                    "q": search_query,
                    "format": "json",
                    "polygon_geojson": "1",
                    "limit": 1,
                }

                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "User-Agent": "WeatherReportsService/1.0 (contact@example.com)"
                    },
                    timeout=10.0,
                )
                response.raise_for_status()

                data = response.json()
                if data and len(data) > 0:
                    geometry = data[0].get("geojson", {})
                    if geometry.get("type") == "Polygon":
                        coordinates = geometry["coordinates"][0]
                        return [(coord[1], coord[0]) for coord in coordinates]

                # Fallback to bounding box
                bbox = data[0].get("boundingbox", [])
                if len(bbox) >= 4:
                    return self._create_bbox_polygon(bbox)

                raise Exception(f"Could not find boundary for city: {city_name}")

        except Exception as e:
            logger.error(f"Error getting city boundary: {e}")
            raise

    async def _get_neighborhood_boundary(
        self, neighborhood_name: str, city_name: Optional[str] = None
    ) -> List[Tuple[float, float]]:
        """Get neighborhood boundary coordinates."""
        try:
            async with httpx.AsyncClient() as client:
                search_query = f"{neighborhood_name}"
                if city_name:
                    search_query += f", {city_name}"

                url = "https://nominatim.openstreetmap.org/search"
                params = {
                    "q": search_query,
                    "format": "json",
                    "polygon_geojson": "1",
                    "limit": 1,
                }

                response = await client.get(
                    url,
                    params=params,
                    headers={
                        "User-Agent": "WeatherReportsService/1.0 (contact@example.com)"
                    },
                    timeout=10.0,
                )
                response.raise_for_status()

                data = response.json()
                if data and len(data) > 0:
                    geometry = data[0].get("geojson", {})
                    if geometry.get("type") == "Polygon":
                        coordinates = geometry["coordinates"][0]
                        return [(coord[1], coord[0]) for coord in coordinates]

                # Fallback to bounding box
                bbox = data[0].get("boundingbox", [])
                if len(bbox) >= 4:
                    return self._create_bbox_polygon(bbox)

                raise Exception(
                    f"Could not find boundary for neighborhood: {neighborhood_name}"
                )

        except Exception as e:
            logger.error(f"Error getting neighborhood boundary: {e}")
            raise

    async def _get_radius_boundary(
        self, center_lat: float, center_lon: float, radius_km: float
    ) -> List[Tuple[float, float]]:
        """Create a circular boundary around a center point."""
        # Ensure coordinates are floats
        center_lat = float(center_lat)
        center_lon = float(center_lon)
        radius_km = float(radius_km)

        # Create a polygon approximation of a circle
        points = []
        num_points = 32  # Number of points to approximate circle

        for i in range(num_points):
            angle = 2 * math.pi * i / num_points
            # Convert km to degrees (approximate)
            lat_offset = (radius_km / 111.0) * math.cos(angle)
            lon_offset = (
                radius_km / (111.0 * math.cos(math.radians(center_lat)))
            ) * math.sin(angle)

            points.append((center_lat + lat_offset, center_lon + lon_offset))

        return points

    def _create_bbox_polygon(self, bbox: List[str]) -> List[Tuple[float, float]]:
        """Create a polygon from bounding box coordinates."""
        min_lat, max_lat, min_lon, max_lon = map(float, bbox)
        return [
            (min_lat, min_lon),
            (max_lat, min_lon),
            (max_lat, max_lon),
            (min_lat, max_lon),
            (min_lat, min_lon),  # Close the polygon
        ]

    async def _generate_analysis_grid(
        self, boundary_coords: List[Tuple[float, float]], boundary_type: str
    ) -> List[Tuple[float, float]]:
        """Generate analysis grid points within the boundary."""
        # Calculate bounding box
        lats = [coord[0] for coord in boundary_coords]
        lons = [coord[1] for coord in boundary_coords]

        min_lat, max_lat = min(lats), max(lats)
        min_lon, max_lon = min(lons), max(lons)

        # Determine grid density based on boundary type
        if boundary_type in ["county"]:
            grid_spacing = 0.025  # ~2.5km spacing (denser for better coverage)
        elif boundary_type in ["city"]:
            grid_spacing = 0.01  # ~1km spacing (increased from 0.02)
        elif boundary_type in ["neighborhood"]:
            grid_spacing = 0.005  # ~0.5km spacing
        else:
            grid_spacing = 0.015  # ~1.5km spacing

        # Generate grid points
        grid_points = []
        lat = min_lat
        while lat <= max_lat:
            lon = min_lon
            while lon <= max_lon:
                # Check if point is within boundary (simplified point-in-polygon)
                if self._point_in_polygon((lat, lon), boundary_coords):
                    grid_points.append((lat, lon))
                lon += grid_spacing
            lat += grid_spacing

        return grid_points

    def _point_in_polygon(
        self, point: Tuple[float, float], polygon: List[Tuple[float, float]]
    ) -> bool:
        """Check if a point is inside a polygon using ray casting algorithm."""
        x, y = point
        n = len(polygon)
        inside = False

        p1x, p1y = polygon[0]
        for i in range(1, n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y

        return inside

    async def _fetch_spatial_weather_data(
        self,
        grid_points: List[Tuple[float, float]],
        analysis_period: Dict[str, str],
        analysis_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Fetch weather data for all grid points."""
        try:
            start_date = datetime.fromisoformat(analysis_period["start"])
            end_date = datetime.fromisoformat(analysis_period["end"])

            # IMPORTANT: Always fetch 24 months of severe weather events for spatial reports
            # This provides comprehensive storm history regardless of selected analysis period
            severe_weather_start = end_date - timedelta(
                days=730
            )  # 24 months = ~730 days

            logger.info(f"Fetching weather data for spatial report:")
            logger.info(
                f"  - Analysis period: {start_date.date()} to {end_date.date()}"
            )
            logger.info(
                f"  - Severe weather events period: {severe_weather_start.date()} to {end_date.date()} (24 months)"
            )

            # Fetch weather data for each grid point
            weather_data = []
            for lat, lon in grid_points:
                try:
                    # Get current weather
                    current = await self.weather_service.get_current_weather(lat, lon)

                    # Get historical weather (uses selected analysis period)
                    historical = await self.weather_service.get_historical_weather(
                        lat, lon, start_date.date(), end_date.date()
                    )

                    # Get weather events (ALWAYS 24 months for comprehensive severe weather history)
                    events = await self.weather_service.get_weather_events(
                        lat,
                        lon,
                        severe_weather_start.date(),
                        end_date.date(),
                        radius_km=50.0,
                    )

                    weather_data.append(
                        {
                            "coordinates": (lat, lon),
                            "current_weather": current,
                            "historical_weather": historical,
                            "weather_events": events,
                        }
                    )

                except Exception as e:
                    logger.warning(
                        f"Failed to fetch weather data for {lat}, {lon}: {e}"
                    )
                    continue

            return {"grid_data": weather_data}

        except Exception as e:
            logger.error(f"Error fetching spatial weather data: {e}")
            raise

    async def _perform_spatial_analysis(
        self,
        weather_data: Dict[str, Any],
        grid_points: List[Tuple[float, float]],
        analysis_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Perform comprehensive spatial weather analysis."""
        try:
            grid_data = weather_data["grid_data"]

            # Calculate summary statistics
            summary = self._calculate_spatial_summary(grid_data)

            # Assess risk levels
            risk_assessment = self._assess_spatial_risk(grid_data, analysis_options)

            # Analyze weather events
            weather_events = self._analyze_spatial_events(grid_data)

            return {
                "summary": summary,
                "risk_assessment": risk_assessment,
                "weather_events": weather_events,
                "grid_data": grid_data,
            }

        except Exception as e:
            logger.error(f"Error in spatial analysis: {e}")
            logger.error(f"Error type: {type(e)}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _calculate_spatial_summary(
        self, grid_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate summary statistics for the spatial area."""
        if not grid_data:
            return {}

        temperatures = []
        wind_speeds = []
        precipitation = []

        for point_data in grid_data:
            current = point_data.get("current_weather", {})
            if current.get("temperature"):
                try:
                    temperatures.append(float(current["temperature"]))
                except (ValueError, TypeError):
                    pass
            if current.get("wind_speed"):
                try:
                    wind_speeds.append(float(current["wind_speed"]))
                except (ValueError, TypeError):
                    pass
            if current.get("precipitation"):
                try:
                    precipitation.append(float(current["precipitation"]))
                except (ValueError, TypeError):
                    pass

        return {
            "total_points_analyzed": len(grid_data),
            "temperature_stats": {
                "average": sum(temperatures) / len(temperatures)
                if temperatures
                else None,
                "min": min(temperatures) if temperatures else None,
                "max": max(temperatures) if temperatures else None,
            },
            "wind_stats": {
                "average": sum(wind_speeds) / len(wind_speeds) if wind_speeds else None,
                "max": max(wind_speeds) if wind_speeds else None,
            },
            "precipitation_stats": {
                "total": sum(precipitation) if precipitation else 0,
                "average": sum(precipitation) / len(precipitation)
                if precipitation
                else 0,
            },
        }

    def _assess_spatial_risk(
        self, grid_data: List[Dict[str, Any]], analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Assess weather-related risks across the spatial area."""
        risk_factors = analysis_options.get("risk_factors", ["hail", "wind", "tornado"])

        risk_scores = []
        high_risk_areas = []
        medium_risk_areas = []
        low_risk_areas = []

        for point_data in grid_data:
            risk_score = 0
            risk_factors_present = []

            # Analyze weather events for risk factors
            events = point_data.get("weather_events", [])
            for event in events:
                event_type = str(event.get("event", "")).lower()
                severity = str(event.get("severity", "")).lower()

                if "hail" in event_type and "hail" in risk_factors:
                    risk_score += 0.3
                    risk_factors_present.append("hail")
                if "wind" in event_type and "wind" in risk_factors:
                    risk_score += 0.2
                    risk_factors_present.append("wind")
                if "tornado" in event_type and "tornado" in risk_factors:
                    risk_score += 0.5
                    risk_factors_present.append("tornado")

            # Categorize risk level
            if risk_score >= 0.7:
                high_risk_areas.append(
                    {
                        "coordinates": point_data["coordinates"],
                        "risk_score": risk_score,
                        "risk_factors": risk_factors_present,
                    }
                )
            elif risk_score >= 0.4:
                medium_risk_areas.append(
                    {
                        "coordinates": point_data["coordinates"],
                        "risk_score": risk_score,
                        "risk_factors": risk_factors_present,
                    }
                )
            else:
                low_risk_areas.append(
                    {
                        "coordinates": point_data["coordinates"],
                        "risk_score": risk_score,
                        "risk_factors": risk_factors_present,
                    }
                )

            risk_scores.append(risk_score)

        overall_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0

        return {
            "overall_risk_score": overall_risk,
            "risk_level": "high"
            if overall_risk >= 0.7
            else "medium"
            if overall_risk >= 0.4
            else "low",
            "high_risk_areas": {
                "count": len(high_risk_areas),
                "areas": high_risk_areas,
            },
            "medium_risk_areas": {
                "count": len(medium_risk_areas),
                "areas": medium_risk_areas,
            },
            "low_risk_areas": {"count": len(low_risk_areas), "areas": low_risk_areas},
        }

    def _analyze_spatial_events(
        self, grid_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze weather events across the spatial area."""
        all_events = []
        event_types = {}
        event_severities = {}

        for point_data in grid_data:
            events = point_data.get("weather_events", [])
            all_events.extend(events)

            for event in events:
                event_type = str(event.get("event", "unknown"))
                severity = str(event.get("severity", "unknown"))

                event_types[event_type] = event_types.get(event_type, 0) + 1
                event_severities[severity] = event_severities.get(severity, 0) + 1

        return {
            "total_events": len(all_events),
            "events_by_type": event_types,
            "events_by_severity": event_severities,
            "unique_events": list(set(event.get("event", "") for event in all_events)),
            "raw_events": all_events,  # Add raw events list for chart generation
        }

    async def _assess_business_impact(
        self,
        analysis_results: Dict[str, Any],
        boundary_data: Dict[str, Any],
        analysis_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Assess business impact of weather within the spatial area."""
        try:
            risk_assessment = analysis_results["risk_assessment"]
            weather_events = analysis_results["weather_events"]

            # Calculate potential business impact
            impact_score = 0
            impact_factors = []

            # High risk areas indicate potential property damage
            if risk_assessment["high_risk_areas"]["count"] > 0:
                impact_score += 0.4
                impact_factors.append("High-risk areas identified")

            # Severe weather events indicate operational disruption
            severe_events = int(weather_events["events_by_severity"].get("Severe", 0))
            if severe_events > 0:
                impact_score += 0.3
                impact_factors.append(f"{severe_events} severe weather events")

            # Multiple event types indicate complex weather patterns
            if len(weather_events["unique_events"]) > 3:
                impact_score += 0.2
                impact_factors.append("Multiple weather event types")

            # Wind events indicate potential service disruption
            wind_events = int(weather_events["events_by_type"].get("Wind", 0))
            if wind_events > 0:
                impact_score += 0.1
                impact_factors.append("Wind events detected")

            return {
                "impact_score": min(impact_score, 1.0),
                "impact_level": "high"
                if impact_score >= 0.7
                else "medium"
                if impact_score >= 0.4
                else "low",
                "impact_factors": impact_factors,
                "recommendations": self._generate_business_recommendations(
                    impact_score, impact_factors
                ),
            }

        except Exception as e:
            logger.error(f"Error assessing business impact: {e}")
            return {
                "impact_score": 0,
                "impact_level": "low",
                "impact_factors": [],
                "recommendations": [],
            }

    def _generate_business_recommendations(
        self, impact_score: float, impact_factors: List[str]
    ) -> List[str]:
        """Generate business recommendations based on impact assessment."""
        recommendations = []

        if impact_score >= 0.7:
            recommendations.extend(
                [
                    "High weather risk detected - consider emergency preparedness protocols",
                    "Schedule immediate property inspections in high-risk areas",
                    "Prepare for potential service disruptions",
                    "Notify customers of potential weather-related delays",
                ]
            )
        elif impact_score >= 0.4:
            recommendations.extend(
                [
                    "Moderate weather risk - monitor conditions closely",
                    "Consider proactive maintenance in affected areas",
                    "Prepare contingency plans for service delivery",
                ]
            )
        else:
            recommendations.extend(
                [
                    "Low weather risk - normal operations can continue",
                    "Monitor weather conditions for any changes",
                ]
            )

        return recommendations

    async def _identify_lead_opportunities(
        self,
        analysis_results: Dict[str, Any],
        boundary_data: Dict[str, Any],
        analysis_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Identify potential lead opportunities based on weather analysis."""
        try:
            risk_assessment = analysis_results["risk_assessment"]
            weather_events = analysis_results["weather_events"]

            lead_opportunities = []
            lead_score = 0

            # High-risk areas are potential leads for property services
            high_risk_areas = risk_assessment["high_risk_areas"]["areas"]
            for area in high_risk_areas:
                if "hail" in area["risk_factors"]:
                    lead_opportunities.append(
                        {
                            "type": "property_damage_assessment",
                            "coordinates": area["coordinates"],
                            "reason": "Hail damage risk detected",
                            "priority": "high",
                        }
                    )
                    lead_score += 0.3

                if "wind" in area["risk_factors"]:
                    lead_opportunities.append(
                        {
                            "type": "wind_damage_inspection",
                            "coordinates": area["coordinates"],
                            "reason": "High wind risk detected",
                            "priority": "medium",
                        }
                    )
                    lead_score += 0.2

            # Severe weather events indicate immediate service needs
            severe_events = weather_events["events_by_severity"].get("Severe", 0)
            if severe_events > 0:
                lead_opportunities.append(
                    {
                        "type": "emergency_response",
                        "reason": f"{severe_events} severe weather events detected",
                        "priority": "high",
                    }
                )
                lead_score += 0.4

            return {
                "lead_score": min(lead_score, 1.0),
                "total_opportunities": len(lead_opportunities),
                "opportunities": lead_opportunities,
                "lead_level": "high"
                if lead_score >= 0.7
                else "medium"
                if lead_score >= 0.4
                else "low",
            }

        except Exception as e:
            logger.error(f"Error identifying lead opportunities: {e}")
            return {
                "lead_score": 0,
                "total_opportunities": 0,
                "opportunities": [],
                "lead_level": "low",
            }

    async def _optimize_routes(
        self, grid_points: List[Tuple[float, float]], analysis_options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Optimize routes for field teams within the spatial area."""
        try:
            # Simple route optimization - cluster nearby points
            clusters = self._cluster_points(grid_points, max_distance_km=5.0)

            optimized_routes = []
            for i, cluster in enumerate(clusters):
                if len(cluster) > 1:
                    # Calculate centroid of cluster
                    center_lat = sum(point[0] for point in cluster) / len(cluster)
                    center_lon = sum(point[1] for point in cluster) / len(cluster)

                    optimized_routes.append(
                        {
                            "route_id": f"route_{i+1}",
                            "center_coordinates": (center_lat, center_lon),
                            "points_count": len(cluster),
                            "estimated_distance_km": self._estimate_route_distance(
                                cluster
                            ),
                            "efficiency_score": len(cluster)
                            / max(1, self._estimate_route_distance(cluster)),
                        }
                    )

            return {
                "total_routes": len(optimized_routes),
                "routes": optimized_routes,
                "total_points": len(grid_points),
                "optimization_score": sum(
                    route["efficiency_score"] for route in optimized_routes
                )
                / max(1, len(optimized_routes)),
            }

        except Exception as e:
            logger.error(f"Error optimizing routes: {e}")
            return {
                "total_routes": 0,
                "routes": [],
                "total_points": len(grid_points),
                "optimization_score": 0,
            }

    def _cluster_points(
        self, points: List[Tuple[float, float]], max_distance_km: float
    ) -> List[List[Tuple[float, float]]]:
        """Cluster nearby points together."""
        clusters = []
        used_points = set()

        for i, point in enumerate(points):
            if i in used_points:
                continue

            cluster = [point]
            used_points.add(i)

            for j, other_point in enumerate(points):
                if j in used_points:
                    continue

                distance = self._calculate_distance(point, other_point)
                if distance <= max_distance_km:
                    cluster.append(other_point)
                    used_points.add(j)

            clusters.append(cluster)

        return clusters

    def _calculate_distance(
        self, point1: Tuple[float, float], point2: Tuple[float, float]
    ) -> float:
        """Calculate distance between two points in kilometers."""
        lat1, lon1 = point1
        lat2, lon2 = point2

        # Haversine formula
        R = 6371  # Earth's radius in kilometers

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(
            math.radians(lat1)
        ) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2)

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def _estimate_route_distance(self, points: List[Tuple[float, float]]) -> float:
        """Estimate total distance for a route through points."""
        if len(points) <= 1:
            return 0

        total_distance = 0
        for i in range(len(points) - 1):
            total_distance += self._calculate_distance(points[i], points[i + 1])

        return total_distance

    def _calculate_area(self, coordinates: List[Tuple[float, float]]) -> float:
        """Calculate area of polygon in square kilometers."""
        if len(coordinates) < 3:
            return 0

        # Shoelace formula for polygon area
        area = 0
        n = len(coordinates)

        for i in range(n):
            j = (i + 1) % n
            area += coordinates[i][0] * coordinates[j][1]
            area -= coordinates[j][0] * coordinates[i][1]

        area = abs(area) / 2

        # Convert from square degrees to square kilometers (approximate)
        return area * 12364  # Rough conversion factor


# Singleton instance
_spatial_analysis_service: Optional[SpatialAnalysisService] = None


def get_spatial_analysis_service() -> SpatialAnalysisService:
    """Get spatial analysis service instance."""
    global _spatial_analysis_service
    if _spatial_analysis_service is None:
        _spatial_analysis_service = SpatialAnalysisService()
    return _spatial_analysis_service
