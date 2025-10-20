"""
Template engine for rendering HTML templates with Jinja2
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.core.config import settings
from app.services.heatmap_service import get_heatmap_service

logger = logging.getLogger(__name__)

class TemplateEngine:
    """Service for rendering HTML templates"""
    
    def __init__(self):
        self.template_dir = Path(__file__).parent.parent.parent / "templates"
        self.template_dir.mkdir(exist_ok=True)
        
        # Initialize Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=select_autoescape(['html', 'xml'])
        )
        
        # Add custom filters
        self.env.filters['format_temp'] = self._format_temperature
        self.env.filters['format_wind'] = self._format_wind_speed
        self.env.filters['format_date'] = self._format_date
    
    async def render_weather_template(
        self,
        template_name: str,
        weather_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Render address weather report template"""

        try:
            # Map template names to actual template files
            template_mapping = {
                "professional": "address_report",
                "address_report": "address_report",
                "default": "address_report"
            }
            
            actual_template = template_mapping.get(template_name, "address_report")
            template_path = f"address/{actual_template}.html"
            template = self.env.get_template(template_path)
            
            # Prepare template context
            context = {
                'weather_data': weather_data,
                'options': options or {},
                'generated_at': self._get_current_time(),
                'location': weather_data.get('location', 'Unknown Location'),
                'current': weather_data.get('current', {}),
                'forecast': weather_data.get('forecast', {}),
                'historical': weather_data.get('historical', {}),
                'risk_assessment': weather_data.get('riskAssessment', {})
            }
            
            return template.render(context)
            
        except Exception as e:
            logger.error(f"Failed to render weather template {template_name}: {e}")
            # Fallback to default template
            return await self._render_default_weather_template(weather_data, options)
    
    async def render_spatial_template(
        self,
        template_name: str,
        spatial_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Render spatial report template with maps"""

        try:
            from app.services.map_generation import MapGenerationService
            import base64
            from pathlib import Path

            # Map template names to actual template files
            template_mapping = {
                "professional": "spatial_report",
                "spatial_report": "spatial_report",
                "default": "spatial_report"
            }
            
            actual_template = template_mapping.get(template_name, "spatial_report")
            template_path = f"spatial/{actual_template}.html"
            template = self.env.get_template(template_path)

            # Extract boundary information
            boundary_info = spatial_data.get('boundary_info', {})
            coordinates = boundary_info.get('coordinates', [])

            # Calculate center point from boundary coordinates
            if coordinates and len(coordinates) > 0:
                lats = [coord[0] for coord in coordinates]
                lngs = [coord[1] for coord in coordinates]
                center_lat = sum(lats) / len(lats)
                center_lng = sum(lngs) / len(lngs)
            else:
                # Fallback center point
                center_lat, center_lng = 39.8283, -98.5795  # Geographic center of US

            # Get boundary name from options or data
            boundary_name = options.get('boundary_name') if options else None
            if not boundary_name:
                boundary_name = spatial_data.get('boundary_info', {}).get('name', 'Analysis Area')

            # Generate maps
            map_service = MapGenerationService()
            satellite_map = await map_service.generate_satellite_map(
                center_lat, center_lng, zoom_level=12, map_size=(600, 400), location_name=boundary_name
            )

            # Generate heat maps
            heatmap_service = get_heatmap_service()

            # Risk heat map
            risk_heatmap = heatmap_service.generate_risk_heatmap(
                coordinates,
                spatial_data.get('risk_assessment', {}),
                width=600,
                height=400
            )

            # Event density heat map
            event_heatmap = heatmap_service.generate_event_density_heatmap(
                coordinates,
                spatial_data.get('grid_data', []),
                width=600,
                height=400
            )

            # Convert maps to base64
            satellite_map_base64 = base64.b64encode(satellite_map).decode('utf-8')
            risk_heatmap_base64 = base64.b64encode(risk_heatmap).decode('utf-8')
            event_heatmap_base64 = base64.b64encode(event_heatmap).decode('utf-8')

            # Load and encode logo
            logo_path = Path(__file__).parent.parent.parent / "templates" / "apexos-icon.png"
            logo_base64 = ""
            if logo_path.exists():
                with open(logo_path, 'rb') as f:
                    logo_base64 = base64.b64encode(f.read()).decode('utf-8')

            # Add boundary name to boundary_info
            boundary_info_with_name = {**boundary_info, 'name': boundary_name}

            # Map weather_summary from spatial analysis summary + weather_events
            weather_events_data = spatial_data.get('weather_events', {})
            summary_data = spatial_data.get('weather_summary', {})

            weather_summary = {
                'temperature_stats': summary_data.get('temperature_stats', {}),
                'wind_stats': summary_data.get('wind_stats', {}),
                'precipitation_stats': summary_data.get('precipitation_stats', {}),
                'total_points_analyzed': summary_data.get('total_points_analyzed', 0),
                'total_events': weather_events_data.get('total_events', 0),
                'events_by_type': weather_events_data.get('events_by_type', {}),
                'events_by_severity': weather_events_data.get('events_by_severity', {}),
                'unique_events': weather_events_data.get('unique_events', [])
            }

            # Prepare template context
            context = {
                'boundary_info': boundary_info_with_name,
                'analysis_period': spatial_data.get('analysis_period', {}),
                'weather_summary': weather_summary,
                'risk_assessment': spatial_data.get('risk_assessment', {}),
                'weather_events': spatial_data.get('weather_events', {}),
                'business_impact': spatial_data.get('business_impact', {}),
                'lead_opportunities': spatial_data.get('lead_opportunities', {}),
                'route_optimization': spatial_data.get('route_optimization', {}),
                'charts': spatial_data.get('charts', {}),  # Add charts for visualizations
                'satellite_map_base64': satellite_map_base64,
                'risk_heatmap_base64': risk_heatmap_base64,
                'event_heatmap_base64': event_heatmap_base64,
                'logo_base64': logo_base64,
                'generated_at': self._get_current_time(),
                'report_id': options.get('report_id') if options else None,
                'options': options or {}
            }

            return template.render(context)

        except Exception as e:
            logger.error(f"Failed to render spatial template: {e}")
            # Fallback to default template
            return await self._render_default_spatial_template(spatial_data, options)

    async def render_address_template(
        self,
        template_name: str,
        address_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Render address report template with maps"""

        try:
            from app.services.map_generation import MapGenerationService
            import base64
            from pathlib import Path

            # Use address_report template
            template_path = "address/address_report.html"
            template = self.env.get_template(template_path)

            # Extract coordinates
            property_info = address_data.get('property_info', {})
            coordinates = property_info.get('coordinates', (0, 0))
            lat, lng = coordinates
            address = property_info.get('geocoded_address', 'Unknown Location')

            # Generate maps
            map_service = MapGenerationService()
            satellite_map = await map_service.generate_satellite_map(
                lat, lng, zoom_level=18, map_size=(600, 400), location_name=address
            )
            street_map = await map_service.generate_street_map(
                lat, lng, zoom_level=15, map_size=(600, 400), location_name=address
            )

            # Convert maps to base64
            satellite_map_base64 = base64.b64encode(satellite_map).decode('utf-8')
            street_map_base64 = base64.b64encode(street_map).decode('utf-8')

            # Load and encode logo
            logo_path = Path(__file__).parent.parent.parent / "templates" / "apexos-icon.png"
            logo_base64 = ""
            if logo_path.exists():
                with open(logo_path, 'rb') as f:
                    logo_base64 = base64.b64encode(f.read()).decode('utf-8')

            # Prepare template context
            context = {
                'property_info': property_info,
                'analysis_period': address_data.get('analysis_period', {}),
                'weather_summary': address_data.get('weather_summary', {}),
                'risk_assessment': address_data.get('risk_assessment', {}),
                'historical_context': address_data.get('historical_context', {}),
                'business_impact': address_data.get('business_impact', {}),
                'lead_qualification': address_data.get('lead_qualification', {}),
                'location_alerts': address_data.get('location_alerts', []),
                'satellite_map_base64': satellite_map_base64,
                'street_map_base64': street_map_base64,
                'logo_base64': logo_base64,
                'generated_at': self._get_current_time(),
                'report_id': options.get('report_id') if options else None,
                'options': options or {}
            }

            return template.render(context)

        except Exception as e:
            logger.error(f"Failed to render address template: {e}", exc_info=True)
            import traceback
            logger.error(f"Template error traceback: {traceback.format_exc()}")
            # Fallback to default template
            logger.warning("⚠️  Using fallback default address template due to error")
            return await self._render_default_address_template(address_data, options)
    
    async def _render_default_weather_template(
        self,
        weather_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback weather template"""
        
        location = weather_data.get('location', 'Unknown Location')
        current = weather_data.get('current', {})
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Weather Report - {location}</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                .summary {{ background: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .current {{ margin: 20px 0; }}
                .forecast {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <h1>Weather Report</h1>
            <h2>{location}</h2>
            
            <div class="summary">
                <h3>Current Conditions</h3>
                <p><strong>Temperature:</strong> {current.get('temperature', 'N/A')}°F</p>
                <p><strong>Condition:</strong> {current.get('condition', 'N/A')}</p>
                <p><strong>Wind:</strong> {current.get('windSpeed', 'N/A')} {current.get('windDirection', '')}</p>
            </div>
            
            <div class="forecast">
                <h3>Forecast</h3>
                <p>Detailed forecast data will be included in the full report.</p>
            </div>
            
            <p><em>Report generated on {self._get_current_time()}</em></p>
        </body>
        </html>
        """
    
    async def _render_default_spatial_template(
        self,
        spatial_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback spatial template"""

        boundary = spatial_data.get('boundary', {})
        boundary_name = boundary.get('name', 'Unknown Area')

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Spatial Weather Report - {boundary_name}</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                .summary {{ background: #f8f9fa; padding: 20px; border-radius: 5px; }}
                .boundary {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <h1>Spatial Weather Report</h1>
            <h2>{boundary_name}</h2>

            <div class="boundary">
                <h3>Area Information</h3>
                <p><strong>Boundary Type:</strong> {boundary.get('type', 'N/A')}</p>
                <p><strong>Area:</strong> {boundary.get('area', 'N/A')}</p>
            </div>

            <div class="summary">
                <h3>Weather Summary</h3>
                <p>Detailed spatial weather analysis will be included in the full report.</p>
            </div>

            <p><em>Report generated on {self._get_current_time()}</em></p>
        </body>
        </html>
        """

    async def _render_default_address_template(
        self,
        address_data: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback address template"""

        property_info = address_data.get('property_info', {})
        address = property_info.get('geocoded_address', 'Unknown Location')

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Property Weather Report - {address}</title>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                .summary {{ background: #f8f9fa; padding: 20px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <h1>Property Weather Report</h1>
            <h2>{address}</h2>

            <div class="summary">
                <h3>Property Information</h3>
                <p><strong>Address:</strong> {address}</p>
                <p><strong>Coordinates:</strong> {property_info.get('coordinates', 'N/A')}</p>
            </div>

            <div class="summary">
                <h3>Weather Analysis</h3>
                <p>Detailed weather analysis will be included in the full report.</p>
            </div>

            <p><em>Report generated on {self._get_current_time()}</em></p>
        </body>
        </html>
        """
    
    def _format_temperature(self, temp: Any, unit: str = "F") -> str:
        """Format temperature value"""
        if temp is None:
            return "N/A"
        return f"{temp}°{unit}"
    
    def _format_wind_speed(self, speed: Any) -> str:
        """Format wind speed"""
        if speed is None:
            return "N/A"
        return str(speed)
    
    def _format_date(self, date_str: Any) -> str:
        """Format date string"""
        if date_str is None:
            return "N/A"
        return str(date_str)
    
    def _get_current_time(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%B %d, %Y at %I:%M %p")