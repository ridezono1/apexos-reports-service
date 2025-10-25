"""
Map generation service for weather reports using Selenium and Folium.
"""

import io
import os
import tempfile
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MapGenerationService:
    """Service for generating maps for weather reports"""
    
    def __init__(self):
        self.temp_dir = tempfile.gettempdir()
    
    async def generate_satellite_map(
        self,
        center_lat: float,
        center_lng: float,
        zoom_level: int = 18,
        map_size: Tuple[int, int] = (600, 400),
        location_name: str = "Location"
    ) -> bytes:
        """Generate satellite map image using Google Static Maps API"""
        try:
            from PIL import Image, ImageDraw
            import requests
            from io import BytesIO

            # Use Google Static Maps API for satellite imagery
            # Format: https://maps.googleapis.com/maps/api/staticmap?parameters
            width, height = map_size

            # Build Google Static Maps URL
            base_url = "https://maps.googleapis.com/maps/api/staticmap"
            params = {
                "center": f"{center_lat},{center_lng}",
                "zoom": zoom_level,
                "size": f"{width}x{height}",
                "maptype": "satellite",
                "markers": f"color:red|{center_lat},{center_lng}",
                "scale": 2  # High resolution
            }

            # Add API key if available
            from app.core.config import settings
            api_key = settings.google_maps_api_key
            if api_key:
                params["key"] = api_key

                # Make request to Google Maps
                response = requests.get(base_url, params=params, timeout=10)

                if response.status_code == 200:
                    return response.content
                else:
                    logger.warning(f"Google Maps API returned status {response.status_code}")
            else:
                logger.warning("GOOGLE_MAPS_API_KEY not set, using placeholder")

            # Fallback to placeholder image
            placeholder_image = self._create_map_placeholder(
                map_size,
                "Satellite View",
                location_name,
                f"{center_lat:.4f}, {center_lng:.4f}",
                "#1a1a1a"
            )

            return placeholder_image

        except Exception as e:
            logger.error(f"Error generating satellite map: {e}")
            return self._create_map_placeholder(map_size, "Satellite View", location_name, "", "#1a1a1a")
    
    async def generate_street_map(
        self,
        center_lat: float,
        center_lng: float,
        zoom_level: int = 15,
        map_size: Tuple[int, int] = (600, 400),
        location_name: str = "Location"
    ) -> bytes:
        """Generate street map image using Google Static Maps API"""
        try:
            from PIL import Image, ImageDraw
            import requests
            from io import BytesIO

            # Use Google Static Maps API for street map
            width, height = map_size

            # Build Google Static Maps URL
            base_url = "https://maps.googleapis.com/maps/api/staticmap"
            params = {
                "center": f"{center_lat},{center_lng}",
                "zoom": zoom_level,
                "size": f"{width}x{height}",
                "maptype": "roadmap",
                "markers": f"color:blue|label:P|{center_lat},{center_lng}",
                "scale": 2  # High resolution
            }

            # Add API key if available
            from app.core.config import settings
            api_key = settings.google_maps_api_key
            if api_key:
                params["key"] = api_key

                # Make request to Google Maps
                response = requests.get(base_url, params=params, timeout=10)

                if response.status_code == 200:
                    return response.content
                else:
                    logger.warning(f"Google Maps API returned status {response.status_code}")
            else:
                logger.warning("GOOGLE_MAPS_API_KEY not set, using placeholder")

            # Fallback to placeholder image
            placeholder_image = self._create_map_placeholder(
                map_size,
                "Street Map",
                location_name,
                f"{center_lat:.4f}, {center_lng:.4f}",
                "#4285F4"
            )

            return placeholder_image

        except Exception as e:
            logger.error(f"Error generating street map: {e}")
            return self._create_map_placeholder(map_size, "Street Map", location_name, "", "#4285F4")
    
    async def generate_weather_events_map(
        self,
        weather_events: List[Dict[str, Any]],
        center_lat: float,
        center_lng: float,
        zoom_level: int = 13,
        map_size: Tuple[int, int] = (800, 600),
        include_impact_areas: bool = True
    ) -> bytes:
        """Generate weather events map with markers"""
        try:
            import folium
            
            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lng],
                zoom_start=zoom_level,
                tiles='OpenStreetMap'
            )
            
            # Add center marker
            folium.Marker(
                [center_lat, center_lng],
                popup="Report Location",
                icon=folium.Icon(color='red', icon='star')
            ).add_to(m)
            
            # Add weather event markers
            for event in weather_events:
                lat = event.get('latitude', center_lat)
                lng = event.get('longitude', center_lng)
                event_type = event.get('event_type', 'unknown')
                severity = event.get('severity', 'unknown')
                
                # Color based on severity
                if severity == 'severe':
                    color = 'red'
                elif severity == 'moderate':
                    color = 'orange'
                else:
                    color = 'yellow'
                
                folium.CircleMarker(
                    [lat, lng],
                    radius=8,
                    popup=f"{event_type.title()}: {event.get('description', 'N/A')}",
                    color='black',
                    weight=1,
                    fillColor=color,
                    fillOpacity=0.7
                ).add_to(m)
            
            # Save map to HTML
            with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as tmp_file:
                m.save(tmp_file.name)
                html_file = tmp_file.name
            
            # For now, return a placeholder image
            placeholder_image = self._create_placeholder_image(
                map_size, 
                f"Weather Events Map\n{len(weather_events)} events\n{center_lat:.4f}, {center_lng:.4f}"
            )
            
            # Clean up
            try:
                os.unlink(html_file)
            except:
                pass
            
            return placeholder_image
            
        except Exception as e:
            logger.error(f"Error generating weather events map: {e}")
            return self._create_placeholder_image(map_size, "Weather events map unavailable")
    
    async def generate_risk_assessment_map(
        self,
        center_lat: float,
        center_lng: float,
        zoom_level: int = 13,
        map_size: Tuple[int, int] = (600, 450),
        location_name: str = "Analysis Area",
        risk_data: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Generate risk assessment map with zones"""
        try:
            import folium
            
            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lng],
                zoom_start=zoom_level,
                tiles='OpenStreetMap'
            )
            
            # Add center marker
            folium.Marker(
                [center_lat, center_lng],
                popup=f"Analysis Center: {location_name}",
                icon=folium.Icon(color='red', icon='star')
            ).add_to(m)
            
            # Add risk zones if data available
            if risk_data:
                high_risk_areas = risk_data.get('high_risk_areas', [])
                for area in high_risk_areas:
                    lat = area.get('latitude', center_lat)
                    lng = area.get('longitude', center_lng)
                    folium.CircleMarker(
                        [lat, lng],
                        radius=15,
                        popup=f"High Risk: {area.get('name', 'Area')}",
                        color='red',
                        weight=2,
                        fillColor='red',
                        fillOpacity=0.3
                    ).add_to(m)
            
            # Save map to HTML
            with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as tmp_file:
                m.save(tmp_file.name)
                html_file = tmp_file.name
            
            # For now, return a placeholder image
            placeholder_image = self._create_placeholder_image(
                map_size, 
                f"Risk Assessment Map\n{location_name}\n{center_lat:.4f}, {center_lng:.4f}"
            )
            
            # Clean up
            try:
                os.unlink(html_file)
            except:
                pass
            
            return placeholder_image
            
        except Exception as e:
            logger.error(f"Error generating risk assessment map: {e}")
            return self._create_placeholder_image(map_size, "Risk assessment map unavailable")
    
    async def generate_spatial_heat_map(
        self,
        parameter: str,
        heat_map_data: Dict[str, Any],
        center_lat: float,
        center_lng: float,
        map_size: Tuple[int, int] = (800, 600),
        zoom_level: int = 14
    ) -> bytes:
        """Generate spatial heat map"""
        try:
            import folium
            
            # Create folium map
            m = folium.Map(
                location=[center_lat, center_lng],
                zoom_start=zoom_level,
                tiles='OpenStreetMap'
            )
            
            # Add data points
            data_points = heat_map_data.get('data_points', [])
            color_scale = heat_map_data.get('color_scale', ['blue', 'green', 'yellow', 'orange', 'red'])
            
            if data_points:
                values = [point.get('value', 0) for point in data_points]
                min_val = min(values) if values else 0
                max_val = max(values) if values else 1
                
                for point in data_points:
                    lat = point.get('lat', center_lat)
                    lng = point.get('lng', center_lng)
                    value = point.get('value', 0)
                    
                    # Map value to color
                    if max_val > min_val:
                        normalized = (value - min_val) / (max_val - min_val)
                        color_index = int(normalized * (len(color_scale) - 1))
                        color = color_scale[min(color_index, len(color_scale) - 1)]
                    else:
                        color = color_scale[0]
                    
                    folium.CircleMarker(
                        [lat, lng],
                        radius=8,
                        popup=f"{parameter}: {value}",
                        color='black',
                        weight=1,
                        fillColor=color,
                        fillOpacity=0.7
                    ).add_to(m)
            
            # Save map to HTML
            with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as tmp_file:
                m.save(tmp_file.name)
                html_file = tmp_file.name
            
            # For now, return a placeholder image
            placeholder_image = self._create_placeholder_image(
                map_size, 
                f"Heat Map: {parameter}\n{len(data_points)} data points\n{center_lat:.4f}, {center_lng:.4f}"
            )
            
            # Clean up
            try:
                os.unlink(html_file)
            except:
                pass
            
            return placeholder_image
            
        except Exception as e:
            logger.error(f"Error generating spatial heat map: {e}")
            return self._create_placeholder_image(map_size, f"Heat map for {parameter} unavailable")
    
    def _create_map_placeholder(
        self,
        size: Tuple[int, int],
        map_type: str,
        location: str,
        coordinates: str,
        accent_color: str = "#3498db"
    ) -> bytes:
        """Create an elegant placeholder map image"""
        try:
            from PIL import Image, ImageDraw, ImageFont

            width, height = size

            # Create gradient background
            image = Image.new('RGB', (width, height), color='#f0f0f0')
            draw = ImageDraw.Draw(image)

            # Draw subtle grid pattern
            grid_color = '#e0e0e0'
            for x in range(0, width, 40):
                draw.line([(x, 0), (x, height)], fill=grid_color, width=1)
            for y in range(0, height, 40):
                draw.line([(0, y), (width, y)], fill=grid_color, width=1)

            # Draw border
            draw.rectangle([(0, 0), (width-1, height-1)], outline=accent_color, width=3)

            # Try to use a default font
            try:
                title_font = ImageFont.truetype("arial.ttf", 20)
                text_font = ImageFont.truetype("arial.ttf", 14)
                small_font = ImageFont.truetype("arial.ttf", 12)
            except:
                title_font = ImageFont.load_default()
                text_font = ImageFont.load_default()
                small_font = ImageFont.load_default()

            # Draw title
            title = map_type.upper()
            title_bbox = draw.textbbox((0, 0), title, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            draw.text(
                ((width - title_width) // 2, height // 2 - 40),
                title,
                fill=accent_color,
                font=title_font
            )

            # Draw location
            if location:
                loc_bbox = draw.textbbox((0, 0), location, font=text_font)
                loc_width = loc_bbox[2] - loc_bbox[0]
                draw.text(
                    ((width - loc_width) // 2, height // 2),
                    location,
                    fill='#333333',
                    font=text_font
                )

            # Draw coordinates
            if coordinates:
                coord_bbox = draw.textbbox((0, 0), coordinates, font=small_font)
                coord_width = coord_bbox[2] - coord_bbox[0]
                draw.text(
                    ((width - coord_width) // 2, height // 2 + 30),
                    coordinates,
                    fill='#666666',
                    font=small_font
                )

            # Draw map icon (simple pin)
            pin_x = width // 2
            pin_y = height // 2 - 80
            draw.ellipse(
                [(pin_x - 15, pin_y - 15), (pin_x + 15, pin_y + 15)],
                fill=accent_color,
                outline='white',
                width=2
            )

            # Save to bytes
            img_bytes = io.BytesIO()
            image.save(img_bytes, format='PNG')
            return img_bytes.getvalue()

        except Exception as e:
            logger.error(f"Error creating map placeholder: {e}")
            return self._create_simple_placeholder(size, map_type)

    def _create_placeholder_image(self, size: Tuple[int, int], text: str) -> bytes:
        """Create a placeholder image with text (legacy method)"""
        return self._create_simple_placeholder(size, text)

    def _create_simple_placeholder(self, size: Tuple[int, int], text: str) -> bytes:
        """Create a simple placeholder image with text"""
        try:
            from PIL import Image, ImageDraw, ImageFont

            width, height = size
            image = Image.new('RGB', (width, height), color='lightgray')
            draw = ImageDraw.Draw(image)

            # Try to use a default font
            try:
                font = ImageFont.truetype("arial.ttf", 16)
            except:
                font = ImageFont.load_default()

            # Draw text in center
            text_lines = text.split('\n')
            line_height = 20
            start_y = height // 2 - (len(text_lines) * line_height) // 2

            for i, line in enumerate(text_lines):
                try:
                    text_bbox = draw.textbbox((0, 0), line, font=font)
                    text_width = text_bbox[2] - text_bbox[0]
                except:
                    text_width = len(line) * 10
                x = (width - text_width) // 2
                y = start_y + i * line_height
                draw.text((x, y), line, fill='black', font=font)

            # Save to bytes
            img_bytes = io.BytesIO()
            image.save(img_bytes, format='PNG')
            return img_bytes.getvalue()

        except Exception as e:
            logger.error(f"Error creating simple placeholder: {e}")
            # Return minimal placeholder
            return b"placeholder"