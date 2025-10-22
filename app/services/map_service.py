"""Map and heat map generation service using Folium."""

import io
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
import folium
from folium.plugins import HeatMap
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image

logger = logging.getLogger(__name__)


class MapService:
    """Service for generating maps and heat maps."""

    def __init__(self):
        self.default_zoom = 10
        # Brand colors
        self.primary_color = '#3498db'
        self.danger_color = '#e74c3c'

    def _get_chrome_driver(self) -> webdriver.Chrome:
        """Initialize headless Chrome driver for rendering."""
        try:
            import os
            import shutil

            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1200,800')

            # Try to detect Chrome/Chromium binary location
            chrome_bin_candidates = [
                '/usr/bin/google-chrome-stable',
                '/usr/bin/google-chrome',
                '/usr/bin/chromium',
                '/usr/bin/chromium-browser',
                shutil.which('google-chrome-stable'),
                shutil.which('google-chrome'),
                shutil.which('chromium'),
                shutil.which('chromium-browser'),
            ]

            chrome_binary = None
            for candidate in chrome_bin_candidates:
                if candidate and os.path.exists(candidate):
                    chrome_binary = candidate
                    logger.info(f"Found Chrome binary at: {chrome_binary}")
                    break

            if chrome_binary:
                chrome_options.binary_location = chrome_binary

            # Try to use system chromedriver first, fall back to webdriver-manager
            chromedriver_candidates = [
                '/usr/bin/chromedriver',
                '/usr/local/bin/chromedriver',
                shutil.which('chromedriver'),
            ]

            service = None
            for candidate in chromedriver_candidates:
                if candidate and os.path.exists(candidate):
                    logger.info(f"Using system chromedriver at: {candidate}")
                    service = Service(executable_path=candidate)
                    break

            if service is None:
                logger.info("Using ChromeDriverManager to install chromedriver")
                service = Service(ChromeDriverManager().install())

            driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Chrome driver initialized successfully")
            return driver
        except Exception as e:
            logger.error(f"Error initializing Chrome driver: {str(e)}")
            logger.error(f"Chrome binary locations checked: {chrome_bin_candidates}")
            logger.error(f"ChromeDriver locations checked: {chromedriver_candidates}")
            raise

    def _render_map_to_image(
        self,
        folium_map: folium.Map,
        width: int = 1200,
        height: int = 800
    ) -> bytes:
        """
        Render a Folium map to PNG image using Selenium.

        Args:
            folium_map: Folium map object
            width: Image width in pixels
            height: Image height in pixels

        Returns:
            PNG image as bytes
        """
        try:
            # Save map to temporary HTML
            html_buffer = io.BytesIO()
            folium_map.save(html_buffer, close_file=False)
            html_buffer.seek(0)
            html_str = html_buffer.read().decode('utf-8')

            # Initialize Chrome driver
            driver = self._get_chrome_driver()

            try:
                # Load HTML
                driver.get("data:text/html;charset=utf-8," + html_str)

                # Wait for map to load
                time.sleep(2)

                # Take screenshot
                screenshot_bytes = driver.get_screenshot_as_png()

                # Convert to PIL Image for potential processing
                image = Image.open(io.BytesIO(screenshot_bytes))

                # Resize if needed
                if image.size != (width, height):
                    image = image.resize((width, height), Image.Resampling.LANCZOS)

                # Convert back to bytes
                output_buffer = io.BytesIO()
                image.save(output_buffer, format='PNG', optimize=True)
                output_buffer.seek(0)

                return output_buffer.read()

            finally:
                driver.quit()

        except Exception as e:
            logger.error(f"Error rendering map to image: {str(e)}")
            raise

    def generate_heat_map(
        self,
        events: List[Dict[str, Any]],
        center_lat: float,
        center_lon: float,
        title: str = "Weather Event Heat Map"
    ) -> bytes:
        """
        Generate a geographic heat map showing event concentrations.

        Args:
            events: List of event dictionaries with 'latitude' and 'longitude' keys
            center_lat: Center latitude for map
            center_lon: Center longitude for map
            title: Map title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating heat map with {len(events)} events")

            # Create base map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=self.default_zoom,
                tiles='OpenStreetMap'
            )

            # Extract coordinates from events
            heat_data = []
            for event in events:
                try:
                    lat = event.get('latitude', event.get('begin_lat'))
                    lon = event.get('longitude', event.get('begin_lon'))

                    if lat is not None and lon is not None:
                        # Add intensity based on event severity if available
                        severity = event.get('severity', 1)
                        heat_data.append([float(lat), float(lon), float(severity)])
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse coordinates for event: {e}")
                    continue

            if heat_data:
                # Add heat map layer
                HeatMap(
                    heat_data,
                    min_opacity=0.3,
                    max_opacity=0.8,
                    radius=15,
                    blur=20,
                    gradient={
                        0.0: 'blue',
                        0.4: 'cyan',
                        0.6: 'lime',
                        0.8: 'yellow',
                        1.0: 'red'
                    }
                ).add_to(m)

                # Add title
                title_html = f'''
                    <div style="position: fixed;
                                top: 10px;
                                left: 50%;
                                transform: translateX(-50%);
                                width: 400px;
                                height: 50px;
                                background-color: white;
                                border: 2px solid {self.primary_color};
                                border-radius: 5px;
                                z-index: 9999;
                                font-size: 18px;
                                font-weight: bold;
                                text-align: center;
                                padding: 10px;
                                box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
                        {title}
                    </div>
                '''
                m.get_root().html.add_child(folium.Element(title_html))

                # Add legend
                legend_html = '''
                    <div style="position: fixed;
                                bottom: 50px;
                                right: 50px;
                                width: 200px;
                                background-color: white;
                                border: 2px solid #ccc;
                                border-radius: 5px;
                                z-index: 9999;
                                padding: 10px;
                                box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
                        <p style="margin: 0; font-weight: bold; margin-bottom: 5px;">Event Intensity</p>
                        <div style="background: linear-gradient(to right, blue, cyan, lime, yellow, red);
                                    height: 20px;
                                    border-radius: 3px;
                                    margin-bottom: 5px;"></div>
                        <div style="display: flex; justify-content: space-between; font-size: 10px;">
                            <span>Low</span>
                            <span>High</span>
                        </div>
                    </div>
                '''
                m.get_root().html.add_child(folium.Element(legend_html))
            else:
                # No events - show marker at center
                folium.Marker(
                    [center_lat, center_lon],
                    popup="No events in this area",
                    icon=folium.Icon(color='gray', icon='info-sign')
                ).add_to(m)

            # Render to image
            return self._render_map_to_image(m)

        except Exception as e:
            logger.error(f"Error generating heat map: {str(e)}")
            raise

    def generate_boundary_map(
        self,
        boundary_coords: List[Tuple[float, float]],
        events: List[Dict[str, Any]],
        center_lat: float,
        center_lon: float,
        title: str = "Spatial Analysis Area"
    ) -> bytes:
        """
        Generate a map showing boundary polygon and event markers.

        Args:
            boundary_coords: List of (lat, lon) tuples defining boundary
            events: List of event dictionaries with location data
            center_lat: Center latitude for map
            center_lon: Center longitude for map
            title: Map title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating boundary map with {len(events)} events")

            # Create base map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=self.default_zoom,
                tiles='OpenStreetMap'
            )

            # Add boundary polygon if provided
            if boundary_coords and len(boundary_coords) > 0:
                folium.Polygon(
                    locations=boundary_coords,
                    color=self.primary_color,
                    fill=True,
                    fillColor=self.primary_color,
                    fillOpacity=0.2,
                    weight=3,
                    popup="Analysis Area"
                ).add_to(m)

            # Add event markers (limited to avoid overcrowding)
            max_markers = 50
            for idx, event in enumerate(events[:max_markers]):
                try:
                    lat = event.get('latitude', event.get('begin_lat'))
                    lon = event.get('longitude', event.get('begin_lon'))
                    event_type = event.get('type', event.get('event_type', 'Unknown'))

                    if lat is not None and lon is not None:
                        # Color by severity
                        severity = event.get('severity', 'low')
                        if severity in ['high', 'severe']:
                            color = 'red'
                            icon = 'exclamation-sign'
                        elif severity == 'medium':
                            color = 'orange'
                            icon = 'warning-sign'
                        else:
                            color = 'blue'
                            icon = 'info-sign'

                        folium.Marker(
                            [float(lat), float(lon)],
                            popup=f"{event_type}",
                            icon=folium.Icon(color=color, icon=icon)
                        ).add_to(m)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse coordinates for event: {e}")
                    continue

            # Add title
            title_html = f'''
                <div style="position: fixed;
                            top: 10px;
                            left: 50%;
                            transform: translateX(-50%);
                            width: 400px;
                            height: 50px;
                            background-color: white;
                            border: 2px solid {self.primary_color};
                            border-radius: 5px;
                            z-index: 9999;
                            font-size: 18px;
                            font-weight: bold;
                            text-align: center;
                            padding: 10px;
                            box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
                    {title}
                </div>
            '''
            m.get_root().html.add_child(folium.Element(title_html))

            # Render to image
            return self._render_map_to_image(m)

        except Exception as e:
            logger.error(f"Error generating boundary map: {str(e)}")
            raise

    def generate_simple_location_map(
        self,
        latitude: float,
        longitude: float,
        location_name: str,
        title: str = "Location Map"
    ) -> bytes:
        """
        Generate a simple map showing a single location.

        Args:
            latitude: Location latitude
            longitude: Location longitude
            location_name: Name of location
            title: Map title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating location map for {location_name}")

            # Create base map
            m = folium.Map(
                location=[latitude, longitude],
                zoom_start=self.default_zoom,
                tiles='OpenStreetMap'
            )

            # Add marker
            folium.Marker(
                [latitude, longitude],
                popup=location_name,
                icon=folium.Icon(color='red', icon='home')
            ).add_to(m)

            # Add circle for area of interest
            folium.Circle(
                [latitude, longitude],
                radius=5000,  # 5km radius
                color=self.primary_color,
                fill=True,
                fillColor=self.primary_color,
                fillOpacity=0.2,
                popup="Analysis Area"
            ).add_to(m)

            # Add title
            title_html = f'''
                <div style="position: fixed;
                            top: 10px;
                            left: 50%;
                            transform: translateX(-50%);
                            width: 400px;
                            height: 50px;
                            background-color: white;
                            border: 2px solid {self.primary_color};
                            border-radius: 5px;
                            z-index: 9999;
                            font-size: 18px;
                            font-weight: bold;
                            text-align: center;
                            padding: 10px;
                            box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
                    {title}
                </div>
            '''
            m.get_root().html.add_child(folium.Element(title_html))

            # Render to image
            return self._render_map_to_image(m)

        except Exception as e:
            logger.error(f"Error generating location map: {str(e)}")
            raise
