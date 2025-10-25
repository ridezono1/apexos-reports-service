"""
DEPRECATED: Abstract heat map generation service.
Spatial reports now use MapService.generate_combined_spatial_heatmap() 
for geographic heat maps with actual map backgrounds.
Kept for backward compatibility with address reports.

Heat map generation service for spatial weather reports.
Creates visual heat maps showing weather risk density across geographic areas.
"""

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from typing import List, Tuple, Dict, Any, Optional
import logging

from app.core.logging import get_logger

logger = get_logger(__name__)


class HeatmapService:
    """Service for generating weather risk heat maps."""

    def __init__(self):
        """Initialize heatmap service."""
        self.default_width = 800
        self.default_height = 600

    def generate_risk_heatmap(
        self,
        boundary_coords: List[Tuple[float, float]],
        risk_areas: Dict[str, List[Dict[str, Any]]],
        width: int = 800,
        height: int = 600
    ) -> bytes:
        """
        Generate a heat map showing weather risk levels across the spatial area.

        Args:
            boundary_coords: List of (lat, lon) coordinates defining the boundary
            risk_areas: Dictionary with 'high_risk_areas', 'medium_risk_areas', 'low_risk_areas'
            width: Image width in pixels
            height: Image height in pixels

        Returns:
            PNG image bytes
        """
        try:
            # Calculate bounds
            lats = [coord[0] for coord in boundary_coords]
            lons = [coord[1] for coord in boundary_coords]
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)

            # Create image with white background
            img = Image.new('RGB', (width, height), color='white')
            draw = ImageDraw.Draw(img, 'RGBA')

            # Draw boundary
            boundary_pixels = self._coords_to_pixels(
                boundary_coords, min_lat, max_lat, min_lon, max_lon, width, height
            )
            if len(boundary_pixels) > 2:
                draw.polygon(boundary_pixels, outline=(150, 150, 150), width=2)

            # Draw risk areas with graduated colors
            # Extract areas from properly nested risk assessment structure
            high_risk = risk_areas.get('high_risk_areas', {}).get('areas', []) if isinstance(risk_areas.get('high_risk_areas'), dict) else []
            medium_risk = risk_areas.get('medium_risk_areas', {}).get('areas', []) if isinstance(risk_areas.get('medium_risk_areas'), dict) else []
            low_risk = risk_areas.get('low_risk_areas', {}).get('areas', []) if isinstance(risk_areas.get('low_risk_areas'), dict) else []

            # Draw low risk areas (green)
            for area in low_risk:
                coords = area.get('coordinates')
                if coords:
                    pixel_coords = self._coords_to_pixels(
                        [coords], min_lat, max_lat, min_lon, max_lon, width, height
                    )[0]
                    # Draw circle with transparency
                    radius = 25
                    draw.ellipse(
                        [pixel_coords[0] - radius, pixel_coords[1] - radius,
                         pixel_coords[0] + radius, pixel_coords[1] + radius],
                        fill=(76, 175, 80, 120),  # Green with transparency
                        outline=(56, 142, 60, 200)
                    )

            # Draw medium risk areas (orange)
            for area in medium_risk:
                coords = area.get('coordinates')
                if coords:
                    pixel_coords = self._coords_to_pixels(
                        [coords], min_lat, max_lat, min_lon, max_lon, width, height
                    )[0]
                    radius = 30
                    draw.ellipse(
                        [pixel_coords[0] - radius, pixel_coords[1] - radius,
                         pixel_coords[0] + radius, pixel_coords[1] + radius],
                        fill=(255, 152, 0, 140),  # Orange with transparency
                        outline=(245, 124, 0, 200)
                    )

            # Draw high risk areas (red)
            for area in high_risk:
                coords = area.get('coordinates')
                if coords:
                    pixel_coords = self._coords_to_pixels(
                        [coords], min_lat, max_lat, min_lon, max_lon, width, height
                    )[0]
                    radius = 35
                    draw.ellipse(
                        [pixel_coords[0] - radius, pixel_coords[1] - radius,
                         pixel_coords[0] + radius, pixel_coords[1] + radius],
                        fill=(244, 67, 54, 160),  # Red with transparency
                        outline=(211, 47, 47, 220)
                    )

            # Add legend
            self._draw_legend(draw, width, height)

            # Add title
            self._draw_title(draw, "Weather Risk Heat Map", width)

            # Convert to bytes
            buffer = BytesIO()
            img.save(buffer, format='PNG')
            return buffer.getvalue()

        except Exception as e:
            logger.error(f"Error generating risk heatmap: {e}")
            # Return a simple error image
            return self._generate_error_image(width, height, str(e))

    def generate_event_density_heatmap(
        self,
        boundary_coords: List[Tuple[float, float]],
        grid_data: List[Dict[str, Any]],
        width: int = 800,
        height: int = 600
    ) -> bytes:
        """
        Generate a heat map showing weather event density across the spatial area.

        Args:
            boundary_coords: List of (lat, lon) coordinates defining the boundary
            grid_data: List of grid point data with weather events
            width: Image width in pixels
            height: Image height in pixels

        Returns:
            PNG image bytes
        """
        try:
            # Calculate bounds
            lats = [coord[0] for coord in boundary_coords]
            lons = [coord[1] for coord in boundary_coords]
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)

            # Create base image
            img = Image.new('RGB', (width, height), color='white')
            draw = ImageDraw.Draw(img, 'RGBA')

            # Draw boundary
            boundary_pixels = self._coords_to_pixels(
                boundary_coords, min_lat, max_lat, min_lon, max_lon, width, height
            )
            if len(boundary_pixels) > 2:
                draw.polygon(boundary_pixels, outline=(150, 150, 150), width=2)

            # Create heat map data
            heat_points = []
            max_events = 0

            for point_data in grid_data:
                coords = point_data.get('coordinates')
                events = point_data.get('weather_events', [])
                event_count = len(events)

                if coords and event_count > 0:
                    heat_points.append({
                        'coords': coords,
                        'intensity': event_count
                    })
                    max_events = max(max_events, event_count)

            # Draw heat points
            for point in heat_points:
                coords = point['coords']
                intensity = point['intensity']

                # Normalize intensity (0-1)
                normalized = intensity / max_events if max_events > 0 else 0

                # Calculate color gradient from blue (low) to red (high)
                color = self._get_heat_color(normalized)

                # Convert coordinates to pixels
                pixel_coords = self._coords_to_pixels(
                    [coords], min_lat, max_lat, min_lon, max_lon, width, height
                )[0]

                # Draw circle with size based on intensity
                radius = int(20 + (normalized * 20))
                alpha = int(100 + (normalized * 100))

                draw.ellipse(
                    [pixel_coords[0] - radius, pixel_coords[1] - radius,
                     pixel_coords[0] + radius, pixel_coords[1] + radius],
                    fill=(*color, alpha),
                    outline=(*color, 220)
                )

            # Add legend for event density
            self._draw_event_density_legend(draw, width, height, max_events)

            # Add title
            self._draw_title(draw, "Weather Event Density Heat Map", width)

            # Convert to bytes
            buffer = BytesIO()
            img.save(buffer, format='PNG')
            return buffer.getvalue()

        except Exception as e:
            logger.error(f"Error generating event density heatmap: {e}")
            return self._generate_error_image(width, height, str(e))

    def _coords_to_pixels(
        self,
        coords: List[Tuple[float, float]],
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        width: int,
        height: int,
        padding: int = 50
    ) -> List[Tuple[int, int]]:
        """Convert geographic coordinates to pixel coordinates."""
        pixels = []

        lat_range = max_lat - min_lat
        lon_range = max_lon - min_lon

        # Avoid division by zero
        if lat_range == 0:
            lat_range = 0.01
        if lon_range == 0:
            lon_range = 0.01

        for lat, lon in coords:
            # Normalize to 0-1
            x_norm = (lon - min_lon) / lon_range
            y_norm = 1 - (lat - min_lat) / lat_range  # Invert Y axis

            # Scale to image size with padding
            x = int(padding + x_norm * (width - 2 * padding))
            y = int(padding + y_norm * (height - 2 * padding))

            pixels.append((x, y))

        return pixels

    def _get_heat_color(self, intensity: float) -> Tuple[int, int, int]:
        """
        Get color for heat map based on intensity (0-1).
        Blue (low) -> Yellow (medium) -> Red (high)
        """
        if intensity < 0.33:
            # Blue to Cyan
            r = 0
            g = int(255 * (intensity / 0.33))
            b = 255
        elif intensity < 0.66:
            # Cyan to Yellow
            t = (intensity - 0.33) / 0.33
            r = int(255 * t)
            g = 255
            b = int(255 * (1 - t))
        else:
            # Yellow to Red
            t = (intensity - 0.66) / 0.34
            r = 255
            g = int(255 * (1 - t))
            b = 0

        return (r, g, b)

    def _draw_legend(self, draw: ImageDraw, width: int, height: int):
        """Draw risk level legend."""
        legend_x = width - 150
        legend_y = height - 120

        # Background
        draw.rectangle(
            [legend_x - 10, legend_y - 10, width - 20, height - 20],
            fill=(255, 255, 255, 230),
            outline=(100, 100, 100, 255)
        )

        # Title
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
        except:
            font = ImageFont.load_default()

        draw.text((legend_x, legend_y), "Risk Levels", fill='black', font=font)

        # Risk levels
        levels = [
            ("High Risk", (244, 67, 54)),
            ("Medium Risk", (255, 152, 0)),
            ("Low Risk", (76, 175, 80))
        ]

        y_offset = legend_y + 20
        for label, color in levels:
            # Color circle
            draw.ellipse(
                [legend_x, y_offset, legend_x + 15, y_offset + 15],
                fill=color
            )
            # Label
            draw.text((legend_x + 20, y_offset), label, fill='black', font=font)
            y_offset += 20

    def _draw_event_density_legend(self, draw: ImageDraw, width: int, height: int, max_events: int):
        """Draw event density legend."""
        legend_x = width - 180
        legend_y = height - 140

        # Background
        draw.rectangle(
            [legend_x - 10, legend_y - 10, width - 20, height - 20],
            fill=(255, 255, 255, 230),
            outline=(100, 100, 100, 255)
        )

        # Title
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
            small_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 10)
        except:
            font = small_font = ImageFont.load_default()

        draw.text((legend_x, legend_y), "Event Density", fill='black', font=font)

        # Gradient bar
        bar_width = 140
        bar_height = 20
        bar_y = legend_y + 25

        for i in range(bar_width):
            intensity = i / bar_width
            color = self._get_heat_color(intensity)
            draw.line(
                [(legend_x + i, bar_y), (legend_x + i, bar_y + bar_height)],
                fill=color,
                width=1
            )

        # Labels
        draw.text((legend_x, bar_y + bar_height + 5), "0", fill='black', font=small_font)
        draw.text((legend_x + bar_width - 20, bar_y + bar_height + 5),
                 f"{max_events}", fill='black', font=small_font)
        draw.text((legend_x + bar_width // 2 - 20, bar_y + bar_height + 5),
                 "Events", fill='black', font=small_font)

    def _draw_title(self, draw: ImageDraw, title: str, width: int):
        """Draw title at top of image."""
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 20)
        except:
            font = ImageFont.load_default()

        # Get text size (approximate)
        text_width = len(title) * 10
        x = (width - text_width) // 2

        # Background rectangle
        draw.rectangle(
            [x - 10, 10, x + text_width + 10, 40],
            fill=(255, 255, 255, 230),
            outline=(100, 100, 100, 255)
        )

        # Title text
        draw.text((x, 15), title, fill='black', font=font)

    def _generate_error_image(self, width: int, height: int, error_msg: str) -> bytes:
        """Generate a simple error image."""
        img = Image.new('RGB', (width, height), color='white')
        draw = ImageDraw.Draw(img)

        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
        except:
            font = ImageFont.load_default()

        # Draw error message
        draw.text((50, height // 2), f"Error generating heat map:", fill='red', font=font)
        draw.text((50, height // 2 + 30), error_msg[:50], fill='black', font=font)

        buffer = BytesIO()
        img.save(buffer, format='PNG')
        return buffer.getvalue()


# Singleton instance
_heatmap_service: Optional[HeatmapService] = None


def get_heatmap_service() -> HeatmapService:
    """Get heatmap service instance."""
    global _heatmap_service
    if _heatmap_service is None:
        _heatmap_service = HeatmapService()
    return _heatmap_service
