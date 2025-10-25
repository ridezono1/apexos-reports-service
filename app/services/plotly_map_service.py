"""
Alternative map service using Plotly for static map generation.
This avoids Selenium/Chrome dependency issues.
"""

import io
import logging
import base64
from typing import Dict, Any, List, Optional, Tuple
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from PIL import Image
import numpy as np

logger = logging.getLogger(__name__)


class PlotlyMapService:
    """Service for generating maps using Plotly (no Selenium required)."""

    def __init__(self):
        self.default_zoom = 10
        # Brand colors
        self.primary_color = '#3498db'
        self.danger_color = '#e74c3c'

    async def generate_combined_spatial_heatmap(
        self,
        events: List[Dict[str, Any]],
        risk_areas: Dict[str, Any],
        center_lat: float,
        center_lon: float,
        boundary_coords: List[List[float]],
        title: str = "Storm Events Analysis"
    ) -> bytes:
        """Generate a combined spatial heat map using Plotly."""
        try:
            logger.info(f"Generating Plotly heat map for {title}")
            
            # Create the main figure
            fig = go.Figure()
            
            # Add boundary polygon if coordinates provided
            if boundary_coords and len(boundary_coords) > 0:
                # Convert boundary coordinates to plotly format
                boundary_lats = [coord[0] for coord in boundary_coords]
                boundary_lons = [coord[1] for coord in boundary_coords]
                
                # Close the polygon
                boundary_lats.append(boundary_lats[0])
                boundary_lons.append(boundary_lons[0])
                
                fig.add_trace(go.Scattermapbox(
                    lat=boundary_lats,
                    lon=boundary_lons,
                    mode='lines',
                    line=dict(color='#6B46C1', width=3),
                    name='Analysis Area',
                    showlegend=True
                ))
            
            # Add storm events as scatter points
            if events and len(events) > 0:
                event_lats = []
                event_lons = []
                event_texts = []
                event_colors = []
                
                for event in events:
                    if 'lat' in event and 'lon' in event:
                        event_lats.append(event['lat'])
                        event_lons.append(event['lon'])
                        
                        # Create event description
                        event_type = event.get('event_type', 'Unknown')
                        magnitude = event.get('magnitude', '')
                        date = event.get('date', '')
                        text = f"{event_type}"
                        if magnitude:
                            text += f" ({magnitude})"
                        if date:
                            text += f"<br>{date}"
                        event_texts.append(text)
                        
                        # Color by event type
                        if 'hail' in event_type.lower():
                            event_colors.append('#e74c3c')  # Red for hail
                        elif 'tornado' in event_type.lower():
                            event_colors.append('#8e44ad')  # Purple for tornado
                        elif 'wind' in event_type.lower():
                            event_colors.append('#f39c12')  # Orange for wind
                        else:
                            event_colors.append('#3498db')  # Blue for others
                
                if event_lats:
                    fig.add_trace(go.Scattermapbox(
                        lat=event_lats,
                        lon=event_lons,
                        mode='markers',
                        marker=dict(
                            size=12,
                            color=event_colors,
                            opacity=0.8
                        ),
                        text=event_texts,
                        hovertemplate='%{text}<extra></extra>',
                        name='Storm Events',
                        showlegend=True
                    ))
            
            # Add risk zones if provided
            if risk_areas and 'high_risk_areas' in risk_areas:
                high_risk = risk_areas['high_risk_areas']
                if 'coordinates' in high_risk:
                    for risk_coords in high_risk['coordinates']:
                        risk_lats = [coord[0] for coord in risk_coords]
                        risk_lons = [coord[1] for coord in risk_coords]
                        risk_lats.append(risk_lats[0])  # Close polygon
                        risk_lons.append(risk_lons[0])
                        
                        fig.add_trace(go.Scattermapbox(
                            lat=risk_lats,
                            lon=risk_lons,
                            mode='lines',
                            line=dict(color='#e74c3c', width=2, dash='dash'),
                            fill='toself',
                            fillcolor='rgba(231, 76, 60, 0.2)',
                            name='High Risk Zone',
                            showlegend=True
                        ))
            
            # Configure the map layout
            fig.update_layout(
                title=dict(
                    text=f"<b>{title}</b><br><sub>Storm Events & Risk Analysis</sub>",
                    x=0.5,
                    font=dict(size=16, color='#2c3e50')
                ),
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=self.default_zoom
                ),
                margin=dict(l=0, r=0, t=60, b=0),
                height=800,
                width=1200,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            # Convert to image bytes (much smaller size for PDF compatibility)
            img_bytes = fig.to_image(format="png", width=400, height=300, scale=1)
            
            logger.info(f"Plotly heat map generated: {len(img_bytes)} bytes")
            return img_bytes
            
        except Exception as e:
            logger.error(f"Error generating Plotly heat map: {e}")
            # Return a placeholder image
            return self._create_placeholder_map(1200, 800, "Geographic Heat Map")

    def _create_placeholder_map(self, width: int, height: int, title: str) -> bytes:
        """Create a placeholder map image."""
        try:
            # Create a simple plotly figure as placeholder
            fig = go.Figure()
            
            # Use default center coordinates
            center_lat, center_lon = 29.7634871, -95.3595563
            
            fig.add_trace(go.Scattermapbox(
                lat=[center_lat],
                lon=[center_lon],
                mode='markers',
                marker=dict(size=20, color='#6B46C1'),
                text=[title],
                hovertemplate='%{text}<extra></extra>'
            ))
            
            fig.update_layout(
                title=dict(
                    text=f"<b>{title}</b><br><sub>Map generation unavailable</sub>",
                    x=0.5,
                    font=dict(size=16, color='#2c3e50')
                ),
                mapbox=dict(
                    style="open-street-map",
                    center=dict(lat=center_lat, lon=center_lon),
                    zoom=10
                ),
                margin=dict(l=0, r=0, t=60, b=0),
                height=height,
                width=width
            )
            
            return fig.to_image(format="png", width=width, height=height, scale=2)
            
        except Exception as e:
            logger.error(f"Failed to create Plotly placeholder: {e}")
            # Fallback to PIL
            img = Image.new('RGB', (width, height), color='#f8f9fa')
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            return buffer.getvalue()


# Global instance for dependency injection
_plotly_map_service_instance = None


def get_plotly_map_service() -> PlotlyMapService:
    """Get the global PlotlyMapService instance."""
    global _plotly_map_service_instance
    if _plotly_map_service_instance is None:
        _plotly_map_service_instance = PlotlyMapService()
    return _plotly_map_service_instance
