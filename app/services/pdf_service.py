"""PDF generation service for weather reports."""

import io
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    Image as RLImage,
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

from app.services.chart_service import ChartService
from app.services.map_service import MapService

logger = logging.getLogger(__name__)


class PDFGenerationService:
    """Service for generating weather report PDFs."""

    def __init__(self):
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
        self.chart_service = ChartService()
        self.map_service = MapService()

    def _setup_custom_styles(self):
        """Setup custom paragraph styles for branding."""
        # Title style
        self.styles.add(
            ParagraphStyle(
                name="CustomTitle",
                parent=self.styles["Title"],
                fontSize=24,
                textColor=colors.HexColor("#1a1a1a"),
                spaceAfter=30,
                alignment=TA_CENTER,
            )
        )

        # Heading style
        self.styles.add(
            ParagraphStyle(
                name="CustomHeading",
                parent=self.styles["Heading1"],
                fontSize=16,
                textColor=colors.HexColor("#2c3e50"),
                spaceAfter=12,
                spaceBefore=12,
            )
        )

        # Subheading style
        self.styles.add(
            ParagraphStyle(
                name="CustomSubheading",
                parent=self.styles["Heading2"],
                fontSize=14,
                textColor=colors.HexColor("#34495e"),
                spaceAfter=10,
                spaceBefore=10,
            )
        )

        # Body text
        self.styles.add(
            ParagraphStyle(
                name="CustomBody",
                parent=self.styles["BodyText"],
                fontSize=11,
                textColor=colors.HexColor("#333333"),
                spaceAfter=6,
            )
        )

    async def generate_weather_report_pdf(
        self, report_data: Dict[str, Any], weather_data: Dict[str, Any]
    ) -> bytes:
        """
        Generate an address-based weather report PDF.

        Args:
            report_data: Report metadata and configuration
            weather_data: Weather statistics and events

        Returns:
            PDF file as bytes
        """
        try:
            logger.info(f"Generating address report PDF: {report_data['report_id']}")

            # Create PDF in memory
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=0.75 * inch,
                leftMargin=0.75 * inch,
                topMargin=0.75 * inch,
                bottomMargin=0.75 * inch,
            )

            # Build PDF content
            story = []

            # Title page
            story.extend(self._build_title_page(report_data))
            story.append(PageBreak())

            # Executive summary
            story.extend(self._build_executive_summary(weather_data))
            story.append(Spacer(1, 0.2 * inch))

            # Weather statistics
            story.extend(self._build_weather_statistics(weather_data))
            story.append(Spacer(1, 0.2 * inch))

            # Build PDF
            doc.build(story)

            # Get PDF bytes
            pdf_bytes = buffer.getvalue()
            buffer.close()

            logger.info(
                f"Successfully generated address report PDF: {len(pdf_bytes)} bytes"
            )
            return pdf_bytes

        except Exception as e:
            logger.error(f"Error generating address report PDF: {str(e)}")
            raise

    async def generate_spatial_report_pdf(self, report_data: Dict[str, Any]) -> bytes:
        """
        Generate a spatial weather report PDF with heat maps and charts.

        Args:
            report_data: Report metadata, spatial data, and configuration

        Returns:
            PDF file as bytes
        """
        try:
            logger.info(f"Generating spatial report PDF: {report_data['report_id']}")

            # Create PDF in memory
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=0.75 * inch,
                leftMargin=0.75 * inch,
                topMargin=0.75 * inch,
                bottomMargin=0.75 * inch,
            )

            # Build PDF content
            story = []

            # Title page
            story.extend(self._build_spatial_title_page(report_data))
            story.append(PageBreak())

            # Executive summary
            spatial_data = report_data.get("spatial_data", {})
            story.extend(self._build_spatial_summary(spatial_data))
            story.append(Spacer(1, 0.2 * inch))

            # Heat map section
            story.extend(self._build_heat_map_section(spatial_data))
            story.append(Spacer(1, 0.2 * inch))

            # Charts section
            story.extend(self._build_charts_section(spatial_data))

            # Build PDF
            doc.build(story)

            # Get PDF bytes
            pdf_bytes = buffer.getvalue()
            buffer.close()

            logger.info(
                f"Successfully generated spatial report PDF: {len(pdf_bytes)} bytes"
            )
            return pdf_bytes

        except Exception as e:
            logger.error(f"Error generating spatial report PDF: {str(e)}")
            raise

    def _build_title_page(self, report_data: Dict[str, Any]) -> List:
        """Build the title page for address reports."""
        elements = []

        # Title
        title = Paragraph(report_data["title"], self.styles["CustomTitle"])
        elements.append(title)
        elements.append(Spacer(1, 0.3 * inch))

        # Location
        location = Paragraph(
            f"<b>Location:</b> {report_data['location']}", self.styles["CustomBody"]
        )
        elements.append(location)
        elements.append(Spacer(1, 0.1 * inch))

        # Date range
        date_range = Paragraph(
            f"<b>Analysis Period:</b> {report_data['start_date']} to {report_data['end_date']}",
            self.styles["CustomBody"],
        )
        elements.append(date_range)
        elements.append(Spacer(1, 0.1 * inch))

        # Generated date
        generated = Paragraph(
            f"<b>Generated:</b> {datetime.utcnow().strftime('%B %d, %Y')}",
            self.styles["CustomBody"],
        )
        elements.append(generated)
        elements.append(Spacer(1, 0.5 * inch))

        # Branding
        branding = Paragraph(
            "ApexOS Weather Analysis", self.styles["CustomHeading"]
        )
        elements.append(branding)

        return elements

    def _build_spatial_title_page(self, report_data: Dict[str, Any]) -> List:
        """Build the title page for spatial reports."""
        elements = []

        # Title
        title = Paragraph(report_data["title"], self.styles["CustomTitle"])
        elements.append(title)
        elements.append(Spacer(1, 0.3 * inch))

        # Location
        location = Paragraph(
            f"<b>Location:</b> {report_data['location']}", self.styles["CustomBody"]
        )
        elements.append(location)
        elements.append(Spacer(1, 0.1 * inch))

        # Boundary info
        spatial_data = report_data.get("spatial_data", {})
        boundary = spatial_data.get("boundary", {})
        if boundary:
            boundary_info = Paragraph(
                f"<b>Area:</b> {boundary.get('name', 'N/A')} ({boundary.get('type', 'N/A')})",
                self.styles["CustomBody"],
            )
            elements.append(boundary_info)
            elements.append(Spacer(1, 0.1 * inch))

        # Date range
        date_range = Paragraph(
            f"<b>Analysis Period:</b> {report_data['start_date']} to {report_data['end_date']}",
            self.styles["CustomBody"],
        )
        elements.append(date_range)
        elements.append(Spacer(1, 0.1 * inch))

        # Generated date
        generated = Paragraph(
            f"<b>Generated:</b> {datetime.utcnow().strftime('%B %d, %Y')}",
            self.styles["CustomBody"],
        )
        elements.append(generated)
        elements.append(Spacer(1, 0.5 * inch))

        # Branding
        branding = Paragraph(
            "ApexOS Spatial Weather Analysis", self.styles["CustomHeading"]
        )
        elements.append(branding)

        return elements

    def _build_executive_summary(self, weather_data: Dict[str, Any]) -> List:
        """Build executive summary section."""
        elements = []

        # Section heading
        heading = Paragraph("Executive Summary", self.styles["CustomHeading"])
        elements.append(heading)
        elements.append(Spacer(1, 0.1 * inch))

        # Key statistics
        summary_text = f"""
        This report analyzes weather conditions and severe weather events for the specified location.
        During the analysis period, there were <b>{weather_data.get('weather_events', 0)} weather events</b> recorded,
        including <b>{weather_data.get('hail_events', 0)} hail events</b>.
        """

        summary = Paragraph(summary_text, self.styles["CustomBody"])
        elements.append(summary)

        return elements

    def _build_spatial_summary(self, spatial_data: Dict[str, Any]) -> List:
        """Build executive summary for spatial reports."""
        elements = []

        # Section heading
        heading = Paragraph("Executive Summary", self.styles["CustomHeading"])
        elements.append(heading)
        elements.append(Spacer(1, 0.1 * inch))

        # Summary text
        boundary = spatial_data.get("boundary", {})
        summary_text = f"""
        This spatial analysis examines weather patterns and severe weather events across
        {boundary.get('name', 'the specified area')}. The analysis includes geographic distribution
        of events, temporal patterns, and risk assessment.
        """

        summary = Paragraph(summary_text, self.styles["CustomBody"])
        elements.append(summary)

        return elements

    def _build_weather_statistics(self, weather_data: Dict[str, Any]) -> List:
        """Build weather statistics table."""
        elements = []

        # Section heading
        heading = Paragraph("Weather Statistics", self.styles["CustomHeading"])
        elements.append(heading)
        elements.append(Spacer(1, 0.1 * inch))

        # Statistics table
        table_data = [
            ["Metric", "Value"],
            ["Maximum Temperature", f"{weather_data.get('max_temp', 'N/A')}°F"],
            ["Minimum Temperature", f"{weather_data.get('min_temp', 'N/A')}°F"],
            [
                "Total Precipitation",
                f"{weather_data.get('total_precip', 'N/A')} inches",
            ],
            ["Maximum Wind Speed", f"{weather_data.get('max_wind', 'N/A')} mph"],
            ["Weather Events", str(weather_data.get("weather_events", 0))],
            ["Hail Events", str(weather_data.get("hail_events", 0))],
            [
                "Maximum Hail Size",
                f"{weather_data.get('max_hail_size', 'N/A')} inches",
            ],
        ]

        table = Table(table_data, colWidths=[3 * inch, 3 * inch])
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3498db")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                    ("FONTNAME", (0, 1), (0, -1), "Helvetica-Bold"),
                ]
            )
        )

        elements.append(table)

        return elements

    def _build_heat_map_section(self, spatial_data: Dict[str, Any]) -> List:
        """Build heat map section with actual Folium-generated heat map."""
        elements = []

        # Section heading
        heading = Paragraph("Geographic Heat Map", self.styles["CustomHeading"])
        elements.append(heading)
        elements.append(Spacer(1, 0.1 * inch))

        # Description
        description = Paragraph(
            """
            The heat map below shows the geographic distribution of severe weather events
            across the analysis area. Red areas indicate higher concentrations of events,
            while blue areas indicate fewer events.
            """,
            self.styles["CustomBody"],
        )
        elements.append(description)
        elements.append(Spacer(1, 0.2 * inch))

        # Generate heat map if we have heat map data
        heat_map_data = spatial_data.get("heat_map_data", [])
        if heat_map_data and len(heat_map_data) > 0:
            try:
                # Extract center coordinates from first event or use provided center
                center_lat = spatial_data.get("center_lat", heat_map_data[0].get("latitude", 0))
                center_lon = spatial_data.get("center_lon", heat_map_data[0].get("longitude", 0))

                # Generate heat map
                heat_map_bytes = self.map_service.generate_heat_map(
                    events=heat_map_data,
                    center_lat=center_lat,
                    center_lon=center_lon,
                    title="Weather Event Heat Map"
                )

                # Add to PDF
                heat_map_img = RLImage(io.BytesIO(heat_map_bytes), width=6*inch, height=4*inch)
                elements.append(heat_map_img)

            except Exception as e:
                logger.error(f"Error generating heat map: {str(e)}")
                placeholder = Paragraph(
                    f"[Heat map could not be generated: {str(e)}]",
                    self.styles["CustomBody"],
                )
                elements.append(placeholder)
        else:
            placeholder = Paragraph(
                "[No heat map data available]",
                self.styles["CustomBody"],
            )
            elements.append(placeholder)

        return elements

    def _build_charts_section(self, spatial_data: Dict[str, Any]) -> List:
        """Build charts section with actual Matplotlib-generated charts."""
        elements = []

        # Section heading
        heading = Paragraph("Weather Event Analysis", self.styles["CustomHeading"])
        elements.append(heading)
        elements.append(Spacer(1, 0.1 * inch))

        # Description
        description = Paragraph(
            """
            The following charts provide detailed analysis of weather events over time
            and by type.
            """,
            self.styles["CustomBody"],
        )
        elements.append(description)
        elements.append(Spacer(1, 0.2 * inch))

        # Get event data
        events = spatial_data.get("events", [])

        if events and len(events) > 0:
            try:
                # Time Series Chart
                subheading1 = Paragraph("Events Over Time", self.styles["CustomSubheading"])
                elements.append(subheading1)
                elements.append(Spacer(1, 0.1 * inch))

                time_series_bytes = self.chart_service.generate_time_series_chart(
                    events=events,
                    title="Weather Events Over Time"
                )
                time_series_img = RLImage(io.BytesIO(time_series_bytes), width=6*inch, height=3.5*inch)
                elements.append(time_series_img)
                elements.append(Spacer(1, 0.3 * inch))

                # Event Distribution Chart
                subheading2 = Paragraph("Event Type Distribution", self.styles["CustomSubheading"])
                elements.append(subheading2)
                elements.append(Spacer(1, 0.1 * inch))

                distribution_bytes = self.chart_service.generate_event_distribution_chart(
                    events=events,
                    title="Weather Event Distribution"
                )
                distribution_img = RLImage(io.BytesIO(distribution_bytes), width=6*inch, height=3.5*inch)
                elements.append(distribution_img)
                elements.append(Spacer(1, 0.3 * inch))

                # Monthly Breakdown Chart
                subheading3 = Paragraph("Monthly Breakdown", self.styles["CustomSubheading"])
                elements.append(subheading3)
                elements.append(Spacer(1, 0.1 * inch))

                monthly_bytes = self.chart_service.generate_monthly_breakdown_chart(
                    events=events,
                    title="Monthly Event Breakdown"
                )
                monthly_img = RLImage(io.BytesIO(monthly_bytes), width=6*inch, height=3.5*inch)
                elements.append(monthly_img)

            except Exception as e:
                logger.error(f"Error generating charts: {str(e)}")
                placeholder = Paragraph(
                    f"[Charts could not be generated: {str(e)}]",
                    self.styles["CustomBody"],
                )
                elements.append(placeholder)
        else:
            placeholder = Paragraph(
                "[No event data available for charts]",
                self.styles["CustomBody"],
            )
            elements.append(placeholder)

        return elements
