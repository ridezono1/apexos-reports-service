"""
PDF generation service using ReportLab and WeasyPrint
"""

import os
import asyncio
import base64
import tempfile
import re
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from app.core.config import settings
from app.models import ReportFormat
from app.services.map_generation import MapGenerationService

logger = logging.getLogger(__name__)

class PDFGenerator:
    """Service for generating PDF reports"""
    
    def __init__(self):
        self.temp_dir = Path(settings.temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.map_service = MapGenerationService()
        self.temp_files = []  # Track temporary files for cleanup
    
    def _process_base64_images(self, html_content: str, report_id: str, cleanup_immediately: bool = False) -> str:
        """Convert base64 images to temporary files and update HTML"""
        try:
            # Find all base64 images in the HTML
            base64_pattern = r'data:image/([^;]+);base64,([A-Za-z0-9+/=]+)'
            matches = re.findall(base64_pattern, html_content)
            
            processed_html = html_content
            
            for i, (image_type, base64_data) in enumerate(matches):
                try:
                    # Decode base64 data
                    image_data = base64.b64decode(base64_data)
                    
                    # Create temporary file
                    temp_file = self.temp_dir / f"{report_id}_image_{i}.{image_type}"
                    with open(temp_file, 'wb') as f:
                        f.write(image_data)
                    
                    # Track for cleanup
                    self.temp_files.append(temp_file)
                    
                    # Replace base64 with file path
                    old_src = f"data:image/{image_type};base64,{base64_data}"
                    new_src = f"file://{temp_file.absolute()}"
                    processed_html = processed_html.replace(old_src, new_src)
                    
                    logger.info(f"Converted base64 image to temporary file: {temp_file}")
                    
                except Exception as e:
                    logger.warning(f"Failed to process base64 image {i}: {e}")
                    continue
            
            # Cleanup immediately if requested
            if cleanup_immediately:
                self._cleanup_temp_files()
            
            return processed_html
            
        except Exception as e:
            logger.error(f"Error processing base64 images: {e}")
            return html_content
    
    def _cleanup_temp_files(self):
        """Clean up temporary files"""
        for temp_file in self.temp_files:
            try:
                if temp_file.exists():
                    temp_file.unlink()
                    logger.debug(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                logger.warning(f"Failed to clean up {temp_file}: {e}")
        self.temp_files.clear()
    
    async def generate_from_html(
        self,
        html_content: str,
        report_id: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate PDF from HTML content using WeasyPrint"""
        
        try:
            # Import WeasyPrint (optional dependency)
            from weasyprint import HTML, CSS
            from weasyprint.text.fonts import FontConfiguration
            
        except ImportError:
            logger.warning("WeasyPrint not available, falling back to ReportLab")
            return await self._generate_with_reportlab(html_content, report_id, options)

        try:
            options = options or {}

            # WeasyPrint can handle base64 images directly, no need to process them
            # Just use the HTML as-is with inline base64 images

            # Create output file path
            output_path = self.temp_dir / f"{report_id}.pdf"

            # Configure font settings
            font_config = FontConfiguration()

            # Generate PDF directly from HTML with base64 images
            logger.info("Generating PDF with WeasyPrint...")
            html_doc = HTML(string=html_content)

            # Enable PDF compression for smaller file sizes (mobile optimization)
            html_doc.write_pdf(
                str(output_path),
                font_config=font_config,
                compress=True,  # Enable PDF stream compression
                pdf_forms=False,  # Disable forms to reduce overhead
                uncompressed_pdf=False  # Ensure compression is enabled
            )

            logger.info(f"PDF generated with WeasyPrint: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to generate PDF with WeasyPrint: {e}")
            # Fallback to ReportLab
            return await self._generate_with_reportlab(html_content, report_id, options)
    
    async def _generate_with_reportlab(
        self,
        html_content: str,
        report_id: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Fallback PDF generation using ReportLab"""
        
        try:
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            import re
            
            options = options or {}
            page_size = options.get("page_size", "A4")
            
            # Process base64 images first (don't cleanup immediately for ReportLab)
            processed_html = self._process_base64_images(html_content, report_id, cleanup_immediately=False)
            
            # Create output file path
            output_path = self.temp_dir / f"{report_id}.pdf"
            
            # Create PDF document
            doc = SimpleDocTemplate(
                str(output_path),
                pagesize=A4 if page_size == "A4" else letter,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )
            
            # Get styles
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=18,
                spaceAfter=30,
                alignment=1  # Center alignment
            )
            
            # Parse HTML content (basic)
            story = []
            
            # Extract title
            title_match = re.search(r'<title>(.*?)</title>', processed_html, re.IGNORECASE)
            if title_match:
                story.append(Paragraph(title_match.group(1), title_style))
                story.append(Spacer(1, 12))
            
            # Extract images and add them to the story
            img_pattern = r'<img[^>]*src=["\']([^"\']*)["\'][^>]*>'
            img_matches = re.findall(img_pattern, processed_html, re.IGNORECASE)
            
            for img_src in img_matches:
                if img_src.startswith('file://'):
                    # Extract file path
                    img_path = img_src.replace('file://', '')
                    if Path(img_path).exists():
                        try:
                            # Add image to PDF
                            img = Image(img_path, width=2*inch, height=1*inch)
                            story.append(img)
                            story.append(Spacer(1, 12))
                            logger.info(f"Added image to PDF: {img_path}")
                        except Exception as e:
                            logger.warning(f"Failed to add image {img_path}: {e}")
            
            # Extract paragraphs
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', processed_html, re.IGNORECASE | re.DOTALL)
            for para_text in paragraphs:
                # Clean HTML tags
                clean_text = re.sub(r'<[^>]+>', '', para_text)
                if clean_text.strip():
                    story.append(Paragraph(clean_text, styles['Normal']))
                    story.append(Spacer(1, 12))
            
            # Build PDF
            doc.build(story)
            
            # Clean up temporary files
            self._cleanup_temp_files()
            
            logger.info(f"PDF generated with ReportLab: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to generate PDF with ReportLab: {e}")
            raise
    
    def _get_default_css(self) -> str:
        """Get default CSS styles for PDF generation"""
        return """
        @page {
            size: A4;
            margin: 0.75in;
        }

        body {
            font-family: 'Helvetica Neue', 'Helvetica', Arial, sans-serif;
            font-size: 11pt;
            line-height: 1.6;
            color: #2c3e50;
        }

        h1 {
            color: #2c3e50;
            font-size: 26pt;
            font-weight: 300;
            margin-bottom: 20pt;
            text-align: center;
            letter-spacing: -0.5px;
        }

        h2 {
            color: #2c3e50;
            font-size: 18pt;
            font-weight: 400;
            margin-top: 20pt;
            margin-bottom: 15pt;
            padding-bottom: 10pt;
            border-bottom: 2pt solid #ecf0f1;
        }

        h3 {
            color: #34495e;
            font-size: 14pt;
            font-weight: 500;
            margin-top: 15pt;
            margin-bottom: 10pt;
        }

        p {
            margin-bottom: 10pt;
            line-height: 1.6;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15pt 0;
        }

        th, td {
            border: 1pt solid #bdc3c7;
            padding: 10pt;
            text-align: left;
        }

        th {
            background-color: #34495e;
            color: white;
            font-weight: 600;
        }

        tr:nth-child(even) {
            background-color: #f8f9fa;
        }

        img {
            max-width: 100%;
            height: auto;
        }

        .weather-summary {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
            color: white;
            padding: 20pt;
            border-radius: 8pt;
            margin: 20pt 0;
        }

        .risk-assessment {
            background-color: #fff3cd;
            border-left: 5pt solid #f39c12;
            padding: 20pt;
            border-radius: 4pt;
            margin: 20pt 0;
        }

        .risk-assessment.high {
            background-color: #fef5f5;
            border-left-color: #e74c3c;
        }

        .risk-assessment.low {
            background-color: #f0f9f4;
            border-left-color: #27ae60;
        }

        .property-section {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20pt;
            border-radius: 8pt;
            margin-bottom: 25pt;
        }

        .map-box {
            background: #ffffff;
            border: 2pt solid #ecf0f1;
            border-radius: 8pt;
            overflow: hidden;
            margin: 10pt;
        }

        .recommendations {
            background: #fff;
            border: 2pt solid #3498db;
            border-radius: 8pt;
            padding: 20pt;
            margin: 20pt 0;
        }

        .alert-box {
            border-left: 4pt solid #f39c12;
            background: #fff8e1;
            padding: 15pt;
            margin: 10pt 0;
            border-radius: 4pt;
        }

        .alert-box.high {
            border-left-color: #e74c3c;
            background: #ffebee;
        }

        .footer {
            margin-top: 40pt;
            padding-top: 20pt;
            border-top: 2pt solid #ecf0f1;
            text-align: center;
            font-size: 9pt;
            color: #95a5a6;
        }
        """