"""
Report generation service for weather and spatial reports
"""

import os
import uuid
import asyncio
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional
from pathlib import Path
import logging

from app.core.config import settings
from app.models import ReportStatus, ReportFormat
from app.services.pdf_generator import PDFGenerator
from app.services.excel_generator import ExcelGenerator
from app.services.template_engine import TemplateEngine
from app.services.weather_data_service import get_weather_data_service
from app.services.geocoding_service import get_geocoding_service
from app.services.spatial_analysis_service import get_spatial_analysis_service
from app.services.address_analysis_service import get_address_analysis_service

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Main service for generating weather and spatial reports"""
    
    def __init__(self):
        self.pdf_generator = PDFGenerator()
        self.excel_generator = ExcelGenerator()
        self.template_engine = TemplateEngine()
        self.weather_service = get_weather_data_service()
        self.geocoding_service = get_geocoding_service()
        self.spatial_analysis_service = get_spatial_analysis_service()
        self.address_analysis_service = get_address_analysis_service()
        
        # In-memory status tracking
        self.report_status: Dict[str, Dict[str, Any]] = {}
        
        # Ensure temp directory exists
        Path(settings.temp_dir).mkdir(parents=True, exist_ok=True)
    
    async def generate_weather_report(
        self,
        location: str,
        analysis_period: str,
        template: str = "professional",
        format: ReportFormat = ReportFormat.PDF,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate a weather report using real weather data from current date"""
        
        report_id = str(uuid.uuid4())
        options = options or {}
        
        # Calculate date range from current date
        today = date.today()
        if analysis_period == "6_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=180)  # Approximately 6 months
        elif analysis_period == "9_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=270)  # Approximately 9 months
        else:
            raise ValueError("Analysis period must be '6_months' or '9_months'")
        
        # Initialize report status
        self.report_status[report_id] = {
            "status": ReportStatus.GENERATING,
            "type": "weather",
            "template": template,
            "format": format,
            "analysis_period": analysis_period,
            "created_at": datetime.utcnow(),
            "estimated_completion": datetime.utcnow() + timedelta(minutes=2)
        }
        
        try:
            # Start generation in background
            asyncio.create_task(self._generate_weather_report_async(
                report_id, location, start_date, today, template, format, options
            ))
            
            return report_id
            
        except Exception as e:
            logger.error(f"Failed to start weather report generation: {e}")
            self.report_status[report_id]["status"] = ReportStatus.FAILED
            self.report_status[report_id]["error_message"] = str(e)
            raise
    
    async def generate_address_report(
        self,
        address: str,
        analysis_period: str,
        template: str = "professional",
        format: ReportFormat = ReportFormat.PDF,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate an address-specific weather report using SkyLink's address analysis from current date"""
        
        report_id = str(uuid.uuid4())
        options = options or {}
        
        # Calculate date range from current date
        today = date.today()
        if analysis_period == "6_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=180)  # Approximately 6 months
        elif analysis_period == "9_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=270)  # Approximately 9 months
        else:
            raise ValueError("Analysis period must be '6_months' or '9_months'")
        
        # Initialize report status
        self.report_status[report_id] = {
            "status": ReportStatus.GENERATING,
            "type": "address",
            "template": template,
            "format": format,
            "analysis_period": analysis_period,
            "created_at": datetime.utcnow(),
            "estimated_completion": datetime.utcnow() + timedelta(minutes=3)
        }
        
        try:
            # Start generation in background
            asyncio.create_task(self._generate_address_report_async(
                report_id, address, start_date, today, template, format, options
            ))
            
            return report_id
            
        except Exception as e:
            logger.error(f"Failed to start address report generation: {e}")
            self.report_status[report_id]["status"] = ReportStatus.FAILED
            self.report_status[report_id]["error_message"] = str(e)
            raise
    
    async def generate_spatial_report(
        self,
        boundary_type: str,
        boundary_data: Dict[str, Any],
        analysis_period: str,
        template: str = "professional",
        format: ReportFormat = ReportFormat.PDF,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate a spatial weather report using SkyLink's spatial analysis from current date"""
        
        report_id = str(uuid.uuid4())
        options = options or {}
        
        # Calculate date range from current date
        today = date.today()
        if analysis_period == "6_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=180)  # Approximately 6 months
        elif analysis_period == "9_months":
            # Use timedelta for more reliable date calculation
            from datetime import timedelta
            start_date = today - timedelta(days=270)  # Approximately 9 months
        else:
            raise ValueError("Analysis period must be '6_months' or '9_months'")
        
        # Initialize report status
        self.report_status[report_id] = {
            "status": ReportStatus.GENERATING,
            "type": "spatial",
            "template": template,
            "format": format,
            "analysis_period": analysis_period,
            "created_at": datetime.utcnow(),
            "estimated_completion": datetime.utcnow() + timedelta(minutes=5)
        }
        
        try:
            # Start generation in background
            asyncio.create_task(self._generate_spatial_report_async(
                report_id, boundary_type, boundary_data, start_date, today, template, format, options
            ))
            
            return report_id
            
        except Exception as e:
            logger.error(f"Failed to start spatial report generation: {e}")
            self.report_status[report_id]["status"] = ReportStatus.FAILED
            self.report_status[report_id]["error_message"] = str(e)
            raise
    
    async def _generate_weather_report_async(
        self,
        report_id: str,
        location: str,
        start_date: date,
        end_date: date,
        template: str,
        format: ReportFormat,
        options: Dict[str, Any]
    ):
        """Generate weather report asynchronously using real weather data"""
        
        try:
            # Geocode the location to get coordinates
            geocode_result = await self.geocoding_service.geocode_address(location)
            if not geocode_result.result:
                raise Exception(f"Could not geocode location: {location}")
            
            latitude = geocode_result.result.latitude
            longitude = geocode_result.result.longitude
            
            # Fetch real weather data
            weather_data = await self._fetch_comprehensive_weather_data(
                latitude, longitude, start_date, end_date, location
            )
            
            # Render template
            html_content = await self.template_engine.render_weather_template(
                template, weather_data, options
            )
            
            # Generate file based on format
            if format == ReportFormat.PDF:
                file_path = await self.pdf_generator.generate_from_html(
                    html_content, report_id
                )
            elif format == ReportFormat.EXCEL:
                file_path = await self.excel_generator.generate_from_data(
                    weather_data, report_id, options
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Update status
            self.report_status[report_id].update({
                "status": ReportStatus.COMPLETED,
                "file_path": file_path,
                "file_size": os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                "generated_at": datetime.utcnow()
            })
            
            logger.info(f"Weather report {report_id} generated successfully")
            
        except Exception as e:
            logger.error(f"Failed to generate weather report {report_id}: {e}")
            self.report_status[report_id].update({
                "status": ReportStatus.FAILED,
                "error_message": str(e)
            })
    
    async def _fetch_comprehensive_weather_data(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
        location_name: str
    ) -> Dict[str, Any]:
        """Fetch comprehensive weather data from multiple sources"""
        
        try:
            # Convert datetime to date for historical data
            start_date_obj = start_date
            end_date_obj = end_date
            
            # Fetch data from multiple sources
            current_weather = await self.weather_service.get_current_weather(
                latitude, longitude, location_name
            )
            
            forecast = await self.weather_service.get_weather_forecast(
                latitude, longitude, days=7
            )
            
            historical_weather = await self.weather_service.get_historical_weather(
                latitude, longitude, start_date_obj, end_date_obj
            )
            
            weather_events = await self.weather_service.get_weather_events(
                latitude, longitude, start_date_obj, end_date_obj
            )
            
            # Combine all data
            comprehensive_data = {
                "location": {
                    "name": location_name,
                    "latitude": latitude,
                    "longitude": longitude
                },
                "report_period": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                },
                "current_weather": current_weather,
                "forecast": forecast,
                "historical_weather": historical_weather,
                "weather_events": weather_events,
                "generated_at": datetime.utcnow().isoformat(),
                "data_sources": ["NOAA"]
            }
            
            logger.info(f"Fetched comprehensive weather data for {location_name}")
            return comprehensive_data
            
        except Exception as e:
            logger.error(f"Error fetching comprehensive weather data: {e}")
            raise
    
    async def _generate_address_report_async(
        self,
        report_id: str,
        address: str,
        start_date: date,
        end_date: date,
        template: str,
        format: ReportFormat,
        options: Dict[str, Any]
    ):
        """Generate address report asynchronously using SkyLink's address analysis"""
        
        try:
            # Perform comprehensive address analysis
            analysis_period = {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
            
            address_analysis = await self.address_analysis_service.analyze_property_address(
                address, analysis_period, options
            )
            
            # Render template
            html_content = await self.template_engine.render_address_template(
                template, address_analysis, options
            )
            
            # Generate file based on format
            if format == ReportFormat.PDF:
                file_path = await self.pdf_generator.generate_from_html(
                    html_content, report_id
                )
            elif format == ReportFormat.EXCEL:
                file_path = await self.excel_generator.generate_from_data(
                    address_analysis, report_id, options
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Update status
            self.report_status[report_id].update({
                "status": ReportStatus.COMPLETED,
                "file_path": file_path,
                "file_size": os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                "generated_at": datetime.utcnow()
            })
            
            logger.info(f"Address report {report_id} generated successfully")
            
        except Exception as e:
            logger.error(f"Failed to generate address report {report_id}: {e}")
            self.report_status[report_id].update({
                "status": ReportStatus.FAILED,
                "error_message": str(e)
            })
    
    async def _generate_spatial_report_async(
        self,
        report_id: str,
        boundary_type: str,
        boundary_data: Dict[str, Any],
        start_date: date,
        end_date: date,
        template: str,
        format: ReportFormat,
        options: Dict[str, Any]
    ):
        """Generate spatial report asynchronously using SkyLink's spatial analysis"""
        
        try:
            # Perform comprehensive spatial analysis
            analysis_period = {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            }
            
            spatial_analysis = await self.spatial_analysis_service.analyze_spatial_area(
                boundary_type, boundary_data, analysis_period, options
            )
            
            # Render template
            html_content = await self.template_engine.render_spatial_template(
                template, spatial_analysis, options
            )
            
            # Generate file based on format
            if format == ReportFormat.PDF:
                file_path = await self.pdf_generator.generate_from_html(
                    html_content, report_id
                )
            elif format == ReportFormat.EXCEL:
                file_path = await self.excel_generator.generate_from_data(
                    spatial_analysis, report_id, options
                )
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            # Update status
            self.report_status[report_id].update({
                "status": ReportStatus.COMPLETED,
                "file_path": file_path,
                "file_size": os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                "generated_at": datetime.utcnow()
            })
            
            logger.info(f"Spatial report {report_id} generated successfully")
            
        except Exception as e:
            logger.error(f"Failed to generate spatial report {report_id}: {e}")
            self.report_status[report_id].update({
                "status": ReportStatus.FAILED,
                "error_message": str(e)
            })
    
    def get_report_status(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of a report"""
        return self.report_status.get(report_id)
    
    def get_report_file_path(self, report_id: str) -> Optional[str]:
        """Get the local file path for a completed report"""
        status = self.get_report_status(report_id)
        if status and status.get("status") == ReportStatus.COMPLETED:
            return status.get("file_path")
        return None
    
    def cleanup_old_reports(self, max_age_hours: int = 24):
        """Clean up old report files and status entries"""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        reports_to_remove = []
        for report_id, status in self.report_status.items():
            if status.get("created_at", datetime.utcnow()) < cutoff_time:
                reports_to_remove.append(report_id)
                
                # Remove local file if it exists
                file_path = status.get("file_path")
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.info(f"Cleaned up old report file: {file_path}")
                    except OSError as e:
                        logger.warning(f"Failed to remove file {file_path}: {e}")
        
        # Remove status entries
        for report_id in reports_to_remove:
            del self.report_status[report_id]
        
        logger.info(f"Cleaned up {len(reports_to_remove)} old reports")