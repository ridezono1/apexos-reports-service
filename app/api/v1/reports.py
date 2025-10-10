"""
API endpoints for weather reports service
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse
from typing import Dict, Any, List
import logging
import sentry_sdk

from app.models import (
    WeatherReportCreate, SpatialWeatherReportCreate, ReportResponse,
    ReportStatusResponse, TemplateInfo, ReportFormat, ReportType,
    AnalysisPeriod
)
from app.services.report_generator import ReportGenerator
from app.core.auth import get_api_key

logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Initialize report generator
report_generator = ReportGenerator()

@router.post("/reports/generate/weather", response_model=ReportResponse)
async def generate_weather_report(request: WeatherReportCreate):
    """Generate a weather report with analysis period from current date"""
    
    try:
        report_id = await report_generator.generate_weather_report(
            location=request.location,
            analysis_period=request.analysis_period.value,
            template=request.template,
            format=request.generate_pdf and ReportFormat.PDF or ReportFormat.EXCEL,
            options={
                "include_charts": request.include_charts,
                "include_forecast": request.include_forecast,
                "include_storm_events": request.include_storm_events,
                "historical_data": request.historical_data,
                "branding": request.branding,
                "color_scheme": request.color_scheme,
                "latitude": request.latitude,
                "longitude": request.longitude
            }
        )
        
        status = report_generator.get_report_status(report_id)
        
        return ReportResponse(
            report_id=report_id,
            status=status["status"],
            estimated_completion=status.get("estimated_completion"),
            message=f"Weather report generation started for {request.location} ({request.analysis_period.value})"
        )
        
    except Exception as e:
        logger.error(f"Failed to generate weather report: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=f"Failed to generate weather report: {str(e)}")

@router.post("/reports/generate/spatial", response_model=ReportResponse)
async def generate_spatial_report(request: SpatialWeatherReportCreate):
    """Generate a spatial report with analysis period from current date"""

    try:
        report_id = await report_generator.generate_spatial_report(
            boundary_type="county",  # Default boundary type
            boundary_data={"name": request.boundary_id},
            analysis_period=request.analysis_period.value,
            template="professional",  # Default template for spatial reports
            format=request.generate_pdf and ReportFormat.PDF or ReportFormat.EXCEL,
            options={
                "boundary_name": request.boundary_id,  # Pass boundary name for template
                "risk_factors": request.risk_factors,
                "storm_types": request.storm_types,
                "include_risk_assessment": request.include_risk_assessment,
                "include_storm_impact_zones": request.include_storm_impact_zones,
                "include_weather_interpolation": request.include_weather_interpolation,
                "include_statistical_summaries": request.include_statistical_summaries,
                "sub_area_level": request.sub_area_level,
                "include_heat_maps": request.include_heat_maps,
                "include_charts": request.include_charts,
                "weather_parameters": request.weather_parameters
            }
        )
        
        status = report_generator.get_report_status(report_id)
        
        return ReportResponse(
            report_id=report_id,
            status=status["status"],
            estimated_completion=status.get("estimated_completion"),
            message=f"Spatial report generation started for {request.boundary_id} ({request.analysis_period.value})"
        )
        
    except Exception as e:
        logger.error(f"Failed to generate spatial report: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/reports/generate/address", response_model=ReportResponse)
async def generate_address_report(
    request: WeatherReportCreate,
    api_key: str = Depends(get_api_key)
):
    """Generate an address-specific weather report with property analysis"""
    
    try:
        report_id = await report_generator.generate_address_report(
            address=request.location,
            analysis_period=request.analysis_period.value,
            template="address_report",
            format=request.generate_pdf and ReportFormat.PDF or ReportFormat.EXCEL,
            options={
                "include_charts": request.include_charts,
                "include_forecast": request.include_forecast,
                "include_storm_events": request.include_storm_events,
                "historical_data": request.historical_data,
                "branding": request.branding,
                "color_scheme": request.color_scheme,
                "latitude": request.latitude,
                "longitude": request.longitude,
                "title": request.title
            }
        )
        
        status = report_generator.get_report_status(report_id)
        
        return ReportResponse(
            report_id=report_id,
            status=status["status"],
            estimated_completion=status.get("estimated_completion"),
            message=f"Address report generation started for {request.location} ({request.analysis_period.value})"
        )
        
    except Exception as e:
        logger.error(f"Failed to generate address report: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/reports/{report_id}/status", response_model=ReportStatusResponse)
async def get_report_status(
    report_id: str,
    api_key: str = Depends(get_api_key)
):
    """Get the current status of a report"""
    
    status = report_generator.get_report_status(report_id)
    
    if not status:
        raise HTTPException(status_code=404, detail="Report not found")
    
    return ReportStatusResponse(
        report_id=report_id,
        status=status["status"],
        file_url=status.get("file_url"),
        file_size=status.get("file_size"),
        generated_at=status.get("generated_at"),
        error_message=status.get("error_message")
    )

@router.get("/reports/{report_id}/download")
async def download_report(
    report_id: str,
    api_key: str = Depends(get_api_key)
):
    """Download a completed report"""
    
    status = report_generator.get_report_status(report_id)
    
    if not status:
        raise HTTPException(status_code=404, detail="Report not found")
    
    if status["status"] != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Report is not ready. Current status: {status['status']}"
        )
    
    file_path = report_generator.get_report_file_path(report_id)
    
    if not file_path:
        raise HTTPException(status_code=404, detail="Report file not found")
    
    # Determine content type based on format
    format = status.get("format", "pdf")
    media_type = "application/pdf" if format == "pdf" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=f"weather_report_{report_id}.{format}"
    )

@router.get("/templates", response_model=List[TemplateInfo])
async def list_templates():
    """List available report templates"""
    
    templates = [
        TemplateInfo(
            name="professional",
            type="weather",
            description="Professional weather report with comprehensive analysis and charts",
            supported_formats=[ReportFormat.PDF, ReportFormat.EXCEL]
        ),
        TemplateInfo(
            name="executive",
            type="weather",
            description="Executive summary weather report for decision makers",
            supported_formats=[ReportFormat.PDF]
        ),
        TemplateInfo(
            name="detailed",
            type="weather",
            description="Detailed weather report with historical data and trends",
            supported_formats=[ReportFormat.PDF, ReportFormat.EXCEL]
        ),
        TemplateInfo(
            name="storm_analysis",
            type="weather",
            description="Storm-focused analysis with impact assessment",
            supported_formats=[ReportFormat.PDF]
        ),
        TemplateInfo(
            name="professional",
            type="spatial",
            description="Professional spatial weather report with heat maps and risk zones",
            supported_formats=[ReportFormat.PDF, ReportFormat.EXCEL]
        ),
        TemplateInfo(
            name="spatial_analysis",
            type="spatial",
            description="Comprehensive spatial analysis with boundary visualization",
            supported_formats=[ReportFormat.PDF]
        )
    ]
    
    return templates

@router.delete("/reports/{report_id}")
async def delete_report(report_id: str):
    """Delete a report and its associated files"""
    
    status = report_generator.get_report_status(report_id)
    
    if not status:
        raise HTTPException(status_code=404, detail="Report not found")
    
    try:
        # Remove from status tracking
        if report_id in report_generator.report_status:
            del report_generator.report_status[report_id]
        
        # Remove local file if it exists
        file_path = report_generator.get_report_file_path(report_id)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        
        return {"message": "Report deleted successfully"}
        
    except Exception as e:
        logger.error(f"Failed to delete report {report_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete report")

@router.post("/reports/cleanup")
async def cleanup_old_reports(max_age_hours: int = 24):
    """Clean up old reports and files"""
    
    try:
        report_generator.cleanup_old_reports(max_age_hours)
        return {"message": f"Cleaned up reports older than {max_age_hours} hours"}
        
    except Exception as e:
        logger.error(f"Failed to cleanup reports: {e}")
        raise HTTPException(status_code=500, detail="Failed to cleanup reports")