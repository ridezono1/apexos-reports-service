"""
Pydantic models for request/response validation
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from enum import Enum

class ReportFormat(str, Enum):
    PDF = "pdf"
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"

class ReportStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"

class ReportType(str, Enum):
    WEATHER_SUMMARY = "weather_summary"
    STORM_ANALYSIS = "storm_analysis"
    ALERT_REPORT = "alert_report"
    WEATHER_TRENDS = "weather_trends"
    IMPACT_ASSESSMENT = "impact_assessment"
    SPATIAL_ANALYSIS = "spatial_analysis"

class WeatherReportRequest(BaseModel):
    """Request model for weather report generation"""
    report_type: ReportType = Field(default=ReportType.WEATHER_SUMMARY, description="Type of weather report")
    weather_data: Dict[str, Any] = Field(..., description="Weather data to include in report")
    location: str = Field(..., description="Location name")
    latitude: Optional[float] = Field(None, description="Location latitude")
    longitude: Optional[float] = Field(None, description="Location longitude")
    start_date: datetime = Field(..., description="Report start date")
    end_date: datetime = Field(..., description="Report end date")
    template: str = Field(default="address_report", description="Report template to use")
    format: ReportFormat = Field(default=ReportFormat.PDF, description="Output format")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional options")
    
class SpatialReportRequest(BaseModel):
    """Request model for spatial report generation"""
    spatial_data: Dict[str, Any] = Field(..., description="Spatial data to include in report")
    boundary_info: Dict[str, Any] = Field(..., description="Boundary information")
    location: str = Field(..., description="Location name")
    latitude: Optional[float] = Field(None, description="Location latitude")
    longitude: Optional[float] = Field(None, description="Location longitude")
    start_date: datetime = Field(..., description="Report start date")
    end_date: datetime = Field(..., description="Report end date")
    template: str = Field(default="address_report", description="Report template to use")
    format: ReportFormat = Field(default=ReportFormat.PDF, description="Output format")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional options")

class ReportResponse(BaseModel):
    """Response model for report generation"""
    report_id: str = Field(..., description="Unique report identifier")
    status: ReportStatus = Field(..., description="Current report status")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion time")
    message: Optional[str] = Field(None, description="Status message")

class ReportStatusResponse(BaseModel):
    """Response model for report status check"""
    report_id: str = Field(..., description="Report identifier")
    status: ReportStatus = Field(..., description="Current status")
    file_url: Optional[str] = Field(None, description="Download URL if completed")
    file_size: Optional[int] = Field(None, description="File size in bytes")
    generated_at: Optional[datetime] = Field(None, description="Generation completion time")
    error_message: Optional[str] = Field(None, description="Error message if failed")

class TemplateInfo(BaseModel):
    """Template information"""
    name: str = Field(..., description="Template name")
    type: str = Field(..., description="Template type (weather/spatial)")
    description: str = Field(..., description="Template description")
    supported_formats: List[ReportFormat] = Field(..., description="Supported output formats")

# Enhanced models for comprehensive weather and spatial reports

class WeatherReportType(str, Enum):
    """Weather report type enumeration."""
    WEATHER_SUMMARY = "weather_summary"
    STORM_ANALYSIS = "storm_analysis"
    ALERT_REPORT = "alert_report"
    WEATHER_TRENDS = "weather_trends"
    IMPACT_ASSESSMENT = "impact_assessment"

class WeatherReportStatus(str, Enum):
    """Weather report status enumeration."""
    DRAFT = "draft"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"
    PUBLISHED = "published"
    ARCHIVED = "archived"

class AnalysisPeriod(str, Enum):
    """Analysis period options for reports."""
    SIX_MONTHS = "6_months"
    NINE_MONTHS = "9_months"
    TWENTY_FOUR_MONTHS = "24_months"

class DateRange(BaseModel):
    """Date range for weather reports - automatically calculated from current date."""
    start: date = Field(..., description="Start date (calculated from current date)")
    end: date = Field(default_factory=lambda: date.today(), description="End date (always current date)")
    
    @validator('end')
    def validate_end_is_today(cls, v):
        """Validate that end date is today or in the past."""
        today = date.today()
        if v > today:
            raise ValueError("End date cannot be in the future")
        return v
    
    @classmethod
    def create_for_period(cls, analysis_period: 'AnalysisPeriod') -> 'DateRange':
        """Create a date range for the specified period ending today."""
        today = date.today()

        if analysis_period == AnalysisPeriod.SIX_MONTHS:
            # Calculate 6 months ago using relativedelta (handles month boundaries)
            start_date = today - relativedelta(months=6)
        elif analysis_period == AnalysisPeriod.NINE_MONTHS:
            # Calculate 9 months ago using relativedelta
            start_date = today - relativedelta(months=9)
        elif analysis_period == AnalysisPeriod.TWENTY_FOUR_MONTHS:
            # Calculate 24 months (2 years) ago using relativedelta
            start_date = today - relativedelta(months=24)
        else:
            raise ValueError(f"Unsupported analysis period: {analysis_period}")

        return cls(start=start_date, end=today)

class ReportLocation(BaseModel):
    """Location data for weather reports."""
    address: str = Field(..., description="Full address")
    coordinates: List[float] = Field(..., description="Latitude and longitude coordinates")
    city: Optional[str] = Field(None, description="City name")
    state: Optional[str] = Field(None, description="State name")
    zip_code: Optional[str] = Field(None, description="ZIP code")
    elevation: Optional[float] = Field(None, description="Elevation in feet")

class ReportSection(BaseModel):
    """Report section data."""
    id: str = Field(..., description="Section ID")
    title: str = Field(..., description="Section title")
    type: str = Field(..., description="Section type (summary, charts, tables, text, images)")
    content: Dict[str, Any] = Field(..., description="Section content")
    order: int = Field(..., description="Section order")
    visible: bool = Field(True, description="Whether section is visible")

class HistoricalDataOptions(BaseModel):
    """Options for including historical data in reports."""
    include_historical: bool = Field(False, description="Include historical weather data")
    historical_years_back: int = Field(5, ge=1, le=20, description="Number of years of historical data to include")
    include_comparison: bool = Field(True, description="Include comparison with historical averages")
    include_trends: bool = Field(True, description="Include trend analysis")
    include_extremes: bool = Field(True, description="Include historical extreme weather events")
    data_resolution: str = Field("daily", description="Data resolution: daily, weekly, monthly")

class ReportMetadata(BaseModel):
    """Report metadata."""
    total_pages: int = Field(..., description="Total number of pages")
    file_size: int = Field(..., description="File size in bytes")
    includes_charts: bool = Field(False, description="Whether report includes charts")
    includes_forecast: bool = Field(False, description="Whether report includes forecast")
    includes_storm_events: bool = Field(False, description="Whether report includes storm events")
    includes_historical_data: bool = Field(False, description="Whether report includes historical data")
    generation_time: Optional[float] = Field(None, description="Generation time in seconds")

class AdministrativeBoundaryResponse(BaseModel):
    """Response schema for administrative boundary."""
    id: str
    name: str
    type: str
    code: Optional[str] = None
    
    # Geographic hierarchy
    state_name: Optional[str] = None
    state_code: Optional[str] = None
    county_name: Optional[str] = None
    county_code: Optional[str] = None
    city_name: Optional[str] = None
    
    # Properties
    area_sq_km: Optional[float] = None
    population: Optional[int] = None
    population_density: Optional[float] = None
    
    # Display properties
    full_name: str
    display_name: str
    
    # Data quality
    data_source: Optional[str] = None
    data_quality: Optional[str] = None

class SpatialGridPoint(BaseModel):
    """Schema for spatial grid point."""
    grid_id: Union[str, int]
    latitude: float
    longitude: float
    risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: str = Field(pattern="^(low|medium|high)$")
    weather_data: Dict[str, Any] = Field(default_factory=dict)

class RiskZone(BaseModel):
    """Schema for risk zone data."""
    count: int = Field(ge=0)
    percentage: float = Field(ge=0.0, le=100.0)
    areas: List[SpatialGridPoint] = Field(default_factory=list)

class RiskAssessment(BaseModel):
    """Schema for risk assessment results."""
    overall_risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: str = Field(pattern="^(low|medium|high)$")
    high_risk_areas: RiskZone
    medium_risk_areas: RiskZone
    low_risk_areas: RiskZone

class WeatherEventSummary(BaseModel):
    """Schema for weather event summary."""
    total_events: int = Field(ge=0)
    events_by_type: Dict[str, int] = Field(default_factory=dict)
    events_by_severity: Dict[str, int] = Field(default_factory=dict)

class SpatialStatistics(BaseModel):
    """Schema for spatial statistics."""
    total_events: int = Field(ge=0)
    events_per_sq_km: float = Field(ge=0.0)
    overall_risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: str = Field(pattern="^(low|medium|high)$")
    analysis_coverage: str

class HeatMapData(BaseModel):
    """Schema for heat map visualization data."""
    parameter: str
    color_scale: List[Dict[str, Any]] = Field(default_factory=list)
    data_points: List[Dict[str, Any]] = Field(default_factory=list)
    legend: Dict[str, Any] = Field(default_factory=dict)

class SubAreaStatistics(BaseModel):
    """Schema for sub-area statistical summary."""
    sub_area_id: str
    sub_area_name: str
    sub_area_type: str
    
    # Weather statistics
    total_events: int = Field(ge=0)
    risk_score: float = Field(ge=0.0, le=1.0)
    risk_level: str = Field(pattern="^(low|medium|high)$")
    
    # Weather measurements
    avg_temperature: Optional[float] = None
    total_precipitation: Optional[float] = None
    max_wind_speed: Optional[float] = None
    
    # Event breakdown
    hail_events: int = Field(default=0, ge=0)
    wind_events: int = Field(default=0, ge=0)
    tornado_events: int = Field(default=0, ge=0)
    
    # Area properties
    area_sq_km: Optional[float] = None
    population: Optional[int] = None

class ComparativeAnalysis(BaseModel):
    """Schema for comparative analysis across sub-areas."""
    highest_risk_area: Optional[str] = None
    most_active_area: Optional[str] = None
    temperature_range: Dict[str, float] = Field(default_factory=dict)
    precipitation_range: Dict[str, float] = Field(default_factory=dict)
    risk_distribution: Dict[str, int] = Field(default_factory=dict)

class StormImpactZone(BaseModel):
    """Schema for storm impact zone."""
    zone_id: str
    impact_level: str = Field(pattern="^(low|medium|high)$")
    storm_events: List[str] = Field(default_factory=list)
    affected_area_sq_km: float = Field(ge=0.0)
    population_exposure: Optional[int] = None
    property_exposure: Optional[int] = None
    center_latitude: float
    center_longitude: float
    radius_km: float = Field(gt=0.0)

class InterpolatedWeatherData(BaseModel):
    """Schema for interpolated weather data."""
    parameter: str
    grid_points: List[Dict[str, Any]] = Field(default_factory=list)
    contour_data: Optional[Dict[str, Any]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    average_value: Optional[float] = None

# Enhanced request models for comprehensive integration

class WeatherReportCreate(BaseModel):
    """Schema for creating weather reports with comprehensive capabilities."""
    title: str = Field(..., min_length=1, max_length=200, description="Report title")
    type: WeatherReportType = Field(..., description="Report type")
    location: str = Field(..., description="Report location")
    analysis_period: AnalysisPeriod = Field(..., description="Analysis period: 6 months or 9 months from current date")
    template: str = Field("address_report", description="Report template")
    include_charts: bool = Field(True, description="Include charts in report")
    include_forecast: bool = Field(False, description="Include weather forecast")
    include_storm_events: bool = Field(True, description="Include storm events")
    historical_data: Optional[HistoricalDataOptions] = Field(None, description="Historical data options")
    generate_pdf: bool = Field(True, description="Generate PDF version")
    branding: bool = Field(True, description="Include company branding")
    color_scheme: str = Field("address_report", description="Report color scheme")
    latitude: Optional[float] = Field(None, description="Location latitude")
    longitude: Optional[float] = Field(None, description="Location longitude")
    
    @property
    def date_range(self) -> DateRange:
        """Automatically generate date range from analysis period."""
        return DateRange.create_for_period(self.analysis_period)

class SpatialWeatherReportCreate(BaseModel):
    """Schema for creating spatial weather reports."""
    title: str = Field(min_length=1, max_length=255)
    boundary_id: str = Field(min_length=1)
    analysis_period: AnalysisPeriod = Field(..., description="Analysis period: 6 months or 9 months from current date")

    # Optional boundary data - if provided, skip OSM lookup
    boundary_name: Optional[str] = Field(None, description="Human-readable boundary name")
    boundary_geometry: Optional[Dict[str, Any]] = Field(None, description="GeoJSON geometry for the boundary")
    boundary_center: Optional[Dict[str, float]] = Field(None, description="Center point with latitude/longitude")

    # Analysis options
    risk_factors: List[str] = Field(default=["hail", "wind", "tornado"])
    storm_types: List[str] = Field(default=["hail", "wind", "tornado"])
    include_risk_assessment: bool = Field(default=True)
    include_storm_impact_zones: bool = Field(default=True)
    include_weather_interpolation: bool = Field(default=True)
    include_statistical_summaries: bool = Field(default=True)

    # Sub-area analysis
    sub_area_level: str = Field(default="auto", pattern="^(auto|county|city|neighborhood)$")

    # Report format options
    include_heat_maps: bool = Field(default=True)
    include_charts: bool = Field(default=True)
    generate_pdf: bool = Field(default=True)

    # Weather parameters for interpolation
    weather_parameters: List[str] = Field(
        default=["temperature_high", "temperature_low", "precipitation", "wind_speed"]
    )
    
    @property
    def date_range(self) -> DateRange:
        """Automatically generate date range from analysis period."""
        return DateRange.create_for_period(self.analysis_period)

class WeatherReportResponse(BaseModel):
    """Schema for weather report response."""
    id: str = Field(..., description="Report ID")
    title: str = Field(..., description="Report title")
    type: WeatherReportType = Field(..., description="Report type")
    location: str = Field(..., description="Report location")
    date_range: DateRange = Field(..., description="Date range for the report")
    template: str = Field(..., description="Report template")
    include_charts: bool = Field(..., description="Include charts in report")
    include_forecast: bool = Field(..., description="Include weather forecast")
    include_storm_events: bool = Field(..., description="Include storm events")
    historical_data: Optional[HistoricalDataOptions] = Field(None, description="Historical data options")
    generate_pdf: bool = Field(..., description="Generate PDF version")
    branding: bool = Field(..., description="Include company branding")
    color_scheme: str = Field(..., description="Report color scheme")
    latitude: Optional[float] = Field(None, description="Location latitude")
    longitude: Optional[float] = Field(None, description="Location longitude")
    generated_at: datetime = Field(..., description="Generation timestamp")
    generated_by: str = Field(..., description="User who generated the report")
    sections: List[ReportSection] = Field(..., description="Report sections")
    metadata: ReportMetadata = Field(..., description="Report metadata")
    status: WeatherReportStatus = Field(..., description="Report status")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    weather_data: Optional[Dict[str, Any]] = Field(None, description="Weather data for the report period")

class SpatialWeatherReportResponse(BaseModel):
    """Response schema for spatial weather reports."""
    id: str
    title: str
    
    # Boundary information
    boundary: AdministrativeBoundaryResponse
    
    # Analysis configuration
    analysis_period: Dict[str, str]
    risk_factors: List[str]
    storm_types: List[str]
    sub_area_level: str
    
    # Analysis results
    weather_events: WeatherEventSummary
    risk_assessment: Optional[RiskAssessment] = None
    storm_impact_zones: Optional[Dict[str, Any]] = None
    interpolated_weather: Optional[Dict[str, Any]] = None
    
    # Statistical summaries
    overall_statistics: SpatialStatistics
    sub_area_statistics: List[SubAreaStatistics] = Field(default_factory=list)
    comparative_analysis: Optional[ComparativeAnalysis] = None
    
    # Visualization data
    heat_map_data: List[HeatMapData] = Field(default_factory=list)
    spatial_data: Dict[str, Any] = Field(default_factory=dict)
    
    # Metadata
    generated_at: datetime
    generated_by: str
    processing_time: Optional[float] = None
    
    # Report files
    pdf_url: Optional[str] = None
    data_export_url: Optional[str] = None