"""
Configuration management for Reports Service
"""

from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings"""

    # Service configuration
    app_name: str = "Reports Service"
    debug: bool = False

    # Google Maps API configuration
    google_maps_api_key: Optional[str] = None

    # API Key Authentication
    api_keys_enabled: bool = True
    api_key_list: Optional[str] = None  # Comma-separated list of valid API keys

    # File storage (local only)
    temp_dir: str = "/tmp/reports"
    max_file_size_mb: int = 50

    # Report generation
    default_template: str = "professional"
    supported_formats: list = ["pdf", "excel"]

    @property
    def valid_api_keys(self) -> list:
        """Parse comma-separated API keys into a list."""
        if not self.api_key_list:
            return []
        return [key.strip() for key in self.api_key_list.split(",") if key.strip()]

    # Weather alert thresholds (based on SkyLink standards)
    # Hail size thresholds (in inches)
    alert_hail_min_size_inches: float = 1.0  # Minimum actionable hail size
    hail_severe_threshold: float = 1.0       # Severe: ≥1.0 inch
    hail_extreme_threshold: float = 2.0      # Extreme: ≥2.0 inches
    hail_moderate_threshold: float = 0.5     # Moderate: ≥0.5 inches

    # Wind speed thresholds (in mph)
    alert_wind_min_speed_mph: float = 60.0   # Minimum actionable wind speed
    wind_severe_threshold: float = 60.0      # Severe: ≥60 mph (damaging winds)
    wind_extreme_threshold: float = 80.0     # Extreme: ≥80 mph
    wind_moderate_threshold: float = 40.0    # Moderate: ≥40 mph

    # Business impact scoring thresholds
    alert_min_business_impact_score: float = 30.0

    # Severity multipliers
    severity_multiplier_extreme: float = 1.5
    severity_multiplier_severe: float = 1.3
    severity_multiplier_moderate: float = 1.1
    severity_multiplier_minor: float = 0.8

    class Config:
        env_file = ".env"
        case_sensitive = False

# Global settings instance
settings = Settings()