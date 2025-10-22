"""
Weather thresholds and classifications based on SkyLink standards.

This module defines actionable thresholds for weather events that represent
business opportunities for property damage assessment, inspections, and
insurance claims in the roofing and property services industry.
"""

from typing import Dict, List, Tuple
from app.core.config import settings


class WeatherThresholds:
    """Weather event thresholds and classifications."""

    # Hail size thresholds (in inches)
    HAIL_MIN_ACTIONABLE = settings.alert_hail_min_size_inches  # 1.0 inch
    HAIL_SEVERE = settings.hail_severe_threshold  # 1.0 inch
    HAIL_EXTREME = settings.hail_extreme_threshold  # 2.0 inches
    HAIL_MODERATE = settings.hail_moderate_threshold  # 0.5 inches

    # Wind speed thresholds (in mph)
    WIND_MIN_ACTIONABLE = settings.alert_wind_min_speed_mph  # 60 mph
    WIND_SEVERE = settings.wind_severe_threshold  # 60 mph (damaging winds)
    WIND_EXTREME = settings.wind_extreme_threshold  # 80 mph
    WIND_MODERATE = settings.wind_moderate_threshold  # 40 mph

    # Hail probability thresholds (percentage)
    HAIL_PROBABILITY_HIGH = 60.0  # High risk threshold
    HAIL_PROBABILITY_MODERATE = 30.0  # Moderate risk threshold
    HAIL_PROBABILITY_LOW = 10.0  # Low risk threshold

    # Fire index thresholds (0-100 scale)
    FIRE_INDEX_EXTREME = 80.0  # Extreme fire danger
    FIRE_INDEX_HIGH = 60.0  # High fire danger
    FIRE_INDEX_MODERATE = 40.0  # Moderate fire danger
    FIRE_INDEX_LOW = 20.0  # Low fire danger

    # Hail size descriptors and their corresponding sizes
    HAIL_SIZE_DESCRIPTORS: Dict[str, float] = {
        "pea": 0.25,
        "marble": 0.5,
        "penny": 0.75,
        "nickel": 0.88,
        "quarter": 1.0,  # ACTIONABLE
        "half dollar": 1.25,
        "ping pong ball": 1.5,
        "walnut": 1.5,
        "golf ball": 1.75,
        "tennis ball": 2.5,  # EXTREME
        "baseball": 2.75,
        "tea cup": 3.0,
        "softball": 4.0,
        "grapefruit": 4.5,
    }

    # Wind speed descriptors
    WIND_DESCRIPTORS: List[str] = [
        "damaging winds",
        "severe winds",
        "straight line winds",
        "high winds",
        "extreme winds",
        "gusts",
    ]

    # Business impact scoring for hail sizes
    HAIL_BUSINESS_IMPACT: Dict[str, int] = {
        "quarter": 25,  # ~1.0 inch
        "half dollar": 30,  # ~1.25 inch
        "walnut": 35,  # ~1.5 inch
        "golf ball": 40,  # ~1.75 inch
        "tennis ball": 50,  # ~2.5 inch
        "baseball": 60,  # ~2.75 inch
        "softball": 70,  # ~4.0 inch
    }

    # Business impact scoring for wind speeds
    WIND_BUSINESS_IMPACT: Dict[int, int] = {
        60: 30,  # Minimum actionable
        70: 35,
        80: 40,
        90: 45,
        100: 50,
    }

    # Severity multipliers
    SEVERITY_MULTIPLIERS: Dict[str, float] = {
        "extreme": settings.severity_multiplier_extreme,  # 1.5x
        "severe": settings.severity_multiplier_severe,  # 1.3x
        "moderate": settings.severity_multiplier_moderate,  # 1.1x
        "minor": settings.severity_multiplier_minor,  # 0.8x
    }

    @classmethod
    def classify_hail_severity(cls, size_inches: float) -> str:
        """
        Classify hail severity based on size.

        Args:
            size_inches: Hail size in inches

        Returns:
            Severity classification: extreme, severe, moderate, or minor
        """
        if size_inches >= cls.HAIL_EXTREME:
            return "extreme"
        elif size_inches >= cls.HAIL_SEVERE:
            return "severe"
        elif size_inches >= cls.HAIL_MODERATE:
            return "moderate"
        else:
            return "minor"

    @classmethod
    def classify_wind_severity(cls, speed_mph: float) -> str:
        """
        Classify wind severity based on speed.

        Args:
            speed_mph: Wind speed in mph

        Returns:
            Severity classification: extreme, severe, moderate, or minor
        """
        if speed_mph >= cls.WIND_EXTREME:
            return "extreme"
        elif speed_mph >= cls.WIND_SEVERE:
            return "severe"
        elif speed_mph >= cls.WIND_MODERATE:
            return "moderate"
        else:
            return "minor"

    @classmethod
    def classify_hail_probability(cls, probability_percent: float) -> str:
        """
        Classify hail probability risk level.

        Args:
            probability_percent: Hail probability percentage (0-100)

        Returns:
            Risk classification: high, moderate, low, or minimal
        """
        if probability_percent >= cls.HAIL_PROBABILITY_HIGH:
            return "high"
        elif probability_percent >= cls.HAIL_PROBABILITY_MODERATE:
            return "moderate"
        elif probability_percent >= cls.HAIL_PROBABILITY_LOW:
            return "low"
        else:
            return "minimal"

    @classmethod
    def classify_fire_index(cls, fire_index: float) -> str:
        """
        Classify fire danger index level.

        Args:
            fire_index: Fire index value (0-100)

        Returns:
            Fire danger classification: extreme, high, moderate, low, or minimal
        """
        if fire_index >= cls.FIRE_INDEX_EXTREME:
            return "extreme"
        elif fire_index >= cls.FIRE_INDEX_HIGH:
            return "high"
        elif fire_index >= cls.FIRE_INDEX_MODERATE:
            return "moderate"
        elif fire_index >= cls.FIRE_INDEX_LOW:
            return "low"
        else:
            return "minimal"

    @classmethod
    def is_hail_actionable(cls, size_inches: float) -> bool:
        """Check if hail size is actionable for business purposes."""
        return size_inches >= cls.HAIL_MIN_ACTIONABLE

    @classmethod
    def is_wind_actionable(cls, speed_mph: float) -> bool:
        """Check if wind speed is actionable for business purposes."""
        return speed_mph >= cls.WIND_MIN_ACTIONABLE

    @classmethod
    def is_hail_probability_actionable(cls, probability_percent: float) -> bool:
        """Check if hail probability is actionable for business purposes."""
        return probability_percent >= cls.HAIL_PROBABILITY_MODERATE

    @classmethod
    def is_fire_index_actionable(cls, fire_index: float) -> bool:
        """Check if fire index is actionable for business purposes."""
        return fire_index >= cls.FIRE_INDEX_MODERATE

    @classmethod
    def get_hail_descriptor(cls, size_inches: float) -> str:
        """
        Get the best descriptor for a hail size.

        Args:
            size_inches: Hail size in inches

        Returns:
            Descriptor string (e.g., "golf ball", "quarter")
        """
        closest_descriptor = "unknown"
        min_diff = float("inf")

        for descriptor, size in cls.HAIL_SIZE_DESCRIPTORS.items():
            diff = abs(size - size_inches)
            if diff < min_diff:
                min_diff = diff
                closest_descriptor = descriptor

        return closest_descriptor

    @classmethod
    def parse_hail_size_from_text(cls, text: str) -> float:
        """
        Parse hail size from text description.

        Args:
            text: Text containing hail size information

        Returns:
            Hail size in inches, or 0 if not found
        """
        text_lower = text.lower()

        # Check for descriptors
        for descriptor, size in cls.HAIL_SIZE_DESCRIPTORS.items():
            if descriptor in text_lower:
                return size

        # Check for numeric sizes
        import re

        # Match patterns like "1 inch", "1.5 inches", "2.0 inch"
        match = re.search(r"(\d+\.?\d*)\s*inch", text_lower)
        if match:
            return float(match.group(1))

        return 0.0

    @classmethod
    def parse_wind_speed_from_text(cls, text: str) -> float:
        """
        Parse wind speed from text description.

        Args:
            text: Text containing wind speed information

        Returns:
            Wind speed in mph, or 0 if not found
        """
        import re

        text_lower = text.lower()

        # Match patterns like "60 mph", "80mph", "70 miles per hour"
        patterns = [
            r"(\d+)\s*mph",
            r"(\d+)\s*miles per hour",
            r"winds?.*?(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text_lower)
            if match:
                return float(match.group(1))

        # Check for wind descriptors
        if any(desc in text_lower for desc in cls.WIND_DESCRIPTORS):
            # If we have a descriptor but no speed, assume minimum actionable
            return cls.WIND_MIN_ACTIONABLE

        return 0.0

    @classmethod
    def calculate_business_impact_score(
        cls, 
        hail_size: float = 0, 
        wind_speed: float = 0, 
        tornado: bool = False,
        hail_probability: float = 0,
        fire_index: float = 0
    ) -> int:
        """
        Calculate business impact score based on weather conditions.

        Args:
            hail_size: Hail size in inches
            wind_speed: Wind speed in mph
            tornado: Whether tornado is present
            hail_probability: Hail probability percentage (0-100)
            fire_index: Fire danger index (0-100)

        Returns:
            Business impact score (0-100+)
        """
        score = 0

        # Hail impact
        if hail_size >= 4.0:  # Softball
            score += 70
        elif hail_size >= 2.75:  # Baseball
            score += 60
        elif hail_size >= 2.5:  # Tennis ball
            score += 50
        elif hail_size >= 1.75:  # Golf ball
            score += 40
        elif hail_size >= 1.5:  # Walnut
            score += 35
        elif hail_size >= 1.25:  # Half dollar
            score += 30
        elif hail_size >= 1.0:  # Quarter - minimum actionable
            score += 25

        # Wind impact
        if wind_speed >= 100:
            score += 50
        elif wind_speed >= 90:
            score += 45
        elif wind_speed >= 80:
            score += 40
        elif wind_speed >= 70:
            score += 35
        elif wind_speed >= 60:  # Minimum actionable
            score += 30

        # Tornado impact (highest)
        if tornado:
            score += 80

        # Hail probability impact (probabilistic risk)
        if hail_probability >= cls.HAIL_PROBABILITY_HIGH:
            score += 20  # High probability adds significant risk
        elif hail_probability >= cls.HAIL_PROBABILITY_MODERATE:
            score += 10  # Moderate probability adds some risk
        elif hail_probability >= cls.HAIL_PROBABILITY_LOW:
            score += 5   # Low probability adds minimal risk

        # Fire index impact (fire danger)
        if fire_index >= cls.FIRE_INDEX_EXTREME:
            score += 25  # Extreme fire danger
        elif fire_index >= cls.FIRE_INDEX_HIGH:
            score += 15  # High fire danger
        elif fire_index >= cls.FIRE_INDEX_MODERATE:
            score += 10  # Moderate fire danger
        elif fire_index >= cls.FIRE_INDEX_LOW:
            score += 5   # Low fire danger

        # Apply severity multiplier
        if hail_size >= cls.HAIL_EXTREME or wind_speed >= cls.WIND_EXTREME or tornado:
            score = int(score * cls.SEVERITY_MULTIPLIERS["extreme"])
        elif hail_size >= cls.HAIL_SEVERE or wind_speed >= cls.WIND_SEVERE:
            score = int(score * cls.SEVERITY_MULTIPLIERS["severe"])
        elif hail_size >= cls.HAIL_MODERATE or wind_speed >= cls.WIND_MODERATE:
            score = int(score * cls.SEVERITY_MULTIPLIERS["moderate"])

        return score


# Export for easy importing
__all__ = ["WeatherThresholds"]
