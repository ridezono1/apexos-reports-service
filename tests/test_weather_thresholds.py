"""
Tests for weather thresholds and classifications.
"""

import pytest
from app.core.weather_thresholds import WeatherThresholds


def test_hail_severity_classifications():
    """Test hail severity classification."""
    # Minor (< 0.5 inches)
    assert WeatherThresholds.classify_hail_severity(0.25) == "minor"
    assert WeatherThresholds.classify_hail_severity(0.4) == "minor"

    # Moderate (≥ 0.5 inches)
    assert WeatherThresholds.classify_hail_severity(0.5) == "moderate"
    assert WeatherThresholds.classify_hail_severity(0.75) == "moderate"

    # Severe (≥ 1.0 inch)
    assert WeatherThresholds.classify_hail_severity(1.0) == "severe"
    assert WeatherThresholds.classify_hail_severity(1.5) == "severe"

    # Extreme (≥ 2.0 inches)
    assert WeatherThresholds.classify_hail_severity(2.0) == "extreme"
    assert WeatherThresholds.classify_hail_severity(3.0) == "extreme"


def test_wind_severity_classifications():
    """Test wind speed severity classification."""
    # Minor (< 40 mph)
    assert WeatherThresholds.classify_wind_severity(20) == "minor"
    assert WeatherThresholds.classify_wind_severity(35) == "minor"

    # Moderate (≥ 40 mph)
    assert WeatherThresholds.classify_wind_severity(40) == "moderate"
    assert WeatherThresholds.classify_wind_severity(55) == "moderate"

    # Severe (≥ 60 mph - damaging winds)
    assert WeatherThresholds.classify_wind_severity(60) == "severe"
    assert WeatherThresholds.classify_wind_severity(75) == "severe"

    # Extreme (≥ 80 mph)
    assert WeatherThresholds.classify_wind_severity(80) == "extreme"
    assert WeatherThresholds.classify_wind_severity(100) == "extreme"


def test_hail_actionable_threshold():
    """Test hail actionable threshold (≥ 1.0 inch)."""
    # Not actionable
    assert WeatherThresholds.is_hail_actionable(0.5) == False
    assert WeatherThresholds.is_hail_actionable(0.9) == False

    # Actionable
    assert WeatherThresholds.is_hail_actionable(1.0) == True
    assert WeatherThresholds.is_hail_actionable(1.5) == True
    assert WeatherThresholds.is_hail_actionable(2.0) == True


def test_wind_actionable_threshold():
    """Test wind actionable threshold (≥ 60 mph)."""
    # Not actionable
    assert WeatherThresholds.is_wind_actionable(40) == False
    assert WeatherThresholds.is_wind_actionable(55) == False

    # Actionable (damaging winds)
    assert WeatherThresholds.is_wind_actionable(60) == True
    assert WeatherThresholds.is_wind_actionable(75) == True
    assert WeatherThresholds.is_wind_actionable(100) == True


def test_hail_descriptors():
    """Test hail size descriptors mapping."""
    assert WeatherThresholds.get_hail_descriptor(0.25) == "pea"
    assert WeatherThresholds.get_hail_descriptor(1.0) == "quarter"
    assert WeatherThresholds.get_hail_descriptor(1.75) == "golf ball"
    assert WeatherThresholds.get_hail_descriptor(2.75) == "baseball"
    assert WeatherThresholds.get_hail_descriptor(4.0) == "softball"


def test_parse_hail_size_from_text():
    """Test parsing hail size from text descriptions."""
    # Descriptors
    assert WeatherThresholds.parse_hail_size_from_text("quarter size hail") == 1.0
    assert WeatherThresholds.parse_hail_size_from_text("golf ball hail") == 1.75
    assert WeatherThresholds.parse_hail_size_from_text("baseball size hail") == 2.75
    assert WeatherThresholds.parse_hail_size_from_text("softball hail") == 4.0

    # Numeric sizes
    assert WeatherThresholds.parse_hail_size_from_text("1 inch hail") == 1.0
    assert WeatherThresholds.parse_hail_size_from_text("1.5 inch hail") == 1.5
    assert WeatherThresholds.parse_hail_size_from_text("2.0 inches") == 2.0

    # No match
    assert WeatherThresholds.parse_hail_size_from_text("no hail mentioned") == 0.0


def test_parse_wind_speed_from_text():
    """Test parsing wind speed from text descriptions."""
    # Numeric speeds
    assert WeatherThresholds.parse_wind_speed_from_text("60 mph winds") == 60.0
    assert WeatherThresholds.parse_wind_speed_from_text("80mph") == 80.0
    assert WeatherThresholds.parse_wind_speed_from_text("winds up to 70 mph") == 70.0

    # Descriptors
    assert WeatherThresholds.parse_wind_speed_from_text("damaging winds") == 60.0
    assert WeatherThresholds.parse_wind_speed_from_text("severe winds") == 60.0
    assert WeatherThresholds.parse_wind_speed_from_text("straight line winds") == 60.0

    # No match
    assert WeatherThresholds.parse_wind_speed_from_text("calm conditions") == 0.0


def test_business_impact_score_hail_only():
    """Test business impact scoring for hail."""
    # Quarter size (minimum actionable)
    assert WeatherThresholds.calculate_business_impact_score(hail_size=1.0) >= 25

    # Golf ball
    assert WeatherThresholds.calculate_business_impact_score(hail_size=1.75) >= 40

    # Baseball
    assert WeatherThresholds.calculate_business_impact_score(hail_size=2.75) >= 60

    # Softball
    assert WeatherThresholds.calculate_business_impact_score(hail_size=4.0) >= 70


def test_business_impact_score_wind_only():
    """Test business impact scoring for wind."""
    # 60 mph (minimum actionable)
    assert WeatherThresholds.calculate_business_impact_score(wind_speed=60) >= 30

    # 80 mph
    assert WeatherThresholds.calculate_business_impact_score(wind_speed=80) >= 40

    # 100 mph
    assert WeatherThresholds.calculate_business_impact_score(wind_speed=100) >= 50


def test_business_impact_score_combined():
    """Test business impact scoring with combined conditions."""
    # Golf ball hail + 60 mph winds
    combined_score = WeatherThresholds.calculate_business_impact_score(
        hail_size=1.75, wind_speed=60
    )
    hail_only = WeatherThresholds.calculate_business_impact_score(hail_size=1.75)
    wind_only = WeatherThresholds.calculate_business_impact_score(wind_speed=60)

    # Combined should be higher than either alone
    assert combined_score > hail_only
    assert combined_score > wind_only


def test_business_impact_score_tornado():
    """Test business impact scoring for tornado."""
    # Tornado alone
    tornado_score = WeatherThresholds.calculate_business_impact_score(tornado=True)
    assert tornado_score >= 80  # High base score

    # Tornado with hail and wind (extreme conditions)
    extreme_score = WeatherThresholds.calculate_business_impact_score(
        hail_size=2.0, wind_speed=80, tornado=True
    )
    # Should be very high with extreme multiplier
    assert extreme_score > tornado_score


def test_severity_multipliers():
    """Test that severity multipliers are applied correctly."""
    # Extreme conditions (hail ≥ 2.0)
    base_score = 40
    extreme_hail = WeatherThresholds.calculate_business_impact_score(hail_size=2.0)
    # Should have extreme multiplier (1.5x)
    assert extreme_hail > base_score * 1.4  # Allow some tolerance

    # Severe conditions (wind ≥ 60)
    severe_wind = WeatherThresholds.calculate_business_impact_score(wind_speed=60)
    # Should have severe multiplier (1.3x)
    assert severe_wind >= 30  # Base score * multiplier


def test_hail_size_thresholds_cypress_tx():
    """Test hail sizes relevant to Cypress, TX area."""
    # Common hail sizes in Texas storms
    test_sizes = {
        "pea": 0.25,
        "marble": 0.5,
        "quarter": 1.0,  # ACTIONABLE - roof damage likely
        "golf ball": 1.75,  # SEVERE - significant roof damage
        "tennis ball": 2.5,  # EXTREME - major damage
    }

    for descriptor, size in test_sizes.items():
        severity = WeatherThresholds.classify_hail_severity(size)
        is_actionable = WeatherThresholds.is_hail_actionable(size)

        print(f"\n{descriptor.title()} hail ({size}\"): {severity}, actionable={is_actionable}")

        # Verify actionable threshold
        if size >= 1.0:
            assert is_actionable == True
        else:
            assert is_actionable == False


def test_wind_speed_thresholds_cypress_tx():
    """Test wind speeds relevant to Cypress, TX area."""
    # Common wind conditions in Texas storms
    test_speeds = {
        "breezy": 20,
        "moderate": 40,
        "damaging": 60,  # ACTIONABLE - roof damage possible
        "severe": 70,
        "extreme": 80,  # Major structural damage
        "catastrophic": 100,
    }

    for condition, speed in test_speeds.items():
        severity = WeatherThresholds.classify_wind_severity(speed)
        is_actionable = WeatherThresholds.is_wind_actionable(speed)

        print(f"\n{condition.title()} winds ({speed} mph): {severity}, actionable={is_actionable}")

        # Verify actionable threshold
        if speed >= 60:
            assert is_actionable == True
        else:
            assert is_actionable == False
