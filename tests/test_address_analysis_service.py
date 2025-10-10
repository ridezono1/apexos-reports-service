"""
Tests for the address analysis service.
"""

import pytest
from datetime import datetime, timedelta
from app.services.address_analysis_service import AddressAnalysisService


@pytest.fixture
def address_service():
    """Create address analysis service instance."""
    return AddressAnalysisService()


@pytest.fixture
def cypress_address():
    """Test address in Cypress, TX."""
    return "10211 Peytons Grace Ln, Cypress, TX 77433"


@pytest.fixture
def analysis_period():
    """Create a 6-month analysis period ending today."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=182)
    return {
        "start": start_date.isoformat(),
        "end": end_date.isoformat()
    }


@pytest.fixture
def analysis_options():
    """Create default analysis options."""
    return {
        "risk_factors": ["hail", "wind", "flooding", "tornado"],
        "include_business_impact": True,
        "include_lead_qualification": True
    }


@pytest.mark.asyncio
async def test_analyze_property_address_cypress(address_service, cypress_address, analysis_period, analysis_options):
    """Test full property analysis for Cypress, TX address."""
    result = await address_service.analyze_property_address(
        address=cypress_address,
        analysis_period=analysis_period,
        analysis_options=analysis_options
    )

    # Verify result structure
    assert result is not None
    assert "property_info" in result
    assert "analysis_period" in result
    assert "weather_summary" in result
    assert "risk_assessment" in result
    assert "historical_context" in result
    assert "business_impact" in result
    assert "lead_qualification" in result
    assert "location_alerts" in result

    # Verify property info
    property_info = result["property_info"]
    assert property_info["address"] == cypress_address
    assert "coordinates" in property_info
    assert "geocoded_address" in property_info
    assert "77433" in property_info["geocoded_address"]

    # Verify risk assessment
    risk_assessment = result["risk_assessment"]
    assert "overall_risk_score" in risk_assessment
    assert "risk_level" in risk_assessment
    assert risk_assessment["risk_level"] in ["low", "medium", "high"]
    assert "risk_factors" in risk_assessment
    assert "risk_details" in risk_assessment

    # Verify business impact
    business_impact = result["business_impact"]
    assert "impact_score" in business_impact
    assert "impact_level" in business_impact
    assert business_impact["impact_level"] in ["low", "medium", "high"]

    # Verify lead qualification
    lead_qualification = result["lead_qualification"]
    assert "lead_score" in lead_qualification
    assert "lead_level" in lead_qualification
    assert "lead_type" in lead_qualification

    print(f"\nProperty analysis for {cypress_address}:")
    print(f"Geocoded: {property_info['geocoded_address']}")
    print(f"Risk level: {risk_assessment['risk_level']} ({risk_assessment['overall_risk_score']:.2f})")
    print(f"Business impact: {business_impact['impact_level']}")
    print(f"Lead qualification: {lead_qualification['lead_level']} - {lead_qualification['lead_type']}")


def test_calculate_property_weather_summary(address_service):
    """Test weather summary calculation."""
    current_weather = {
        "temperature": 75.0,
        "weather_condition": "Partly Cloudy",
        "wind_speed": 10.0,
        "humidity": 65.0
    }

    forecast = {
        "forecasts": [
            {"temperature": 78, "precipitation_probability": 20},
            {"temperature": 72, "precipitation_probability": 30},
            {"temperature": 80, "precipitation_probability": 10}
        ]
    }

    historical_weather = {
        "observations": [
            {"temperature": 70, "precipitation": 0.5},
            {"temperature": 75, "precipitation": 1.0},
            {"temperature": 73, "precipitation": 0.0}
        ]
    }

    weather_events = [
        {"event": "High Wind", "severity": "Moderate"},
        {"event": "Thunderstorm", "severity": "Severe"}
    ]

    summary = address_service._calculate_property_weather_summary(
        current_weather, forecast, historical_weather, weather_events
    )

    # Verify summary structure
    assert "current_conditions" in summary
    assert summary["current_conditions"]["temperature"] == 75.0

    assert "forecast_summary" in summary
    assert summary["forecast_summary"]["next_7_days"] == 3

    assert "historical_summary" in summary
    assert summary["historical_summary"]["observations_count"] == 3

    assert "weather_events_summary" in summary
    assert summary["weather_events_summary"]["total_events"] == 2
    assert summary["weather_events_summary"]["severe_events"] == 1


def test_assess_hail_risk(address_service):
    """Test hail risk assessment."""
    weather_events = [
        {"event": "Hail Storm", "severity": "Severe"},
        {"event": "Hail", "severity": "Moderate"}
    ]

    historical_weather = {
        "observations": [
            {"precipitation": 15.0},
            {"precipitation": 12.0},
            {"precipitation": 3.0}
        ]
    }

    risk = address_service._assess_hail_risk(weather_events, historical_weather)

    # Verify risk assessment
    assert "score" in risk
    assert "level" in risk
    assert "factors" in risk
    assert "events_count" in risk

    assert risk["events_count"] == 2
    assert risk["score"] > 0.5  # Should have high score due to events


def test_assess_wind_risk(address_service):
    """Test wind risk assessment."""
    weather_events = [
        {"event": "High Wind Warning", "severity": "Severe"}
    ]

    historical_weather = {
        "observations": [
            {"wind_speed": 20},
            {"wind_speed": 18},
            {"wind_speed": 5}
        ]
    }

    current_weather = {
        "wind_speed": 25.0
    }

    risk = address_service._assess_wind_risk(weather_events, historical_weather, current_weather)

    # Verify risk assessment
    assert risk["events_count"] == 1
    assert risk["current_wind_speed"] == 25.0
    assert risk["score"] > 0.5  # High current wind + event


def test_assess_flooding_risk(address_service):
    """Test flooding risk assessment."""
    historical_weather = {
        "observations": [
            {"precipitation": 25.0},
            {"precipitation": 30.0},
            {"precipitation": 5.0}
        ]
    }

    # Mock coordinates with elevation
    class MockCoordinates:
        elevation = 50  # Low elevation

    risk = address_service._assess_flooding_risk(historical_weather, MockCoordinates())

    # Verify risk assessment
    assert "score" in risk
    assert "total_precipitation" in risk
    assert "heavy_rain_days" in risk

    assert risk["heavy_rain_days"] == 2
    assert risk["score"] > 0.4  # Should have elevated risk


def test_assess_tornado_risk(address_service):
    """Test tornado risk assessment."""
    weather_events = [
        {"event": "Tornado Warning", "severity": "Severe"}
    ]

    risk = address_service._assess_tornado_risk(weather_events)

    # Verify risk assessment
    assert risk["events_count"] == 1
    assert risk["score"] >= 0.8  # Should have very high score


def test_calculate_temperature_trend(address_service):
    """Test temperature trend calculation."""
    # Increasing trend
    increasing_temps = [70.0, 72.0, 74.0, 76.0, 78.0]
    trend = address_service._calculate_temperature_trend(increasing_temps)

    assert trend["trend"] == "increasing"
    assert trend["slope"] > 0
    assert "average" in trend
    assert "range" in trend

    # Decreasing trend
    decreasing_temps = [78.0, 76.0, 74.0, 72.0, 70.0]
    trend = address_service._calculate_temperature_trend(decreasing_temps)

    assert trend["trend"] == "decreasing"
    assert trend["slope"] < 0

    # Stable trend
    stable_temps = [75.0, 75.1, 74.9, 75.0, 75.2]
    trend = address_service._calculate_temperature_trend(stable_temps)

    assert trend["trend"] == "stable"


def test_analyze_precipitation_pattern(address_service):
    """Test precipitation pattern analysis."""
    # Wet pattern
    wet_precipitation = [5.0, 3.0, 7.0, 2.0, 8.0, 4.0]
    pattern = address_service._analyze_precipitation_pattern(wet_precipitation)

    assert pattern["pattern"] == "wet"
    assert pattern["total_precipitation"] > 0
    assert pattern["rainy_days"] > len(wet_precipitation) * 0.5

    # Dry pattern
    dry_precipitation = [0.0, 0.5, 0.0, 0.0, 0.2, 0.0]
    pattern = address_service._analyze_precipitation_pattern(dry_precipitation)

    assert pattern["pattern"] == "dry"
    assert pattern["rainy_days"] < len(dry_precipitation) * 0.5


def test_analyze_event_frequency(address_service):
    """Test weather event frequency analysis."""
    # High frequency
    many_events = [
        {"event": "Thunderstorm"},
        {"event": "High Wind"},
        {"event": "Hail"},
        {"event": "Heavy Rain"},
        {"event": "Tornado Warning"},
        {"event": "Thunderstorm"},
        {"event": "High Wind"},
        {"event": "Hail"},
        {"event": "Heavy Rain"},
        {"event": "Tornado Warning"},
        {"event": "Thunderstorm"}
    ]

    frequency = address_service._analyze_event_frequency(many_events)

    assert frequency["frequency"] == "high"
    assert frequency["total_events"] > 10

    # Low frequency
    few_events = [
        {"event": "Thunderstorm"},
        {"event": "High Wind"}
    ]

    frequency = address_service._analyze_event_frequency(few_events)

    assert frequency["frequency"] == "low"
    assert frequency["total_events"] <= 5


def test_generate_risk_recommendations(address_service):
    """Test risk recommendation generation."""
    # High hail risk
    risk_scores = {"hail": 0.8, "wind": 0.3, "flooding": 0.2, "tornado": 0.1}
    risk_details = {
        "hail": {"score": 0.8},
        "wind": {"score": 0.3},
        "flooding": {"score": 0.2},
        "tornado": {"score": 0.1}
    }

    recommendations = address_service._generate_risk_recommendations(risk_scores, risk_details)

    # Should have recommendations for high hail risk
    assert len(recommendations) > 0
    assert any("hail" in rec.lower() for rec in recommendations)


def test_generate_qualification_reason(address_service):
    """Test lead qualification reason generation."""
    # High priority lead
    high_factors = ["High weather risk property", "Hail damage assessment needed"]
    reason = address_service._generate_qualification_reason(0.8, high_factors)

    assert "High-priority" in reason
    assert "High weather risk property" in reason

    # Low priority lead
    low_factors = []
    reason = address_service._generate_qualification_reason(0.2, low_factors)

    assert "Low-priority" in reason or "routine" in reason.lower()
