"""
Tests for weather reports API endpoints.
"""

import pytest
from httpx import AsyncClient
from app.main import app


@pytest.fixture
async def client():
    """Create async test client."""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_list_templates(client):
    """Test listing available templates."""
    response = await client.get("/api/v1/reports/templates")

    assert response.status_code == 200
    templates = response.json()

    assert isinstance(templates, list)
    assert len(templates) > 0

    # Verify template structure
    first_template = templates[0]
    assert "name" in first_template
    assert "type" in first_template
    assert "description" in first_template
    assert "supported_formats" in first_template

    print(f"\nAvailable templates ({len(templates)}):")
    for template in templates:
        print(f"- {template['name']} ({template['type']}): {template['description']}")


@pytest.mark.asyncio
async def test_generate_weather_report(client):
    """Test generating a weather report."""
    response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "location": "Cypress, TX 77433",
            "latitude": 29.9924,
            "longitude": -95.6981,
            "analysis_period": "6_month",
            "template": "professional",
            "generate_pdf": True,
            "include_charts": True,
            "include_forecast": True,
            "include_storm_events": True,
            "historical_data": True,
            "branding": {
                "company_name": "Test Company",
                "logo_url": "https://example.com/logo.png"
            },
            "color_scheme": "blue"
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert "report_id" in data
    assert "status" in data
    assert "message" in data

    report_id = data["report_id"]
    assert len(report_id) > 0

    print(f"\nGenerated weather report:")
    print(f"Report ID: {report_id}")
    print(f"Status: {data['status']}")
    print(f"Message: {data['message']}")

    return report_id


@pytest.mark.asyncio
async def test_generate_spatial_report(client):
    """Test generating a spatial weather report."""
    response = await client.post(
        "/api/v1/reports/reports/generate/spatial",
        json={
            "boundary_id": "Harris County, TX",
            "analysis_period": "6_month",
            "template": "professional",
            "generate_pdf": True,
            "risk_factors": ["hail", "wind", "tornado"],
            "storm_types": ["thunderstorm", "tornado", "hail"],
            "include_risk_assessment": True,
            "include_storm_impact_zones": True,
            "include_weather_interpolation": False,
            "include_statistical_summaries": True,
            "sub_area_level": "zipcode",
            "include_heat_maps": True,
            "include_charts": True,
            "weather_parameters": ["temperature", "precipitation", "wind"]
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert "report_id" in data
    assert "status" in data
    assert "message" in data

    report_id = data["report_id"]

    print(f"\nGenerated spatial report:")
    print(f"Report ID: {report_id}")
    print(f"Status: {data['status']}")

    return report_id


@pytest.mark.asyncio
async def test_get_report_status(client):
    """Test getting report status."""
    # First generate a report
    generate_response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "location": "Houston, TX",
            "latitude": 29.7604,
            "longitude": -95.3698,
            "analysis_period": "6_month",
            "template": "professional",
            "generate_pdf": False,
            "include_charts": False,
            "include_forecast": False,
            "include_storm_events": False
        }
    )

    assert generate_response.status_code == 200
    report_id = generate_response.json()["report_id"]

    # Now check status
    status_response = await client.get(f"/api/v1/reports/reports/{report_id}/status")

    assert status_response.status_code == 200
    status_data = status_response.json()

    assert "report_id" in status_data
    assert "status" in status_data
    assert status_data["report_id"] == report_id
    assert status_data["status"] in ["pending", "processing", "completed", "failed"]

    print(f"\nReport status for {report_id}:")
    print(f"Status: {status_data['status']}")
    if "file_url" in status_data:
        print(f"File URL: {status_data['file_url']}")


@pytest.mark.asyncio
async def test_get_nonexistent_report_status(client):
    """Test getting status of nonexistent report."""
    response = await client.get("/api/v1/reports/reports/nonexistent-id/status")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_delete_report(client):
    """Test deleting a report."""
    # First generate a report
    generate_response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "location": "Dallas, TX",
            "latitude": 32.7767,
            "longitude": -96.7970,
            "analysis_period": "6_month",
            "template": "executive",
            "generate_pdf": True,
            "include_charts": False,
            "include_forecast": False,
            "include_storm_events": False
        }
    )

    assert generate_response.status_code == 200
    report_id = generate_response.json()["report_id"]

    # Delete the report
    delete_response = await client.delete(f"/api/v1/reports/reports/{report_id}")

    assert delete_response.status_code == 200
    data = delete_response.json()
    assert "message" in data

    # Verify it's deleted
    status_response = await client.get(f"/api/v1/reports/reports/{report_id}/status")
    assert status_response.status_code == 404


@pytest.mark.asyncio
async def test_cleanup_old_reports(client):
    """Test cleanup endpoint."""
    response = await client.post(
        "/api/v1/reports/reports/cleanup",
        params={"max_age_hours": 24}
    )

    assert response.status_code == 200
    data = response.json()
    assert "message" in data


@pytest.mark.asyncio
async def test_generate_report_missing_location(client):
    """Test generating report without location fails."""
    response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "analysis_period": "6_month",
            "template": "professional",
            "generate_pdf": True
        }
    )

    # Should return validation error
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_generate_report_invalid_period(client):
    """Test generating report with invalid period."""
    response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "location": "Houston, TX",
            "latitude": 29.7604,
            "longitude": -95.3698,
            "analysis_period": "invalid_period",
            "template": "professional",
            "generate_pdf": True,
            "include_charts": False,
            "include_forecast": False,
            "include_storm_events": False
        }
    )

    # Should return validation error
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_weather_report_with_9_month_period(client):
    """Test generating weather report with 9-month period."""
    response = await client.post(
        "/api/v1/reports/reports/generate/weather",
        json={
            "location": "Cypress, TX",
            "latitude": 29.9924,
            "longitude": -95.6981,
            "analysis_period": "9_month",
            "template": "detailed",
            "generate_pdf": False,
            "include_charts": True,
            "include_forecast": True,
            "include_storm_events": True
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert "report_id" in data
    assert "9_month" in data["message"] or "9 month" in data["message"]
