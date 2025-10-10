"""
Tests for geocoding API endpoints.
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
async def test_autocomplete_address(client):
    """Test address autocomplete endpoint."""
    response = await client.get(
        "/api/v1/geocoding/autocomplete",
        params={"input": "10211 Peytons Grace"}
    )

    assert response.status_code == 200
    data = response.json()

    assert "suggestions" in data
    assert "status" in data
    assert data["status"] in ["OK", "ZERO_RESULTS"]

    if data["status"] == "OK":
        suggestions = data["suggestions"]
        assert len(suggestions) > 0

        first_suggestion = suggestions[0]
        assert "place_id" in first_suggestion
        assert "description" in first_suggestion
        assert "main_text" in first_suggestion

        print(f"\nAutocomplete suggestions for '10211 Peytons Grace':")
        for suggestion in suggestions[:3]:
            print(f"- {suggestion['description']}")


@pytest.mark.asyncio
async def test_autocomplete_with_location_bias(client):
    """Test autocomplete with location bias."""
    response = await client.get(
        "/api/v1/geocoding/autocomplete",
        params={
            "input": "Main St",
            "location": "29.9924,-95.6981",  # Cypress, TX
            "radius": 5000
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["OK", "ZERO_RESULTS"]


@pytest.mark.asyncio
async def test_autocomplete_empty_input(client):
    """Test autocomplete with empty input fails."""
    response = await client.get(
        "/api/v1/geocoding/autocomplete",
        params={"input": ""}
    )

    # Should return validation error
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_geocode_address_cypress(client):
    """Test geocoding a Cypress, TX address."""
    response = await client.post(
        "/api/v1/geocoding/geocode",
        json={"address": "10211 Peytons Grace Ln, Cypress, TX 77433"}
    )

    assert response.status_code == 200
    data = response.json()

    assert "result" in data
    assert "status" in data
    assert data["status"] == "OK"

    result = data["result"]
    assert "formatted_address" in result
    assert "latitude" in result
    assert "longitude" in result

    # Verify coordinates are in Cypress, TX area
    assert 29.9 <= result["latitude"] <= 30.1
    assert -95.8 <= result["longitude"] <= -95.6

    # Verify address components
    assert "77433" in result["formatted_address"]

    print(f"\nGeocoded address:")
    print(f"Address: {result['formatted_address']}")
    print(f"Coordinates: {result['latitude']}, {result['longitude']}")
    print(f"City: {result.get('city')}")
    print(f"State: {result.get('state')}")


@pytest.mark.asyncio
async def test_geocode_with_place_id(client):
    """Test geocoding with a place ID."""
    # First get a place ID from autocomplete
    autocomplete_response = await client.get(
        "/api/v1/geocoding/autocomplete",
        params={"input": "Houston, TX"}
    )

    assert autocomplete_response.status_code == 200
    autocomplete_data = autocomplete_response.json()

    if autocomplete_data["status"] == "OK" and autocomplete_data["suggestions"]:
        place_id = autocomplete_data["suggestions"][0]["place_id"]

        # Now geocode using the place ID
        response = await client.post(
            "/api/v1/geocoding/geocode",
            json={"place_id": place_id}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "OK"
        assert "result" in data


@pytest.mark.asyncio
async def test_geocode_invalid_address(client):
    """Test geocoding an invalid address."""
    response = await client.post(
        "/api/v1/geocoding/geocode",
        json={"address": "XYZ123 Nonexistent Street, Nowhere, XX 00000"}
    )

    # Should return 404 or error status
    assert response.status_code in [404, 500]


@pytest.mark.asyncio
async def test_geocode_missing_parameters(client):
    """Test geocoding without address or place_id."""
    response = await client.post(
        "/api/v1/geocoding/geocode",
        json={}
    )

    # Should return validation error
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_reverse_geocode_cypress(client):
    """Test reverse geocoding for Cypress, TX coordinates."""
    response = await client.post(
        "/api/v1/geocoding/reverse-geocode",
        json={
            "latitude": 29.9924,
            "longitude": -95.6981
        }
    )

    assert response.status_code == 200
    data = response.json()

    assert "results" in data
    assert "status" in data
    assert data["status"] == "OK"

    results = data["results"]
    assert len(results) > 0

    first_result = results[0]
    assert "formatted_address" in first_result
    assert "latitude" in first_result
    assert "longitude" in first_result

    # Should be near Cypress or Harris County
    address_lower = first_result["formatted_address"].lower()
    assert "cypress" in address_lower or "harris" in address_lower or "tx" in address_lower

    print(f"\nReverse geocoded coordinates (29.9924, -95.6981):")
    print(f"Address: {first_result['formatted_address']}")


@pytest.mark.asyncio
async def test_reverse_geocode_with_result_type(client):
    """Test reverse geocoding with result type filter."""
    response = await client.post(
        "/api/v1/geocoding/reverse-geocode",
        json={
            "latitude": 29.9924,
            "longitude": -95.6981,
            "result_type": "locality"
        }
    )

    assert response.status_code in [200, 404]  # May not find locality-specific result


@pytest.mark.asyncio
async def test_reverse_geocode_invalid_coordinates(client):
    """Test reverse geocoding with invalid coordinates."""
    response = await client.post(
        "/api/v1/geocoding/reverse-geocode",
        json={
            "latitude": 999.0,  # Invalid latitude
            "longitude": -95.6981
        }
    )

    # Should return error
    assert response.status_code in [404, 500]


@pytest.mark.asyncio
async def test_autocomplete_houston(client):
    """Test autocomplete for Houston addresses."""
    response = await client.get(
        "/api/v1/geocoding/autocomplete",
        params={"input": "1600 Smith St, Houston"}
    )

    assert response.status_code == 200
    data = response.json()

    if data["status"] == "OK":
        assert len(data["suggestions"]) > 0
        # Should find Houston addresses
        first_suggestion = data["suggestions"][0]
        assert "houston" in first_suggestion["description"].lower()
