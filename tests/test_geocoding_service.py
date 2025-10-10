"""
Tests for the geocoding service.
"""

import pytest
from app.services.geocoding_service import GeocodingService


@pytest.mark.asyncio
async def test_geocode_cypress_tx_address():
    """Test geocoding a real address in Cypress, TX."""
    service = GeocodingService()

    # Test address: 10211 Peytons Grace Ln, Cypress, TX 77433
    address = "10211 Peytons Grace Ln, Cypress, TX 77433"

    result = await service.geocode_address(address=address)

    # Check status
    assert result.status == "OK", f"Geocoding failed with status: {result.status}"
    assert result.result is not None, "No geocoding result returned"

    # Check coordinates are reasonable for Cypress, TX area
    # Cypress, TX is approximately 29.97°N, -95.70°W
    assert 29.9 <= result.result.latitude <= 30.1, f"Latitude {result.result.latitude} out of expected range"
    assert -95.8 <= result.result.longitude <= -95.6, f"Longitude {result.result.longitude} out of expected range"

    # Check formatted address contains key elements
    assert "Peytons Grace" in result.result.formatted_address or "Peyton" in result.result.formatted_address
    assert "Cypress" in result.result.formatted_address
    assert "TX" in result.result.formatted_address or "Texas" in result.result.formatted_address
    assert "77433" in result.result.formatted_address

    # Check structured address fields
    assert result.result.city is not None
    assert result.result.state is not None
    assert result.result.postal_code == "77433"
    assert result.result.country == "United States"

    # Print for verification
    print(f"\nGeocoded Address: {result.result.formatted_address}")
    print(f"Coordinates: {result.result.latitude}, {result.result.longitude}")
    print(f"City: {result.result.city}")
    print(f"State: {result.result.state}")
    print(f"Postal Code: {result.result.postal_code}")


@pytest.mark.asyncio
async def test_reverse_geocode_cypress_tx():
    """Test reverse geocoding coordinates near Cypress, TX."""
    service = GeocodingService()

    # Approximate coordinates for the address
    latitude = 29.9924
    longitude = -95.6981

    result = await service.reverse_geocode(latitude=latitude, longitude=longitude)

    # Check status
    assert result.status == "OK", f"Reverse geocoding failed with status: {result.status}"
    assert len(result.results) > 0, "No reverse geocoding results returned"

    # Check first result
    first_result = result.results[0]
    assert first_result.formatted_address is not None

    # Should contain Cypress or Harris County
    address_lower = first_result.formatted_address.lower()
    assert "cypress" in address_lower or "harris" in address_lower or "tx" in address_lower

    # Print for verification
    print(f"\nReverse Geocoded Address: {first_result.formatted_address}")
    print(f"City: {first_result.city}")
    print(f"State: {first_result.state}")


@pytest.mark.asyncio
async def test_autocomplete_cypress_address():
    """Test address autocomplete for the Cypress address."""
    service = GeocodingService()

    # Test partial address
    input_text = "10211 Peytons Grace"

    result = await service.autocomplete_address(input_text=input_text)

    # Check we get suggestions
    assert len(result.suggestions) > 0, "No autocomplete suggestions returned"

    # Print suggestions
    print(f"\nAutocomplete suggestions for '{input_text}':")
    for i, suggestion in enumerate(result.suggestions, 1):
        print(f"{i}. {suggestion.description}")
