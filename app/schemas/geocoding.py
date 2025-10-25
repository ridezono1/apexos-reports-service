"""
Pydantic schemas for geocoding and address autocomplete.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


class AddressSuggestion(BaseModel):
    """Address autocomplete suggestion."""
    
    place_id: str = Field(..., description="Google Place ID")
    description: str = Field(..., description="Human-readable address description")
    main_text: str = Field(..., description="Main address text")
    secondary_text: Optional[str] = Field(None, description="Secondary address text (city, state)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
                "description": "123 Main St, Houston, TX, USA",
                "main_text": "123 Main St",
                "secondary_text": "Houston, TX, USA"
            }
        }


class AutocompleteRequest(BaseModel):
    """Request for address autocomplete."""
    
    input: str = Field(..., description="Search input text", min_length=1)
    location: Optional[str] = Field(None, description="Bias results near location (lat,lng)")
    radius: Optional[int] = Field(2000, description="Radius in meters for location bias", ge=0, le=50000)
    types: Optional[str] = Field("address", description="Place types to return")
    
    class Config:
        json_schema_extra = {
            "example": {
                "input": "123 Main",
                "location": "29.7604,-95.3698",
                "radius": 5000,
                "types": "address"
            }
        }


class AutocompleteResponse(BaseModel):
    """Response for address autocomplete."""
    
    suggestions: List[AddressSuggestion] = Field(..., description="List of address suggestions")
    status: str = Field(..., description="API status")
    
    class Config:
        json_schema_extra = {
            "example": {
                "suggestions": [
                    {
                        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
                        "description": "123 Main St, Houston, TX, USA",
                        "main_text": "123 Main St",
                        "secondary_text": "Houston, TX, USA"
                    }
                ],
                "status": "OK"
            }
        }


class AddressComponent(BaseModel):
    """Individual address component."""
    
    long_name: str = Field(..., description="Full name of the component")
    short_name: str = Field(..., description="Abbreviated name")
    types: List[str] = Field(..., description="Component types")


class GeocodeResult(BaseModel):
    """Geocoded address result."""
    
    formatted_address: str = Field(..., description="Human-readable formatted address")
    address_components: List[AddressComponent] = Field(..., description="Structured address components")
    latitude: float = Field(..., description="Latitude coordinate", ge=-90, le=90)
    longitude: float = Field(..., description="Longitude coordinate", ge=-180, le=180)
    place_id: str = Field(..., description="Google Place ID")
    location_type: str = Field(..., description="Geocode accuracy type")
    
    # Optional fields
    street_number: Optional[str] = Field(None, description="Street number")
    street: Optional[str] = Field(None, description="Street name")
    city: Optional[str] = Field(None, description="City name")
    county: Optional[str] = Field(None, description="County name")
    state: Optional[str] = Field(None, description="State name")
    state_code: Optional[str] = Field(None, description="State abbreviation")
    postal_code: Optional[str] = Field(None, description="Postal/ZIP code")
    country: Optional[str] = Field(None, description="Country name")
    country_code: Optional[str] = Field(None, description="Country code")
    
    class Config:
        json_schema_extra = {
            "example": {
                "formatted_address": "123 Main St, Houston, TX 77002, USA",
                "latitude": 29.7604,
                "longitude": -95.3698,
                "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
                "location_type": "ROOFTOP",
                "street_number": "123",
                "street": "Main St",
                "city": "Houston",
                "county": "Harris County",
                "state": "Texas",
                "state_code": "TX",
                "postal_code": "77002",
                "country": "United States",
                "country_code": "US",
                "address_components": []
            }
        }


class GeocodeRequest(BaseModel):
    """Request to geocode an address or place ID."""
    
    address: Optional[str] = Field(None, description="Address to geocode")
    place_id: Optional[str] = Field(None, description="Google Place ID to geocode")
    
    class Config:
        json_schema_extra = {
            "example": {
                "address": "123 Main St, Houston, TX"
            }
        }


class GeocodeResponse(BaseModel):
    """Response from geocoding request."""
    
    result: Optional[GeocodeResult] = Field(None, description="Geocoded result")
    status: str = Field(..., description="API status")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "result": {
                    "formatted_address": "123 Main St, Houston, TX 77002, USA",
                    "latitude": 29.7604,
                    "longitude": -95.3698,
                    "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
                    "location_type": "ROOFTOP",
                    "city": "Houston",
                    "state": "Texas",
                    "state_code": "TX",
                    "postal_code": "77002",
                    "address_components": []
                },
                "status": "OK"
            }
        }


class ReverseGeocodeRequest(BaseModel):
    """Request to reverse geocode coordinates."""
    
    latitude: float = Field(..., description="Latitude coordinate", ge=-90, le=90)
    longitude: float = Field(..., description="Longitude coordinate", ge=-180, le=180)
    result_type: Optional[str] = Field("street_address", description="Filter by result type")
    
    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 29.7604,
                "longitude": -95.3698,
                "result_type": "street_address"
            }
        }


class ReverseGeocodeResponse(BaseModel):
    """Response from reverse geocoding request."""
    
    results: List[GeocodeResult] = Field(..., description="List of geocoded results")
    status: str = Field(..., description="API status")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "results": [
                    {
                        "formatted_address": "123 Main St, Houston, TX 77002, USA",
                        "latitude": 29.7604,
                        "longitude": -95.3698,
                        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
                        "location_type": "ROOFTOP",
                        "city": "Houston",
                        "state": "Texas",
                        "state_code": "TX",
                        "postal_code": "77002",
                        "address_components": []
                    }
                ],
                "status": "OK"
            }
        }