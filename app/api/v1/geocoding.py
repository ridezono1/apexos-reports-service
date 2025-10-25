"""
API endpoints for geocoding and address autocomplete.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
import sentry_sdk

from app.schemas.geocoding import (
    AutocompleteRequest,
    AutocompleteResponse,
    GeocodeRequest,
    GeocodeResponse,
    ReverseGeocodeRequest,
    ReverseGeocodeResponse
)
from app.services.geocoding_service import get_geocoding_service, GeocodingService
from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(
    tags=["Geocoding"],
    responses={
        401: {"description": "Unauthorized - Invalid API key"},
        429: {"description": "Too many requests"},
        500: {"description": "Internal server error"}
    }
)


@router.get(
    "/autocomplete",
    response_model=AutocompleteResponse,
    summary="Address Autocomplete",
    description="Get address suggestions as user types. Useful for address input fields.",
)
async def autocomplete_address(
    input: str = Query(..., description="Search input text", min_length=1),
    location: Optional[str] = Query(None, description="Bias results near location (lat,lng)", example="29.7604,-95.3698"),
    radius: int = Query(2000, description="Radius in meters for location bias", ge=0, le=50000),
    types: str = Query("address", description="Place types to return"),
    service: GeocodingService = Depends(get_geocoding_service)
) -> AutocompleteResponse:
    """
    Get address autocomplete suggestions.
    
    **Usage:**
    - As user types in address field, call this endpoint with current input
    - Optionally bias results near current location for better relevance
    - Returns list of suggested addresses with place IDs
    
    **Example:**
    ```
    GET /api/v1/geocoding/autocomplete?input=123+Main&location=29.7604,-95.3698
    ```
    
    **Response:**
    ```json
    {
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
    ```
    """
    try:
        result = await service.autocomplete_address(
            input_text=input,
            location=location,
            radius=radius,
            types=types
        )
        
        if result.status not in ["OK", "ZERO_RESULTS"]:
            logger.warning(f"Autocomplete API returned status: {result.status}")
            raise HTTPException(
                status_code=500,
                detail=f"Autocomplete failed with status: {result.status}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in autocomplete endpoint: {str(e)}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/geocode",
    response_model=GeocodeResponse,
    summary="Geocode Address",
    description="Convert address or place ID to coordinates and structured address data.",
)
async def geocode_address(
    request: GeocodeRequest,
    service: GeocodingService = Depends(get_geocoding_service)
) -> GeocodeResponse:
    """
    Geocode an address or place ID to coordinates.
    
    **Usage:**
    - After user selects an address from autocomplete, use the place_id to get full details
    - Or directly geocode a typed address
    - Returns coordinates and structured address components
    
    **Example with place_id:**
    ```json
    {
      "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4"
    }
    ```
    
    **Example with address:**
    ```json
    {
      "address": "123 Main St, Houston, TX"
    }
    ```
    
    **Response:**
    ```json
    {
      "result": {
        "formatted_address": "123 Main St, Houston, TX 77002, USA",
        "latitude": 29.7604,
        "longitude": -95.3698,
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "location_type": "ROOFTOP",
        "street_number": "123",
        "street": "Main St",
        "city": "Houston",
        "state": "Texas",
        "state_code": "TX",
        "postal_code": "77002",
        "country": "United States",
        "country_code": "US"
      },
      "status": "OK"
    }
    ```
    """
    try:
        if not request.address and not request.place_id:
            raise HTTPException(
                status_code=400,
                detail="Either 'address' or 'place_id' is required"
            )
        
        result = await service.geocode_address(
            address=request.address,
            place_id=request.place_id
        )
        
        if result.status == "ZERO_RESULTS":
            raise HTTPException(
                status_code=404,
                detail="No results found for the given address or place ID"
            )
        elif result.status not in ["OK"]:
            logger.warning(f"Geocoding API returned status: {result.status}")
            raise HTTPException(
                status_code=500,
                detail=result.error_message or f"Geocoding failed with status: {result.status}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in geocode endpoint: {str(e)}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/reverse-geocode",
    response_model=ReverseGeocodeResponse,
    summary="Reverse Geocode Coordinates",
    description="Convert coordinates to address. Useful for 'Use Current Location' feature.",
)
async def reverse_geocode(
    request: ReverseGeocodeRequest,
    service: GeocodingService = Depends(get_geocoding_service)
) -> ReverseGeocodeResponse:
    """
    Reverse geocode coordinates to address.
    
    **Usage:**
    - When user clicks "Use Current Location", get device GPS coordinates
    - Send coordinates to this endpoint to get formatted address
    - Returns list of addresses (usually best result is first)
    
    **Example:**
    ```json
    {
      "latitude": 29.7604,
      "longitude": -95.3698,
      "result_type": "street_address"
    }
    ```
    
    **Response:**
    ```json
    {
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
          "postal_code": "77002"
        }
      ],
      "status": "OK"
    }
    ```
    """
    try:
        result = await service.reverse_geocode(
            latitude=request.latitude,
            longitude=request.longitude,
            result_type=request.result_type
        )
        
        if result.status == "ZERO_RESULTS":
            raise HTTPException(
                status_code=404,
                detail="No address found for the given coordinates"
            )
        elif result.status not in ["OK"]:
            logger.warning(f"Reverse geocoding API returned status: {result.status}")
            raise HTTPException(
                status_code=500,
                detail=result.error_message or f"Reverse geocoding failed with status: {result.status}"
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reverse geocode endpoint: {str(e)}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(status_code=500, detail=str(e))