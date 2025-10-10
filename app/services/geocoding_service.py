"""
Service for geocoding and address autocomplete using Google Maps API.
Based on SkyLink's geocoding service.
"""

import httpx
from typing import List, Optional, Dict, Any
from app.core.config import settings
from app.core.logging import get_logger
from app.schemas.geocoding import (
    AddressSuggestion,
    AutocompleteResponse,
    GeocodeResult,
    GeocodeResponse,
    ReverseGeocodeResponse,
    AddressComponent
)

logger = get_logger(__name__)


class GeocodingService:
    """Service for geocoding and address operations."""
    
    GOOGLE_MAPS_AUTOCOMPLETE_URL = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    GOOGLE_MAPS_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
    GOOGLE_MAPS_PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
    
    def __init__(self):
        """Initialize geocoding service."""
        self.api_key = settings.google_maps_api_key
        if not self.api_key:
            logger.warning("Google Maps API key not configured - geocoding will use fallback services")
            self.api_key = None
    
    async def autocomplete_address(
        self,
        input_text: str,
        location: Optional[str] = None,
        radius: int = 2000,
        types: str = "address"
    ) -> AutocompleteResponse:
        """
        Get address autocomplete suggestions.
        
        Args:
            input_text: Search input text
            location: Optional location bias (lat,lng)
            radius: Radius in meters for location bias
            types: Place types to return
            
        Returns:
            AutocompleteResponse with suggestions
        """
        try:
            params = {
                "input": input_text,
                "key": self.api_key,
                "types": types
            }
            
            if location:
                params["location"] = location
                params["radius"] = radius
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.GOOGLE_MAPS_AUTOCOMPLETE_URL,
                    params=params,
                    timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
            
            # Parse suggestions
            suggestions = []
            for prediction in data.get("predictions", []):
                suggestion = AddressSuggestion(
                    place_id=prediction["place_id"],
                    description=prediction["description"],
                    main_text=prediction["structured_formatting"]["main_text"],
                    secondary_text=prediction["structured_formatting"].get("secondary_text")
                )
                suggestions.append(suggestion)
            
            logger.info(f"Found {len(suggestions)} autocomplete suggestions for '{input_text}'")
            
            return AutocompleteResponse(
                suggestions=suggestions,
                status=data.get("status", "OK")
            )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during autocomplete: {str(e)}")
            return AutocompleteResponse(suggestions=[], status="ERROR")
        except Exception as e:
            logger.error(f"Error during address autocomplete: {str(e)}")
            return AutocompleteResponse(suggestions=[], status="ERROR")
    
    async def geocode_address(
        self,
        address: Optional[str] = None,
        place_id: Optional[str] = None
    ) -> GeocodeResponse:
        """
        Geocode an address or place ID to coordinates.
        
        Args:
            address: Address string to geocode
            place_id: Google Place ID to geocode
            
        Returns:
            GeocodeResponse with result
        """
        try:
            params = {"key": self.api_key}
            
            if place_id:
                params["place_id"] = place_id
            elif address:
                params["address"] = address
            else:
                return GeocodeResponse(
                    result=None,
                    status="INVALID_REQUEST",
                    error_message="Either address or place_id is required"
                )
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.GOOGLE_MAPS_GEOCODE_URL,
                    params=params,
                    timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
            
            status = data.get("status", "ERROR")
            
            if status != "OK" or not data.get("results"):
                error_msg = data.get("error_message", f"Geocoding failed with status: {status}")
                logger.warning(f"Geocoding failed: {error_msg}")
                return GeocodeResponse(
                    result=None,
                    status=status,
                    error_message=error_msg
                )
            
            # Parse first result
            result_data = data["results"][0]
            geocode_result = self._parse_geocode_result(result_data)
            
            logger.info(f"Geocoded address to {geocode_result.latitude}, {geocode_result.longitude}")
            
            return GeocodeResponse(
                result=geocode_result,
                status=status
            )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during geocoding: {str(e)}")
            return GeocodeResponse(
                result=None,
                status="ERROR",
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"Error during geocoding: {str(e)}")
            return GeocodeResponse(
                result=None,
                status="ERROR",
                error_message=str(e)
            )
    
    async def reverse_geocode(
        self,
        latitude: float,
        longitude: float,
        result_type: Optional[str] = None
    ) -> ReverseGeocodeResponse:
        """
        Reverse geocode coordinates to address.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            result_type: Optional filter by result type
            
        Returns:
            ReverseGeocodeResponse with results
        """
        try:
            # Try Google Maps API first if API key is available
            if self.api_key:
                result = await self._reverse_geocode_with_google_maps(latitude, longitude, result_type)
                if result.status == "OK" and result.results:
                    return result
                logger.warning(f"Google Maps reverse geocoding failed for coordinates: {latitude}, {longitude}, trying fallback")
            
            # Fallback to Nominatim/OpenStreetMap
            return await self._reverse_geocode_with_nominatim(latitude, longitude)
            
        except Exception as e:
            logger.error(f"Error during reverse geocoding: {str(e)}")
            return ReverseGeocodeResponse(
                results=[],
                status="ERROR",
                error_message=str(e)
            )
    
    async def _reverse_geocode_with_google_maps(
        self,
        latitude: float,
        longitude: float,
        result_type: Optional[str] = None
    ) -> ReverseGeocodeResponse:
        """Reverse geocode using Google Maps API."""
        try:
            params = {
                "latlng": f"{latitude},{longitude}",
                "key": self.api_key
            }
            
            if result_type:
                params["result_type"] = result_type
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.GOOGLE_MAPS_GEOCODE_URL,
                    params=params,
                    timeout=10.0
                )
                response.raise_for_status()
                data = response.json()
            
            status = data.get("status", "ERROR")
            
            if status != "OK" or not data.get("results"):
                error_msg = data.get("error_message", f"Reverse geocoding failed with status: {status}")
                logger.warning(f"Google Maps reverse geocoding failed: {error_msg}")
                return ReverseGeocodeResponse(
                    results=[],
                    status=status,
                    error_message=error_msg
                )
            
            # Parse all results
            results = []
            for result_data in data["results"]:
                geocode_result = self._parse_geocode_result(result_data)
                results.append(geocode_result)
            
            logger.info(f"Google Maps reverse geocoded {latitude},{longitude} to {len(results)} results")
            
            return ReverseGeocodeResponse(
                results=results,
                status=status
            )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error during Google Maps reverse geocoding: {str(e)}")
            return ReverseGeocodeResponse(
                results=[],
                status="ERROR",
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f"Error during Google Maps reverse geocoding: {str(e)}")
            return ReverseGeocodeResponse(
                results=[],
                status="ERROR",
                error_message=str(e)
            )
    
    async def _reverse_geocode_with_nominatim(
        self,
        latitude: float,
        longitude: float
    ) -> ReverseGeocodeResponse:
        """Reverse geocode using Nominatim/OpenStreetMap as fallback."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://nominatim.openstreetmap.org/reverse",
                    params={
                        "lat": latitude,
                        "lon": longitude,
                        "format": "json",
                        "addressdetails": 1
                    },
                    headers={"User-Agent": "Weather-Reports-Service/1.0"},
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        # Parse Nominatim response
                        geocode_result = self._parse_nominatim_result(data, latitude, longitude)
                        
                        logger.info(f"Nominatim reverse geocoded {latitude},{longitude} successfully")
                        
                        return ReverseGeocodeResponse(
                            results=[geocode_result],
                            status="OK"
                        )
                
                logger.warning(f"Nominatim reverse geocoding failed for coordinates: {latitude}, {longitude}")
                return ReverseGeocodeResponse(
                    results=[],
                    status="ZERO_RESULTS",
                    error_message="No address found for the given coordinates"
                )
                
        except Exception as e:
            logger.error(f"Error with Nominatim reverse geocoding for coordinates ({latitude}, {longitude}): {e}")
            return ReverseGeocodeResponse(
                results=[],
                status="ERROR",
                error_message=str(e)
            )
    
    def _parse_nominatim_result(self, data: Dict[str, Any], latitude: float, longitude: float) -> GeocodeResult:
        """Parse Nominatim reverse geocoding result."""
        address = data.get("address", {})
        formatted_address = data.get("display_name", "")
        
        # Extract structured address fields
        city = (
            address.get("city") or 
            address.get("town") or 
            address.get("village") or 
            address.get("hamlet") or
            ""
        )
        state = address.get("state") or ""
        country = address.get("country") or ""
        postal_code = address.get("postcode") or ""
        
        # Create address components
        address_components = []
        if city:
            address_components.append(AddressComponent(
                long_name=city,
                short_name=city,
                types=["locality"]
            ))
        if state:
            address_components.append(AddressComponent(
                long_name=state,
                short_name=state,
                types=["administrative_area_level_1"]
            ))
        if country:
            address_components.append(AddressComponent(
                long_name=country,
                short_name=country,
                types=["country"]
            ))
        if postal_code:
            address_components.append(AddressComponent(
                long_name=postal_code,
                short_name=postal_code,
                types=["postal_code"]
            ))
        
        return GeocodeResult(
            formatted_address=formatted_address,
            address_components=address_components,
            latitude=latitude,
            longitude=longitude,
            place_id=str(data.get("place_id", "")),
            location_type="APPROXIMATE",
            
            # Structured fields
            street_number=None,
            street=None,
            city=city,
            county=address.get("county") or "",
            state=state,
            state_code=address.get("state_code") or "",
            postal_code=postal_code,
            country=country,
            country_code=address.get("country_code") or ""
        )
    
    def _parse_geocode_result(self, result_data: Dict[str, Any]) -> GeocodeResult:
        """
        Parse geocode result from Google Maps API response.
        
        Args:
            result_data: Raw result from Google Maps API
            
        Returns:
            Parsed GeocodeResult
        """
        # Extract location
        location = result_data["geometry"]["location"]
        
        # Parse address components
        address_components = []
        component_map = {}
        
        for component in result_data.get("address_components", []):
            # Store in map for easy lookup
            for comp_type in component["types"]:
                component_map[comp_type] = component
            
            # Create AddressComponent
            address_component = AddressComponent(
                long_name=component["long_name"],
                short_name=component["short_name"],
                types=component["types"]
            )
            address_components.append(address_component)
        
        # Extract structured address fields
        geocode_result = GeocodeResult(
            formatted_address=result_data.get("formatted_address", ""),
            address_components=address_components,
            latitude=float(location["lat"]),
            longitude=float(location["lng"]),
            place_id=result_data.get("place_id", ""),
            location_type=result_data["geometry"].get("location_type", "APPROXIMATE"),
            
            # Structured fields
            street_number=component_map.get("street_number", {}).get("long_name"),
            street=component_map.get("route", {}).get("long_name"),
            city=component_map.get("locality", {}).get("long_name"),
            county=component_map.get("administrative_area_level_2", {}).get("long_name"),
            state=component_map.get("administrative_area_level_1", {}).get("long_name"),
            state_code=component_map.get("administrative_area_level_1", {}).get("short_name"),
            postal_code=component_map.get("postal_code", {}).get("long_name"),
            country=component_map.get("country", {}).get("long_name"),
            country_code=component_map.get("country", {}).get("short_name")
        )
        
        return geocode_result


    async def _fallback_autocomplete(self, input_text: str) -> AutocompleteResponse:
        """Fallback autocomplete using OpenStreetMap Nominatim."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "q": input_text,
                        "format": "json",
                        "limit": 5,
                        "addressdetails": 1
                    },
                    timeout=10.0
                )
                response.raise_for_status()
                
                data = response.json()
                suggestions = []
                
                for item in data:
                    suggestions.append(AddressSuggestion(
                        place_id=f"osm_{item['place_id']}",
                        description=item.get("display_name", ""),
                        structured_formatting={
                            "main_text": item.get("display_name", ""),
                            "secondary_text": ""
                        }
                    ))
                
                return AutocompleteResponse(suggestions=suggestions)
                
        except Exception as e:
            logger.error(f"Fallback autocomplete error: {e}")
            return AutocompleteResponse(suggestions=[])


# Singleton instance
_geocoding_service: Optional[GeocodingService] = None


def get_geocoding_service() -> GeocodingService:
    """Get geocoding service instance."""
    global _geocoding_service
    if _geocoding_service is None:
        _geocoding_service = GeocodingService()
    return _geocoding_service