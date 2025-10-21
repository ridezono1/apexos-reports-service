#!/usr/bin/env python3
"""
Test script for Comprehensive Weather Service

This script tests the new comprehensive weather service that combines
multiple NOAA data sources for roofing marketing and insurance analysis.
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, '.')

from app.services.comprehensive_weather_service import ComprehensiveWeatherService

async def test_comprehensive_weather_service():
    """Test the comprehensive weather service with the Chute Forest address"""
    
    # Test address: 12335 Chute Forest Ct, Houston, TX 77014, USA
    # Coordinates: ~29.7°N, 95.2°W
    latitude = 29.7
    longitude = -95.2
    
    # Set up 24-month analysis period
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)  # 24 months
    
    print(f"Testing Comprehensive Weather Service")
    print(f"Location: {latitude}°N, {longitude}°W")
    print(f"Analysis period: {start_date} to {end_date}")
    print(f"Search radius: 25 miles")
    print("=" * 80)
    
    try:
        # Initialize service
        cdo_token = os.getenv("NOAA_CDO_API_TOKEN", "dEDIUlflGGFFxRTUVGZZoWmkbPkZUsqu")
        weather_service = ComprehensiveWeatherService(cdo_token)
        
        # Get comprehensive weather events
        events = await weather_service.get_comprehensive_weather_events(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            radius_miles=25.0
        )
        
        print(f"✅ Comprehensive weather analysis completed!")
        print(f"🌤️  Total events found: {len(events)}")
        
        if events:
            print(f"\n📊 Severe Weather Events Analysis:")
            
            # Group by event type
            event_types = {}
            insurance_relevant = 0
            
            for event in events:
                event_type = event.event_type
                if event_type not in event_types:
                    event_types[event_type] = []
                event_types[event_type].append(event)
                
                if event.insurance_relevant:
                    insurance_relevant += 1
            
            print(f"\n📈 Event Summary:")
            print(f"  - Insurance relevant events: {insurance_relevant}")
            print(f"  - Event types found: {len(event_types)}")
            
            for event_type, type_events in event_types.items():
                print(f"\n🔍 {event_type.title()} Events ({len(type_events)}):")
                
                # Show first 5 events of each type
                for i, event in enumerate(type_events[:5]):
                    print(f"  {i+1}. {event.severity.title()} - {event.description}")
                    print(f"     Date: {event.timestamp}")
                    print(f"     Source: {event.source}")
                    print(f"     Insurance Relevant: {'Yes' if event.insurance_relevant else 'No'}")
                    if event.magnitude:
                        print(f"     Magnitude: {event.magnitude}")
                    print()
                
                if len(type_events) > 5:
                    print(f"     ... and {len(type_events) - 5} more {event_type} events")
            
            # Compare with HailTrace expectations
            print(f"\n🎯 Comparison with HailTrace Report:")
            print(f"  - HailTrace found: 7 severe weather events")
            print(f"  - Our service found: {len(events)} events")
            print(f"  - Insurance relevant: {insurance_relevant} events")
            
            if len(events) >= 7:
                print(f"  ✅ Event count matches or exceeds HailTrace")
            else:
                print(f"  ⚠️  Event count lower than HailTrace - may need additional data sources")
                
        else:
            print(f"\n⚠️  No severe weather events found")
            print(f"This could indicate:")
            print(f"  - No severe weather in the area during this period")
            print(f"  - Data source limitations")
            print(f"  - Need for additional data sources")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing comprehensive weather service: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_swdi_endpoints():
    """Test individual SWDI endpoints to see what data is available"""
    
    print(f"\n🔬 Testing SWDI Endpoints:")
    print("=" * 50)
    
    # Test coordinates for Houston area
    bbox = "-95.2,29.7,-95.1,29.8"  # Small area around Houston
    date_range = "20240101:20240131"  # January 2024
    
    endpoints = [
        "nx3hail",    # Hail signatures
        "nx3tvs",     # Tornado vortex signatures  
        "nx3meso",    # Mesocyclone signatures
        "nx3vil",     # Vertically integrated liquid
    ]
    
    import httpx
    
    for endpoint in endpoints:
        try:
            url = f"https://www.ncei.noaa.gov/swdiws/json/{endpoint}/{date_range}?bbox={bbox}"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                count = len(data.get("result", []))
                print(f"  {endpoint}: {count} records found")
                
                if count > 0:
                    # Show sample record
                    sample = data["result"][0]
                    props = sample.get("properties", {})
                    print(f"    Sample: {props.get('time', 'N/A')} - {props.get('max', 'N/A')}")
                
        except Exception as e:
            print(f"  {endpoint}: Error - {e}")
    
    return True

if __name__ == "__main__":
    print("🚀 Starting Comprehensive Weather Service Tests")
    
    # Run tests
    asyncio.run(test_comprehensive_weather_service())
    asyncio.run(test_swdi_endpoints())
    
    print("\n✅ Tests completed!")
