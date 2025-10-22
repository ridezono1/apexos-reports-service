#!/usr/bin/env python3
"""
Test script for Roofing Weather Service

This script tests the specialized roofing weather service that provides
comprehensive severe weather analysis for roofing marketing companies.
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, '.')

from app.services.roofing_weather_service import RoofingWeatherService

async def test_roofing_weather_service():
    """Test the roofing weather service with the Chute Forest address"""
    
    # Test address: 12335 Chute Forest Ct, Houston, TX 77014, USA
    # Coordinates: ~29.7°N, 95.2°W
    latitude = 29.7
    longitude = -95.2
    
    # Set up 24-month analysis period
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=730)  # 24 months
    
    print(f"🏠 Testing Roofing Weather Service")
    print(f"📍 Location: {latitude}°N, {longitude}°W")
    print(f"📅 Analysis period: {start_date} to {end_date}")
    print(f"🔍 Search radius: 25 miles")
    print("=" * 80)
    
    try:
        # Initialize service
        cdo_token = os.getenv("NOAA_CDO_API_TOKEN", "dEDIUlflGGFFxRTUVGZZoWmkbPkZUsqu")
        weather_service = RoofingWeatherService(cdo_token)
        
        # Get roofing weather analysis
        analysis = await weather_service.get_roofing_weather_analysis(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            radius_miles=25.0
        )
        
        print(f"✅ Roofing weather analysis completed!")
        
        # Display analysis results
        print(f"\n📊 Analysis Summary:")
        print(f"  - Total events: {analysis['analysis_period']['total_events']}")
        print(f"  - Insurance relevant: {analysis['analysis_period']['insurance_relevant_events']}")
        print(f"  - High risk events: {analysis['analysis_period']['high_risk_events']}")
        print(f"  - Risk score: {analysis['analysis_period']['risk_score']}")
        print(f"  - Marketing score: {analysis['roofing_marketing_score']}/10")
        print(f"  - Priority level: {analysis['priority_level']}")
        
        print(f"\n🎯 Event Summary:")
        for event_type, count in analysis['event_summary'].items():
            print(f"  - {event_type.title()}: {count} events")
        
        print(f"\n📋 Marketing Recommendations:")
        for i, rec in enumerate(analysis['marketing_recommendations'], 1):
            print(f"  {i}. {rec}")
        
        print(f"\n🌤️  Detailed Events:")
        for i, event in enumerate(analysis['events'][:10], 1):  # Show first 10
            print(f"  {i}. {event['event_type'].title()} - {event['severity'].title()}")
            print(f"     Description: {event['description']}")
            print(f"     Date: {event['timestamp']}")
            print(f"     Source: {event['source']}")
            print(f"     Insurance Relevant: {'Yes' if event['insurance_relevant'] else 'No'}")
            print(f"     Roofing Risk: {event['roofing_damage_risk'].title()}")
            if event['magnitude']:
                print(f"     Magnitude: {event['magnitude']}")
            print()
        
        if len(analysis['events']) > 10:
            print(f"     ... and {len(analysis['events']) - 10} more events")
        
        # Compare with HailTrace expectations
        print(f"\n🎯 Comparison with HailTrace Report:")
        print(f"  - HailTrace found: 7 severe weather events")
        print(f"  - Our service found: {analysis['analysis_period']['total_events']} events")
        print(f"  - Insurance relevant: {analysis['analysis_period']['insurance_relevant_events']} events")
        print(f"  - High risk events: {analysis['analysis_period']['high_risk_events']} events")
        
        if analysis['analysis_period']['total_events'] >= 7:
            print(f"  ✅ Event count matches or exceeds HailTrace")
        else:
            print(f"  ⚠️  Event count lower than HailTrace")
        
        # Roofing marketing assessment
        print(f"\n🏠 Roofing Marketing Assessment:")
        print(f"  - Marketing Score: {analysis['roofing_marketing_score']}/10")
        print(f"  - Priority Level: {analysis['priority_level']}")
        
        if analysis['priority_level'] == "High":
            print(f"  ✅ HIGH PRIORITY - Strong potential for roofing marketing")
        elif analysis['priority_level'] == "Medium":
            print(f"  ⚠️  MEDIUM PRIORITY - Moderate potential for roofing marketing")
        else:
            print(f"  ℹ️  LOW PRIORITY - Limited potential for roofing marketing")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing roofing weather service: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting Roofing Weather Service Test")
    
    # Run test
    asyncio.run(test_roofing_weather_service())
    
    print("\n✅ Test completed!")
