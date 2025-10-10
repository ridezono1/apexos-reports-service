#!/usr/bin/env python3
"""
Test spatial report generation for Katy, Texas
"""

import asyncio
import sys
import os

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from app.services.spatial_analysis_service import get_spatial_analysis_service
from datetime import date, timedelta

async def test_katy_spatial():
    """Test spatial analysis for Katy, Texas"""

    service = get_spatial_analysis_service()

    # Katy, Texas boundaries
    boundary_type = "city"
    boundary_data = {
        "city_name": "Katy",
        "state_code": "TX"
    }

    # 6 months analysis period
    end_date = date.today()
    start_date = end_date - timedelta(days=180)

    analysis_period = {
        "start": start_date.isoformat(),
        "end": end_date.isoformat()
    }

    analysis_options = {
        "risk_factors": ["hail", "wind", "tornado"]
    }

    print(f"Testing spatial analysis for Katy, TX")
    print(f"Period: {start_date} to {end_date}")

    try:
        result = await service.analyze_spatial_area(
            boundary_type,
            boundary_data,
            analysis_period,
            analysis_options
        )

        print("\n✅ Spatial analysis successful!")
        print(f"Grid points analyzed: {result['boundary_info']['grid_points']}")
        print(f"Total events: {result['weather_events']['total_events']}")
        print(f"Risk level: {result['risk_assessment']['risk_level']}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_katy_spatial())
