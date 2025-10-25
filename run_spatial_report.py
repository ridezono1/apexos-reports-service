#!/usr/bin/env python3
"""
Interactive test script to generate spatial reports using the reports service API.
Tests different types of geographic areas: cities, counties, states, neighborhoods, etc.
Usage: python test_spatial_report.py "Houston, TX"
Or run without arguments to be prompted for a location.
"""

import asyncio
import httpx
import json
import time
import sys
from datetime import date, timedelta

# Test configuration
BASE_URL = "http://localhost:8001"  # Reports service local URL
API_KEY = "test-api-key"  # Default test API key

# Predefined test areas for different geographic types
TEST_AREAS = {
    "city": [
        "Houston, TX",
        "Austin, TX", 
        "Dallas, TX",
        "San Antonio, TX",
        "Phoenix, AZ",
        "Denver, CO"
    ],
    "county": [
        "Harris County, TX",
        "Travis County, TX",
        "Dallas County, TX",
        "Bexar County, TX",
        "Maricopa County, AZ",
        "Denver County, CO"
    ],
    "state": [
        "Texas",
        "California", 
        "Florida",
        "New York",
        "Colorado",
        "Arizona"
    ],
    "neighborhood": [
        "Montrose, Houston, TX",
        "Downtown Austin, TX",
        "Deep Ellum, Dallas, TX",
        "River Walk, San Antonio, TX",
        "Old Town Scottsdale, AZ",
        "LoDo, Denver, CO"
    ],
    "region": [
        "Greater Houston Area, TX",
        "Austin Metro Area, TX",
        "Dallas-Fort Worth Metroplex, TX",
        "San Antonio Metro Area, TX",
        "Phoenix Metro Area, AZ",
        "Denver Metro Area, CO"
    ]
}

async def test_spatial_report_generation(boundary_id, boundary_type="auto"):
    """Test generating a spatial report for the specified boundary."""
    
    print("🗺️  Testing Spatial Report Generation")
    print("=" * 60)
    print(f"Boundary: {boundary_id}")
    print(f"Boundary Type: {boundary_type}")
    print(f"Service URL: {BASE_URL}")
    print("-" * 60)
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        
        # Step 1: Generate the spatial report
        print("📋 Step 1: Generating spatial report...")
        
        report_request = {
            "title": f"Spatial Weather Analysis - {boundary_id}",
            "boundary_id": boundary_id,
            "analysis_period": "24_months",
            "boundary_name": boundary_id,
            "risk_factors": ["hail", "wind", "tornado"],
            "storm_types": ["thunderstorm", "tornado", "hail"],
            "include_risk_assessment": True,
            "include_storm_impact_zones": True,
            "include_weather_interpolation": True,
            "include_statistical_summaries": True,
            "sub_area_level": boundary_type,
            "include_heat_maps": True,
            "include_charts": True,
            "generate_pdf": True,
            "weather_parameters": ["temperature_high", "temperature_low", "precipitation", "wind_speed"]
        }
        
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/reports/generate/spatial",
                json=report_request,
                headers={"X-API-Key": API_KEY}
            )
            
            if response.status_code != 200:
                print(f"❌ Failed to generate report: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return False, None
            
            data = response.json()
            report_id = data["report_id"]
            
            print(f"✅ Report generation started")
            print(f"   Report ID: {report_id}")
            print(f"   Status: {data['status']}")
            print(f"   Message: {data['message']}")
            
        except Exception as e:
            print(f"❌ Error generating report: {e}")
            return False, None
        
        # Step 2: Monitor report status
        print("\n⏳ Step 2: Monitoring report generation...")
        
        max_attempts = 60  # 10 minutes max (spatial reports take longer)
        attempt = 0
        
        while attempt < max_attempts:
            try:
                status_response = await client.get(
                    f"{BASE_URL}/api/v1/reports/{report_id}/status",
                    headers={"X-API-Key": API_KEY}
                )
                
                if status_response.status_code != 200:
                    print(f"❌ Failed to get status: HTTP {status_response.status_code}")
                    break
                
                status_data = status_response.json()
                status = status_data["status"]
                
                print(f"   Attempt {attempt + 1}: Status = {status}")
                
                if status == "completed":
                    print(f"✅ Report generation completed!")
                    print(f"   File path: {status_data.get('file_path', 'N/A')}")
                    print(f"   File size: {status_data.get('file_size', 0)} bytes")
                    print(f"   Generated at: {status_data.get('generated_at', 'N/A')}")
                    
                    # Step 3: Download the report
                    print("\n📥 Step 3: Downloading report...")
                    
                    download_response = await client.get(
                        f"{BASE_URL}/api/v1/reports/{report_id}/download",
                        headers={"X-API-Key": API_KEY}
                    )
                    
                    if download_response.status_code == 200:
                        # Save the PDF file
                        safe_filename = boundary_id.replace(",", "_").replace(" ", "_").replace("/", "_")
                        filename = f"spatial_report_{safe_filename}_{report_id[:8]}.pdf"
                        with open(filename, "wb") as f:
                            f.write(download_response.content)
                        
                        print(f"✅ Report downloaded successfully!")
                        print(f"   Filename: {filename}")
                        print(f"   Size: {len(download_response.content)} bytes")
                        
                        return True, filename
                    else:
                        print(f"❌ Failed to download report: HTTP {download_response.status_code}")
                        return False, None
                
                elif status == "failed":
                    print(f"❌ Report generation failed!")
                    print(f"   Error: {status_data.get('error_message', 'Unknown error')}")
                    return False, None
                
                elif status in ["pending", "processing", "generating"]:
                    print(f"   Estimated completion: {status_data.get('estimated_completion', 'N/A')}")
                    await asyncio.sleep(10)  # Wait 10 seconds
                    attempt += 1
                else:
                    print(f"   Unknown status: {status}")
                    await asyncio.sleep(10)
                    attempt += 1
                    
            except Exception as e:
                print(f"❌ Error checking status: {e}")
                break
        
        if attempt >= max_attempts:
            print(f"❌ Timeout: Report generation took too long")
            return False, None
        
        return False, None

def get_user_input():
    """Get boundary input from user or command line arguments."""
    if len(sys.argv) > 1:
        # Boundary provided as command line argument
        return sys.argv[1]
    
    print("🗺️  ApexOS Reports Service - Interactive Spatial Report Generator")
    print("=" * 80)
    print("Choose a test area or enter a custom location:")
    print("-" * 80)
    
    # Show predefined options
    for area_type, areas in TEST_AREAS.items():
        print(f"\n{area_type.title()} Examples:")
        for i, area in enumerate(areas[:3], 1):  # Show first 3 examples
            print(f"  {i}. {area}")
        if len(areas) > 3:
            print(f"  ... and {len(areas) - 3} more")
    
    print(f"\nOptions:")
    print(f"1. Enter a custom location (city, county, state, etc.)")
    print(f"2. Choose from predefined examples")
    print(f"3. Test multiple areas automatically")
    
    try:
        choice = input("\nEnter your choice (1-3) or a custom location: ").strip()
        
        if choice == "1":
            location = input("Enter custom location: ").strip()
            if location:
                return location
        elif choice == "2":
            print("\nPredefined Examples:")
            all_areas = []
            for area_type, areas in TEST_AREAS.items():
                for area in areas:
                    all_areas.append((area, area_type))
            
            for i, (area, area_type) in enumerate(all_areas, 1):
                print(f"  {i}. {area} ({area_type})")
            
            try:
                selection = int(input("Enter number (1-{}): ".format(len(all_areas))))
                if 1 <= selection <= len(all_areas):
                    return all_areas[selection - 1][0]
            except ValueError:
                pass
        elif choice == "3":
            return "auto_test"  # Special flag for auto-testing
        
        print("Invalid choice, using default: Houston, TX")
        return "Houston, TX"
        
    except (EOFError, KeyboardInterrupt):
        print("\n\nUsage: python test_spatial_report.py \"Houston, TX\"")
        sys.exit(1)

async def auto_test_multiple_areas():
    """Test multiple predefined areas automatically."""
    print("🚀 Auto-testing multiple spatial report areas...")
    print("=" * 80)
    
    test_results = []
    
    # Test one example from each category
    test_cases = [
        ("Houston, TX", "city"),
        ("Harris County, TX", "county"), 
        ("Texas", "state"),
        ("Montrose, Houston, TX", "neighborhood")
    ]
    
    for boundary_id, boundary_type in test_cases:
        print(f"\n{'='*60}")
        print(f"Testing: {boundary_id} ({boundary_type})")
        print(f"{'='*60}")
        
        success, filename = await test_spatial_report_generation(boundary_id, boundary_type)
        test_results.append({
            "boundary": boundary_id,
            "type": boundary_type,
            "success": success,
            "filename": filename
        })
        
        if success:
            print(f"✅ {boundary_id} - SUCCESS")
        else:
            print(f"❌ {boundary_id} - FAILED")
        
        # Wait between tests
        if boundary_id != test_cases[-1][0]:  # Not the last one
            print("\n⏳ Waiting 30 seconds before next test...")
            await asyncio.sleep(30)
    
    # Summary
    print(f"\n{'='*80}")
    print("AUTO-TEST SUMMARY")
    print(f"{'='*80}")
    
    successful = sum(1 for result in test_results if result["success"])
    total = len(test_results)
    
    for result in test_results:
        status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
        print(f"{result['boundary']} ({result['type']}): {status}")
        if result["filename"]:
            print(f"  📄 {result['filename']}")
    
    print(f"\nOverall: {successful}/{total} tests passed")
    
    return successful == total

async def main():
    """Main interactive function."""
    
    # Get boundary from user
    boundary_input = get_user_input()
    
    if boundary_input == "auto_test":
        success = await auto_test_multiple_areas()
    else:
        print(f"\n🚀 Generating spatial weather report for: {boundary_input}")
        print(f"Analysis period: 24 months")
        print(f"Template: spatial_report")
        print("=" * 80)
        
        # Determine boundary type
        boundary_lower = boundary_input.lower()
        if "county" in boundary_lower:
            boundary_type = "county"
        elif any(state in boundary_lower for state in ["texas", "california", "florida", "new york", "colorado", "arizona"]):
            boundary_type = "state"
        elif "," in boundary_input and len(boundary_input.split(",")) >= 2:
            boundary_type = "city"
        else:
            boundary_type = "auto"
        
        print(f"Detected boundary type: {boundary_type}")
        
        # Generate report
        success, filename = await test_spatial_report_generation(boundary_input, boundary_type)
    
    print("\n" + "=" * 80)
    if success:
        print("✅ Spatial report test completed successfully!")
        if boundary_input != "auto_test":
            print(f"📄 Report saved as: {filename}")
        print("The spatial_report template is working correctly.")
        print("\nYou can now open the PDF file(s) to view the generated report(s).")
    else:
        print("❌ Spatial report test failed!")
        print("Check the error messages above for troubleshooting.")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
