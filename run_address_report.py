#!/usr/bin/env python3
"""
Interactive test script to generate address reports using the reports service API.
Usage: python test_address_report.py "123 Main St, Houston, TX 77002"
Or run without arguments to be prompted for an address.
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

async def test_address_report_generation(address, lat=None, lng=None):
    """Test generating an address report for the specified address."""
    
    print("🏠 Testing Address Report Generation")
    print("=" * 60)
    print(f"Address: {address}")
    print(f"Service URL: {BASE_URL}")
    print("-" * 60)
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as client:
        
        # Step 1: Generate the address report
        print("📋 Step 1: Generating address report...")
        
        report_request = {
            "title": f"Weather Report - {address}",
            "type": "weather_summary",
            "location": address,
            "analysis_period": "24_months",
            "template": "address_report",
            "include_charts": True,
            "include_forecast": True,
            "include_storm_events": True,
            "historical_data": None,
            "generate_pdf": True,
            "branding": True,
            "color_scheme": "address_report",
            "latitude": lat,
            "longitude": lng
        }
        
        try:
            response = await client.post(
                f"{BASE_URL}/api/v1/reports/generate/address",
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
        
        max_attempts = 30  # 5 minutes max
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
                        filename = f"address_report_{report_id}.pdf"
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

async def test_geocoding_first(address):
    """Test geocoding the address first to verify coordinates."""
    
    print("🗺️  Testing Geocoding First")
    print("=" * 60)
    
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        try:
            geocode_request = {
                "address": address,
                "components": {
                    "country": "US"
                }
            }
            
            response = await client.post(
                f"{BASE_URL}/api/v1/geocoding/geocode",
                json=geocode_request,
                headers={"X-API-Key": API_KEY}
            )
            
            if response.status_code == 200:
                data = response.json()
                result = data["result"]
                
                print(f"✅ Address geocoded successfully!")
                print(f"   Formatted Address: {result['formatted_address']}")
                print(f"   Latitude: {result['latitude']}")
                print(f"   Longitude: {result['longitude']}")
                print(f"   Place ID: {result['place_id']}")
                
                return result['latitude'], result['longitude']
            else:
                print(f"❌ Geocoding failed: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                return None, None
                
        except Exception as e:
            print(f"❌ Geocoding error: {e}")
            return None, None

def get_user_input():
    """Get address input from user or command line arguments."""
    if len(sys.argv) > 1:
        # Address provided as command line argument
        return sys.argv[1]
    
    print("🏠 ApexOS Reports Service - Interactive Address Report Generator")
    print("=" * 80)
    print("Enter an address to generate a weather report:")
    print("(Examples: '123 Main St, Houston, TX 77002' or 'Houston, TX')")
    print("-" * 80)
    
    try:
        while True:
            address = input("Address: ").strip()
            if address:
                return address
            print("Please enter a valid address.")
    except (EOFError, KeyboardInterrupt):
        print("\n\nUsage: python test_address_report.py \"123 Main St, Houston, TX 77002\"")
        sys.exit(1)

async def main():
    """Main interactive function."""
    
    # Get address from user
    address = get_user_input()
    
    print(f"\n🚀 Generating weather report for: {address}")
    print(f"Analysis period: 24 months")
    print(f"Template: address_report")
    print("=" * 80)
    
    # First test geocoding
    lat, lng = await test_geocoding_first(address)
    
    if lat is None or lng is None:
        print("\n❌ Cannot proceed without valid coordinates")
        return
    
    print(f"\n📍 Using coordinates: {lat}, {lng}")
    
    # Then test report generation
    success, filename = await test_address_report_generation(address, lat, lng)
    
    print("\n" + "=" * 80)
    if success:
        print("✅ Address report test completed successfully!")
        print(f"📄 Report saved as: {filename}")
        print("The address_report template is working correctly.")
        print("\nYou can now open the PDF file to view the generated report.")
    else:
        print("❌ Address report test failed!")
        print("Check the error messages above for troubleshooting.")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
