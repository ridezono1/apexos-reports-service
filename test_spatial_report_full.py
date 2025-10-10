#!/usr/bin/env python3
"""
Test full spatial report generation (including PDF) for Katy, Texas
"""

import asyncio
import sys
import os

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from app.services.report_generator import ReportGenerator
from app.models import ReportFormat
from datetime import date, timedelta

async def test_katy_spatial_report():
    """Test complete spatial report generation for Katy, Texas"""

    generator = ReportGenerator()

    # Katy, Texas boundaries
    boundary_type = "city"
    boundary_data = {
        "city_name": "Katy",
        "state_code": "TX"
    }

    analysis_period = "6_months"
    template = "professional"
    format = ReportFormat.PDF

    options = {
        "risk_factors": ["hail", "wind", "tornado"],
        "boundary_name": "Katy, Texas"
    }

    print(f"Testing spatial report generation for Katy, TX")
    print(f"Analysis period: {analysis_period}")
    print(f"Template: {template}")
    print(f"Format: {format}")

    try:
        # Generate report
        report_id = await generator.generate_spatial_report(
            boundary_type,
            boundary_data,
            analysis_period,
            template,
            format,
            options
        )

        print(f"\n✅ Report ID: {report_id}")
        print(f"Status: {generator.report_status[report_id]['status']}")

        # Wait for completion (poll status)
        import time
        max_wait = 120  # 2 minutes
        waited = 0
        while waited < max_wait:
            status = generator.get_report_status(report_id)
            print(f"Status after {waited}s: {status['status']}")

            if status['status'] == 'completed':
                print(f"\n✅ Report generated successfully!")
                print(f"File path: {status.get('file_path')}")
                print(f"File size: {status.get('file_size')} bytes")

                # Copy to current directory for inspection
                import shutil
                if status.get('file_path') and os.path.exists(status['file_path']):
                    dest = f"katy_spatial_report.pdf"
                    shutil.copy(status['file_path'], dest)
                    print(f"Copied report to: {dest}")
                break
            elif status['status'] == 'failed':
                print(f"\n❌ Report generation failed!")
                print(f"Error: {status.get('error_message')}")
                break

            await asyncio.sleep(5)
            waited += 5

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_katy_spatial_report())
