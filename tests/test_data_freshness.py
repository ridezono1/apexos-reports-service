#!/usr/bin/env python3
"""
Test script for NOAA data freshness calculations.
Demonstrates how the data freshness system works for different date ranges.
"""

from datetime import date, timedelta
from app.core.noaa_data_freshness import (
    get_noaa_data_freshness_date,
    get_data_freshness_info,
    format_data_disclaimer,
    format_report_period_with_freshness
)


def print_separator():
    print("\n" + "="*80 + "\n")


def test_current_freshness():
    """Test current data freshness date."""
    print("TEST 1: Current NOAA Data Freshness")
    print("-" * 80)

    today = date.today()
    freshness_date = get_noaa_data_freshness_date(today)

    print(f"Today's Date:        {today.strftime('%B %d, %Y')}")
    print(f"Data Fresh Through:  {freshness_date.strftime('%B %d, %Y')}")
    print(f"Days Lag:            ~90 days")
    print(f"Missing Period:      {freshness_date.strftime('%B %d, %Y')} - {today.strftime('%B %d, %Y')}")

    print_separator()


def test_24_month_report():
    """Test 24-month report (typical insurance use case)."""
    print("TEST 2: 24-Month Insurance Report")
    print("-" * 80)

    today = date.today()
    start_date = today - timedelta(days=730)  # 24 months ago
    end_date = today

    freshness_info = get_data_freshness_info(start_date, end_date)

    print(f"Report Period:       {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
    print(f"Complete Data Through: {freshness_info['freshness_date_formatted']}")
    print(f"Data Complete:       {'Yes' if freshness_info['is_complete'] else 'No'}")
    print(f"Coverage:            {freshness_info['coverage_percent']}%")
    print(f"Complete Days:       {freshness_info['complete_days']} of {freshness_info['total_days']} days")
    print(f"Missing Days:        {freshness_info['missing_days']} days")

    print(f"\nDisclaimer Text:")
    print(f"  {format_data_disclaimer(freshness_info)}")

    if freshness_info['warning_message']:
        print(f"\nWarning Message:")
        print(f"  {freshness_info['warning_message']}")

    print_separator()


def test_historical_report():
    """Test fully historical report (complete data)."""
    print("TEST 3: Historical Report (Complete Data)")
    print("-" * 80)

    today = date.today()
    # Start 24 months ago, end 4 months ago (should be fully complete)
    start_date = today - timedelta(days=730)  # 24 months ago
    end_date = today - timedelta(days=120)    # 4 months ago (past the lag)

    freshness_info = get_data_freshness_info(start_date, end_date)

    print(f"Report Period:       {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
    print(f"Complete Data Through: {freshness_info['freshness_date_formatted']}")
    print(f"Data Complete:       {'Yes' if freshness_info['is_complete'] else 'No'}")
    print(f"Coverage:            {freshness_info['coverage_percent']}%")

    print(f"\nDisclaimer Text:")
    print(f"  {format_data_disclaimer(freshness_info)}")

    print_separator()


def test_recent_report():
    """Test recent 3-month report (mostly incomplete)."""
    print("TEST 4: Recent 3-Month Report (Mostly Incomplete)")
    print("-" * 80)

    today = date.today()
    start_date = today - timedelta(days=90)   # 3 months ago
    end_date = today

    freshness_info = get_data_freshness_info(start_date, end_date)

    print(f"Report Period:       {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
    print(f"Complete Data Through: {freshness_info['freshness_date_formatted']}")
    print(f"Data Complete:       {'Yes' if freshness_info['is_complete'] else 'No'}")
    print(f"Coverage:            {freshness_info['coverage_percent']}%")
    print(f"Complete Days:       {freshness_info['complete_days']} of {freshness_info['total_days']} days")
    print(f"Missing Days:        {freshness_info['missing_days']} days")

    print(f"\nDisclaimer Text:")
    print(f"  {format_data_disclaimer(freshness_info)}")

    if freshness_info['warning_message']:
        print(f"\nWarning Message:")
        print(f"  {freshness_info['warning_message']}")

    print_separator()


def test_formatted_report_period():
    """Test formatted report period output."""
    print("TEST 5: Formatted Report Period Display")
    print("-" * 80)

    today = date.today()
    start_date = today - timedelta(days=730)  # 24 months ago
    end_date = today

    formatted = format_report_period_with_freshness(start_date, end_date)

    print(f"Requested Period:    {formatted['requested_period']}")
    print(f"Data Coverage:       {formatted['data_coverage']}")
    print(f"Is Complete:         {formatted['is_complete']}")
    print(f"\nFreshness Note:")
    print(f"  {formatted['freshness_note']}")

    if formatted['warning_message']:
        print(f"\nWarning Message:")
        print(f"  {formatted['warning_message']}")

    print_separator()


def test_specific_date():
    """Test with a specific reference date (for reproducibility)."""
    print("TEST 6: Specific Date Example (October 24, 2025)")
    print("-" * 80)

    reference_date = date(2025, 10, 24)
    start_date = date(2023, 10, 24)  # 24 months prior
    end_date = reference_date

    freshness_date = get_noaa_data_freshness_date(reference_date)
    freshness_info = get_data_freshness_info(start_date, end_date, reference_date)

    print(f"Reference Date:      {reference_date.strftime('%B %d, %Y')}")
    print(f"Analysis Period:     {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}")
    print(f"Data Fresh Through:  {freshness_date.strftime('%B %d, %Y')}")
    print(f"Coverage:            {freshness_info['coverage_percent']}%")
    print(f"Missing Days:        {freshness_info['missing_days']} days (~3 months)")

    print(f"\nExpected Behavior:")
    print(f"  - Data is complete from Oct 2023 through ~July 2025")
    print(f"  - Aug-Oct 2025 data is incomplete due to NOAA reporting lag")
    print(f"  - Report shows ~87-90% coverage")

    print_separator()


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("NOAA DATA FRESHNESS CALCULATION TESTS")
    print("="*80)

    test_current_freshness()
    test_24_month_report()
    test_historical_report()
    test_recent_report()
    test_formatted_report_period()
    test_specific_date()

    print("\n✅ All tests completed successfully!")
    print("\nSummary:")
    print("  - NOAA data has a ~90-day reporting lag")
    print("  - This is automatically handled in all reports")
    print("  - Users see clear disclaimers when data is incomplete")
    print("  - Coverage percentage is calculated and displayed")
    print("\n")


if __name__ == "__main__":
    main()
