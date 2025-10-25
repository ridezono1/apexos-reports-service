"""
NOAA Data Freshness Utility

Handles calculation of NOAA Storm Events Database data freshness and availability.

NOAA updates the Storm Events Database monthly with a 75-120 day lag after the end of each month.
This means the most recent 3 months of data are typically unavailable.
"""

from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional


# NOAA Storm Events Database update lag (in days)
NOAA_UPDATE_LAG_DAYS = 120  # Official range: 75-120 days, use worst-case  # Conservative estimate (75-120 days)


def get_noaa_data_freshness_date(reference_date: Optional[date] = None) -> date:
    """
    Calculate the most recent date for which NOAA Storm Events data is likely available.

    Args:
        reference_date: Date to calculate from (defaults to today)

    Returns:
        Date representing the freshness cutoff for NOAA data

    Example:
        If today is October 24, 2025, this returns approximately July 26, 2025
        (90 days prior), meaning data is complete through July 2025.
    """
    if reference_date is None:
        reference_date = date.today()

    return reference_date - timedelta(days=NOAA_UPDATE_LAG_DAYS)


def get_data_freshness_info(
    start_date: date,
    end_date: date,
    reference_date: Optional[date] = None
) -> Dict[str, Any]:
    """
    Get comprehensive data freshness information for a date range.

    Args:
        start_date: Start of the analysis period
        end_date: End of the analysis period
        reference_date: Date to calculate from (defaults to today)

    Returns:
        Dictionary with freshness information including:
        - freshness_date: Latest date with complete data
        - days_lag: Number of days of lag
        - is_complete: Whether the entire period has complete data
        - missing_days: Number of days at the end that may be incomplete
        - coverage_percent: Percentage of period with complete data
        - warning_message: Human-readable warning if data is incomplete
    """
    if reference_date is None:
        reference_date = date.today()

    freshness_date = get_noaa_data_freshness_date(reference_date)

    # Calculate if the analysis period falls within complete data
    is_complete = end_date <= freshness_date

    # Calculate missing days (if any)
    missing_days = 0
    if end_date > freshness_date:
        missing_days = (end_date - freshness_date).days

    # Calculate coverage percentage
    total_days = (end_date - start_date).days
    complete_days = total_days - missing_days
    coverage_percent = (complete_days / total_days * 100) if total_days > 0 else 100.0

    # Generate warning message
    warning_message = None
    if not is_complete:
        if missing_days <= 30:
            warning_message = (
                f"Recent events (last {missing_days} days) may not be included due to NOAA reporting lag. "
                f"Data is complete through {freshness_date.strftime('%B %d, %Y')}."
            )
        elif missing_days <= 90:
            warning_message = (
                f"The most recent {missing_days} days may have incomplete data due to NOAA reporting lag. "
                f"Complete data is available through {freshness_date.strftime('%B %d, %Y')}."
            )
        else:
            warning_message = (
                f"Significant portion of requested period may have incomplete data. "
                f"NOAA data is complete through {freshness_date.strftime('%B %d, %Y')}. "
                f"Approximately {missing_days} days of data may be incomplete."
            )

    return {
        "freshness_date": freshness_date,
        "freshness_date_formatted": freshness_date.strftime("%B %d, %Y"),
        "freshness_month_year": freshness_date.strftime("%B %Y"),
        "days_lag": NOAA_UPDATE_LAG_DAYS,
        "is_complete": is_complete,
        "missing_days": missing_days,
        "complete_days": complete_days,
        "total_days": total_days,
        "coverage_percent": round(coverage_percent, 1),
        "warning_message": warning_message,
        "reference_date": reference_date,
        "analysis_start": start_date,
        "analysis_end": end_date
    }


def format_data_disclaimer(freshness_info: Dict[str, Any]) -> str:
    """
    Format a concise disclaimer text for reports.

    Args:
        freshness_info: Output from get_data_freshness_info()

    Returns:
        Formatted disclaimer string
    """
    if freshness_info["is_complete"]:
        return (
            f"Data Source: NOAA Storm Events Database (verified). "
            f"Data current through {freshness_info['freshness_date_formatted']}."
        )
    else:
        return (
            f"Data Sources:\n"
            f"• Historical (>{freshness_info['days_lag']} days ago): NOAA Storm Events Database (verified)\n"
            f"• Recent (last {freshness_info['days_lag']} days): NWS Storm Prediction Center Preliminary Reports\n\n"
            f"Complete verified data available through {freshness_info['freshness_date_formatted']}. "
            f"Preliminary reports included for recent period. Final verification typically takes 75-120 days."
        )


def format_report_period_with_freshness(
    start_date: date,
    end_date: date,
    reference_date: Optional[date] = None
) -> Dict[str, str]:
    """
    Format report period description with data freshness context.

    Args:
        start_date: Start of analysis period
        end_date: End of analysis period
        reference_date: Date to calculate from (defaults to today)

    Returns:
        Dictionary with formatted strings for display
    """
    freshness_info = get_data_freshness_info(start_date, end_date, reference_date)

    # Format the requested period
    period_description = f"{start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}"

    # Format the actual data coverage
    if freshness_info["is_complete"]:
        data_coverage = f"Complete data for entire period"
    else:
        actual_end_date = min(end_date, freshness_info["freshness_date"])
        data_coverage = (
            f"Complete data: {start_date.strftime('%B %d, %Y')} to "
            f"{actual_end_date.strftime('%B %d, %Y')} "
            f"({freshness_info['coverage_percent']}% coverage)"
        )

    return {
        "requested_period": period_description,
        "data_coverage": data_coverage,
        "freshness_note": format_data_disclaimer(freshness_info),
        "is_complete": freshness_info["is_complete"],
        "warning_message": freshness_info["warning_message"]
    }
