# NOAA Data Freshness Implementation

## Overview

This document describes how the Reports Service handles NOAA Storm Events Database update lag and ensures proper data freshness disclosure in all generated reports.

## Background: NOAA Update Lag

The **NOAA Storm Events Database** is the authoritative source for severe weather event data in the United States. However, it has a built-in reporting lag:

- **Update Frequency**: Monthly
- **Reporting Lag**: 75-120 days after the end of each data month
- **Conservative Estimate**: We use 90 days as the expected lag

### What This Means

If you generate a report on **October 24, 2025**:
- Complete data is available through approximately **July 26, 2025**
- Data from August-October 2025 (~90 days) is incomplete or unavailable
- For a 24-month report, this represents ~87-90% coverage

## Implementation

### Core Utility Module

**Location**: [`app/core/noaa_data_freshness.py`](app/core/noaa_data_freshness.py)

This module provides:

1. **`get_noaa_data_freshness_date(reference_date)`**
   - Calculates the most recent date with complete NOAA data
   - Subtracts 90 days from the reference date

2. **`get_data_freshness_info(start_date, end_date, reference_date)`**
   - Comprehensive freshness analysis for a date range
   - Returns coverage percentage, missing days, and warning messages

3. **`format_data_disclaimer(freshness_info)`**
   - Generates user-friendly disclaimer text for reports

4. **`format_report_period_with_freshness(start_date, end_date, reference_date)`**
   - Formats report period descriptions with data coverage context

### Integration Points

#### 1. Address Analysis Service
**File**: `app/services/address_analysis_service.py`

```python
# Calculate data freshness information
start_date_obj = datetime.fromisoformat(analysis_period["start"]).date()
end_date_obj = datetime.fromisoformat(analysis_period["end"]).date()
freshness_info = get_data_freshness_info(start_date_obj, end_date_obj)

return {
    # ... other fields ...
    "data_freshness": {
        "freshness_date": freshness_info["freshness_date"].isoformat(),
        "freshness_date_formatted": freshness_info["freshness_date_formatted"],
        "is_complete": freshness_info["is_complete"],
        "coverage_percent": freshness_info["coverage_percent"],
        "warning_message": freshness_info["warning_message"],
        "disclaimer": format_data_disclaimer(freshness_info)
    }
}
```

#### 2. Spatial Analysis Service
**File**: `app/services/spatial_analysis_service.py`

Same implementation as address analysis - adds `data_freshness` field to all spatial reports.

#### 3. PDF Generation
**File**: `app/services/pdf_service.py`

Both address and spatial PDF title pages now include:

```python
# Data freshness disclaimer (if available)
data_freshness = report_data.get("data_freshness", {})
if data_freshness:
    disclaimer = Paragraph(
        f"<b>Data Source:</b> {data_freshness.get('disclaimer', 'NOAA Storm Events Database')}",
        self.styles["CustomBody"],
    )
    elements.append(disclaimer)
```

#### 4. HTML Templates

**Address Template**: `templates/address/address_report.html`
**Spatial Template**: `templates/spatial/spatial_report.html`

Both templates include a highlighted disclaimer box:

```html
<!-- Data Freshness Information -->
{% if data_freshness %}
<div style="background: #fff9e6; border-left: 4px solid #f39c12; padding: 15px 20px; margin: 20px 0; border-radius: 4px;">
    <div style="font-size: 10pt; color: #7f6003; line-height: 1.5;">
        <strong>📊 Data Source & Freshness:</strong><br>
        {{ data_freshness.disclaimer }}
        {% if data_freshness.warning_message %}
        <br><br>
        <em>{{ data_freshness.warning_message }}</em>
        {% endif %}
    </div>
</div>
{% endif %}
```

## Example Output

### Complete Data (Historical Report)

```
Data Source: NOAA Storm Events Database.
Data current through July 26, 2025.
```

### Incomplete Data (Current 24-Month Report)

```
Data Source: NOAA Storm Events Database.
Complete data available through July 26, 2025.
Recent events (last ~90 days) may not be included due to
NOAA's reporting lag (typical 75-120 day update cycle).
```

With additional warning:
```
The most recent 90 days may have incomplete data due to NOAA reporting lag.
Complete data is available through July 26, 2025.
```

## Testing

Run the test suite to verify data freshness calculations:

```bash
cd /Users/aavernon2/Documents/GitHub/apexos/reports-service
python test_data_freshness.py
```

The test suite includes:
1. Current data freshness calculation
2. 24-month insurance report scenario
3. Historical report (complete data)
4. Recent report (mostly incomplete)
5. Formatted display output
6. Specific date examples

## Insurance & Legal Considerations

### Why This Matters for Insurance

1. **Claim Validation**: Insurance adjusters need to know if storm data is complete
2. **Audit Compliance**: Documentation of data sources and limitations
3. **Timeline Accuracy**: Clear understanding of what events are included
4. **Risk Assessment**: Confidence in coverage percentages

### Best Practices

1. **Always Include Disclaimers**: Every report should show data freshness
2. **Document Lag**: Make the 75-120 day lag explicit
3. **Show Coverage %**: Quantify how much of the period has complete data
4. **Cite Official Source**: Reference NOAA as the authoritative source

### For Audits

The system provides:
- ✅ Transparent data sourcing (NOAA official database)
- ✅ Clear documentation of update lag
- ✅ Quantified coverage percentages
- ✅ Explicit warnings when data is incomplete
- ✅ Timestamped report generation dates

## Configuration

The reporting lag is configurable in [`app/core/noaa_data_freshness.py`](app/core/noaa_data_freshness.py):

```python
# NOAA Storm Events Database update lag (in days)
NOAA_UPDATE_LAG_DAYS = 90  # Conservative estimate (75-120 days)
```

Adjust this value if NOAA's update cycle changes.

## API Response Format

All report APIs now include a `data_freshness` object:

```json
{
  "data_freshness": {
    "freshness_date": "2025-07-26",
    "freshness_date_formatted": "July 26, 2025",
    "is_complete": false,
    "coverage_percent": 87.7,
    "warning_message": "The most recent 90 days may have incomplete data...",
    "disclaimer": "Data Source: NOAA Storm Events Database. Complete data available..."
  }
}
```

## Future Enhancements

Potential improvements:

1. **Real-Time Verification**: Query NOAA API to check actual latest available data month
2. **Per-Year Accuracy**: Different lag times for different years (older data = more complete)
3. **Supplemental Sources**: Integrate real-time NWS alerts for very recent events
4. **Auto-Refresh**: Scheduled jobs to re-generate reports when new NOAA data becomes available

## Support & Questions

For questions about this implementation:
- Review test examples in `test_data_freshness.py`
- Check NOAA documentation: https://www.ncei.noaa.gov/stormevents/
- Refer to NOAA FAQ: https://www.ncdc.noaa.gov/stormevents/faq.jsp

## Version History

- **v1.0** (October 24, 2025): Initial implementation
  - 90-day reporting lag
  - Automatic freshness calculation
  - PDF and HTML template integration
  - Comprehensive test suite
