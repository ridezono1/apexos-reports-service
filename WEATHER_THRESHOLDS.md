# Weather Thresholds Documentation

This document explains the wind speed and hail size thresholds used in the Weather Reports Service, based on SkyLink standards for the roofing and property services industry.

## Overview

These thresholds identify weather events that represent **actionable business opportunities** for:
- Property damage assessment
- Roof inspections
- Insurance claims
- Emergency response services

## Hail Size Thresholds

### Actionable Threshold: ≥ 1.0 inch

Hail of 1 inch or larger is considered **damaging to roofs, vehicles, and property**.

### Severity Classifications

| Severity | Size Range | Description | Business Impact |
|----------|-----------|-------------|-----------------|
| **Minor** | < 0.5" | Pea to marble size | Low - cosmetic damage only |
| **Moderate** | 0.5" - 0.99" | Marble to nickel size | Medium - potential minor damage |
| **Severe** | 1.0" - 1.99" | Quarter to golf ball | High - roof damage likely |
| **Extreme** | ≥ 2.0" | Tennis ball or larger | Critical - major structural damage |

### Common Hail Size Descriptors

| Descriptor | Size (inches) | Severity | Actionable |
|------------|---------------|----------|------------|
| Pea | 0.25 | Minor | No |
| Marble | 0.5 | Moderate | No |
| Penny | 0.75 | Moderate | No |
| Nickel | 0.88 | Moderate | No |
| **Quarter** | **1.0** | **Severe** | **✓ YES** |
| Half Dollar | 1.25 | Severe | ✓ YES |
| Walnut | 1.5 | Severe | ✓ YES |
| Golf Ball | 1.75 | Severe | ✓ YES |
| Tennis Ball | 2.5 | Extreme | ✓ YES |
| Baseball | 2.75 | Extreme | ✓ YES |
| Softball | 4.0 | Extreme | ✓ YES |

### Business Impact Scoring (Hail)

| Size | Descriptor | Base Points |
|------|------------|-------------|
| 1.0" | Quarter | +25 |
| 1.25" | Half Dollar | +30 |
| 1.5" | Walnut | +35 |
| 1.75" | Golf Ball | +40 |
| 2.5" | Tennis Ball | +50 |
| 2.75" | Baseball | +60 |
| 4.0" | Softball | +70 |

## Wind Speed Thresholds

### Actionable Threshold: ≥ 60 mph

Wind speeds of 60 mph or greater are considered **damaging straight-line winds** that can cause roof damage similar to hail.

### Severity Classifications

| Severity | Speed Range | Description | Business Impact |
|----------|-------------|-------------|-----------------|
| **Minor** | < 40 mph | Light to moderate winds | Low - minimal damage risk |
| **Moderate** | 40-59 mph | Strong winds | Medium - potential minor damage |
| **Severe** | 60-79 mph | Damaging winds | High - roof damage likely |
| **Extreme** | ≥ 80 mph | Destructive winds | Critical - major structural damage |

### Wind Speed Details

| Speed (mph) | Description | Severity | Actionable |
|-------------|-------------|----------|------------|
| < 40 | Light to moderate | Minor | No |
| 40-59 | Strong winds | Moderate | No |
| **60-69** | **Damaging winds** | **Severe** | **✓ YES** |
| 70-79 | Severe damaging winds | Severe | ✓ YES |
| 80-89 | Destructive winds | Extreme | ✓ YES |
| 90-99 | Very destructive winds | Extreme | ✓ YES |
| ≥ 100 | Catastrophic winds | Extreme | ✓ YES |

### Wind Indicators

Text analysis recognizes these wind descriptors:
- "damaging winds" → Assumes ≥60 mph
- "severe winds" → Assumes ≥60 mph
- "straight line winds" → Assumes ≥60 mph
- "high winds" → Assumes ≥40 mph
- "extreme winds" → Assumes ≥80 mph
- "gusts" → Combined with speed mentions

### Business Impact Scoring (Wind)

| Speed | Description | Base Points |
|-------|-------------|-------------|
| 60 mph | Damaging winds | +30 |
| 70 mph | Severe winds | +35 |
| 80 mph | Destructive winds | +40 |
| 90 mph | Very destructive | +45 |
| 100 mph | Catastrophic | +50 |

## Tornado Events

**Tornadoes are always actionable** - even EF0-EF1 tornadoes cause major localized property damage.

- Base Impact Score: **+80 points**
- Severity Multiplier: **Extreme (1.5x)**
- Always triggers high-priority alerts

## Severity Multipliers

Business impact scores are multiplied based on overall severity:

| Severity | Multiplier | Applied When |
|----------|-----------|--------------|
| **Extreme** | 1.5x | Hail ≥2.0" OR Wind ≥80 mph OR Tornado |
| **Severe** | 1.3x | Hail ≥1.0" OR Wind ≥60 mph |
| **Moderate** | 1.1x | Hail ≥0.5" OR Wind ≥40 mph |
| **Minor** | 0.8x | Below moderate thresholds |

## Configuration

Thresholds are configurable via environment variables or settings:

```python
# Hail thresholds (inches)
ALERT_HAIL_MIN_SIZE_INCHES = 1.0
HAIL_SEVERE_THRESHOLD = 1.0
HAIL_EXTREME_THRESHOLD = 2.0
HAIL_MODERATE_THRESHOLD = 0.5

# Wind thresholds (mph)
ALERT_WIND_MIN_SPEED_MPH = 60.0
WIND_SEVERE_THRESHOLD = 60.0
WIND_EXTREME_THRESHOLD = 80.0
WIND_MODERATE_THRESHOLD = 40.0

# Business impact
ALERT_MIN_BUSINESS_IMPACT_SCORE = 30.0

# Severity multipliers
SEVERITY_MULTIPLIER_EXTREME = 1.5
SEVERITY_MULTIPLIER_SEVERE = 1.3
SEVERITY_MULTIPLIER_MODERATE = 1.1
SEVERITY_MULTIPLIER_MINOR = 0.8
```

## Real-World Examples

### Example 1: Cypress, TX Storm
- **Conditions**: Golf ball hail (1.75") + 65 mph winds
- **Hail Severity**: Severe
- **Wind Severity**: Severe
- **Business Impact Score**: ~92 points (40 + 30 = 70, × 1.3 severe multiplier)
- **Actionable**: ✓ YES - High priority for roof inspections

### Example 2: Minor Storm
- **Conditions**: Marble hail (0.5") + 35 mph winds
- **Hail Severity**: Moderate
- **Wind Severity**: Minor
- **Business Impact Score**: ~0 points (below actionable thresholds)
- **Actionable**: ✗ NO

### Example 3: Extreme Event
- **Conditions**: Baseball hail (2.75") + Tornado
- **Hail Severity**: Extreme
- **Tornado**: Extreme
- **Business Impact Score**: ~195 points (60 + 80 = 140, × 1.5 extreme multiplier)
- **Actionable**: ✓ YES - Emergency response priority

## Industry Standards

These thresholds align with:
- **Insurance Industry**: Most insurance policies consider 1"+ hail as claim-worthy
- **NOAA Standards**: Severe hail is classified as ≥1" diameter
- **NWS Criteria**: Severe wind is ≥58 mph (rounded to 60 for business purposes)
- **Roofing Industry**: Industry standard for inspections after 1"+ hail or 60+ mph winds

## Usage in Code

```python
from app.core.weather_thresholds import WeatherThresholds

# Check if hail is actionable
hail_size = 1.5  # inches
if WeatherThresholds.is_hail_actionable(hail_size):
    severity = WeatherThresholds.classify_hail_severity(hail_size)
    print(f"Actionable hail event: {severity}")

# Check if wind is actionable
wind_speed = 65  # mph
if WeatherThresholds.is_wind_actionable(wind_speed):
    severity = WeatherThresholds.classify_wind_severity(wind_speed)
    print(f"Actionable wind event: {severity}")

# Calculate business impact score
score = WeatherThresholds.calculate_business_impact_score(
    hail_size=1.75,
    wind_speed=65,
    tornado=False
)
print(f"Business impact score: {score}")

# Parse from text
text = "Golf ball size hail and 60 mph winds reported"
hail = WeatherThresholds.parse_hail_size_from_text(text)  # 1.75
wind = WeatherThresholds.parse_wind_speed_from_text(text)  # 60.0
```

## Testing

Comprehensive tests verify all thresholds:

```bash
pytest tests/test_weather_thresholds.py -v
```

Tests cover:
- Severity classifications
- Actionable thresholds
- Text parsing
- Business impact scoring
- Multiplier calculations
- Real-world scenarios

## Updates and Maintenance

Thresholds are based on:
1. **Insurance industry standards** - claim thresholds
2. **NOAA/NWS criteria** - severe weather definitions
3. **Business analysis** - historical claim data
4. **Industry feedback** - roofing contractor input

Update thresholds only when:
- Insurance industry standards change
- NOAA updates severe weather criteria
- Business data shows different optimal thresholds
- Regional variations require adjustment

---

**Last Updated**: 2025-01-07
**Based On**: SkyLink Production Standards
**Industry**: Roofing and Property Services
