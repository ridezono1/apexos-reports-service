"""
Tests for the spatial analysis service.
"""

import pytest
from datetime import datetime, timedelta
from app.services.spatial_analysis_service import SpatialAnalysisService


@pytest.fixture
def spatial_service():
    """Create spatial analysis service instance."""
    return SpatialAnalysisService()


@pytest.fixture
def analysis_period():
    """Create a 6-month analysis period ending today."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=182)
    return {
        "start": start_date.isoformat(),
        "end": end_date.isoformat()
    }


@pytest.fixture
def analysis_options():
    """Create default analysis options."""
    return {
        "risk_factors": ["hail", "wind", "tornado"],
        "include_route_optimization": True
    }


@pytest.mark.asyncio
async def test_analyze_radius_boundary_cypress(spatial_service, analysis_period, analysis_options):
    """Test spatial analysis for a radius around Cypress, TX."""
    # Define a 5km radius around Cypress, TX
    boundary_data = {
        "center_lat": 29.9924,
        "center_lon": -95.6981,
        "radius_km": 5.0
    }

    result = await spatial_service.analyze_spatial_area(
        boundary_type="radius",
        boundary_data=boundary_data,
        analysis_period=analysis_period,
        analysis_options=analysis_options
    )

    # Verify result structure
    assert result is not None
    assert "boundary_info" in result
    assert "analysis_period" in result
    assert "weather_summary" in result
    assert "risk_assessment" in result
    assert "weather_events" in result
    assert "business_impact" in result
    assert "lead_opportunities" in result
    assert "route_optimization" in result

    # Verify boundary info
    boundary_info = result["boundary_info"]
    assert boundary_info["type"] == "radius"
    assert boundary_info["grid_points"] > 0
    assert boundary_info["area_sq_km"] > 0

    # Verify risk assessment
    risk_assessment = result["risk_assessment"]
    assert "overall_risk_score" in risk_assessment
    assert "risk_level" in risk_assessment
    assert risk_assessment["risk_level"] in ["low", "medium", "high"]

    print(f"\nSpatial analysis for 5km radius around Cypress, TX:")
    print(f"Grid points analyzed: {boundary_info['grid_points']}")
    print(f"Area: {boundary_info['area_sq_km']:.2f} sq km")
    print(f"Overall risk level: {risk_assessment['risk_level']}")
    print(f"Risk score: {risk_assessment['overall_risk_score']:.2f}")


@pytest.mark.asyncio
async def test_get_radius_boundary(spatial_service):
    """Test creating a radius boundary."""
    center_lat = 29.9924
    center_lon = -95.6981
    radius_km = 5.0

    boundary = await spatial_service._get_radius_boundary(
        center_lat, center_lon, radius_km
    )

    # Should create a polygon with multiple points
    assert len(boundary) >= 32  # Default is 32 points
    assert all(isinstance(point, tuple) and len(point) == 2 for point in boundary)

    # All points should be approximately the right distance from center
    for lat, lon in boundary:
        distance = spatial_service._calculate_distance(
            (center_lat, center_lon), (lat, lon)
        )
        # Allow some tolerance for approximation
        assert abs(distance - radius_km) < 0.5


@pytest.mark.asyncio
async def test_get_city_boundary(spatial_service):
    """Test getting city boundary for Houston, TX."""
    boundary = await spatial_service._get_city_boundary("Houston", "TX")

    # Should get a list of coordinates
    assert len(boundary) > 0
    assert all(isinstance(point, tuple) and len(point) == 2 for point in boundary)

    print(f"\nHouston, TX boundary: {len(boundary)} points")


def test_point_in_polygon(spatial_service):
    """Test point-in-polygon algorithm."""
    # Define a simple square polygon
    polygon = [
        (0, 0),
        (10, 0),
        (10, 10),
        (0, 10),
        (0, 0)
    ]

    # Test points inside
    assert spatial_service._point_in_polygon((5, 5), polygon) == True
    assert spatial_service._point_in_polygon((1, 1), polygon) == True

    # Test points outside
    assert spatial_service._point_in_polygon((15, 5), polygon) == False
    assert spatial_service._point_in_polygon((-1, 5), polygon) == False

    # Test points on edge (behavior may vary)
    edge_result = spatial_service._point_in_polygon((0, 5), polygon)
    assert isinstance(edge_result, bool)


def test_calculate_distance(spatial_service):
    """Test distance calculation between two points."""
    # Houston to Dallas (approximately 385 km)
    houston = (29.7604, -95.3698)
    dallas = (32.7767, -96.7970)

    distance = spatial_service._calculate_distance(houston, dallas)

    # Should be approximately 385 km (allow 10% tolerance)
    assert 340 < distance < 430

    # Test short distance (Cypress to Houston, ~40 km)
    cypress = (29.9924, -95.6981)
    distance = spatial_service._calculate_distance(cypress, houston)
    assert 30 < distance < 50


def test_cluster_points(spatial_service):
    """Test clustering nearby points."""
    # Create points with clear clusters
    points = [
        (29.99, -95.69),
        (29.99, -95.70),  # Cluster 1
        (30.05, -95.80),
        (30.05, -95.81),  # Cluster 2
        (30.20, -96.00),  # Cluster 3 (single point)
    ]

    clusters = spatial_service._cluster_points(points, max_distance_km=5.0)

    # Should create multiple clusters
    assert len(clusters) >= 1
    assert all(len(cluster) > 0 for cluster in clusters)

    # Total points should match
    total_points = sum(len(cluster) for cluster in clusters)
    assert total_points == len(points)

    print(f"\nClustering results:")
    for i, cluster in enumerate(clusters):
        print(f"Cluster {i+1}: {len(cluster)} points")


def test_estimate_route_distance(spatial_service):
    """Test route distance estimation."""
    # Create a route with known points
    route = [
        (29.99, -95.69),
        (30.00, -95.70),
        (30.01, -95.71)
    ]

    distance = spatial_service._estimate_route_distance(route)

    # Should be a positive distance
    assert distance > 0

    # Single point route should be 0
    single_point = [(29.99, -95.69)]
    assert spatial_service._estimate_route_distance(single_point) == 0


def test_calculate_area(spatial_service):
    """Test polygon area calculation."""
    # Define a simple square (approximately 1 degree x 1 degree)
    square = [
        (30.0, -96.0),
        (31.0, -96.0),
        (31.0, -95.0),
        (30.0, -95.0),
        (30.0, -96.0)
    ]

    area = spatial_service._calculate_area(square)

    # Should be a positive area
    assert area > 0

    # Triangle should have smaller area than square
    triangle = [
        (30.0, -96.0),
        (31.0, -96.0),
        (30.0, -95.0),
        (30.0, -96.0)
    ]
    triangle_area = spatial_service._calculate_area(triangle)
    assert triangle_area < area


def test_calculate_spatial_summary(spatial_service):
    """Test spatial summary calculation."""
    # Mock grid data
    grid_data = [
        {
            "current_weather": {
                "temperature": 75.0,
                "wind_speed": 10.0,
                "precipitation": 0.5
            }
        },
        {
            "current_weather": {
                "temperature": 78.0,
                "wind_speed": 12.0,
                "precipitation": 0.2
            }
        },
        {
            "current_weather": {
                "temperature": 72.0,
                "wind_speed": 8.0,
                "precipitation": 0.0
            }
        }
    ]

    summary = spatial_service._calculate_spatial_summary(grid_data)

    # Verify summary structure
    assert "total_points_analyzed" in summary
    assert summary["total_points_analyzed"] == 3

    assert "temperature_stats" in summary
    assert summary["temperature_stats"]["average"] == pytest.approx(75.0, abs=0.1)
    assert summary["temperature_stats"]["min"] == 72.0
    assert summary["temperature_stats"]["max"] == 78.0

    assert "wind_stats" in summary
    assert summary["wind_stats"]["average"] == pytest.approx(10.0, abs=0.1)

    assert "precipitation_stats" in summary
    assert summary["precipitation_stats"]["total"] == pytest.approx(0.7, abs=0.01)


def test_assess_spatial_risk(spatial_service, analysis_options):
    """Test spatial risk assessment."""
    # Mock grid data with weather events
    grid_data = [
        {
            "coordinates": (29.99, -95.69),
            "weather_events": [
                {"event": "Hail Storm", "severity": "Severe"},
                {"event": "High Wind", "severity": "Moderate"}
            ]
        },
        {
            "coordinates": (30.00, -95.70),
            "weather_events": [
                {"event": "Tornado Warning", "severity": "Severe"}
            ]
        },
        {
            "coordinates": (30.01, -95.71),
            "weather_events": []
        }
    ]

    risk_assessment = spatial_service._assess_spatial_risk(grid_data, analysis_options)

    # Verify risk assessment structure
    assert "overall_risk_score" in risk_assessment
    assert "risk_level" in risk_assessment
    assert "high_risk_areas" in risk_assessment
    assert "medium_risk_areas" in risk_assessment
    assert "low_risk_areas" in risk_assessment

    # Should identify some risk areas
    assert risk_assessment["overall_risk_score"] > 0

    # Total areas should equal grid points
    total_areas = (
        risk_assessment["high_risk_areas"]["count"] +
        risk_assessment["medium_risk_areas"]["count"] +
        risk_assessment["low_risk_areas"]["count"]
    )
    assert total_areas == len(grid_data)

    print(f"\nRisk assessment:")
    print(f"Overall risk: {risk_assessment['risk_level']} ({risk_assessment['overall_risk_score']:.2f})")
    print(f"High risk areas: {risk_assessment['high_risk_areas']['count']}")
    print(f"Medium risk areas: {risk_assessment['medium_risk_areas']['count']}")
    print(f"Low risk areas: {risk_assessment['low_risk_areas']['count']}")
