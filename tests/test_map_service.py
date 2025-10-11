"""Tests for map generation service."""

import pytest
from io import BytesIO
from PIL import Image
from unittest.mock import Mock, patch, MagicMock

from app.services.map_service import MapService


@pytest.fixture
def map_service():
    """Create map service instance."""
    return MapService()


@pytest.fixture
def sample_events():
    """Create sample event data with coordinates."""
    return [
        {
            'latitude': 29.7604,
            'longitude': -95.3698,
            'type': 'Thunderstorm Wind',
            'severity': 2
        },
        {
            'latitude': 29.8,
            'longitude': -95.4,
            'type': 'Hail',
            'severity': 3
        },
        {
            'latitude': 29.75,
            'longitude': -95.35,
            'type': 'Tornado',
            'severity': 1
        }
    ]


@pytest.fixture
def sample_boundary():
    """Create sample boundary coordinates."""
    return [
        (29.7, -95.5),
        (29.9, -95.5),
        (29.9, -95.3),
        (29.7, -95.3),
        (29.7, -95.5)  # Close polygon
    ]


class TestMapService:
    """Test suite for MapService."""

    def test_initialization(self, map_service):
        """Test service initializes correctly."""
        assert map_service is not None
        assert map_service.default_zoom == 10
        assert map_service.primary_color == '#3498db'
        assert map_service.danger_color == '#e74c3c'

    @patch('app.services.map_service.webdriver.Chrome')
    def test_get_chrome_driver(self, mock_chrome, map_service):
        """Test Chrome driver initialization."""
        mock_driver = Mock()
        mock_chrome.return_value = mock_driver

        driver = map_service._get_chrome_driver()

        assert driver is not None
        mock_chrome.assert_called_once()

    @patch('app.services.map_service.webdriver.Chrome')
    def test_render_map_to_image(self, mock_chrome, map_service):
        """Test rendering Folium map to PNG."""
        # Mock Chrome driver
        mock_driver = Mock()
        mock_screenshot = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000  # Fake PNG header
        mock_driver.get_screenshot_as_png.return_value = mock_screenshot
        mock_chrome.return_value = mock_driver

        # Create simple Folium map
        import folium
        test_map = folium.Map(location=[29.7604, -95.3698], zoom_start=10)

        # Render to image
        result = map_service._render_map_to_image(test_map)

        assert isinstance(result, bytes)
        assert len(result) > 0
        mock_driver.quit.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_heat_map_with_events(self, mock_render, map_service, sample_events):
        """Test heat map generation with valid events."""
        # Mock render to return fake PNG
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        result = map_service.generate_heat_map(
            events=sample_events,
            center_lat=29.7604,
            center_lon=-95.3698,
            title="Test Heat Map"
        )

        assert isinstance(result, bytes)
        assert result == fake_png
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_heat_map_empty_events(self, mock_render, map_service):
        """Test heat map with no events."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        result = map_service.generate_heat_map(
            events=[],
            center_lat=29.7604,
            center_lon=-95.3698,
            title="Empty Heat Map"
        )

        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_heat_map_invalid_coordinates(self, mock_render, map_service):
        """Test heat map handles invalid coordinates."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        events = [
            {'latitude': None, 'longitude': -95.3698},
            {'latitude': 29.7604, 'longitude': None},
            {'latitude': 'invalid', 'longitude': 'invalid'},
            {'latitude': 29.7604, 'longitude': -95.3698}  # Valid
        ]

        result = map_service.generate_heat_map(
            events=events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        assert isinstance(result, bytes)
        # Should only process the valid event
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_heat_map_with_severity(self, mock_render, map_service):
        """Test heat map uses severity values."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        events = [
            {'latitude': 29.7604, 'longitude': -95.3698, 'severity': 5},
            {'latitude': 29.8, 'longitude': -95.4, 'severity': 1}
        ]

        result = map_service.generate_heat_map(
            events=events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_boundary_map(self, mock_render, map_service, sample_events, sample_boundary):
        """Test boundary map generation."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        result = map_service.generate_boundary_map(
            boundary_coords=sample_boundary,
            events=sample_events,
            center_lat=29.7604,
            center_lon=-95.3698,
            title="Test Boundary Map"
        )

        assert isinstance(result, bytes)
        assert result == fake_png
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_boundary_map_empty_boundary(self, mock_render, map_service, sample_events):
        """Test boundary map with no boundary."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        result = map_service.generate_boundary_map(
            boundary_coords=[],
            events=sample_events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_boundary_map_limits_markers(self, mock_render, map_service, sample_boundary):
        """Test boundary map limits markers to 50."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        # Create 100 events
        many_events = []
        for i in range(100):
            many_events.append({
                'latitude': 29.7604 + (i * 0.01),
                'longitude': -95.3698 + (i * 0.01),
                'type': f'Event {i}',
                'severity': 'high'
            })

        result = map_service.generate_boundary_map(
            boundary_coords=sample_boundary,
            events=many_events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        # Should only add 50 markers (max_markers limit)
        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_boundary_map_severity_colors(self, mock_render, map_service, sample_boundary):
        """Test boundary map uses different colors for severity."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        events = [
            {'latitude': 29.7604, 'longitude': -95.3698, 'type': 'High', 'severity': 'high'},
            {'latitude': 29.8, 'longitude': -95.4, 'type': 'Medium', 'severity': 'medium'},
            {'latitude': 29.75, 'longitude': -95.35, 'type': 'Low', 'severity': 'low'}
        ]

        result = map_service.generate_boundary_map(
            boundary_coords=sample_boundary,
            events=events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_generate_simple_location_map(self, mock_render, map_service):
        """Test simple location map generation."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        result = map_service.generate_simple_location_map(
            latitude=29.7604,
            longitude=-95.3698,
            location_name="Houston, TX",
            title="Test Location Map"
        )

        assert isinstance(result, bytes)
        assert result == fake_png
        mock_render.assert_called_once()

    @patch.object(MapService, '_render_map_to_image')
    def test_all_map_types_produce_different_outputs(self, mock_render, map_service, sample_events, sample_boundary):
        """Test that different map types can be generated."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        # Generate all map types
        result1 = map_service.generate_heat_map(
            events=sample_events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        result2 = map_service.generate_boundary_map(
            boundary_coords=sample_boundary,
            events=sample_events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        result3 = map_service.generate_simple_location_map(
            latitude=29.7604,
            longitude=-95.3698,
            location_name="Test Location"
        )

        # All should be valid
        assert all(isinstance(r, bytes) for r in [result1, result2, result3])
        assert mock_render.call_count == 3

    @patch.object(MapService, '_render_map_to_image')
    def test_map_handles_alternative_field_names(self, mock_render, map_service):
        """Test maps handle both 'latitude' and 'begin_lat' field names."""
        fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
        mock_render.return_value = fake_png

        events = [
            {'begin_lat': 29.7604, 'begin_lon': -95.3698},  # Alternative names
            {'latitude': 29.8, 'longitude': -95.4}  # Standard names
        ]

        result = map_service.generate_heat_map(
            events=events,
            center_lat=29.7604,
            center_lon=-95.3698
        )

        assert isinstance(result, bytes)
        mock_render.assert_called_once()

    @patch.object(MapService, '_get_chrome_driver')
    def test_render_map_handles_driver_error(self, mock_get_driver, map_service):
        """Test render handles Chrome driver errors."""
        mock_get_driver.side_effect = Exception("Chrome driver failed")

        import folium
        test_map = folium.Map(location=[29.7604, -95.3698])

        with pytest.raises(Exception, match="Chrome driver failed"):
            map_service._render_map_to_image(test_map)

    @patch.object(MapService, '_render_map_to_image')
    def test_heat_map_error_propagates(self, mock_render, map_service, sample_events):
        """Test heat map propagates rendering errors."""
        mock_render.side_effect = Exception("Render failed")

        with pytest.raises(Exception, match="Render failed"):
            map_service.generate_heat_map(
                events=sample_events,
                center_lat=29.7604,
                center_lon=-95.3698
            )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
