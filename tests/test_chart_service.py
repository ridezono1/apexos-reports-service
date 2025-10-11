"""Tests for chart generation service."""

import pytest
from datetime import datetime, timedelta
from io import BytesIO
from PIL import Image

from app.services.chart_service import ChartService


@pytest.fixture
def chart_service():
    """Create chart service instance."""
    return ChartService()


@pytest.fixture
def sample_events():
    """Create sample event data for testing."""
    base_date = datetime(2024, 1, 1)
    events = []

    event_types = ['Thunderstorm Wind', 'Hail', 'Tornado', 'Flood', 'Lightning']

    for i in range(50):
        events.append({
            'date': (base_date + timedelta(days=i*7)).strftime('%Y-%m-%d'),
            'type': event_types[i % len(event_types)],
            'begin_date': (base_date + timedelta(days=i*7)).strftime('%Y-%m-%d'),
            'event_type': event_types[i % len(event_types)],
            'severity': ['low', 'medium', 'high'][i % 3]
        })

    return events


class TestChartService:
    """Test suite for ChartService."""

    def test_initialization(self, chart_service):
        """Test service initializes correctly."""
        assert chart_service is not None
        assert chart_service.primary_color == '#3498db'
        assert chart_service.secondary_color == '#e74c3c'

    def test_time_series_chart_with_events(self, chart_service, sample_events):
        """Test time series chart generation with valid events."""
        result = chart_service.generate_time_series_chart(
            events=sample_events,
            title="Test Time Series"
        )

        # Check result is bytes
        assert isinstance(result, bytes)
        assert len(result) > 0

        # Verify it's a valid PNG
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'
        assert img.size[0] > 0
        assert img.size[1] > 0

    def test_time_series_chart_empty_events(self, chart_service):
        """Test time series chart with no events."""
        result = chart_service.generate_time_series_chart(
            events=[],
            title="Empty Chart"
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

        # Should still produce valid image with message
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'

    def test_time_series_chart_invalid_dates(self, chart_service):
        """Test time series chart handles invalid dates gracefully."""
        events = [
            {'date': 'invalid-date', 'type': 'Hail'},
            {'date': '2024-01-15', 'type': 'Wind'},
            {'date': None, 'type': 'Tornado'}
        ]

        result = chart_service.generate_time_series_chart(events=events)

        # Should still work with valid dates only
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_event_distribution_chart_with_events(self, chart_service, sample_events):
        """Test event distribution chart generation."""
        result = chart_service.generate_event_distribution_chart(
            events=sample_events,
            title="Test Distribution"
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'
        assert img.size == (1500, 900)  # 10*150 DPI, 6*150 DPI

    def test_event_distribution_chart_empty(self, chart_service):
        """Test distribution chart with no events."""
        result = chart_service.generate_event_distribution_chart(
            events=[],
            title="Empty Distribution"
        )

        assert isinstance(result, bytes)
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'

    def test_event_distribution_chart_many_types(self, chart_service):
        """Test distribution chart limits to top 10 types."""
        events = []
        for i in range(20):  # Create 20 different types
            events.append({
                'type': f'Event Type {i}',
                'event_type': f'Event Type {i}'
            })

        result = chart_service.generate_event_distribution_chart(events=events)

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_monthly_breakdown_chart_with_events(self, chart_service, sample_events):
        """Test monthly breakdown chart generation."""
        result = chart_service.generate_monthly_breakdown_chart(
            events=sample_events,
            title="Test Monthly Breakdown"
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'
        assert img.size == (1800, 900)  # 12*150 DPI, 6*150 DPI

    def test_monthly_breakdown_chart_empty(self, chart_service):
        """Test monthly breakdown with no events."""
        result = chart_service.generate_monthly_breakdown_chart(
            events=[],
            title="Empty Monthly"
        )

        assert isinstance(result, bytes)
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'

    def test_monthly_breakdown_chart_single_month(self, chart_service):
        """Test monthly breakdown with events in one month."""
        events = [
            {'date': '2024-01-05', 'type': 'Hail'},
            {'date': '2024-01-10', 'type': 'Wind'},
            {'date': '2024-01-15', 'type': 'Hail'}
        ]

        result = chart_service.generate_monthly_breakdown_chart(events=events)

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_severity_heatmap_with_events(self, chart_service, sample_events):
        """Test severity heatmap generation."""
        result = chart_service.generate_severity_heatmap(
            events=sample_events,
            title="Test Severity Heatmap"
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'
        assert img.size == (2100, 600)  # 14*150 DPI, 4*150 DPI

    def test_severity_heatmap_empty(self, chart_service):
        """Test severity heatmap with no events."""
        result = chart_service.generate_severity_heatmap(
            events=[],
            title="Empty Heatmap"
        )

        assert isinstance(result, bytes)
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'

    def test_chart_with_custom_title(self, chart_service, sample_events):
        """Test charts respect custom titles."""
        custom_title = "Custom Title for Testing"

        result = chart_service.generate_time_series_chart(
            events=sample_events,
            title=custom_title
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_chart_handles_missing_fields(self, chart_service):
        """Test charts handle events with missing fields."""
        events = [
            {'date': '2024-01-15'},  # Missing type
            {'type': 'Hail'},  # Missing date
            {},  # Empty event
            {'date': '2024-02-20', 'type': 'Wind'}  # Valid
        ]

        # Should not raise errors
        result1 = chart_service.generate_time_series_chart(events=events)
        result2 = chart_service.generate_event_distribution_chart(events=events)
        result3 = chart_service.generate_monthly_breakdown_chart(events=events)

        assert all(isinstance(r, bytes) for r in [result1, result2, result3])

    def test_chart_handles_exception_gracefully(self, chart_service):
        """Test charts return error images on exceptions."""
        # Pass invalid data that might cause errors
        invalid_events = [{'date': 'not-a-date', 'type': None}]

        result = chart_service.generate_time_series_chart(events=invalid_events)

        # Should still return valid image (error chart)
        assert isinstance(result, bytes)
        img = Image.open(BytesIO(result))
        assert img.format == 'PNG'

    def test_all_charts_produce_different_outputs(self, chart_service, sample_events):
        """Test that different chart types produce different outputs."""
        result1 = chart_service.generate_time_series_chart(events=sample_events)
        result2 = chart_service.generate_event_distribution_chart(events=sample_events)
        result3 = chart_service.generate_monthly_breakdown_chart(events=sample_events)
        result4 = chart_service.generate_severity_heatmap(events=sample_events)

        # All should be different
        assert result1 != result2
        assert result2 != result3
        assert result3 != result4

        # All should be valid PNGs
        for result in [result1, result2, result3, result4]:
            img = Image.open(BytesIO(result))
            assert img.format == 'PNG'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
