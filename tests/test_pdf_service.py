"""Tests for PDF generation service."""

import pytest
from datetime import datetime
from io import BytesIO
from PyPDF2 import PdfReader
from unittest.mock import Mock, patch, MagicMock

from app.services.pdf_service import PDFGenerationService


@pytest.fixture
def pdf_service():
    """Create PDF service instance."""
    return PDFGenerationService()


@pytest.fixture
def sample_address_report_data():
    """Create sample address report data."""
    return {
        'report_id': 'test_addr_001',
        'title': 'Test Address Weather Report',
        'location': '123 Main St, Houston, TX 77002',
        'latitude': 29.7604,
        'longitude': -95.3698,
        'start_date': '2024-01-01',
        'end_date': '2024-10-04',
        'type': 'address',
        'template': 'professional',
        'include_charts': True,
        'include_storm_events': True,
        'branding': True,
        'color_scheme': 'professional'
    }


@pytest.fixture
def sample_weather_data():
    """Create sample weather statistics."""
    return {
        'max_temp': 95.0,
        'min_temp': 45.0,
        'total_precip': 12.5,
        'max_wind': 65.0,
        'weather_events': 15,
        'hail_events': 3,
        'max_hail_size': 1.5
    }


@pytest.fixture
def sample_spatial_report_data():
    """Create sample spatial report data."""
    return {
        'report_id': 'test_spatial_001',
        'title': 'Test Spatial Weather Report',
        'location': 'Harris County, TX',
        'latitude': 29.8,
        'longitude': -95.4,
        'start_date': '2024-01-01',
        'end_date': '2024-10-04',
        'type': 'spatial',
        'template': 'professional',
        'spatial_data': {
            'boundary': {
                'name': 'Harris County',
                'type': 'county',
                'area_sq_km': 4602.0,
                'population': 4731145
            },
            'events': [
                {
                    'date': '2024-03-15',
                    'type': 'Thunderstorm Wind',
                    'latitude': 29.85,
                    'longitude': -95.45,
                    'severity': 'high'
                },
                {
                    'date': '2024-04-20',
                    'type': 'Hail',
                    'latitude': 29.8,
                    'longitude': -95.4,
                    'severity': 'medium'
                }
            ],
            'heat_map_data': [
                {
                    'latitude': 29.85,
                    'longitude': -95.45,
                    'severity': 2
                }
            ],
            'center_lat': 29.8,
            'center_lon': -95.4
        }
    }


class TestPDFService:
    """Test suite for PDFGenerationService."""

    def test_initialization(self, pdf_service):
        """Test service initializes correctly."""
        assert pdf_service is not None
        assert pdf_service.styles is not None
        assert pdf_service.chart_service is not None
        assert pdf_service.map_service is not None

    def test_custom_styles_created(self, pdf_service):
        """Test custom paragraph styles are created."""
        assert 'CustomTitle' in pdf_service.styles
        assert 'CustomHeading' in pdf_service.styles
        assert 'CustomSubheading' in pdf_service.styles
        assert 'CustomBody' in pdf_service.styles

    @pytest.mark.asyncio
    async def test_generate_address_report_pdf(
        self,
        pdf_service,
        sample_address_report_data,
        sample_weather_data
    ):
        """Test address report PDF generation."""
        result = await pdf_service.generate_weather_report_pdf(
            report_data=sample_address_report_data,
            weather_data=sample_weather_data
        )

        # Check result is bytes
        assert isinstance(result, bytes)
        assert len(result) > 0

        # Verify it's a valid PDF
        pdf_reader = PdfReader(BytesIO(result))
        assert len(pdf_reader.pages) > 0

    @pytest.mark.asyncio
    async def test_generate_address_report_minimal_data(self, pdf_service):
        """Test address report with minimal data."""
        minimal_report = {
            'report_id': 'test_min_001',
            'title': 'Minimal Report',
            'location': 'Test Location',
            'latitude': 0.0,
            'longitude': 0.0,
            'start_date': '2024-01-01',
            'end_date': '2024-01-31'
        }

        minimal_weather = {
            'weather_events': 0,
            'hail_events': 0
        }

        result = await pdf_service.generate_weather_report_pdf(
            report_data=minimal_report,
            weather_data=minimal_weather
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch.object(PDFGenerationService, '_build_title_page')
    @patch.object(PDFGenerationService, '_build_executive_summary')
    @patch.object(PDFGenerationService, '_build_weather_statistics')
    async def test_address_report_calls_build_methods(
        self,
        mock_stats,
        mock_summary,
        mock_title,
        pdf_service,
        sample_address_report_data,
        sample_weather_data
    ):
        """Test address report calls expected build methods."""
        mock_title.return_value = []
        mock_summary.return_value = []
        mock_stats.return_value = []

        await pdf_service.generate_weather_report_pdf(
            report_data=sample_address_report_data,
            weather_data=sample_weather_data
        )

        mock_title.assert_called_once()
        mock_summary.assert_called_once()
        mock_stats.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_spatial_report_pdf(
        self,
        pdf_service,
        sample_spatial_report_data
    ):
        """Test spatial report PDF generation without actual charts/maps."""
        # Mock chart and map services to avoid Selenium/Matplotlib overhead
        with patch.object(pdf_service.chart_service, 'generate_time_series_chart') as mock_chart, \
             patch.object(pdf_service.map_service, 'generate_heat_map') as mock_map:

            # Return fake image bytes
            fake_img = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
            mock_chart.return_value = fake_img
            mock_map.return_value = fake_img

            result = await pdf_service.generate_spatial_report_pdf(
                report_data=sample_spatial_report_data
            )

            assert isinstance(result, bytes)
            assert len(result) > 0

            # Verify it's a valid PDF
            pdf_reader = PdfReader(BytesIO(result))
            assert len(pdf_reader.pages) > 0

    @pytest.mark.asyncio
    async def test_generate_spatial_report_no_events(self, pdf_service):
        """Test spatial report with no event data."""
        report_data = {
            'report_id': 'test_spatial_no_events',
            'title': 'Empty Spatial Report',
            'location': 'Test County',
            'latitude': 29.8,
            'longitude': -95.4,
            'start_date': '2024-01-01',
            'end_date': '2024-10-04',
            'spatial_data': {
                'boundary': {},
                'events': [],
                'heat_map_data': []
            }
        }

        result = await pdf_service.generate_spatial_report_pdf(
            report_data=report_data
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    @pytest.mark.asyncio
    @patch.object(PDFGenerationService, '_build_spatial_title_page')
    @patch.object(PDFGenerationService, '_build_spatial_summary')
    @patch.object(PDFGenerationService, '_build_heat_map_section')
    @patch.object(PDFGenerationService, '_build_charts_section')
    async def test_spatial_report_calls_build_methods(
        self,
        mock_charts,
        mock_heat_map,
        mock_summary,
        mock_title,
        pdf_service,
        sample_spatial_report_data
    ):
        """Test spatial report calls expected build methods."""
        mock_title.return_value = []
        mock_summary.return_value = []
        mock_heat_map.return_value = []
        mock_charts.return_value = []

        await pdf_service.generate_spatial_report_pdf(
            report_data=sample_spatial_report_data
        )

        mock_title.assert_called_once()
        mock_summary.assert_called_once()
        mock_heat_map.assert_called_once()
        mock_charts.assert_called_once()

    def test_build_title_page(self, pdf_service, sample_address_report_data):
        """Test title page building."""
        elements = pdf_service._build_title_page(sample_address_report_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_spatial_title_page(self, pdf_service, sample_spatial_report_data):
        """Test spatial title page building."""
        elements = pdf_service._build_spatial_title_page(sample_spatial_report_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_executive_summary(self, pdf_service, sample_weather_data):
        """Test executive summary building."""
        elements = pdf_service._build_executive_summary(sample_weather_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_spatial_summary(self, pdf_service):
        """Test spatial summary building."""
        spatial_data = {
            'boundary': {
                'name': 'Test County',
                'type': 'county'
            }
        }

        elements = pdf_service._build_spatial_summary(spatial_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_weather_statistics(self, pdf_service, sample_weather_data):
        """Test weather statistics table building."""
        elements = pdf_service._build_weather_statistics(sample_weather_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_heat_map_section_no_data(self, pdf_service):
        """Test heat map section with no data."""
        spatial_data = {
            'heat_map_data': []
        }

        elements = pdf_service._build_heat_map_section(spatial_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_heat_map_section_with_data(self, pdf_service):
        """Test heat map section with data (mocked)."""
        with patch.object(pdf_service.map_service, 'generate_heat_map') as mock_map:
            fake_img = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
            mock_map.return_value = fake_img

            spatial_data = {
                'heat_map_data': [
                    {'latitude': 29.8, 'longitude': -95.4, 'severity': 2}
                ],
                'center_lat': 29.8,
                'center_lon': -95.4
            }

            elements = pdf_service._build_heat_map_section(spatial_data)

            assert isinstance(elements, list)
            assert len(elements) > 0
            mock_map.assert_called_once()

    def test_build_heat_map_section_handles_error(self, pdf_service):
        """Test heat map section handles generation errors."""
        with patch.object(pdf_service.map_service, 'generate_heat_map') as mock_map:
            mock_map.side_effect = Exception("Map generation failed")

            spatial_data = {
                'heat_map_data': [
                    {'latitude': 29.8, 'longitude': -95.4}
                ],
                'center_lat': 29.8,
                'center_lon': -95.4
            }

            elements = pdf_service._build_heat_map_section(spatial_data)

            # Should return elements with error message
            assert isinstance(elements, list)
            assert len(elements) > 0

    def test_build_charts_section_no_data(self, pdf_service):
        """Test charts section with no event data."""
        spatial_data = {
            'events': []
        }

        elements = pdf_service._build_charts_section(spatial_data)

        assert isinstance(elements, list)
        assert len(elements) > 0

    def test_build_charts_section_with_data(self, pdf_service):
        """Test charts section with event data (mocked)."""
        with patch.object(pdf_service.chart_service, 'generate_time_series_chart') as mock_ts, \
             patch.object(pdf_service.chart_service, 'generate_event_distribution_chart') as mock_dist, \
             patch.object(pdf_service.chart_service, 'generate_monthly_breakdown_chart') as mock_monthly:

            fake_img = b'\x89PNG\r\n\x1a\n' + b'\x00' * 1000
            mock_ts.return_value = fake_img
            mock_dist.return_value = fake_img
            mock_monthly.return_value = fake_img

            spatial_data = {
                'events': [
                    {'date': '2024-03-15', 'type': 'Hail'}
                ]
            }

            elements = pdf_service._build_charts_section(spatial_data)

            assert isinstance(elements, list)
            assert len(elements) > 0
            mock_ts.assert_called_once()
            mock_dist.assert_called_once()
            mock_monthly.assert_called_once()

    def test_build_charts_section_handles_error(self, pdf_service):
        """Test charts section handles generation errors."""
        with patch.object(pdf_service.chart_service, 'generate_time_series_chart') as mock_chart:
            mock_chart.side_effect = Exception("Chart generation failed")

            spatial_data = {
                'events': [
                    {'date': '2024-03-15', 'type': 'Hail'}
                ]
            }

            elements = pdf_service._build_charts_section(spatial_data)

            # Should return elements with error message
            assert isinstance(elements, list)
            assert len(elements) > 0

    @pytest.mark.asyncio
    async def test_address_report_error_handling(self, pdf_service):
        """Test address report handles errors gracefully."""
        with pytest.raises(Exception):
            await pdf_service.generate_weather_report_pdf(
                report_data=None,  # Invalid
                weather_data={}
            )

    @pytest.mark.asyncio
    async def test_spatial_report_error_handling(self, pdf_service):
        """Test spatial report handles errors gracefully."""
        with pytest.raises(Exception):
            await pdf_service.generate_spatial_report_pdf(
                report_data=None  # Invalid
            )

    @pytest.mark.asyncio
    async def test_address_report_includes_all_weather_metrics(
        self,
        pdf_service,
        sample_address_report_data
    ):
        """Test that all weather metrics appear in report."""
        weather_data = {
            'max_temp': 100.0,
            'min_temp': 30.0,
            'total_precip': 25.5,
            'max_wind': 80.0,
            'weather_events': 50,
            'hail_events': 10,
            'max_hail_size': 2.5
        }

        result = await pdf_service.generate_weather_report_pdf(
            report_data=sample_address_report_data,
            weather_data=weather_data
        )

        assert isinstance(result, bytes)
        # PDF should contain the metrics (basic check)
        assert len(result) > 1000  # Should be substantial

    @pytest.mark.asyncio
    async def test_pdf_reports_have_multiple_pages(
        self,
        pdf_service,
        sample_address_report_data,
        sample_weather_data
    ):
        """Test that generated PDFs have multiple pages."""
        result = await pdf_service.generate_weather_report_pdf(
            report_data=sample_address_report_data,
            weather_data=sample_weather_data
        )

        pdf_reader = PdfReader(BytesIO(result))
        # Should have at least 2 pages (title + content)
        assert len(pdf_reader.pages) >= 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
