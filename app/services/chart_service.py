"""Chart generation service using Matplotlib."""

import io
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import Counter
import numpy as np

logger = logging.getLogger(__name__)


class ChartService:
    """Service for generating charts for weather reports."""

    def __init__(self):
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')

        # Brand colors
        self.primary_color = '#3498db'
        self.secondary_color = '#e74c3c'
        self.success_color = '#2ecc71'
        self.warning_color = '#f39c12'
        self.dark_color = '#2c3e50'

    def generate_time_series_chart(
        self,
        events: List[Dict[str, Any]],
        title: str = "Weather Events Over Time"
    ) -> bytes:
        """
        Generate a time series chart showing weather events over time.

        Args:
            events: List of event dictionaries with 'date' and 'type' keys
            title: Chart title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating time series chart with {len(events)} events")

            fig, ax = plt.subplots(figsize=(10, 6))

            if not events:
                # Empty chart with message
                ax.text(0.5, 0.5, 'No events to display',
                       ha='center', va='center', fontsize=14)
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
            else:
                # Parse dates and count events by month
                dates = []
                for event in events:
                    try:
                        date_str = event.get('date', event.get('begin_date', ''))
                        if date_str:
                            dates.append(datetime.strptime(date_str[:10], '%Y-%m-%d'))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse date: {date_str}")
                        continue

                if dates:
                    # Count events by month
                    month_counts = Counter([d.replace(day=1) for d in dates])
                    sorted_months = sorted(month_counts.keys())
                    counts = [month_counts[m] for m in sorted_months]

                    # Plot
                    ax.plot(sorted_months, counts,
                           marker='o', linewidth=2, markersize=6,
                           color=self.primary_color, label='Events')

                    # Fill area under curve
                    ax.fill_between(sorted_months, counts, alpha=0.3, color=self.primary_color)

                    # Format x-axis
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
                    plt.xticks(rotation=45, ha='right')

                    # Labels
                    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
                    ax.set_ylabel('Number of Events', fontsize=12, fontweight='bold')
                    ax.grid(True, alpha=0.3)
                    ax.legend(loc='upper left')

            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            plt.tight_layout()

            # Save to bytes
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)

            buffer.seek(0)
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating time series chart: {str(e)}")
            # Return empty chart on error
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f'Error generating chart',
                   ha='center', va='center', fontsize=14)
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buffer.seek(0)
            return buffer.read()

    def generate_event_distribution_chart(
        self,
        events: List[Dict[str, Any]],
        title: str = "Weather Event Distribution"
    ) -> bytes:
        """
        Generate a bar chart showing distribution of event types.

        Args:
            events: List of event dictionaries with 'type' key
            title: Chart title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating event distribution chart with {len(events)} events")

            fig, ax = plt.subplots(figsize=(10, 6))

            if not events:
                ax.text(0.5, 0.5, 'No events to display',
                       ha='center', va='center', fontsize=14)
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
            else:
                # Count event types
                event_types = [e.get('type', e.get('event_type', 'Unknown')) for e in events]
                type_counts = Counter(event_types)

                # Sort by count
                sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
                types = [t[0] for t in sorted_types[:10]]  # Top 10
                counts = [t[1] for t in sorted_types[:10]]

                # Create color gradient
                colors = plt.cm.Blues(np.linspace(0.4, 0.9, len(types)))

                # Plot horizontal bar chart
                y_pos = np.arange(len(types))
                bars = ax.barh(y_pos, counts, color=colors, edgecolor='white', linewidth=1.5)

                # Add value labels
                for i, (bar, count) in enumerate(zip(bars, counts)):
                    width = bar.get_width()
                    ax.text(width + max(counts) * 0.02, bar.get_y() + bar.get_height()/2,
                           f'{count}', ha='left', va='center', fontweight='bold', fontsize=10)

                ax.set_yticks(y_pos)
                ax.set_yticklabels(types, fontsize=11)
                ax.set_xlabel('Number of Events', fontsize=12, fontweight='bold')
                ax.set_ylabel('Event Type', fontsize=12, fontweight='bold')
                ax.grid(True, alpha=0.3, axis='x')

            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            plt.tight_layout()

            # Save to bytes
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)

            buffer.seek(0)
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating distribution chart: {str(e)}")
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, f'Error generating chart',
                   ha='center', va='center', fontsize=14)
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buffer.seek(0)
            return buffer.read()

    def generate_monthly_breakdown_chart(
        self,
        events: List[Dict[str, Any]],
        title: str = "Monthly Event Breakdown"
    ) -> bytes:
        """
        Generate a stacked bar chart showing monthly breakdown by event type.

        Args:
            events: List of event dictionaries with 'date' and 'type' keys
            title: Chart title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating monthly breakdown chart with {len(events)} events")

            fig, ax = plt.subplots(figsize=(12, 6))

            if not events:
                ax.text(0.5, 0.5, 'No events to display',
                       ha='center', va='center', fontsize=14)
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
            else:
                # Parse dates and organize by month and type
                monthly_data = {}
                for event in events:
                    try:
                        date_str = event.get('date', event.get('begin_date', ''))
                        event_type = event.get('type', event.get('event_type', 'Unknown'))

                        if date_str:
                            date = datetime.strptime(date_str[:10], '%Y-%m-%d')
                            month = date.strftime('%Y-%m')

                            if month not in monthly_data:
                                monthly_data[month] = Counter()
                            monthly_data[month][event_type] += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not parse date: {date_str}")
                        continue

                if monthly_data:
                    # Get all unique event types and months
                    all_types = set()
                    for month_counts in monthly_data.values():
                        all_types.update(month_counts.keys())

                    sorted_months = sorted(monthly_data.keys())
                    top_types = sorted(all_types)[:6]  # Top 6 types for readability

                    # Prepare data for stacking
                    data_by_type = {event_type: [] for event_type in top_types}
                    for month in sorted_months:
                        for event_type in top_types:
                            data_by_type[event_type].append(monthly_data[month].get(event_type, 0))

                    # Create stacked bar chart
                    x = np.arange(len(sorted_months))
                    width = 0.6
                    bottom = np.zeros(len(sorted_months))

                    colors = plt.cm.Set3(np.linspace(0, 1, len(top_types)))

                    for idx, (event_type, color) in enumerate(zip(top_types, colors)):
                        ax.bar(x, data_by_type[event_type], width,
                              label=event_type, bottom=bottom, color=color,
                              edgecolor='white', linewidth=0.5)
                        bottom += data_by_type[event_type]

                    # Format x-axis
                    ax.set_xticks(x)
                    month_labels = [datetime.strptime(m, '%Y-%m').strftime('%b %Y')
                                   for m in sorted_months]
                    ax.set_xticklabels(month_labels, rotation=45, ha='right')

                    ax.set_xlabel('Month', fontsize=12, fontweight='bold')
                    ax.set_ylabel('Number of Events', fontsize=12, fontweight='bold')
                    ax.legend(loc='upper left', fontsize=9, ncol=2)
                    ax.grid(True, alpha=0.3, axis='y')

            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            plt.tight_layout()

            # Save to bytes
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)

            buffer.seek(0)
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating monthly breakdown chart: {str(e)}")
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, f'Error generating chart',
                   ha='center', va='center', fontsize=14)
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buffer.seek(0)
            return buffer.read()

    def generate_severity_heatmap(
        self,
        events: List[Dict[str, Any]],
        title: str = "Event Severity Calendar"
    ) -> bytes:
        """
        Generate a calendar heatmap showing event severity by day.

        Args:
            events: List of event dictionaries with 'date' and 'severity' keys
            title: Chart title

        Returns:
            PNG image as bytes
        """
        try:
            logger.info(f"Generating severity heatmap with {len(events)} events")

            fig, ax = plt.subplots(figsize=(14, 4))

            if not events:
                ax.text(0.5, 0.5, 'No events to display',
                       ha='center', va='center', fontsize=14)
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
            else:
                # Count events by date
                date_counts = Counter()
                for event in events:
                    try:
                        date_str = event.get('date', event.get('begin_date', ''))
                        if date_str:
                            date = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
                            date_counts[date] += 1
                    except (ValueError, TypeError):
                        continue

                if date_counts:
                    # Create matrix for heatmap (weeks x days)
                    dates = sorted(date_counts.keys())
                    min_date = dates[0]
                    max_date = dates[-1]

                    # Calculate weeks
                    total_days = (max_date - min_date).days + 1
                    weeks = (total_days + 6) // 7

                    # Create matrix
                    matrix = np.zeros((7, weeks))
                    for date, count in date_counts.items():
                        days_since_start = (date - min_date).days
                        week = days_since_start // 7
                        day = days_since_start % 7
                        if week < weeks:
                            matrix[day, week] = count

                    # Plot heatmap
                    im = ax.imshow(matrix, cmap='YlOrRd', aspect='auto', interpolation='nearest')

                    # Add colorbar
                    cbar = plt.colorbar(im, ax=ax, orientation='horizontal',
                                       pad=0.1, shrink=0.8)
                    cbar.set_label('Events per Day', fontsize=10, fontweight='bold')

                    # Set labels
                    ax.set_yticks(np.arange(7))
                    ax.set_yticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
                    ax.set_xlabel('Week', fontsize=12, fontweight='bold')
                    ax.set_xticks([])

            ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
            plt.tight_layout()

            # Save to bytes
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)

            buffer.seek(0)
            return buffer.read()

        except Exception as e:
            logger.error(f"Error generating severity heatmap: {str(e)}")
            fig, ax = plt.subplots(figsize=(14, 4))
            ax.text(0.5, 0.5, f'Error generating chart',
                   ha='center', va='center', fontsize=14)
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buffer.seek(0)
            return buffer.read()
