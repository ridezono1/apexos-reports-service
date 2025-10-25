"""
DuckDB Query Service for Fast CSV Analytics

Provides high-performance CSV querying using DuckDB's columnar analytics engine.
Falls back to csv.DictReader if DuckDB is unavailable.
"""

import os
import csv
from typing import List, Dict, Any, Optional
from datetime import date
import logging

from app.core.logging import get_logger

logger = get_logger(__name__)

# Safe DuckDB import with fallback
try:
    import duckdb
    DUCKDB_AVAILABLE = True
    logger.info("DuckDB available - using fast columnar analytics")
except ImportError:
    DUCKDB_AVAILABLE = False
    logger.warning("DuckDB not available - falling back to csv.DictReader")


class DuckDBQueryService:
    """Service for querying NOAA CSV files using DuckDB."""
    
    def __init__(self):
        """Initialize DuckDB connection (in-memory, thread-safe)."""
        self.available = DUCKDB_AVAILABLE
        self.conn = None
        
        if self.available:
            try:
                self.conn = duckdb.connect()  # In-memory connection
                logger.info("DuckDB connection initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize DuckDB: {e}")
                self.available = False
                self.conn = None
    
    def query_storm_events(
        self,
        csv_files: List[str],
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        start_date: str,  # 'YYYY-MM-DD'
        end_date: str,    # 'YYYY-MM-DD'
    ) -> List[Dict[str, Any]]:
        """
        Query storm events from CSV files using DuckDB.
        
        Args:
            csv_files: List of CSV file paths (can be .csv or .csv.gz)
            min_lat, max_lat, min_lon, max_lon: Geographic bounding box
            start_date, end_date: Date range in ISO format
            
        Returns:
            List of event dictionaries
        """
        if not self.available or not self.conn:
            logger.warning("DuckDB unavailable, falling back to csv.DictReader")
            return self._fallback_csv_reader(csv_files, min_lat, max_lat, min_lon, max_lon, start_date, end_date)
        
        try:
            # Build file list parameter for DuckDB
            if len(csv_files) == 1:
                files_param = f"'{csv_files[0]}'"
            else:
                files_list = ", ".join(f"'{f}'" for f in csv_files)
                files_param = f"[{files_list}]"
            
            # Query with read_csv_auto for automatic detection
            query = f"""
                SELECT 
                    EVENT_ID as event_id,
                    EVENT_TYPE as event_type,
                    BEGIN_DATE_TIME as begin_date_time,
                    BEGIN_LAT as begin_lat,
                    BEGIN_LON as begin_lon,
                    MAGNITUDE as magnitude,
                    MAGNITUDE_TYPE as magnitude_type,
                    CZ_NAME as cz_name,
                    STATE as state,
                    DAMAGE_PROPERTY as damage_property,
                    INJURIES_DIRECT as injuries_direct,
                    DEATHS_DIRECT as deaths_direct,
                    EVENT_NARRATIVE as event_narrative
                FROM read_csv_auto({files_param}, 
                    ignore_errors=true,  -- Skip malformed rows
                    header=true,
                    auto_detect=true
                )
                WHERE TRY_CAST(BEGIN_LAT AS DOUBLE) BETWEEN ? AND ?
                  AND TRY_CAST(BEGIN_LON AS DOUBLE) BETWEEN ? AND ?
                  AND TRY_CAST(BEGIN_DATE_TIME AS DATE) BETWEEN ? AND ?
            """
            
            result_df = self.conn.execute(query, [
                min_lat, max_lat,
                min_lon, max_lon,
                start_date, end_date
            ]).fetchdf()
            
            # Convert to list of dicts (your existing format)
            events = result_df.to_dict('records')
            
            logger.info(f"DuckDB query returned {len(events)} events from {len(csv_files)} files")
            return events
            
        except Exception as e:
            logger.error(f"DuckDB query failed: {e}, falling back to csv.DictReader")
            return self._fallback_csv_reader(csv_files, min_lat, max_lat, min_lon, max_lon, start_date, end_date)
    
    def _fallback_csv_reader(
        self,
        csv_files: List[str],
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Fallback to csv.DictReader when DuckDB is unavailable.
        
        This replicates the original NOAAWeatherService._fetch_nws_storm_events logic.
        """
        events = []
        
        for csv_file in csv_files:
            if not os.path.exists(csv_file):
                logger.warning(f"CSV file not found: {csv_file}")
                continue
            
            try:
                logger.debug(f"Processing CSV file with csv.DictReader: {csv_file}")
                
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    
                    for record in reader:
                        # Check if event is within our date range and geographic bounds
                        try:
                            # Parse event date - handle different date formats
                            begin_date_str = record.get('BEGIN_DATE_TIME', '')
                            if not begin_date_str:
                                continue
                                
                            # Try different date formats
                            event_date = None
                            date_formats = [
                                '%d-%b-%y %H:%M:%S',  # DD-MMM-YY HH:MM:SS (NOAA format)
                                '%Y-%m-%d %H:%M:%S',  # YYYY-MM-DD HH:MM:SS
                                '%Y-%m-%d %H:%M:%S.%f',  # YYYY-MM-DD HH:MM:SS.microseconds
                                '%Y-%m-%d'  # YYYY-MM-DD
                            ]
                            
                            from datetime import datetime
                            for date_format in date_formats:
                                try:
                                    event_date = datetime.strptime(begin_date_str, date_format).date()
                                    break
                                except ValueError:
                                    continue

                            if not event_date:
                                continue
                                
                            # Check date range
                            start_date_obj = date.fromisoformat(start_date)
                            end_date_obj = date.fromisoformat(end_date)
                            if not (start_date_obj <= event_date <= end_date_obj):
                                continue
                            
                            # Parse coordinates
                            event_lat_str = record.get('BEGIN_LAT', '')
                            event_lon_str = record.get('BEGIN_LON', '')

                            if not event_lat_str or not event_lon_str:
                                continue

                            try:
                                event_lat = float(event_lat_str)
                                event_lon = float(event_lon_str)
                            except (ValueError, TypeError):
                                continue

                            # Check geographic bounds
                            if not (min_lat <= event_lat <= max_lat and min_lon <= event_lon <= max_lon):
                                continue
                            
                            # Convert to our event format
                            event = self._convert_storm_event_to_severe_weather_event(record)
                            if event:
                                events.append(event)
                                
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Error parsing storm event record: {e}")
                            continue
                
                logger.debug(f"Successfully processed {len(events)} events from {csv_file}")
                
            except Exception as e:
                logger.error(f"Error processing CSV file {csv_file}: {e}")
        
        logger.info(f"csv.DictReader fallback returned {len(events)} events from {len(csv_files)} files")
        return events
    
    def _convert_storm_event_to_severe_weather_event(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert NWS Storm Event record to severe weather event format."""
        try:
            event_type_raw = record.get('EVENT_TYPE', '').strip()
            magnitude_str = record.get('MAGNITUDE', '').strip()
            magnitude_type = record.get('MAGNITUDE_TYPE', '').strip()
            
            # Map NOAA event types to our standardized types
            event_type_mapping = {
                'hail': 'hail',
                'thunderstorm wind': 'wind',
                'high wind': 'wind',
                'strong wind': 'wind',
                'tornado': 'tornado',
                'flash flood': 'flood',
                'flood': 'flood',
                'hurricane': 'hurricane',
                'tropical storm': 'hurricane',
                'tropical depression': 'hurricane',
                'winter storm': 'winter',
                'ice storm': 'winter',
                'blizzard': 'winter',
                'heavy snow': 'winter',
                'extreme cold': 'cold',
                'extreme heat': 'heat',
                'heat wave': 'heat',
                'wildfire': 'fire',
                'fire weather': 'fire'
            }
            
            event_type = event_type_mapping.get(event_type_raw.lower(), event_type_raw.lower())
            
            # Parse magnitude with proper handling
            mag_value = None
            if magnitude_str and magnitude_str != '':
                try:
                    mag_value = float(magnitude_str)
                except (ValueError, TypeError):
                    mag_value = None
            
            # Determine severity and insurance relevance based on thresholds
            severity = "minor"
            insurance_relevant = False
            
            if event_type == "hail":
                if mag_value and mag_value >= 2.0:  # ≥2.0 inches
                    severity = "extreme"
                    insurance_relevant = True
                elif mag_value and mag_value >= 1.0:  # ≥1.0 inch
                    severity = "severe"
                    insurance_relevant = True
                elif mag_value and mag_value >= 0.5:  # ≥0.5 inch
                    severity = "moderate"
                    insurance_relevant = True
                else:
                    severity = "minor"
                    
            elif event_type == "wind":
                if mag_value and mag_value >= 80:  # ≥80 mph
                    severity = "extreme"
                    insurance_relevant = True
                elif mag_value and mag_value >= 60:  # ≥60 mph
                    severity = "severe"
                    insurance_relevant = True
                elif mag_value and mag_value >= 40:  # ≥40 mph
                    severity = "moderate"
                    insurance_relevant = True
                else:
                    severity = "minor"
                    
            elif event_type in ["tornado", "hurricane"]:
                # All tornadoes and hurricanes are severe and insurance-relevant
                severity = "severe"
                insurance_relevant = True
                
            elif event_type in ["flood", "winter", "fire"]:
                # Floods, winter storms, and fires are moderate-severe and insurance-relevant
                severity = "moderate"
                insurance_relevant = True
                
            elif event_type in ["heat", "cold"]:
                # Extreme temperatures are moderate severity
                severity = "moderate"
                insurance_relevant = False  # Less relevant for roofing
                
            else:
                # Unknown event types default to minor
                severity = "minor"
                insurance_relevant = False
            
            # Format description with magnitude if available
            description = event_type_raw.title()
            if mag_value is not None and magnitude_type:
                if magnitude_type.lower() in ['inches', 'inch', 'in']:
                    description += f" ({mag_value:.2f}\")"
                elif magnitude_type.lower() in ['mph', 'miles per hour']:
                    description += f" ({mag_value:.0f} mph)"
                elif magnitude_type.lower() in ['knots', 'kt']:
                    description += f" ({mag_value:.0f} kt)"
                else:
                    description += f" ({mag_value:.1f} {magnitude_type})"
            
            # Format timestamp - handle different date formats
            timestamp_str = record.get('BEGIN_DATE_TIME', '')
            formatted_date = timestamp_str
            if timestamp_str:
                try:
                    # Try different date formats
                    from datetime import datetime
                    for date_format in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                        try:
                            date_obj = datetime.strptime(timestamp_str, date_format)
                            formatted_date = date_obj.strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                except Exception:
                    formatted_date = timestamp_str
                
            return {
                "event_type": event_type,
                "severity": severity,
                "urgency": "immediate" if severity in ["severe", "extreme"] else "expected",
                "description": description,
                "timestamp": formatted_date,
                "source": "NWS-StormEvents",
                "magnitude": mag_value,
                "magnitude_type": magnitude_type,
                "location": record.get('CZ_NAME', ''),
                "state": record.get('STATE', ''),
                "county": record.get('CZ_FIPS', ''),
                "insurance_relevant": insurance_relevant,
                "roofing_damage_risk": "high" if event_type in ["hail", "tornado", "hurricane"] else "medium" if event_type in ["wind", "flood"] else "low"
            }
            
        except Exception as e:
            logger.error(f"Error converting storm event record: {e}")
            return None


# Singleton instance
_duckdb_query_service: Optional[DuckDBQueryService] = None


def get_duckdb_query_service() -> DuckDBQueryService:
    """Get DuckDB query service instance."""
    global _duckdb_query_service
    if _duckdb_query_service is None:
        _duckdb_query_service = DuckDBQueryService()
    return _duckdb_query_service
