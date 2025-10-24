#!/usr/bin/env python3
"""
Test script to show raw NOAA data for Houston area
"""
import csv
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

def parse_noaa_csv_for_houston(csv_file: str, target_lat: float = 29.7604, target_lon: float = -95.3698, radius_km: float = 50.0) -> List[Dict[str, Any]]:
    """
    Parse NOAA CSV and filter for Houston area events
    """
    events = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Check if coordinates exist
                begin_lat = row.get('BEGIN_LAT')
                begin_lon = row.get('BEGIN_LON')
                
                if not begin_lat or not begin_lon:
                    continue
                    
                begin_lat = float(begin_lat)
                begin_lon = float(begin_lon)
                
                # Calculate distance (simple approximation)
                lat_diff = abs(begin_lat - target_lat)
                lon_diff = abs(begin_lon - target_lon)
                distance_km = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111  # Rough km conversion
                
                if distance_km <= radius_km:
                    # Parse date
                    begin_date_time = row.get('BEGIN_DATE_TIME', '')
                    if begin_date_time:
                        try:
                            # Parse format: "25-OCT-23 02:30:00"
                            event_date = datetime.strptime(begin_date_time, '%d-%b-%y %H:%M:%S')
                        except:
                            event_date = None
                    else:
                        event_date = None
                    
                    event = {
                        'event_id': row.get('EVENT_ID'),
                        'event_type': row.get('EVENT_TYPE'),
                        'begin_date_time': begin_date_time,
                        'parsed_date': event_date.isoformat() if event_date else None,
                        'begin_lat': begin_lat,
                        'begin_lon': begin_lon,
                        'magnitude': row.get('MAGNITUDE'),
                        'magnitude_type': row.get('MAGNITUDE_TYPE'),
                        'injuries_direct': row.get('INJURIES_DIRECT'),
                        'deaths_direct': row.get('DEATHS_DIRECT'),
                        'damage_property': row.get('DAMAGE_PROPERTY'),
                        'event_narrative': row.get('EVENT_NARRATIVE', '')[:100] + '...' if row.get('EVENT_NARRATIVE') else '',
                        'distance_km': round(distance_km, 2)
                    }
                    events.append(event)
                    
            except (ValueError, TypeError) as e:
                continue
    
    return events

def main():
    print("🔍 Analyzing NOAA Storm Events Data for Houston Area")
    print("=" * 60)
    
    # Test with 2023 data
    csv_file_2023 = "noaa_data/StormEvents_details-ftp_v1.0_d2023_c20250731.csv"
    csv_file_2024 = "noaa_data/StormEvents_details-ftp_v1.0_d2024_c20250818.csv"
    
    houston_lat = 29.7604
    houston_lon = -95.3698
    radius_km = 50.0
    
    print(f"📍 Target Location: Houston, TX ({houston_lat}, {houston_lon})")
    print(f"📏 Search Radius: {radius_km} km")
    print()
    
    all_events = []
    
    # Process 2023 data
    print("📅 Processing 2023 data...")
    events_2023 = parse_noaa_csv_for_houston(csv_file_2023, houston_lat, houston_lon, radius_km)
    print(f"   Found {len(events_2023)} events in 2023")
    all_events.extend(events_2023)
    
    # Process 2024 data
    print("📅 Processing 2024 data...")
    events_2024 = parse_noaa_csv_for_houston(csv_file_2024, houston_lat, houston_lon, radius_km)
    print(f"   Found {len(events_2024)} events in 2024")
    all_events.extend(events_2024)
    
    print(f"\n📊 Total Events Found: {len(all_events)}")
    print()
    
    # Show event type breakdown
    event_types = {}
    for event in all_events:
        event_type = event['event_type']
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    print("📈 Event Types Breakdown:")
    for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
        print(f"   {event_type}: {count}")
    print()
    
    # Show sample events
    print("🔍 Sample Events (first 5):")
    for i, event in enumerate(all_events[:5]):
        print(f"\nEvent {i+1}:")
        print(f"   Type: {event['event_type']}")
        print(f"   Date: {event['begin_date_time']}")
        print(f"   Location: ({event['begin_lat']}, {event['begin_lon']})")
        print(f"   Distance: {event['distance_km']} km")
        print(f"   Magnitude: {event['magnitude']} {event['magnitude_type']}")
        print(f"   Damage: {event['damage_property']}")
        print(f"   Narrative: {event['event_narrative']}")
    
    # Save raw data to JSON for inspection
    with open('houston_storm_events_raw.json', 'w') as f:
        json.dump(all_events, f, indent=2, default=str)
    
    print(f"\n💾 Raw data saved to: houston_storm_events_raw.json")
    print(f"📄 This shows exactly what data should be flowing through your reports system")

if __name__ == "__main__":
    main()
