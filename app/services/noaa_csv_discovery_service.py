"""
NOAA CSV Auto-Discovery Service

Automatically discovers the latest NOAA Storm Events Database CSV files
from the FTP directory instead of using hardcoded URLs.

FTP Directory: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
"""

import os
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime
import httpx
import logging

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class NOAACSVAutoDiscoveryService:
    """Service for automatically discovering latest NOAA CSV files."""
    
    def __init__(self):
        """Initialize CSV discovery service."""
        self.ftp_base_url = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
        self.timeout = 30.0
        self.user_agent = "ApexOS-Reports-Service/1.0"
        
        # Regex patterns for different file types
        self.file_patterns = {
            'details': re.compile(r'StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz'),
            'fatalities': re.compile(r'StormEvents_fatalities-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz'),
            'locations': re.compile(r'StormEvents_locations-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz')
        }
    
    async def discover_latest_csv_files(self, years: List[int]) -> Dict[int, str]:
        """
        Discover the latest CSV files for given years with graceful fallbacks.
        
        Args:
            years: List of years to find files for
            
        Returns:
            Dictionary mapping year -> latest CSV URL
        """
        try:
            # Get the FTP directory listing
            directory_listing = await self._fetch_ftp_directory_listing()
            if not directory_listing:
                logger.warning("Failed to fetch FTP directory listing, falling back to hardcoded URLs")
                return self._get_fallback_urls(years)
            
            # Parse files and find latest for each year
            latest_files = {}
            
            for year in years:
                latest_file = self._find_latest_file_for_year(directory_listing, year)
                if latest_file:
                    latest_files[year] = f"{self.ftp_base_url}/{latest_file}"
                    logger.info(f"Found latest CSV for {year}: {latest_file}")
                else:
                    logger.warning(f"No CSV file found for {year}, using fallback")
                    latest_files[year] = self._get_fallback_url_for_year(year)
            
            return latest_files
            
        except Exception as e:
            logger.error(f"Error discovering CSV files: {e}")
            return self._get_fallback_urls(years)
    
    async def _fetch_ftp_directory_listing(self) -> Optional[str]:
        """Fetch the FTP directory listing HTML."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    self.ftp_base_url,
                    headers={"User-Agent": self.user_agent}
                )
                
                if response.status_code == 200:
                    logger.debug("Successfully fetched FTP directory listing")
                    return response.text
                else:
                    logger.warning(f"Failed to fetch FTP directory: HTTP {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching FTP directory listing: {e}")
            return None
    
    def _find_latest_file_for_year(self, directory_html: str, year: int) -> Optional[str]:
        """Find the latest CSV file for a given year."""
        try:
            # Extract all CSV file links from the HTML
            file_links = self._extract_csv_links(directory_html)
            
            # Filter files for the target year
            year_files = []
            for link in file_links:
                match = self.file_patterns['details'].match(link)
                if match:
                    file_year = int(match.group(1))
                    compilation_date = match.group(2)
                    
                    if file_year == year:
                        year_files.append((link, compilation_date))
            
            if not year_files:
                logger.debug(f"No CSV files found for year {year}")
                return None
            
            # Sort by compilation date (latest first)
            year_files.sort(key=lambda x: x[1], reverse=True)
            
            latest_file = year_files[0][0]
            latest_compilation = year_files[0][1]
            
            logger.debug(f"Latest file for {year}: {latest_file} (compiled {latest_compilation})")
            return latest_file
            
        except Exception as e:
            logger.error(f"Error finding latest file for year {year}: {e}")
            return None
    
    def _extract_csv_links(self, html_content: str) -> List[str]:
        """Extract CSV file links from HTML directory listing."""
        try:
            import re
            
            # Pattern to match CSV.gz file links
            csv_pattern = re.compile(r'href="([^"]*StormEvents_details[^"]*\.csv\.gz)"')
            matches = csv_pattern.findall(html_content)
            
            logger.debug(f"Found {len(matches)} CSV file links in directory listing")
            return matches
            
        except Exception as e:
            logger.error(f"Error extracting CSV links: {e}")
            return []
    
    def _get_fallback_urls(self, years: List[int]) -> Dict[int, str]:
        """Get fallback URLs for years when auto-discovery fails."""
        fallback_urls = {}
        for year in years:
            fallback_urls[year] = self._get_fallback_url_for_year(year)
        return fallback_urls
    
    def _get_fallback_url_for_year(self, year: int) -> str:
        """Get fallback URL for a specific year."""
        # Use the existing hardcoded logic as fallback
        if year == 2023:
            return f"{self.ftp_base_url}/StormEvents_details-ftp_v1.0_d{year}_c20250731.csv.gz"
        elif year == 2024:
            return f"{self.ftp_base_url}/StormEvents_details-ftp_v1.0_d{year}_c20250818.csv.gz"
        else:
            # For other years, try the general pattern
            return f"{self.ftp_base_url}/StormEvents_details-ftp_v1.0_d{year}_c20250520.csv.gz"
    
    async def get_file_metadata(self, csv_url: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata about a CSV file (size, last modified, etc.).
        
        Args:
            csv_url: URL of the CSV file
            
        Returns:
            Dictionary with file metadata
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Make a HEAD request to get file metadata
                response = await client.head(csv_url, headers={"User-Agent": self.user_agent})
                
                if response.status_code == 200:
                    return {
                        'url': csv_url,
                        'size_bytes': int(response.headers.get('content-length', 0)),
                        'last_modified': response.headers.get('last-modified'),
                        'content_type': response.headers.get('content-type'),
                        'status_code': response.status_code
                    }
                else:
                    logger.warning(f"Failed to get metadata for {csv_url}: HTTP {response.status_code}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting file metadata for {csv_url}: {e}")
            return None
    
    def parse_filename_info(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Parse NOAA CSV filename to extract year and compilation date.
        
        Args:
            filename: CSV filename (e.g., "StormEvents_details-ftp_v1.0_d2023_c20250731.csv.gz")
            
        Returns:
            Dictionary with parsed information
        """
        try:
            match = self.file_patterns['details'].match(filename)
            if match:
                year = int(match.group(1))
                compilation_date_str = match.group(2)
                
                # Parse compilation date
                compilation_date = datetime.strptime(compilation_date_str, '%Y%m%d').date()
                
                return {
                    'year': year,
                    'compilation_date': compilation_date,
                    'compilation_date_str': compilation_date_str,
                    'filename': filename,
                    'days_since_compilation': (date.today() - compilation_date).days
                }
            else:
                logger.debug(f"Filename {filename} doesn't match expected pattern")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing filename {filename}: {e}")
            return None


# Singleton instance
_csv_discovery_service: Optional[NOAACSVAutoDiscoveryService] = None


def get_csv_discovery_service() -> NOAACSVAutoDiscoveryService:
    """Get CSV auto-discovery service instance."""
    global _csv_discovery_service
    if _csv_discovery_service is None:
        _csv_discovery_service = NOAACSVAutoDiscoveryService()
    return _csv_discovery_service
