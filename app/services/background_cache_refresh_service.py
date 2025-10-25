"""
Background Cache Refresh Service

Automatically refreshes NOAA CSV cache files in the background to ensure
data freshness without impacting user requests.

Schedule: Daily at 2 AM UTC (configurable)
"""

import os
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
import logging

from app.core.config import settings
from app.core.logging import get_logger
from app.services.noaa_csv_discovery_service import get_csv_discovery_service
from app.services.noaa_weather_service import NOAAWeatherService

logger = get_logger(__name__)

# Safe APScheduler import with fallback
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.jobstores.memory import MemoryJobStore
    from apscheduler.executors.asyncio import AsyncIOExecutor
    APSCHEDULER_AVAILABLE = True
    logger.info("APScheduler available - background cache refresh enabled")
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logger.warning("APScheduler not available - background cache refresh disabled")


class BackgroundCacheRefreshService:
    """Service for background cache refresh operations."""
    
    def __init__(self):
        """Initialize background cache refresh service."""
        self.scheduler = None
        self.noaa_service = NOAAWeatherService()
        self.csv_discovery_service = get_csv_discovery_service()
        
        if APSCHEDULER_AVAILABLE:
            self._setup_scheduler()
    
    def _setup_scheduler(self):
        """Setup APScheduler for background jobs."""
        try:
            # Configure job stores and executors
            jobstores = {
                'default': MemoryJobStore()
            }
            executors = {
                'default': AsyncIOExecutor()
            }
            job_defaults = {
                'coalesce': True,  # Combine multiple pending executions
                'max_instances': 1,  # Only one instance at a time
                'misfire_grace_time': 300  # 5 minutes grace period
            }
            
            self.scheduler = AsyncIOScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone='UTC'
            )
            
            logger.info("APScheduler configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup APScheduler: {e}")
            self.scheduler = None
    
    def start(self):
        """Start the background cache refresh service."""
        if not self.scheduler:
            logger.warning("Scheduler not available, cannot start background refresh")
            return False
        
        try:
            # Schedule daily cache refresh at 2 AM UTC
            self.scheduler.add_job(
                func=self.refresh_cache_job,
                trigger=CronTrigger(hour=2, minute=0, timezone='UTC'),
                id='daily_cache_refresh',
                name='Daily NOAA CSV Cache Refresh',
                replace_existing=True
            )
            
            # Schedule weekly cache cleanup at 3 AM UTC on Sundays
            self.scheduler.add_job(
                func=self.cleanup_cache_job,
                trigger=CronTrigger(day_of_week=0, hour=3, minute=0, timezone='UTC'),
                id='weekly_cache_cleanup',
                name='Weekly Cache Cleanup',
                replace_existing=True
            )
            
            self.scheduler.start()
            logger.info("Background cache refresh service started")
            logger.info("Scheduled jobs:")
            logger.info("  - Daily cache refresh: 2:00 AM UTC")
            logger.info("  - Weekly cache cleanup: 3:00 AM UTC (Sundays)")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start background cache refresh service: {e}")
            return False
    
    def stop(self):
        """Stop the background cache refresh service."""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Background cache refresh service stopped")
    
    async def refresh_cache_job(self):
        """Background job to refresh CSV cache files."""
        try:
            logger.info("Starting background cache refresh job")
            
            # Get years to refresh (last 3 years)
            current_year = date.today().year
            years_to_refresh = list(range(current_year - 2, current_year + 1))
            
            # Discover latest CSV URLs
            latest_urls = await self.csv_discovery_service.discover_latest_csv_files(years_to_refresh)
            
            refreshed_count = 0
            failed_count = 0
            
            for year, url in latest_urls.items():
                try:
                    # Force refresh by downloading new file
                    cache_file = await self.noaa_service._download_and_cache_csv(url, year)
                    if cache_file:
                        refreshed_count += 1
                        logger.debug(f"Refreshed cache for year {year}")
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to refresh cache for year {year}")
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error refreshing cache for year {year}: {e}")
            
            logger.info(f"Cache refresh completed: {refreshed_count} refreshed, {failed_count} failed")
            
        except Exception as e:
            logger.error(f"Error in background cache refresh job: {e}")
    
    async def cleanup_cache_job(self):
        """Background job to cleanup old cache files."""
        try:
            logger.info("Starting background cache cleanup job")
            
            cache_dir = self.noaa_service.cache_dir
            if not os.path.exists(cache_dir):
                logger.debug("Cache directory does not exist, nothing to cleanup")
                return
            
            # Find all cache files
            cache_files = []
            for filename in os.listdir(cache_dir):
                if filename.startswith('storm_events_') and filename.endswith('.csv'):
                    filepath = os.path.join(cache_dir, filename)
                    cache_files.append(filepath)
            
            cleaned_count = 0
            total_size_freed = 0
            
            for cache_file in cache_files:
                try:
                    # Check if file is older than 60 days
                    file_age_days = (datetime.now().timestamp() - os.path.getmtime(cache_file)) / (24 * 3600)
                    
                    if file_age_days > 60:
                        file_size = os.path.getsize(cache_file)
                        os.remove(cache_file)
                        cleaned_count += 1
                        total_size_freed += file_size
                        logger.debug(f"Cleaned up old cache file: {os.path.basename(cache_file)}")
                        
                except Exception as e:
                    logger.error(f"Error cleaning up cache file {cache_file}: {e}")
            
            logger.info(f"Cache cleanup completed: {cleaned_count} files removed, {total_size_freed / (1024*1024):.1f} MB freed")
            
        except Exception as e:
            logger.error(f"Error in background cache cleanup job: {e}")
    
    async def warmup_cache(self, years: Optional[List[int]] = None):
        """
        Warmup cache by pre-loading CSV files for specified years.

        Args:
            years: List of years to warmup (default: last 3 years excluding current year)
        """
        try:
            if years is None:
                current_year = date.today().year
                # Only warmup previous years - current year data not available yet
                years = list(range(current_year - 2, current_year))
            
            logger.info(f"Starting cache warmup for years: {years}")
            
            # Discover latest CSV URLs
            latest_urls = await self.csv_discovery_service.discover_latest_csv_files(years)
            
            warmed_count = 0
            failed_count = 0
            
            for year, url in latest_urls.items():
                try:
                    # Check if cache already exists and is valid
                    cache_file = self.noaa_service._get_cache_file_path(year)
                    if self.noaa_service._is_cache_valid(cache_file, year):
                        logger.debug(f"Cache for year {year} already valid, skipping")
                        continue
                    
                    # Download and cache the file
                    cache_file = await self.noaa_service._download_and_cache_csv(url, year)
                    if cache_file:
                        warmed_count += 1
                        logger.debug(f"Warmed up cache for year {year}")
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to warmup cache for year {year}")
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Error warming up cache for year {year}: {e}")
            
            logger.info(f"Cache warmup completed: {warmed_count} warmed, {failed_count} failed")
            
        except Exception as e:
            logger.error(f"Error in cache warmup: {e}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get current scheduler status."""
        if not self.scheduler:
            return {
                'available': False,
                'running': False,
                'jobs': []
            }
        
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return {
            'available': True,
            'running': self.scheduler.running,
            'jobs': jobs
        }


# Singleton instance
_cache_refresh_service: Optional[BackgroundCacheRefreshService] = None


def get_cache_refresh_service() -> BackgroundCacheRefreshService:
    """Get background cache refresh service instance."""
    global _cache_refresh_service
    if _cache_refresh_service is None:
        _cache_refresh_service = BackgroundCacheRefreshService()
    return _cache_refresh_service
