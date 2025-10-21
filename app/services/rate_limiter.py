"""
Rate limiter for NOAA CDO API to enforce API limits.
Implements token bucket algorithm for per-second limits and daily request tracking.
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class CDORateLimiter:
    """
    Rate limiter for NOAA CDO API with the following limits:
    - 5 requests per second
    - 10,000 requests per day
    
    Uses token bucket algorithm for per-second limiting and daily counter tracking.
    """
    
    def __init__(
        self,
        requests_per_second: int = 5,
        requests_per_day: int = 10000,
        buffer_factor: float = 0.8
    ):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second (default: 5)
            requests_per_day: Maximum requests per day (default: 10,000)
            buffer_factor: Safety factor to use less than full limits (default: 0.8)
        """
        self.requests_per_second = int(requests_per_second * buffer_factor)
        self.requests_per_day = int(requests_per_day * buffer_factor)
        
        # Token bucket for per-second limiting
        self.tokens = self.requests_per_second
        self.last_refill = time.time()
        self.refill_rate = self.requests_per_second  # tokens per second
        self.bucket_lock = asyncio.Lock()
        
        # Daily counter tracking
        self.daily_requests = 0
        self.last_reset_date = self._get_current_utc_date()
        self.daily_lock = asyncio.Lock()
        
        logger.info(
            f"CDO Rate Limiter initialized: {self.requests_per_second} req/sec, "
            f"{self.requests_per_day} req/day (buffer: {buffer_factor})"
        )
    
    def _get_current_utc_date(self) -> str:
        """Get current UTC date as string (YYYY-MM-DD)."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    async def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time since last refill."""
        current_time = time.time()
        elapsed = current_time - self.last_refill
        
        if elapsed > 0:
            # Add tokens based on elapsed time
            tokens_to_add = elapsed * self.refill_rate
            self.tokens = min(self.requests_per_second, self.tokens + tokens_to_add)
            self.last_refill = current_time
    
    async def _check_daily_limit(self) -> bool:
        """Check and reset daily counter if needed. Returns True if under daily limit."""
        async with self.daily_lock:
            current_date = self._get_current_utc_date()
            
            # Reset daily counter if it's a new day
            if current_date != self.last_reset_date:
                logger.info(f"Daily rate limit reset: {self.last_reset_date} -> {current_date}")
                self.daily_requests = 0
                self.last_reset_date = current_date
            
            # Check if we're under the daily limit
            if self.daily_requests >= self.requests_per_day:
                logger.warning(
                    f"Daily rate limit exceeded: {self.daily_requests}/{self.requests_per_day} requests used"
                )
                return False
            
            return True
    
    async def _increment_daily_counter(self) -> None:
        """Increment the daily request counter."""
        async with self.daily_lock:
            self.daily_requests += 1
    
    async def wait_if_needed(self) -> None:
        """
        Wait if necessary to respect rate limits.
        
        Raises:
            RateLimitExceededError: If daily limit is exceeded
        """
        # Check daily limit first
        if not await self._check_daily_limit():
            raise RateLimitExceededError(
                f"Daily rate limit exceeded: {self.daily_requests}/{self.requests_per_day} requests used today"
            )
        
        # Check per-second limit
        async with self.bucket_lock:
            await self._refill_tokens()
            
            if self.tokens >= 1:
                # We have tokens available
                self.tokens -= 1
                await self._increment_daily_counter()
                
                logger.debug(
                    f"Request allowed: {self.tokens:.1f} tokens remaining, "
                    f"{self.daily_requests}/{self.requests_per_day} daily requests used"
                )
            else:
                # Need to wait for tokens to refill
                wait_time = (1 - self.tokens) / self.refill_rate
                logger.info(f"Rate limit: waiting {wait_time:.2f} seconds for token refill")
                
                await asyncio.sleep(wait_time)
                
                # Refill and consume token
                await self._refill_tokens()
                self.tokens -= 1
                await self._increment_daily_counter()
                
                logger.debug(
                    f"Request allowed after wait: {self.tokens:.1f} tokens remaining, "
                    f"{self.daily_requests}/{self.requests_per_day} daily requests used"
                )
    
    def get_status(self) -> dict:
        """Get current rate limiter status."""
        current_date = self._get_current_utc_date()
        
        return {
            "tokens_remaining": self.tokens,
            "requests_per_second_limit": self.requests_per_second,
            "daily_requests_used": self.daily_requests,
            "daily_requests_limit": self.requests_per_day,
            "daily_reset_date": self.last_reset_date,
            "is_new_day": current_date != self.last_reset_date,
            "daily_requests_remaining": max(0, self.requests_per_day - self.daily_requests)
        }


class RateLimitExceededError(Exception):
    """Raised when rate limit is exceeded."""
    pass
