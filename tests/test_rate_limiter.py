"""
Tests for CDO Rate Limiter functionality.
"""

import pytest
import asyncio
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from app.services.rate_limiter import CDORateLimiter, RateLimitExceededError


class TestCDORateLimiter:
    """Test CDO Rate Limiter functionality."""
    
    @pytest.fixture
    def rate_limiter(self):
        """Create a rate limiter for testing."""
        return CDORateLimiter(
            requests_per_second=2,  # Lower limit for testing
            requests_per_day=100,   # Lower limit for testing
            buffer_factor=1.0       # No buffer for testing
        )
    
    def test_initialization(self):
        """Test rate limiter initialization."""
        limiter = CDORateLimiter(
            requests_per_second=5,
            requests_per_day=1000,
            buffer_factor=0.8
        )
        
        assert limiter.requests_per_second == 4  # 5 * 0.8
        assert limiter.requests_per_day == 800  # 1000 * 0.8
        assert limiter.tokens == 4
        assert limiter.daily_requests == 0
    
    @pytest.mark.asyncio
    async def test_token_bucket_refill(self, rate_limiter):
        """Test token bucket refill mechanism."""
        # Consume all tokens
        for _ in range(2):
            await rate_limiter.wait_if_needed()
        
        assert rate_limiter.tokens == 0
        
        # Wait for refill
        await asyncio.sleep(0.6)  # Should refill 1.2 tokens
        
        # Should be able to make one more request
        await rate_limiter.wait_if_needed()
        assert rate_limiter.tokens == 0.2  # 1.2 - 1.0
    
    @pytest.mark.asyncio
    async def test_daily_limit_reset(self, rate_limiter):
        """Test daily limit reset at midnight UTC."""
        # Set up limiter to think it's yesterday
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        rate_limiter.last_reset_date = yesterday
        rate_limiter.daily_requests = 50
        
        # Should reset daily counter
        await rate_limiter._check_daily_limit()
        
        assert rate_limiter.daily_requests == 0
        assert rate_limiter.last_reset_date == datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    @pytest.mark.asyncio
    async def test_daily_limit_enforcement(self, rate_limiter):
        """Test daily limit enforcement."""
        # Set daily requests to limit
        rate_limiter.daily_requests = 100
        
        with pytest.raises(RateLimitExceededError):
            await rate_limiter.wait_if_needed()
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, rate_limiter):
        """Test concurrent request handling."""
        async def make_request():
            await rate_limiter.wait_if_needed()
            return True
        
        # Make multiple concurrent requests
        tasks = [make_request() for _ in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should succeed (no exceptions)
        assert all(result is True for result in results)
        assert rate_limiter.daily_requests == 5
    
    @pytest.mark.asyncio
    async def test_rate_limit_status(self, rate_limiter):
        """Test rate limiter status reporting."""
        # Make a few requests
        for _ in range(3):
            await rate_limiter.wait_if_needed()
        
        status = rate_limiter.get_status()
        
        assert status["daily_requests_used"] == 3
        assert status["daily_requests_limit"] == 100
        assert status["daily_requests_remaining"] == 97
        assert "tokens_remaining" in status
        assert "daily_reset_date" in status
    
    @pytest.mark.asyncio
    async def test_rate_limit_wait_time(self, rate_limiter):
        """Test that rate limiter waits appropriate time."""
        # Consume all tokens
        for _ in range(2):
            await rate_limiter.wait_if_needed()
        
        start_time = time.time()
        
        # This should wait for token refill
        await rate_limiter.wait_if_needed()
        
        elapsed = time.time() - start_time
        
        # Should have waited approximately 0.5 seconds (1 token / 2 tokens per second)
        assert 0.4 <= elapsed <= 0.6
    
    def test_rate_limit_exceeded_error(self):
        """Test RateLimitExceededError exception."""
        error = RateLimitExceededError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)
