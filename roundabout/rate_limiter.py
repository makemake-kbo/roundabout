"""Rate limiting utilities for API request throttling."""

from __future__ import annotations

import threading
import time


class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter.

    Implements the token bucket algorithm to limit requests per second.
    Tokens are added at a constant rate, and each request consumes tokens.
    If no tokens are available, the request blocks until tokens become available.

    Attributes:
        tokens_per_second: Rate at which tokens are added (e.g., 50 for 50 rps).
        bucket_capacity: Maximum tokens that can accumulate.
        tokens: Current number of available tokens.
        last_refill: Last time tokens were refilled (monotonic time).
        lock: Thread lock for concurrent access safety.

    Example:
        >>> limiter = TokenBucketRateLimiter(50.0)  # 50 requests per second
        >>> limiter.acquire()  # Blocks until token available
        >>> # Make API request
    """

    def __init__(self, tokens_per_second: float, bucket_capacity: int | None = None):
        """
        Initialize the rate limiter.

        Args:
            tokens_per_second: Rate at which tokens are added (requests/sec).
            bucket_capacity: Maximum token capacity (default: 2x tokens_per_second).
        """
        if tokens_per_second <= 0:
            raise ValueError(f"tokens_per_second must be positive, got {tokens_per_second}")

        self.tokens_per_second = tokens_per_second
        self.bucket_capacity = bucket_capacity or int(tokens_per_second * 2)
        self.tokens = float(self.bucket_capacity)
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire tokens from the bucket.

        Blocks until the requested number of tokens becomes available.
        This method is thread-safe.

        Args:
            tokens: Number of tokens to acquire (default: 1).

        Returns:
            True when tokens have been acquired.

        Example:
            >>> limiter = TokenBucketRateLimiter(10.0)
            >>> limiter.acquire()  # Waits if necessary
            True
        """
        if tokens < 0:
            raise ValueError(f"tokens must be non-negative, got {tokens}")

        if tokens == 0:
            return True

        with self.lock:
            while True:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
                # Release lock briefly and sleep before retrying
                time.sleep(0.01)

    def _refill(self) -> None:
        """
        Refill tokens based on elapsed time since last refill.

        This method should only be called while holding self.lock.
        """
        now = time.monotonic()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.tokens_per_second
        self.tokens = min(self.bucket_capacity, self.tokens + new_tokens)
        self.last_refill = now

    def get_available_tokens(self) -> float:
        """
        Get the current number of available tokens.

        Thread-safe method for monitoring rate limiter state.

        Returns:
            Current number of tokens in the bucket.
        """
        with self.lock:
            self._refill()
            return self.tokens
