"""
Retry strategies and DLQ policies for ACIS-X Kafka operations.

Provides configurable retry logic with exponential backoff and
circuit breaker patterns for resilient event processing.
"""

import logging
import time
from typing import Any, Callable, Optional, Type
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# Retry strategies
# =============================================================================

class RetryStrategy(str, Enum):
    """Retry strategy types."""
    IMMEDIATE = "immediate"
    LINEAR_BACKOFF = "linear_backoff"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIBONACCI_BACKOFF = "fibonacci_backoff"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay_ms: int = 100
    max_delay_ms: int = 30000
    jitter: bool = True

    # Exponential backoff multiplier
    exponential_multiplier: float = 2.0

    # Retryable exceptions (None = retry all)
    retryable_exceptions: Optional[tuple] = None


def retry_with_backoff(
    func: Callable,
    config: RetryConfig,
    *args,
    **kwargs
) -> Any:
    """
    Execute function with retry logic.

    Args:
        func: Function to execute
        config: Retry configuration
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            return func(*args, **kwargs)

        except Exception as e:
            last_exception = e

            # Check if exception is retryable
            if config.retryable_exceptions:
                if not isinstance(e, config.retryable_exceptions):
                    logger.error(f"Non-retryable exception: {e}")
                    raise

            # Last attempt - don't retry
            if attempt >= config.max_retries:
                logger.error(f"Max retries ({config.max_retries}) exhausted: {e}")
                raise

            # Calculate backoff delay
            delay_ms = calculate_backoff_delay(
                attempt=attempt,
                strategy=config.strategy,
                base_delay=config.base_delay_ms,
                max_delay=config.max_delay_ms,
                multiplier=config.exponential_multiplier,
                jitter=config.jitter,
            )

            logger.warning(
                f"Retry attempt {attempt + 1}/{config.max_retries} after {delay_ms}ms "
                f"(error: {type(e).__name__})"
            )

            time.sleep(delay_ms / 1000.0)

    # Should never reach here, but for type safety
    if last_exception:
        raise last_exception


def calculate_backoff_delay(
    attempt: int,
    strategy: RetryStrategy,
    base_delay: int,
    max_delay: int,
    multiplier: float = 2.0,
    jitter: bool = True,
) -> int:
    """
    Calculate backoff delay for retry attempt.

    Args:
        attempt: Current attempt number (0-indexed)
        strategy: Retry strategy enum
        base_delay: Base delay in milliseconds
        max_delay: Maximum delay in milliseconds
        multiplier: Exponential multiplier
        jitter: Add random jitter

    Returns:
        Delay in milliseconds
    """
    import random

    if strategy == RetryStrategy.IMMEDIATE:
        delay = 0

    elif strategy == RetryStrategy.LINEAR_BACKOFF:
        delay = base_delay * (attempt + 1)

    elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
        delay = int(base_delay * (multiplier ** attempt))

    elif strategy == RetryStrategy.FIBONACCI_BACKOFF:
        # Fibonacci sequence for delays
        fib = [1, 1]
        for i in range(2, attempt + 2):
            fib.append(fib[i - 1] + fib[i - 2])
        delay = base_delay * fib[min(attempt + 1, len(fib) - 1)]

    else:
        delay = base_delay

    # Cap at max delay
    delay = min(delay, max_delay)

    # Add jitter (±25%)
    if jitter and delay > 0:
        jitter_range = int(delay * 0.25)
        delay += random.randint(-jitter_range, jitter_range)

    return max(0, delay)


# =============================================================================
# Circuit breaker
# =============================================================================

class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, block requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: int = 60


class CircuitBreaker:
    """
    Circuit breaker for fault tolerance.

    Prevents cascading failures by temporarily blocking operations
    after repeated failures.
    """

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            # Check if timeout has elapsed
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.config.timeout_seconds:
                    logger.info("Circuit breaker: transitioning to HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise RuntimeError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful execution."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                logger.info("Circuit breaker: transitioning to CLOSED")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed execution."""
        self.last_failure_time = time.time()
        self.success_count = 0

        if self.state == CircuitState.HALF_OPEN:
            logger.warning("Circuit breaker: transitioning to OPEN (half-open failure)")
            self.state = CircuitState.OPEN
            self.failure_count = 0

        elif self.state == CircuitState.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.config.failure_threshold:
                logger.warning(
                    f"Circuit breaker: transitioning to OPEN "
                    f"({self.failure_count} failures)"
                )
                self.state = CircuitState.OPEN

    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset circuit breaker to closed state."""
        logger.info("Circuit breaker: manual reset to CLOSED")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
