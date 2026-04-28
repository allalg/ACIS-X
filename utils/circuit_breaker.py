import time
from enum import Enum
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

class CircuitOpenError(Exception):
    pass

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0, half_open_max_calls: int = 2, on_state_change: Callable = None):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.on_state_change = on_state_change
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.last_state_change = time.time()
        self.half_open_calls = 0
        self.half_open_successes = 0

    def _transition_to(self, new_state: CircuitState):
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()
        if new_state == CircuitState.CLOSED:
            self.failure_count = 0
            self.half_open_calls = 0
            self.half_open_successes = 0
        elif new_state == CircuitState.HALF_OPEN:
            self.half_open_calls = 0
            self.half_open_successes = 0
            
        if old_state != new_state and self.on_state_change:
            self.on_state_change(old_state.value, new_state.value)

    def call(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        now = time.time()
        
        if self.state == CircuitState.OPEN:
            if now - self.last_failure_time >= self.recovery_timeout:
                self._transition_to(CircuitState.HALF_OPEN)
            else:
                raise CircuitOpenError("Circuit is OPEN")
                
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.half_open_max_calls:
                raise CircuitOpenError("Circuit is HALF_OPEN and probe limit reached")
            self.half_open_calls += 1

        try:
            result = fn(*args, **kwargs)
            
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_max_calls:
                    self._transition_to(CircuitState.CLOSED)
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0  # reset on success
                
            return result
            
        except Exception as e:
            self.last_failure_time = time.time()
            self.failure_count += 1
            
            if self.state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
                    
            raise e

    def get_state(self) -> dict:
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "last_state_change": self.last_state_change
        }
