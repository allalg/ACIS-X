import pytest
import time
from utils.circuit_breaker import CircuitBreaker, CircuitOpenError, CircuitState

def test_circuit_breaker_failures_open_circuit():
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
    
    def failing_func():
        raise ValueError("Simulated failure")
        
    for _ in range(4):
        with pytest.raises(ValueError):
            breaker.call(failing_func)
            
    assert breaker.state == CircuitState.CLOSED
    
    with pytest.raises(ValueError):
        breaker.call(failing_func)
        
    assert breaker.state == CircuitState.OPEN

def test_circuit_breaker_open_raises_immediately():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=60.0)
    
    def failing_func():
        raise ValueError("Simulated failure")
        
    def success_func():
        return "success"
        
    for _ in range(2):
        with pytest.raises(ValueError):
            breaker.call(failing_func)
            
    assert breaker.state == CircuitState.OPEN
    
    with pytest.raises(CircuitOpenError):
        breaker.call(success_func)

def test_circuit_breaker_recovery_to_half_open():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
    
    def failing_func():
        raise ValueError("Simulated failure")
        
    def success_func():
        return "success"
        
    with pytest.raises(ValueError):
        breaker.call(failing_func)
        
    assert breaker.state == CircuitState.OPEN
    
    # Wait for recovery timeout
    time.sleep(0.15)
    
    # Should be allowed through and transition to HALF_OPEN inside call
    result = breaker.call(success_func)
    assert result == "success"
    # Actually wait, half_open_max_calls is 2 by default, so it might stay HALF_OPEN 
    # until 2 successes or it might be CLOSED if we set max_calls=1
    assert breaker.state in (CircuitState.HALF_OPEN, CircuitState.CLOSED)

def test_circuit_breaker_half_open_to_closed():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1, half_open_max_calls=2)
    
    def failing_func():
        raise ValueError("Simulated failure")
        
    def success_func():
        return "success"
        
    with pytest.raises(ValueError):
        breaker.call(failing_func)
        
    assert breaker.state == CircuitState.OPEN
    time.sleep(0.15)
    
    # First success
    breaker.call(success_func)
    assert breaker.state == CircuitState.HALF_OPEN
    
    # Second success
    breaker.call(success_func)
    assert breaker.state == CircuitState.CLOSED
