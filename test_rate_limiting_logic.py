#!/usr/bin/env python3
"""
Focused test of the rate limiting logic without full Ray dependencies.
"""

import time
from collections import defaultdict
from typing import Dict, Set

# Extract the core rate limiting logic for testing
_error_tracker: Dict[tuple, Dict[str, float]] = defaultdict(lambda: {'first_seen': 0, 'count': 0, 'last_logged': 0})
_TRACEBACK_LOG_COOLDOWN_SECONDS = 30
_ENGINE_DEAD_ERROR_PATTERNS: Set[str] = {'EngineDeadError', 'MockEngineDeadError'}


def _get_error_key(e: Exception) -> tuple:
    """Generate a key for error tracking based on exception type and core message."""
    exc_type = e.__class__.__name__
    message = str(e)
    # Remove request-specific details for better grouping
    core_message = message.split('(Request ID:')[0].strip()
    return (exc_type, core_message)


def _should_log_full_traceback(error_key: tuple) -> bool:
    """Determine if we should log the full traceback for this error."""
    now = time.time()
    error_info = _error_tracker[error_key]
    
    # First time seeing this error - always log
    if error_info['first_seen'] == 0:
        error_info['first_seen'] = now
        error_info['last_logged'] = now
        error_info['count'] = 1
        return True
    
    error_info['count'] += 1
    
    # Check if we should log based on cooldown
    time_since_last_log = now - error_info['last_logged']
    if time_since_last_log >= _TRACEBACK_LOG_COOLDOWN_SECONDS:
        error_info['last_logged'] = now
        return True
    
    return False


def _is_engine_dead_error(e: Exception) -> bool:
    """Check if this is an engine dead error that indicates fatal engine failure."""
    exc_type = e.__class__.__name__
    return exc_type in _ENGINE_DEAD_ERROR_PATTERNS or 'EngineDeadError' in str(e)


class MockEngineDeadError(Exception):
    """Mock EngineDeadError for testing."""
    pass


def test_rate_limiting_core_logic():
    """Test the core rate limiting functionality."""
    print("Testing core rate limiting logic...")
    
    # Clear state
    _error_tracker.clear()
    
    # Test 1: First occurrence should return True
    error1 = MockEngineDeadError("Engine dead due to OOM")
    error_key1 = _get_error_key(error1)
    result1 = _should_log_full_traceback(error_key1)
    assert result1 == True, "First occurrence should return True"
    assert _error_tracker[error_key1]['count'] == 1, "Count should be 1"
    print("✓ First occurrence correctly returns True")
    
    # Test 2: Immediate repetition should return False
    error2 = MockEngineDeadError("Engine dead due to OOM")  # Same error
    error_key2 = _get_error_key(error2)
    assert error_key1 == error_key2, "Same errors should have same key"
    
    result2 = _should_log_full_traceback(error_key2)
    assert result2 == False, "Repeated error should return False"
    assert _error_tracker[error_key2]['count'] == 2, "Count should be 2"
    print("✓ Repeated error correctly returns False")
    
    # Test 3: After cooldown should return True
    # Simulate cooldown by adjusting last_logged timestamp
    _error_tracker[error_key1]['last_logged'] = time.time() - 35  # 35 seconds ago
    
    error3 = MockEngineDeadError("Engine dead due to OOM")  # Same error again
    result3 = _should_log_full_traceback(error_key1)
    assert result3 == True, "After cooldown should return True"
    assert _error_tracker[error_key1]['count'] == 3, "Count should be 3"
    print("✓ Cooldown mechanism working correctly")
    
    # Test 4: Different error should return True
    error4 = ValueError("Different error")
    error_key4 = _get_error_key(error4)
    result4 = _should_log_full_traceback(error_key4)
    assert result4 == True, "Different error should return True"
    assert error_key1 != error_key4, "Different errors should have different keys"
    print("✓ Different errors correctly handled")


def test_engine_dead_error_detection():
    """Test engine dead error detection."""
    print("\nTesting engine dead error detection...")
    
    # Test MockEngineDeadError
    error1 = MockEngineDeadError("Engine crashed")
    assert _is_engine_dead_error(error1) == True, "Should detect MockEngineDeadError"
    
    # Test error with EngineDeadError in message
    error2 = RuntimeError("vllm.v1.engine.exceptions.EngineDeadError: Engine died")
    assert _is_engine_dead_error(error2) == True, "Should detect EngineDeadError in message"
    
    # Test normal error
    error3 = ValueError("Normal error")
    assert _is_engine_dead_error(error3) == False, "Should not detect normal errors"
    
    print("✓ Engine dead error detection working")


def test_error_key_consistency():
    """Test that similar errors generate consistent keys."""
    print("\nTesting error key consistency...")
    
    # Same error type and message
    error1 = ValueError("Connection failed")
    error2 = ValueError("Connection failed")
    key1 = _get_error_key(error1)
    key2 = _get_error_key(error2)
    assert key1 == key2, "Identical errors should have same key"
    
    # Same type, different message
    error3 = ValueError("Different message")
    key3 = _get_error_key(error3)
    assert key1 != key3, "Different messages should have different keys"
    
    # Different type, same message
    error4 = RuntimeError("Connection failed")
    key4 = _get_error_key(error4)
    assert key1 != key4, "Different types should have different keys"
    
    print("✓ Error key generation consistent")


def test_error_tracker_state():
    """Test error tracker state management."""
    print("\nTesting error tracker state management...")
    
    # Clear state
    _error_tracker.clear()
    
    # Add some errors
    error1 = ValueError("Test error 1")
    error2 = RuntimeError("Test error 2")
    
    key1 = _get_error_key(error1)
    key2 = _get_error_key(error2)
    
    # First calls
    _should_log_full_traceback(key1)
    _should_log_full_traceback(key2)
    
    # Check state
    assert len(_error_tracker) == 2, "Should track 2 different errors"
    assert _error_tracker[key1]['count'] == 1, "Error 1 count should be 1"
    assert _error_tracker[key2]['count'] == 1, "Error 2 count should be 1"
    assert _error_tracker[key1]['first_seen'] > 0, "Should set first_seen timestamp"
    
    # Repeat error 1
    _should_log_full_traceback(key1)
    assert _error_tracker[key1]['count'] == 2, "Error 1 count should be 2"
    assert _error_tracker[key2]['count'] == 1, "Error 2 count should still be 1"
    
    print("✓ Error tracker state management working")


def main():
    """Run all tests."""
    print("Ray Serve LLM Error Rate Limiting - Core Logic Tests")
    print("=" * 55)
    
    try:
        test_rate_limiting_core_logic()
        test_engine_dead_error_detection()
        test_error_key_consistency()
        test_error_tracker_state()
        
        print("\n" + "=" * 55)
        print("✅ ALL CORE TESTS PASSED!")
        print("\nVerified functionality:")
        print("- ✓ First error occurrence always logs full traceback")
        print("- ✓ Repeated errors within cooldown period are rate-limited")
        print("- ✓ Errors log full traceback again after cooldown")
        print("- ✓ Different error types are handled independently")
        print("- ✓ Engine dead errors are properly detected")
        print("- ✓ Error tracking state is managed correctly")
        print("\nThe implementation should significantly reduce log spam")
        print("while preserving essential debugging information!")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)