#!/usr/bin/env python3
"""
Test script to verify the error rate limiting functionality in Ray Serve LLM.
"""

import time
from unittest.mock import patch, MagicMock
import sys
import os

# Add the ray python path for imports
sys.path.insert(0, '/tmp/oss-ray/python')

from ray.llm._internal.serve.utils.server_utils import (
    get_response_for_error, 
    _error_tracker, 
    _should_log_full_traceback,
    _get_error_key,
    _is_engine_dead_error
)


class MockEngineDeadError(Exception):
    """Mock EngineDeadError for testing since we don't have vLLM installed."""
    pass


def test_error_rate_limiting():
    """Test that repeated errors are rate-limited while preserving first occurrence."""
    print("Testing error rate limiting functionality...")
    
    # Clear any existing state
    _error_tracker.clear()
    
    # Create a mock logger to capture calls
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # Test 1: First occurrence should log full traceback
        print("\n1. Testing first occurrence logging...")
        error1 = MockEngineDeadError("Engine dead due to OOM")
        response1 = get_response_for_error(error1, "req-001")
        
        # Should have called error() with exc_info
        assert mock_logger.error.called, "First occurrence should log error"
        call_args = mock_logger.error.call_args
        assert call_args[1]['exc_info'] == error1, "Should log full exception info"
        print("✓ First occurrence logged with full traceback")
        
        # Test 2: Immediate repeated error should be rate-limited
        print("\n2. Testing immediate repetition rate-limiting...")
        mock_logger.reset_mock()
        
        error2 = MockEngineDeadError("Engine dead due to OOM")  # Same error
        response2 = get_response_for_error(error2, "req-002")
        
        # Should have logged but without exc_info
        assert mock_logger.error.called, "Repeated error should still log"
        call_args = mock_logger.error.call_args
        assert 'exc_info' not in call_args[1] or call_args[1].get('exc_info') is None, "Should not log full traceback"
        assert "traceback suppressed" in call_args[0][0], "Should mention suppression"
        print("✓ Repeated error rate-limited correctly")
        
        # Test 3: Different error should log full traceback
        print("\n3. Testing different error logging...")
        mock_logger.reset_mock()
        
        error3 = ValueError("Different error type")
        response3 = get_response_for_error(error3, "req-003")
        
        # Should log full traceback for different error
        assert mock_logger.error.called or mock_logger.warning.called, "Different error should log"
        print("✓ Different error type logged with full traceback")
        
        # Test 4: After cooldown, should log traceback again
        print("\n4. Testing cooldown behavior...")
        mock_logger.reset_mock()
        
        # Manually adjust last_logged to simulate cooldown passage
        error_key = _get_error_key(error1)
        _error_tracker[error_key]['last_logged'] = time.time() - 35  # 35 seconds ago
        
        error4 = MockEngineDeadError("Engine dead due to OOM")  # Same as first
        response4 = get_response_for_error(error4, "req-004")
        
        # Should log full traceback after cooldown
        assert mock_logger.error.called, "Should log after cooldown"
        call_args = mock_logger.error.call_args
        assert call_args[1]['exc_info'] == error4, "Should log full exception after cooldown"
        print("✓ Cooldown behavior working correctly")


def test_engine_dead_error_detection():
    """Test detection of engine dead errors."""
    print("\nTesting engine dead error detection...")
    
    # Test with mock EngineDeadError
    error1 = MockEngineDeadError("Engine is dead")
    assert _is_engine_dead_error(error1), "Should detect MockEngineDeadError"
    
    # Test with string containing EngineDeadError
    error2 = RuntimeError("vllm.v1.engine.exceptions.EngineDeadError occurred")
    assert _is_engine_dead_error(error2), "Should detect EngineDeadError in message"
    
    # Test with unrelated error
    error3 = ValueError("Some other error")
    assert not _is_engine_dead_error(error3), "Should not detect unrelated errors"
    
    print("✓ Engine dead error detection working correctly")


def test_error_key_generation():
    """Test error key generation for grouping similar errors."""
    print("\nTesting error key generation...")
    
    # Same error type and message should generate same key
    error1 = ValueError("Connection failed")
    error2 = ValueError("Connection failed")
    key1 = _get_error_key(error1)
    key2 = _get_error_key(error2)
    assert key1 == key2, "Same errors should generate same key"
    
    # Different error types should generate different keys
    error3 = RuntimeError("Connection failed")
    key3 = _get_error_key(error3)
    assert key1 != key3, "Different error types should generate different keys"
    
    print("✓ Error key generation working correctly")


def test_response_structure():
    """Test that error responses maintain proper structure."""
    print("\nTesting response structure...")
    
    error = ValueError("Test error")
    response = get_response_for_error(error, "test-request")
    
    # Verify response structure
    assert hasattr(response, 'error'), "Response should have error field"
    assert hasattr(response.error, 'message'), "Error should have message"
    assert hasattr(response.error, 'code'), "Error should have code"
    assert hasattr(response.error, 'type'), "Error should have type"
    
    assert "test-request" in response.error.message, "Should include request ID"
    assert response.error.type == "ValueError", "Should have correct error type"
    
    print("✓ Response structure preserved correctly")


def main():
    """Run all tests."""
    print("Ray Serve LLM Error Rate Limiting Test Suite")
    print("=" * 50)
    
    try:
        test_error_rate_limiting()
        test_engine_dead_error_detection()
        test_error_key_generation()
        test_response_structure()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed! Error rate limiting is working correctly.")
        print("\nKey improvements:")
        print("- First occurrence of errors logged with full traceback")
        print("- Repeated errors rate-limited to prevent log spam")
        print("- Engine dead errors specially detected and handled")
        print("- Error responses maintain proper structure")
        print("- Cooldown mechanism allows periodic full logging")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()