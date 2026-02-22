"""Tests for error rate limiting functionality in server_utils.py."""

import time
from unittest.mock import patch, MagicMock
import pytest

from ray.llm._internal.serve.utils.server_utils import (
    get_response_for_error,
    _error_tracker,
    _get_error_key,
    _should_log_full_traceback,
    _is_engine_dead_error,
    _TRACEBACK_LOG_COOLDOWN_SECONDS,
)


class MockEngineDeadError(Exception):
    """Mock EngineDeadError for testing."""
    pass


@pytest.fixture(autouse=True)
def clear_error_tracker():
    """Clear error tracker before each test."""
    _error_tracker.clear()
    yield
    _error_tracker.clear()


def test_first_error_occurrence_logs_full_traceback():
    """Test that the first occurrence of an error logs full traceback."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        error = ValueError("Test error message")
        response = get_response_for_error(error, "req-001")
        
        # Should have called logger with exc_info
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert call_args[1]['exc_info'] == error
        assert "Encountered failure while handling request req-001" in call_args[0][0]


def test_repeated_error_is_rate_limited():
    """Test that repeated errors are rate-limited."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # First occurrence
        error1 = ValueError("Same error message")
        get_response_for_error(error1, "req-001")
        
        mock_logger.reset_mock()
        
        # Second occurrence immediately after
        error2 = ValueError("Same error message")
        get_response_for_error(error2, "req-002")
        
        # Should log but without full traceback
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert 'exc_info' not in call_args[1] or call_args[1]['exc_info'] is None
        assert "traceback suppressed" in call_args[0][0]
        assert "same error type occurred 2 times" in call_args[0][0]


def test_different_errors_not_rate_limited():
    """Test that different error types are not rate-limited against each other."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # First error type
        error1 = ValueError("Error message")
        get_response_for_error(error1, "req-001")
        
        mock_logger.reset_mock()
        
        # Different error type - should log full traceback
        error2 = RuntimeError("Different error")
        get_response_for_error(error2, "req-002")
        
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert call_args[1]['exc_info'] == error2


def test_cooldown_allows_full_logging_again():
    """Test that errors log full traceback again after cooldown period."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # First occurrence
        error1 = ValueError("Same error")
        get_response_for_error(error1, "req-001")
        
        # Simulate cooldown by manually adjusting timestamp
        error_key = _get_error_key(error1)
        _error_tracker[error_key]['last_logged'] = time.time() - (_TRACEBACK_LOG_COOLDOWN_SECONDS + 5)
        
        mock_logger.reset_mock()
        
        # Same error after cooldown
        error2 = ValueError("Same error")
        get_response_for_error(error2, "req-002")
        
        # Should log full traceback again
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert call_args[1]['exc_info'] == error2


def test_engine_dead_error_detection():
    """Test detection of engine dead errors."""
    # Mock EngineDeadError should be detected
    error1 = MockEngineDeadError("Engine crashed")
    assert _is_engine_dead_error(error1) == True
    
    # Error with EngineDeadError in message should be detected
    error2 = RuntimeError("vllm.v1.engine.exceptions.EngineDeadError occurred")
    assert _is_engine_dead_error(error2) == True
    
    # Normal errors should not be detected
    error3 = ValueError("Normal error")
    assert _is_engine_dead_error(error3) == False


def test_engine_dead_error_circuit_breaker_logging():
    """Test that engine dead errors trigger circuit breaker logging after threshold."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # Generate enough engine dead errors to trigger circuit breaker
        for i in range(6):  # Threshold is 5, so 6th should trigger extra logging
            error = MockEngineDeadError("Engine dead")
            get_response_for_error(error, f"req-{i:03d}")
        
        # Should have logged circuit breaker message on the 6th occurrence
        error_calls = [call for call in mock_logger.error.call_args_list 
                      if "Engine dead error has occurred" in str(call)]
        assert len(error_calls) >= 1, "Should log circuit breaker message"


def test_error_key_generation():
    """Test error key generation for grouping similar errors."""
    # Same error types and messages should have same key
    error1 = ValueError("Connection failed")
    error2 = ValueError("Connection failed")
    assert _get_error_key(error1) == _get_error_key(error2)
    
    # Different error types should have different keys
    error3 = RuntimeError("Connection failed")
    assert _get_error_key(error1) != _get_error_key(error3)
    
    # Same types, different messages should have different keys
    error4 = ValueError("Different message")
    assert _get_error_key(error1) != _get_error_key(error4)


def test_error_response_structure_preserved():
    """Test that error response structure is preserved."""
    error = ValueError("Test error")
    response = get_response_for_error(error, "test-request-id")
    
    # Verify response structure
    assert hasattr(response, 'error')
    assert hasattr(response.error, 'message')
    assert hasattr(response.error, 'code')
    assert hasattr(response.error, 'type')
    
    # Verify content
    assert "test-request-id" in response.error.message
    assert response.error.type == "ValueError"
    assert response.error.code == 500  # Default for ValueError


def test_should_log_full_traceback_logic():
    """Test the core rate limiting logic."""
    error_key = ("ValueError", "Test error")
    
    # First call should return True
    result1 = _should_log_full_traceback(error_key)
    assert result1 == True
    assert _error_tracker[error_key]['count'] == 1
    
    # Immediate second call should return False
    result2 = _should_log_full_traceback(error_key)
    assert result2 == False
    assert _error_tracker[error_key]['count'] == 2
    
    # After cooldown, should return True again
    _error_tracker[error_key]['last_logged'] = time.time() - (_TRACEBACK_LOG_COOLDOWN_SECONDS + 1)
    result3 = _should_log_full_traceback(error_key)
    assert result3 == True
    assert _error_tracker[error_key]['count'] == 3


def test_http_exception_status_codes():
    """Test that different HTTP exceptions get correct status codes and logging levels."""
    from fastapi import HTTPException
    
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # 4xx error should use warning level
        error_4xx = HTTPException(status_code=404, detail="Not found")
        get_response_for_error(error_4xx, "req-001")
        
        # Should have called warning, not error
        assert mock_logger.warning.called
        assert not mock_logger.error.called


def test_rate_limiting_with_request_specific_details():
    """Test that request-specific details don't affect error grouping."""
    with patch('ray.llm._internal.serve.utils.server_utils.logger') as mock_logger:
        # Two errors with same core message but different request IDs in message
        error1 = ValueError("Connection failed (Request ID: req-001)")
        error2 = ValueError("Connection failed (Request ID: req-002)")
        
        get_response_for_error(error1, "req-001")
        mock_logger.reset_mock()
        
        get_response_for_error(error2, "req-002")
        
        # Should be rate-limited because core message is the same
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert 'exc_info' not in call_args[1] or call_args[1]['exc_info'] is None
        assert "traceback suppressed" in call_args[0][0]