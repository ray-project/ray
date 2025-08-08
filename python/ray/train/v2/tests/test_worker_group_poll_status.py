import pytest

from ray.train.v2._internal.execution.worker_group.poll import (
    ERR_CHAR_LIMIT,
    WorkerGroupPollStatus,
    WorkerStatus,
    _normalize_error_string,
)


def test_get_error_string_basic():
    """
    Simulate four workers, two with the same error, one with a different error,
    and one without an error.
    """

    statuses = {
        0: WorkerStatus(running=False, error=ValueError("An error")),
        1: WorkerStatus(running=False, error=None),
        2: WorkerStatus(running=False, error=RuntimeError("Different error")),
        3: WorkerStatus(running=False, error=ValueError("An error")),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = "[Rank 0, 3]:\nAn error\n[Rank 2]:\nDifferent error"
    assert error_str == expected_error_str


def test_get_error_string_with_numbers():
    """
    Simulate workers with similar errors that differ only by numbers.
    These should be grouped together.
    """
    statuses = {
        0: WorkerStatus(
            running=False, error=ValueError("Error parsing object at 0x7f8b12345678")
        ),
        1: WorkerStatus(
            running=False, error=ValueError("Error parsing object at 0x7f8b12345679")
        ),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    assert error_str == "[Rank 0, 1]:\nError parsing object at <HEX>"


def test_get_error_string_long_error():
    """
    Simulate two workers with identical long error string.
    """
    long_error_str = "test string" * 200
    statuses = {
        0: WorkerStatus(running=False, error=long_error_str),
        1: WorkerStatus(running=False, error=long_error_str),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = (
        "[Rank 0, 1]:\n"
        + long_error_str[:ERR_CHAR_LIMIT]
        + "...\nView individual worker logs for more details."
    )
    assert error_str == expected_error_str


def test_get_error_string_running_worker():
    """
    Simulate two workers with identical error strings but one is running
    and the other is not.
    """
    statuses = {
        0: WorkerStatus(running=False, error="error"),
        1: WorkerStatus(running=True, error="error"),
    }
    poll_status = WorkerGroupPollStatus(worker_statuses=statuses)
    error_str = poll_status.get_error_string()

    expected_error_str = "[Rank 0]:\nerror"
    assert error_str == expected_error_str


def test_normalize_error_string():
    """Test that _normalize_error_string properly handles all types of numbers."""

    # Test file paths with timestamps and session IDs
    file_path = 'File "/tmp/ray/session_2025-01-07_18-45-16_587057_54497/runtime_resources/working_dir_files/_ray_pkg_5abd79ca51ba0ed4/runner.py"'
    expected = 'File "/tmp/ray/session_<NUM>-<NUM>-<NUM>_<NUM>-<NUM>-<NUM>_<NUM>_<NUM>/runtime_resources/working_dir_files/_ray_pkg_<HEX>/runner.py"'
    assert _normalize_error_string(file_path) == expected

    # Test various number formats
    test_cases = [
        ("Error on line 42", "Error on line <NUM>"),
        (
            "Connection timeout after 30 seconds",
            "Connection timeout after <NUM> seconds",
        ),
        ("Object at 0x7f8b12345678 not found", "Object at <ADDR> not found"),
        ("Invalid handle 0xdeadbeef", "Invalid handle <HEX>"),
        ("Port 8080 is already in use", "Port <NUM> is already in use"),
        ("worker_123.log", "worker_<NUM>.log"),
        (
            "Mixed: line 42, port 8080, addr 0x123abc",
            "Mixed: line <NUM>, port <NUM>, addr <HEX>",
        ),
    ]

    for original, expected in test_cases:
        result = _normalize_error_string(original)
        assert (
            result == expected
        ), f"Expected '{expected}', got '{result}' for input '{original}'"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
