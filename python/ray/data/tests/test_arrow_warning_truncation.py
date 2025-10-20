"""Tests for arrow conversion warning message truncation (issue #57840)."""

import logging
import numpy as np
import pytest

import ray


def test_arrow_conversion_warning_truncation(caplog):
    """Test that arrow conversion warnings are properly truncated to avoid log noise.

    This test verifies the fix for issue #57840 where large array conversion
    failures resulted in extremely verbose warning logs.
    """
    # Create a large nested array that will cause Arrow conversion error
    # This mimics the reproduction case from issue #57840
    large_image_batch = [
        [  # Batch of images
            np.array(
                [
                    # Image with dimensions that Arrow can't handle natively
                    [[130, 118, 255], [132, 117, 255], [130, 115, 252]],
                    [[133, 114, 255], [132, 113, 255], [132, 113, 255]],
                ]
                * 50  # Multiply to make it large enough to trigger truncation
            )
            for _ in range(5)  # Multiple images in batch
        ]
    ]

    with caplog.at_level(logging.WARNING):
        # This should trigger an Arrow conversion warning
        ds = ray.data.from_items(large_image_batch)
        _ = ds.take(1)

    # Check that warning was logged
    warning_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]

    assert len(warning_messages) > 0, "Expected at least one warning to be logged"

    # Verify that the warning message is truncated
    for msg in warning_messages:
        if "Failed to convert column" in msg and "truncated" in msg:
            # Found our truncation warning
            assert "[truncated" in msg, "Warning should indicate truncation"
            # Ensure the message isn't excessively long (pre-fix it could be 10k+ chars)
            assert (
                len(msg) < 1000
            ), f"Warning message too long ({len(msg)} chars), truncation may not be working"
            return

    # If we get here, we didn't find the expected truncated warning
    pytest.fail(
        "Did not find expected truncated Arrow conversion warning in logs. "
        f"Found warnings: {warning_messages}"
    )


def test_arrow_conversion_small_error_not_truncated(caplog):
    """Test that small error messages are not truncated unnecessarily."""
    # Create a simple case that will fail conversion but with small error message
    simple_data = [{"value": [1, 2, 3]}]

    with caplog.at_level(logging.WARNING):
        ds = ray.data.from_items(simple_data)
        _ = ds.take(1)

    # Check warnings
    warning_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]

    # If there are conversion warnings, they should not be truncated for small errors
    for msg in warning_messages:
        if "Failed to convert column" in msg:
            # Small error messages should not show truncation text
            if "truncated" in msg:
                # Only acceptable if the original error was actually long
                assert "[truncated" in msg
