"""Tests for arrow conversion warning message truncation (issue #57840)."""

import logging
import numpy as np
import pytest

import ray


@pytest.mark.parametrize(
    "dataset_size,ids",
    [
        (1, "full_msg"),
        (50, "truncated_msg"),
    ],
)
def test_arrow_conversion_warning(caplog, dataset_size, ids):
    """Test that arrow conversion warnings are properly truncated to avoid log noise.

    This test verifies the fix for issue #57840 where large array conversion
    failures resulted in extremely verbose warning logs.

    Parameters:
        dataset_size: Multiplier for array size. Small (1) produces small error,
            large (50) produces truncation.
        ids: Test ID identifier for parametrize.
    """
    # Create a nested array that will cause Arrow conversion error
    # Using dataset_size to control whether truncation occurs
    image_batch = [
        [  # Batch of images
            np.array(
                [
                    # Image with dimensions that Arrow can't handle natively
                    [[130, 118, 255], [132, 117, 255], [130, 115, 252]],
                    [[133, 114, 255], [132, 113, 255], [132, 113, 255]],
                ]
                * dataset_size  # Scale array size
            )
            for _ in range(5)  # Multiple images in batch
        ]
    ]

    with caplog.at_level(logging.WARNING):
        # This should potentially trigger an Arrow conversion warning
        ds = ray.data.from_items(image_batch)
        try:
            _ = ds.take(1)
        except Exception:
            # Some conversions may raise instead of warn, that's okay for this test
            pass

    # Check that warning was logged (if any)
    warning_messages = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]

    # For truncated_msg case, verify truncation is working
    if ids == "truncated_msg":
        assert len(warning_messages) > 0, "Expected at least one warning to be logged"

        # Verify that the warning message is truncated
        found_truncated = False
        for msg in warning_messages:
            if "Failed to convert column" in msg:
                # Ensure the message isn't excessively long (pre-fix it could be 10k+ chars)
                assert (
                    len(msg) < 1000
                ), f"Warning message too long ({len(msg)} chars), truncation may not be working"
                found_truncated = True
                break

        assert found_truncated, (
            "Did not find expected Arrow conversion warning in logs. "
            f"Found warnings: {warning_messages}"
        )
    else:
        # For full_msg case, just verify warnings are reasonable length
        for msg in warning_messages:
            if "Failed to convert column" in msg:
                # Any conversion warning should be reasonable length
                assert len(msg) < 10000, (
                    f"Warning message unexpectedly long ({len(msg)} chars): {msg}"
                )
