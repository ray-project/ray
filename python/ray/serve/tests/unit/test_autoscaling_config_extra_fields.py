"""Test that AutoscalingConfig rejects misplaced deployment-level fields.

Regression test for https://github.com/ray-project/ray/issues/61439
"""

import pytest
from pydantic import ValidationError

from ray.serve.config import AutoscalingConfig


def test_autoscaling_config_rejects_max_ongoing_requests():
    """max_ongoing_requests belongs at deployment level, not inside autoscaling_config."""
    with pytest.raises(ValidationError, match="extra"):
        AutoscalingConfig(
            max_ongoing_requests=10,
            target_ongoing_requests=5,
            min_replicas=1,
            max_replicas=10,
        )


def test_autoscaling_config_rejects_arbitrary_extra_field():
    """Any unknown field should be rejected."""
    with pytest.raises(ValidationError, match="extra"):
        AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            not_a_real_field="oops",
        )


def test_autoscaling_config_accepts_valid_fields():
    """Verify valid configs still work."""
    config = AutoscalingConfig(
        target_ongoing_requests=5,
        min_replicas=1,
        max_replicas=10,
        upscale_delay_s=10,
        downscale_delay_s=300,
    )
    assert config.min_replicas == 1
    assert config.max_replicas == 10
    assert config.target_ongoing_requests == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
