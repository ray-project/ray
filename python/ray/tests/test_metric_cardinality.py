import os
import sys
from unittest.mock import patch

import pytest

from ray._private.telemetry.metric_cardinality import MetricCardinality


def test_default_cardinality_level_is_legacy():
    """Test that default cardinality level is LEGACY when no env var is set."""
    with patch.dict(os.environ, {}, clear=True):
        cardinality_level = MetricCardinality.get_cardinality_level()
        assert cardinality_level == MetricCardinality.LEGACY


def test_legacy_cardinality_level_from_env():
    """Test that LEGACY cardinality level is set correctly from environment."""
    with patch.dict(os.environ, {"RAY_metric_cardinality_level": "legacy"}):
        cardinality_level = MetricCardinality.get_cardinality_level()
        assert cardinality_level == MetricCardinality.LEGACY


def test_recommended_cardinality_level_from_env():
    """Test that RECOMMENDED cardinality level is set correctly from environment."""
    with patch.dict(os.environ, {"RAY_metric_cardinality_level": "recommended"}):
        cardinality_level = MetricCardinality.get_cardinality_level()
        assert cardinality_level == MetricCardinality.RECOMMENDED


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
