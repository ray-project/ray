import pytest

from ray.util.metrics import Metric


def test_invalid_metric_name():
    with pytest.raises(ValueError) as e:
        Metric("faulty-metric", "Test metric")
    assert (
        str(e.value) == "Metric name faulty-metric is invalid. "
        "Please use metric names that match the regex ^[a-zA-Z_:][a-zA-Z0-9_:]*$"
    )


def test_empty_metric_name():
    with pytest.raises(ValueError) as e:
        Metric("", "Test metric")
    assert str(e.value) == "Empty name is not allowed. Please provide a metric name."
