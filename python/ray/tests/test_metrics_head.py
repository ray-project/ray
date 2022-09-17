# coding: utf-8
import logging
import os
import sys

import pytest

from ray.tests.conftest import _ray_start

logger = logging.getLogger(__name__)


def test_metrics_folder():
    """
    Tests that the default dashboard files get created
    Tests that custom dashbaords do not get overwritten.
    """
    with _ray_start(include_dashboard=True):
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/dashboards/default.yml"
        )
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/dashboards/default_grafana_dashboard.json"
        )
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert os.path.exists("/tmp/ray/metrics/prometheus/prometheus.yml")

        assert os.path.exists("/tmp/ray/metrics/custom/grafana-dashboards")

    # Create a custom dashboard file
    with open(
        "/tmp/ray/metrics/custom/grafana-dashboards/custom-dashboard-for-testing.json",
        "w",
    ) as f:
        f.write('{ "name": "custom-dashboard"}')

    with _ray_start(include_dashboard=True):
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/dashboards/default.yml"
        )
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/dashboards/default_grafana_dashboard.json"
        )
        assert os.path.exists(
            "/tmp/ray/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert os.path.exists("/tmp/ray/metrics/prometheus/prometheus.yml")

        # Check custom-dashboard file exists and was not deleted.
        assert os.path.exists(
            "/tmp/ray/metrics/custom/grafana-dashboards/custom-dashboard-for-testing.json"
        )

    os.remove(
        "/tmp/ray/metrics/custom/grafana-dashboards/custom-dashboard-for-testing.json"
    )


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
