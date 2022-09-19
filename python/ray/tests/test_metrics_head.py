# coding: utf-8
import logging
import os
import sys

import pytest

from ray.tests.conftest import _ray_start

logger = logging.getLogger(__name__)


def test_metrics_folder():
    """
    Tests that the default dashboard files get created.
    """
    with _ray_start(include_dashboard=True) as context:
        session_dir = context["session_dir"]
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml"
        )
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default_grafana_dashboard.json"
        )
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert os.path.exists(f"{session_dir}/metrics/prometheus/prometheus.yml")


def test_metrics_folder_when_dashboard_disabled():
    """
    Tests that the default dashboard files do not get created when dashboard is disabled.
    """
    with _ray_start(include_dashboard=False) as context:
        session_dir = context["session_dir"]
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml"
        )
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default_grafana_dashboard.json"
        )
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert not os.path.exists(f"{session_dir}/metrics/prometheus/prometheus.yml")


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
