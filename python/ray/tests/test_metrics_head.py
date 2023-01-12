# coding: utf-8
import logging
import os
import pytest
import sys
import tempfile

from ray.dashboard.modules.metrics.grafana_dashboard_factory import GRAFANA_PANELS
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
            f"{session_dir}/metrics/grafana/dashboards"
            "/default_grafana_dashboard.json"
        )
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert os.path.exists(f"{session_dir}/metrics/grafana/grafana.ini")
        assert os.path.exists(f"{session_dir}/metrics/prometheus/prometheus.yml")


@pytest.fixture
def override_dashboard_dir():
    with tempfile.TemporaryDirectory() as tempdir:
        os.environ["RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR"] = tempdir
        yield tempdir
        del os.environ["RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR"]


def test_metrics_folder_with_dashboard_override(override_dashboard_dir):
    """
    Tests that the default dashboard files get created.
    """
    with _ray_start(include_dashboard=True) as context:
        session_dir = context["session_dir"]
        assert os.path.exists(
            f"{override_dashboard_dir}/default_grafana_dashboard.json"
        )
        with open(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml"
        ) as f:
            contents = f.read()
            assert override_dashboard_dir in contents


def test_metrics_folder_when_dashboard_disabled():
    """
    Tests that the default dashboard files do not get created when dashboard
    is disabled.
    """
    with _ray_start(include_dashboard=False) as context:
        session_dir = context["session_dir"]
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml"
        )
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/dashboards"
            "/default_grafana_dashboard.json"
        )
        assert not os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/datasources/default.yml"
        )
        assert not os.path.exists(f"{session_dir}/metrics/prometheus/prometheus.yml")


def test_default_dashboard_utilizes_global_filters():
    for panel in GRAFANA_PANELS:
        for target in panel.targets:
            assert "{global_filters}" in target.expr


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
