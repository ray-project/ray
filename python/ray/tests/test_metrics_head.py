# coding: utf-8
import json
import logging
import os
import pytest
import sys
import tempfile

from ray.dashboard.modules.metrics.dashboards.default_dashboard_panels import (
    DEFAULT_GRAFANA_PANELS,
)
from ray.dashboard.modules.metrics.dashboards.serve_dashboard_panels import (
    SERVE_GRAFANA_PANELS,
)
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


@pytest.fixture
def override_default_dashboard():
    uid = "test_uid_ses_12345"
    global_filters = 'TestVar="StaticValue"'

    os.environ["RAY_GRAFANA_DEFAULT_DASHBOARD_UID"] = uid
    os.environ["RAY_GRAFANA_DEFAULT_DASHBOARD_GLOBAL_FILTERS"] = global_filters
    yield uid, global_filters
    del os.environ["RAY_GRAFANA_DEFAULT_DASHBOARD_GLOBAL_FILTERS"]
    del os.environ["RAY_GRAFANA_DEFAULT_DASHBOARD_UID"]


@pytest.fixture
def override_serve_dashboard():
    uid = "test_serve_uid_ses_12345"
    global_filters = 'TestVar="$TestVariableValue"'

    os.environ["RAY_GRAFANA_SERVE_DASHBOARD_UID"] = uid
    os.environ["RAY_GRAFANA_SERVE_DASHBOARD_GLOBAL_FILTERS"] = global_filters
    yield uid, global_filters
    del os.environ["RAY_GRAFANA_SERVE_DASHBOARD_GLOBAL_FILTERS"]
    del os.environ["RAY_GRAFANA_SERVE_DASHBOARD_UID"]


def test_metrics_folder_with_dashboard_override(
    override_dashboard_dir, override_default_dashboard, override_serve_dashboard
):
    """
    Tests that the default dashboard files get created.
    """
    uid, global_filters = override_default_dashboard
    serve_uid, serve_global_filters = override_serve_dashboard

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

        # Default Dashboard
        with open(f"{override_dashboard_dir}/default_grafana_dashboard.json") as f:
            contents = json.loads(f.read())
            assert contents["uid"] == uid
            for panel in contents["panels"]:
                for target in panel["targets"]:
                    # Check for standard_global_filters
                    assert 'SessionName=~"$SessionName"' in target["expr"]
                    # Check for custom global_filters
                    assert global_filters in target["expr"]
            for variable in contents["templating"]["list"]:
                if variable["name"] == "datasource":
                    continue
                assert global_filters in variable["definition"]
                assert global_filters in variable["query"]["query"]
            assert "supportsGlobalFilterOverride" in contents["rayMeta"]

        # Serve Dashboard
        with open(f"{override_dashboard_dir}/serve_grafana_dashboard.json") as f:
            contents = json.loads(f.read())
            assert contents["uid"] == serve_uid
            for panel in contents["panels"]:
                for target in panel["targets"]:
                    assert serve_global_filters in target["expr"]
            for variable in contents["templating"]["list"]:
                if variable["name"] == "datasource":
                    continue
                assert serve_global_filters in variable["definition"]
                assert serve_global_filters in variable["query"]["query"]
            assert "supportsGlobalFilterOverride" in contents["rayMeta"]

        # Serve Deployment Dashboard
        with open(
            f"{override_dashboard_dir}/serve_deployment_grafana_dashboard.json"
        ) as f:
            contents = json.loads(f.read())
            assert contents["uid"] == "rayServeDeploymentDashboard"
            assert "supportsGlobalFilterOverride" in contents["rayMeta"]


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
    for panel in DEFAULT_GRAFANA_PANELS:
        for target in panel.targets:
            assert "{global_filters}" in target.expr


def test_serve_dashboard_utilizes_global_filters():
    for panel in SERVE_GRAFANA_PANELS:
        for target in panel.targets:
            assert "{global_filters}" in target.expr


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
