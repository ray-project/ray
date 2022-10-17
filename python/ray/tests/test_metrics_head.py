# coding: utf-8
import logging
import os
import pytest
import sys
import tempfile

from ray.dashboard.modules.metrics.metrics_head import (
    _format_prometheus_output,
    TaskProgress,
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
    with _ray_start(include_dashboard=True):
        assert os.path.exists(
            f"{override_dashboard_dir}/default_grafana_dashboard.json"
        )


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


def test_format_prometheus_output():
    prom_output = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"State": "RUNNING"}, "value": [1664330796.832, "2"]},
                {
                    "metric": {"State": "RUNNING_IN_RAY_GET"},
                    "value": [1664330796.832, "4"],
                },
                {
                    "metric": {"State": "RUNNING_IN_RAY_WAIT"},
                    "value": [1664330796.832, "3"],
                },
                {
                    "metric": {"State": "SUBMITTED_TO_WORKER"},
                    "value": [1664330796.832, "5"],
                },
                {"metric": {"State": "FINISHED"}, "value": [1664330796.832, "3"]},
                {
                    "metric": {"State": "PENDING_ARGS_AVAIL"},
                    "value": [1664330796.832, "5"],
                },
                {
                    "metric": {"State": "PENDING_NODE_ASSIGNMENT"},
                    "value": [1664330796.832, "2"],
                },
                {
                    "metric": {"State": "PENDING_ARGS_FETCH"},
                    "value": [1664330796.832, "7"],
                },
                {
                    "metric": {"State": "PENDING_OBJ_STORE_MEM_AVAIL"},
                    "value": [1664330796.832, "8"],
                },
            ],
        },
    }
    assert _format_prometheus_output(prom_output) == TaskProgress(
        num_finished=3,
        num_pending_args_avail=5,
        num_pending_node_assignment=17,
        num_running=9,
        num_submitted_to_worker=5,
        num_unknown=0,
    )

    # With unknown states from prometheus
    prom_output_with_unknown = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": {"State": "RUNNING"}, "value": [1664330796.832, "10"]},
                {
                    "metric": {"State": "RUNNING_IN_RAY_GET"},
                    "value": [1664330796.832, "4"],
                },
                {"metric": {"State": "FINISHED"}, "value": [1664330796.832, "20"]},
                {
                    "metric": {"State": "PENDING_ARGS_AVAIL"},
                    "value": [1664330796.832, "5"],
                },
                {
                    "metric": {"State": "SOME_NEW_VARIABLE"},
                    "value": [1664330796.832, "3"],
                },
            ],
        },
    }
    assert _format_prometheus_output(prom_output_with_unknown) == TaskProgress(
        num_finished=20,
        num_pending_args_avail=5,
        num_pending_node_assignment=0,
        num_running=14,
        num_submitted_to_worker=0,
        num_unknown=3,
    )


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
