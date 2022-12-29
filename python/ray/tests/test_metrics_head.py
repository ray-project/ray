# coding: utf-8
import logging
import os
import pytest
import sys
import tempfile

from ray.dashboard.modules.metrics.metrics_head import (
    _format_prometheus_output,
    _format_prometheus_output_by_task_names,
    TaskProgressByTaskNameResponse,
    TaskProgressWithTaskName,
    TaskProgress,
)
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
                {
                    "metric": {"State": "FAILED"},
                    "value": [1664330796.832, "6"],
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
        num_failed=6,
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
                {
                    "metric": {"State": "FAILED"},
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
        num_failed=3,
    )


def test_format_prometheus_output_by_task_names():
    prom_output = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"Name": "step1", "State": "RUNNING"},
                    "value": [1666390500.167, "3"],
                },
                {
                    "metric": {"Name": "step1", "State": "SUBMITTED_TO_WORKER"},
                    "value": [1666390500.167, "3"],
                },
                {
                    "metric": {"Name": "step1", "State": "PENDING_ARGS_AVAIL"},
                    "value": [1666390500.167, "0"],
                },
                {
                    "metric": {"Name": "step1", "State": "PENDING_NODE_ASSIGNMENT"},
                    "value": [1666390500.167, "0"],
                },
                {
                    "metric": {"Name": "step2", "State": "RUNNING"},
                    "value": [1666390500.167, "2"],
                },
                {
                    "metric": {"Name": "step2", "State": "SUBMITTED_TO_WORKER"},
                    "value": [1666390500.167, "0"],
                },
                {
                    "metric": {"Name": "step2", "State": "PENDING_ARGS_AVAIL"},
                    "value": [1666390500.167, "3"],
                },
                {
                    "metric": {"Name": "step3", "State": "PENDING_ARGS_AVAIL"},
                    "value": [1666390500.167, "1"],
                },
            ],
        },
    }
    assert _format_prometheus_output_by_task_names(
        prom_output
    ) == TaskProgressByTaskNameResponse(
        tasks=[
            TaskProgressWithTaskName(
                name="step1",
                progress=TaskProgress(
                    num_running=3,
                    num_submitted_to_worker=3,
                ),
            ),
            TaskProgressWithTaskName(
                name="step2",
                progress=TaskProgress(
                    num_running=2,
                    num_pending_args_avail=3,
                ),
            ),
            TaskProgressWithTaskName(
                name="step3", progress=TaskProgress(num_pending_args_avail=1)
            ),
        ]
    )


def test_default_dashboard_utilizes_global_filters():
    for panel in GRAFANA_PANELS:
        for target in panel.targets:
            assert "{global_filters}" in target.expr


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
