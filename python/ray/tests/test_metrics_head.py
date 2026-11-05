# coding: utf-8
import json
import logging
import os
import re
import sys
import tempfile

import pytest

from ray._common.utils import get_default_ray_temp_dir
from ray._private.ray_constants import SESSION_LATEST
from ray.dashboard.modules.metrics.dashboards.data_llm_dashboard_panels import (
    DATA_LLM_GRAFANA_PANELS,
    data_llm_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.default_dashboard_panels import (
    DEFAULT_GRAFANA_ROWS,
    MAX_NPUS,
    MAX_PERCENTAGE_EXPRESSION,
    MAX_PLUS_PENDING_NPUS,
    OVERVIEW_AND_HEALTH_PANELS,
    RAY_RESOURCES_PANELS,
)
from ray.dashboard.modules.metrics.dashboards.serve_dashboard_panels import (
    SERVE_GRAFANA_PANELS,
)
from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
    GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE,
    generate_data_llm_grafana_dashboard,
)
from ray.tests.conftest import _ray_start

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "is_temp_dir_set, temp_dir_val",
    [
        (False, ""),
        pytest.param(
            True,
            "/tmp/test-temp-dir",
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="Path doesn't apply to Windows"
            ),
        ),
    ],
)
def test_metrics_folder_and_content(is_temp_dir_set, temp_dir_val):
    """
    Tests that the default dashboard files get created. It also verifies paths in the
    dashboard config files are set correctly.

    It checks both the default case and the case where the _temp_dir is specified.
    """
    with _ray_start(
        include_dashboard=True, _temp_dir=temp_dir_val if is_temp_dir_set else None
    ) as context:
        session_dir = context["session_dir"]
        temp_dir = temp_dir_val if is_temp_dir_set else get_default_ray_temp_dir()
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml"
        )
        with open(
            f"{session_dir}/metrics/grafana/provisioning/dashboards/default.yml", "r"
        ) as f:
            target_path = os.path.join(session_dir, "metrics", "grafana", "dashboards")
            assert f"path: {target_path}" in f.read()

        assert os.path.exists(
            f"{session_dir}/metrics/grafana/dashboards"
            "/default_grafana_dashboard.json"
        )
        assert os.path.exists(
            f"{session_dir}/metrics/grafana/provisioning/datasources/default.yml"
        )

        assert os.path.exists(f"{session_dir}/metrics/grafana/grafana.ini")
        with open(f"{session_dir}/metrics/grafana/grafana.ini", "r") as f:
            target_path = os.path.join(
                temp_dir, SESSION_LATEST, "metrics", "grafana", "provisioning"
            )
            assert f"provisioning = {target_path}" in f.read()

        assert os.path.exists(f"{session_dir}/metrics/prometheus/prometheus.yml")
        with open(f"{session_dir}/metrics/prometheus/prometheus.yml", "r") as f:
            target_path = os.path.join(
                session_dir, "prom_metrics_service_discovery.json"
            )
            assert f"- '{target_path}'" in f.read()


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
                if panel["type"] == "row":
                    # Row panels don't have targets
                    continue
                for target in panel["targets"]:
                    if target["expr"] == MAX_PERCENTAGE_EXPRESSION:
                        # We skip expressions that are constant value targets
                        continue
                    # Check for standard_global_filters
                    assert 'SessionName=~"$SessionName"' in target["expr"]
                    # Check for custom global_filters
                    assert global_filters in target["expr"]
            for variable in contents["templating"]["list"]:
                if variable["name"] == "datasource":
                    continue
                if variable["name"] == "RayNodeType":
                    # RayNodeType uses hardcoded values instead of a query
                    continue
                assert global_filters in variable["definition"]
                assert global_filters in variable["query"]["query"]
            assert "supportsGlobalFilterOverride" in contents["rayMeta"]
            # Check that panels have some width and height
            for panel in contents["panels"]:
                assert panel["gridPos"]["h"] > 0
                assert panel["gridPos"]["w"] > 0
            # Check for series overrides for MAX and MAX + PENDING
            found_max = False
            found_max_pending = False
            for panel in contents["panels"]:
                if panel["type"] == "row":
                    # Row panels don't have series overrides
                    continue
                for override in panel.get("seriesOverrides", []):
                    if override.get("alias") == "MAX":
                        assert override["fill"] == 0
                        assert override["stack"] is False
                        found_max = True
                    if override.get("alias") == "MAX + PENDING":
                        assert override["fill"] == 0
                        assert override["stack"] is False
                        found_max_pending = True
            assert found_max
            assert found_max_pending

        # Serve Dashboard
        with open(f"{override_dashboard_dir}/serve_grafana_dashboard.json") as f:
            contents = json.loads(f.read())
            assert contents["uid"] == serve_uid
            for panel in contents["panels"]:
                if panel["type"] == "row":
                    # Row panels contain nested panels, not targets directly
                    for nested_panel in panel.get("panels", []):
                        for target in nested_panel["targets"]:
                            assert serve_global_filters in target["expr"]
                else:
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
    for row in DEFAULT_GRAFANA_ROWS:
        for panel in row.panels:
            for target in panel.targets:
                if target.legend == "MAX" and target.expr == MAX_PERCENTAGE_EXPRESSION:
                    # We skip expressions that are constant value targets serving as visual aids
                    continue
                assert "{global_filters}" in target.expr


def test_cluster_npu_utilization_averages_reported_samples():
    cluster_utilization = next(
        panel
        for panel in OVERVIEW_AND_HEALTH_PANELS
        if panel.title == "Cluster Utilization"
    )
    npu_target = next(
        target
        for target in cluster_utilization.targets
        if target.legend == "NPU AI Core (physical)"
    )

    assert npu_target.expr == (
        'avg(ray_node_npus_utilization{{instance=~"$Instance",{global_filters}}})'
    )


def test_cluster_npu_hbm_utilization_uses_reported_capacity():
    cluster_utilization = next(
        panel
        for panel in OVERVIEW_AND_HEALTH_PANELS
        if panel.title == "Cluster Utilization"
    )
    hbm_target = next(
        target for target in cluster_utilization.targets if target.legend == "NPU HBM"
    )

    assert hbm_target.expr == (
        'sum(ray_node_npu_hbm_used{{instance=~"$Instance",{global_filters}}}) '
        '/ on() (sum(ray_node_npu_hbm_available{{instance=~"$Instance",'
        '{global_filters}}}) + sum(ray_node_npu_hbm_used{{instance=~"$Instance",'
        "{global_filters}}})) * 100"
    )


def test_npu_health_panel_preserves_dcmi_severity():
    health_panel = next(
        panel
        for row in DEFAULT_GRAFANA_ROWS
        for panel in row.panels
        if panel.title == "Node NPU Health"
    )

    assert "0 is OK" in health_panel.description
    assert "3 is critical" in health_panel.description
    assert "4 means that health information is missing" in health_panel.description
    assert health_panel.targets[0].expr.startswith("max(ray_node_npu_health")


def test_logical_npu_usage_panel():
    logical_npu_usage = next(
        panel for panel in RAY_RESOURCES_PANELS if panel.title == "Logical NPU Usage"
    )

    assert logical_npu_usage.unit == "NPUs"
    assert [
        (target.legend, target.expr) for target in logical_npu_usage.targets[:2]
    ] == [
        (
            "NPU Usage: {{instance}}",
            'sum(ray_resources{{Name="NPU",State="USED",instance=~"$Instance",'
            "{global_filters}}}) by (instance)",
        ),
        (
            "MAX",
            'sum(ray_resources{{Name="NPU",instance=~"$Instance",'
            "{global_filters}}})",
        ),
    ]
    pending_target = logical_npu_usage.targets[2]
    assert pending_target.legend == "MAX + PENDING"
    assert pending_target.expr == (
        f"({MAX_PLUS_PENDING_NPUS} and {MAX_PLUS_PENDING_NPUS} > "
        f"({MAX_NPUS} or vector(0)))"
    )
    assert 'resource="NPU"' in pending_target.expr
    assert "autoscaler_pending_resources" in pending_target.expr


def test_serve_dashboard_utilizes_global_filters():
    for panel in SERVE_GRAFANA_PANELS:
        for target in panel.targets:
            if target.legend == "MAX" and target.expr == MAX_PERCENTAGE_EXPRESSION:
                # We skip expressions that are constant value targets serving as visual aids
                continue
            assert "{global_filters}" in target.expr


DATA_LLM_GLOBAL_FILTERS_ENV_VAR = (
    GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE.format(
        name=data_llm_dashboard_config.name
    )
)
SESSION_FILTER = 'SessionName=~"$SessionName"'
RAY_IO_CLUSTER_FILTER = 'ray_io_cluster=~"$Cluster"'
# The filter Anyscale's managed Grafana injects through the env override.
CLUSTER_ID_FILTER = 'ClusterId=~"$ClusterId"'


def _generate_data_llm_dashboard(monkeypatch, override_filter):
    if override_filter is None:
        monkeypatch.delenv(DATA_LLM_GLOBAL_FILTERS_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(DATA_LLM_GLOBAL_FILTERS_ENV_VAR, override_filter)
    content, _ = generate_data_llm_grafana_dashboard()
    return json.loads(content)


def _panel_exprs(dashboard):
    return [t["expr"] for panel in dashboard["panels"] for t in panel["targets"]]


def _variables_by_name(dashboard):
    return {v["name"]: v for v in dashboard["templating"]["list"]}


def test_data_llm_dashboard_utilizes_global_filters():
    """Every label selector of every Data LLM target carries `{global_filters}`.

    A mean divides a sum by a count, so each selector is checked, not just
    each expression.
    """
    for panel in DATA_LLM_GRAFANA_PANELS:
        for target in panel.targets:
            # In the template, each selector opens with an escaped "{{".
            num_selectors = target.expr.count("{{")
            num_filtered = target.expr.count("{global_filters}")

            assert num_selectors > 0, f"{panel.title}: {target.expr}"
            assert num_filtered == num_selectors, f"{panel.title}: {target.expr}"


def test_data_llm_dashboard_has_session_and_cluster_variables(monkeypatch):
    """The generated Data LLM dashboard offers SessionName and Cluster
    dropdowns ahead of the model and worker dropdowns that they narrow.
    """
    dashboard = _generate_data_llm_dashboard(monkeypatch, override_filter=None)

    names = [v["name"] for v in dashboard["templating"]["list"]]

    assert names == [
        "datasource",
        "SessionName",
        "Cluster",
        "vllm_model_name",
        "workerid",
        "interval",
    ]


@pytest.mark.parametrize("override_filter", [None, CLUSTER_ID_FILTER])
def test_data_llm_dashboard_panels_apply_all_filters(monkeypatch, override_filter):
    """Every selector of every generated panel query carries both standard
    filters, plus the env override filter when one is set.
    """
    dashboard = _generate_data_llm_dashboard(monkeypatch, override_filter)

    exprs = _panel_exprs(dashboard)

    assert exprs
    expected = [SESSION_FILTER, RAY_IO_CLUSTER_FILTER]
    if override_filter is not None:
        expected.append(override_filter)
    for expr in exprs:
        # After formatting, each selector opens with a single "{".
        num_selectors = expr.count("{")
        for label_filter in expected:
            assert expr.count(label_filter) == num_selectors, expr


@pytest.mark.parametrize("override_filter", [None, CLUSTER_ID_FILTER])
def test_data_llm_dashboard_variables_avoid_self_reference(
    monkeypatch, override_filter
):
    """The SessionName and Cluster dropdowns carry the env override filter but
    never their own variable, and the model and worker dropdowns narrow to the
    chosen session and cluster.
    """
    dashboard = _generate_data_llm_dashboard(monkeypatch, override_filter)

    variables = _variables_by_name(dashboard)

    for name in ("SessionName", "Cluster"):
        query = variables[name]["query"]["query"]
        # Match the quoted value so "$ClusterId" doesn't count as "$Cluster".
        assert f'"${name}"' not in query, query
        if override_filter is not None:
            assert override_filter in query, query
    for name in ("vllm_model_name", "workerid"):
        query = variables[name]["query"]["query"]
        assert SESSION_FILTER in query, query
        assert RAY_IO_CLUSTER_FILTER in query, query


@pytest.mark.parametrize("override_filter", [None, CLUSTER_ID_FILTER])
def test_data_llm_dashboard_has_no_dangling_commas(monkeypatch, override_filter):
    """No generated panel or dropdown selector has an empty matcher between
    commas, or a comma next to a brace, which Prometheus would reject.
    """
    dashboard = _generate_data_llm_dashboard(monkeypatch, override_filter)

    queries = _panel_exprs(dashboard) + [
        v["query"]["query"]
        for v in dashboard["templating"]["list"]
        if isinstance(v.get("query"), dict)
    ]

    assert queries
    for query in queries:
        assert not re.search(r",\s*,|,\s*}|{\s*,", query), query


def test_serve_dashboard_excludes_system_traffic():
    for panel in SERVE_GRAFANA_PANELS:
        for target in panel.targets:
            expr = target.expr
            # Combined targets select by __name__ and carry neither selector, so both apply.
            combined = bool(
                re.search(r'__name__\s*=~\s*["\']ray_serve_\(http\|grpc\)', expr)
            )
            if combined or re.search(r"ray_serve_(num_)?http_\w*request", expr):
                assert 'route!~"/-/.*"' in expr, f"{panel.title}: {expr}"
            if combined or re.search(r"ray_serve_(num_)?grpc_\w*request", expr):
                assert (
                    r'method!~"/ray\\.serve\\.RayServeAPIService/.*"' in expr
                ), f"{panel.title}: {expr}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
