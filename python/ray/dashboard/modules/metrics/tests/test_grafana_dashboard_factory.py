import json
import sys

import pytest

from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
    ANNOTATION_DATASOURCE_VARIABLE,
    ANNOTATION_SESSION_VARIABLE,
    ANNOTATION_STREAM_SELECTOR,
    GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR,
    GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE,
    generate_train_grafana_dashboard,
)

GLOBAL_FILTERS_ENV_VAR = (
    GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE.format(name="TRAIN")
)


@pytest.fixture(autouse=True)
def enable_annotations(monkeypatch):
    # Off by default; see `test_annotations_are_off_by_default`.
    monkeypatch.setenv(GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR, "1")
    monkeypatch.delenv(GLOBAL_FILTERS_ENV_VAR, raising=False)


def generate_train_dashboard() -> dict:
    content, _ = generate_train_grafana_dashboard()
    return json.loads(content)


def get_variable(dashboard: dict, name: str) -> dict:
    (variable,) = [
        variable
        for variable in dashboard["templating"]["list"]
        if variable["name"] == name
    ]
    return variable


def dashboard_annotations(dashboard: dict) -> list:
    return [
        annotation
        for annotation in dashboard["annotations"]["list"]
        # Grafana's own built-in annotation, which is always present.
        if not annotation.get("builtIn")
    ]


def variable_names(dashboard: dict) -> list:
    return [variable["name"] for variable in dashboard["templating"]["list"]]


def test_annotations_render_unconfigured():
    """The datasource resolves through a variable Grafana binds to the first Loki
    datasource, and the selector is the label contract Ray publishes for the log
    collector, so nothing but the enable flag has to be configured."""
    dashboard = generate_train_dashboard()
    annotations = dashboard_annotations(dashboard)

    assert annotations
    assert ANNOTATION_DATASOURCE_VARIABLE in variable_names(dashboard)
    for annotation in annotations:
        assert annotation["datasource"] == {
            "type": "loki",
            "uid": f"${{{ANNOTATION_DATASOURCE_VARIABLE}}}",
        }
        assert annotation["expr"].startswith(f"{ANNOTATION_STREAM_SELECTOR} ")
        # The nested target query must match the top-level one exactly.
        assert annotation["target"]["expr"] == annotation["expr"]


def test_annotations_follow_the_session_variable():
    """Annotations match the sessions in a hidden variable chained off
    `SessionName`, never `SessionName` itself, whose `All` is `.*` and would
    match every cluster's events on a shared log backend."""
    dashboard = generate_train_dashboard()
    annotations = dashboard_annotations(dashboard)

    assert annotations
    for annotation in annotations:
        assert (
            f'| session_name=~"${{{ANNOTATION_SESSION_VARIABLE}:regex}}"'
            in annotation["expr"]
        )
        assert "$SessionName" not in annotation["expr"]
        # The run stays viewer-selectable: that is a choice within one cluster,
        # which is what separates concurrent runs sharing a workspace.
        assert '| ray_train_run_id=~"$TrainRunId"' in annotation["expr"]

    variable = get_variable(dashboard, ANNOTATION_SESSION_VARIABLE)
    # `All` expands to the sessions Prometheus has, not to a custom `.*`.
    assert variable["includeAll"] and "allValue" not in variable
    assert variable["hide"] == 2
    assert (
        variable["query"]["query"]
        == 'label_values(ray_train_worker_group_start_total_time_s{SessionName=~"$SessionName"}, SessionName)'
    )
    # `SessionName` also scopes every metric panel, so it keeps its default.
    assert get_variable(dashboard, "SessionName")["current"]["value"] == ["$__all"]


def test_annotation_session_variable_applies_global_filters(monkeypatch):
    """The global filters scope the sessions `All` expands to, which is what keeps
    other clusters' events out of a shared log backend."""
    monkeypatch.setenv(GLOBAL_FILTERS_ENV_VAR, 'ray_io_cluster="my-cluster"')

    variable = get_variable(generate_train_dashboard(), ANNOTATION_SESSION_VARIABLE)
    assert (
        variable["query"]["query"]
        == 'label_values(ray_train_worker_group_start_total_time_s{SessionName=~"$SessionName",ray_io_cluster="my-cluster"}, SessionName)'
    )


def test_annotations_preserve_go_template_braces():
    """`label_format`/`line_format` stages carry Go template braces, which must
    survive generation untouched."""
    (report_annotation,) = [
        annotation
        for annotation in dashboard_annotations(generate_train_dashboard())
        if annotation["name"] == "ray.train.report call"
    ]

    assert "{{.metrics}}" in report_annotation["expr"]


@pytest.mark.parametrize("enabled", [None, "0"])
def test_annotations_are_off_by_default(monkeypatch, enabled):
    """Without the Loki pipeline Ray doesn't provision, every annotation query
    would fail on each dashboard load, so they're omitted unless enabled."""
    if enabled is None:
        monkeypatch.delenv(GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR)
    else:
        monkeypatch.setenv(GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR, enabled)

    dashboard = generate_train_dashboard()
    assert dashboard_annotations(dashboard) == []
    assert ANNOTATION_DATASOURCE_VARIABLE not in variable_names(dashboard)
    assert ANNOTATION_SESSION_VARIABLE not in variable_names(dashboard)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
