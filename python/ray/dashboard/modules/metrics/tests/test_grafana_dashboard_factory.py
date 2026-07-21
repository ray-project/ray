import json
import re
import sys

import pytest

from ray.dashboard.modules.metrics.dashboards.train_dashboard_panels import (
    TRAIN_ANNOTATIONS,
)
from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
    generate_train_grafana_dashboard,
)


def test_train_dashboard_annotations():
    """The Train dashboard should expose the Loki event annotations defined in
    ``train_dashboard_panels.TRAIN_ANNOTATIONS`` (controller state changes,
    ray.train.report calls, and custom info/warning/error markers) in addition
    to the built-in Grafana annotations entry from the base JSON."""
    content, _ = generate_train_grafana_dashboard()
    dashboard = json.loads(content)

    annotations = dashboard["annotations"]["list"]

    # The base JSON contributes the built-in Grafana entry (no target); every
    # config-defined annotation is rendered with a target block.
    builtin = [a for a in annotations if "target" not in a]
    rendered = [a for a in annotations if "target" in a]
    assert len(builtin) == 1
    assert builtin[0]["name"] == "Annotations & Alerts"
    assert len(rendered) == len(TRAIN_ANNOTATIONS)

    by_name = {a["name"]: a for a in rendered}
    assert "Train Controller State Changes" in by_name
    assert "ray.train.report call" in by_name
    assert "Train Custom Annotations (Info)" in by_name
    assert "Train Custom Annotations (Warnings)" in by_name
    assert "Train Custom Annotations (Errors)" in by_name

    for ann in rendered:
        # Query fields are mirrored into the target block that Grafana requires.
        assert ann["expr"] == ann["target"]["expr"]
        assert ann["queryType"] == ann["target"]["queryType"]
        # Loki-backed annotations against the templated loki datasource.
        assert ann["datasource"] == {"type": "loki", "uid": "${loki_datasource}"}
        assert ann["target"]["datasource"] == ann["datasource"]
        # refId is unique per annotation.
        assert ann["target"]["refId"]

    # refIds are unique across the rendered annotations.
    ref_ids = [a["target"]["refId"] for a in rendered]
    assert len(ref_ids) == len(set(ref_ids))


def test_train_dashboard_panel_exprs_well_formed():
    """All query `expr` strings in panels and annotations should have balanced
    braces — a cheap proxy for syntactic validity that catches
    template-substitution mistakes — and no leftover `{global_filters}`."""
    content, _ = generate_train_grafana_dashboard()
    dashboard = json.loads(content)

    exprs = []
    for panel in dashboard.get("panels", []):
        for target in panel.get("targets", []):
            if "expr" in target:
                exprs.append(target["expr"])
    for ann in dashboard.get("annotations", {}).get("list", []):
        target = ann.get("target")
        if target and "expr" in target:
            exprs.append(target["expr"])

    metric_name_re = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
    for expr in exprs:
        assert expr.count("{") == expr.count("}"), expr
        assert metric_name_re.search(expr), expr
        assert "{global_filters}" not in expr, expr


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
