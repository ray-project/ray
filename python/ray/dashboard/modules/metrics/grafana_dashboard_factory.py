import copy
import json
import os
from dataclasses import asdict
from typing import List, Tuple

import ray
from ray.dashboard.modules.metrics.dashboards.common import DashboardConfig, Panel
from ray.dashboard.modules.metrics.dashboards.data_dashboard_panels import (
    data_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.default_dashboard_panels import (
    default_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_dashboard_panels import (
    serve_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_deployment_dashboard_panels import (
    serve_deployment_dashboard_config,
)

GRAFANA_DASHBOARD_UID_OVERRIDE_ENV_VAR_TEMPLATE = "RAY_GRAFANA_{name}_DASHBOARD_UID"
GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE = (
    "RAY_GRAFANA_{name}_DASHBOARD_GLOBAL_FILTERS"
)

TARGET_TEMPLATE = {
    "exemplar": True,
    "expr": "0",
    "interval": "",
    "legendFormat": "",
    "queryType": "randomWalk",
    "refId": "A",
}


PANEL_TEMPLATE = {
    "aliasColors": {},
    "bars": False,
    "dashLength": 10,
    "dashes": False,
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {"defaults": {}, "overrides": []},
    "fill": 10,
    "fillGradient": 0,
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
    "hiddenSeries": False,
    "id": 26,
    "legend": {
        "alignAsTable": True,
        "avg": False,
        "current": True,
        "hideEmpty": False,
        "hideZero": True,
        "max": False,
        "min": False,
        "rightSide": False,
        "show": True,
        "sort": "current",
        "sortDesc": True,
        "total": False,
        "values": True,
    },
    "lines": True,
    "linewidth": 1,
    "nullPointMode": "null",
    "options": {"alertThreshold": True},
    "percentage": False,
    "pluginVersion": "7.5.17",
    "pointradius": 2,
    "points": False,
    "renderer": "flot",
    "seriesOverrides": [
        {
            "$$hashKey": "object:2987",
            "alias": "MAX",
            "dashes": True,
            "color": "#1F60C4",
            "fill": 0,
            "stack": False,
        },
        {
            "$$hashKey": "object:78",
            "alias": "/FINISHED|FAILED|DEAD|REMOVED|Failed Nodes:/",
            "hiddenSeries": True,
        },
        {
            "$$hashKey": "object:2987",
            "alias": "MAX + PENDING",
            "dashes": True,
            "color": "#777777",
            "fill": 0,
            "stack": False,
        },
    ],
    "spaceLength": 10,
    "stack": True,
    "steppedLine": False,
    "targets": [],
    "thresholds": [],
    "timeFrom": None,
    "timeRegions": [],
    "timeShift": None,
    "title": "<Title>",
    "tooltip": {"shared": True, "sort": 0, "value_type": "individual"},
    "type": "graph",
    "xaxis": {
        "buckets": None,
        "mode": "time",
        "name": None,
        "show": True,
        "values": [],
    },
    "yaxes": [
        {
            "$$hashKey": "object:628",
            "format": "units",
            "label": "",
            "logBase": 1,
            "max": None,
            "min": "0",
            "show": True,
        },
        {
            "$$hashKey": "object:629",
            "format": "short",
            "label": None,
            "logBase": 1,
            "max": None,
            "min": None,
            "show": True,
        },
    ],
    "yaxis": {"align": False, "alignLevel": None},
}


def _read_configs_for_dashboard(
    dashboard_config: DashboardConfig,
) -> Tuple[str, List[str]]:
    """
    Reads environment variable configs for overriding uid or global_filters for a given
    dashboard.

    Returns:
      Tuple with format uid, global_filters
    """
    uid = (
        os.environ.get(
            GRAFANA_DASHBOARD_UID_OVERRIDE_ENV_VAR_TEMPLATE.format(
                name=dashboard_config.name
            )
        )
        or dashboard_config.default_uid
    )
    global_filters_str = (
        os.environ.get(
            GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE.format(
                name=dashboard_config.name
            )
        )
        or ""
    )
    global_filters = global_filters_str.split(",")

    return uid, global_filters


def generate_default_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the default dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(default_dashboard_config)


def generate_serve_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the serve dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_dashboard_config)


def generate_serve_deployment_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the serve dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_deployment_dashboard_config)


def generate_data_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the data dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(data_dashboard_config)


def _generate_grafana_dashboard(dashboard_config: DashboardConfig) -> str:
    """
    Returns:
      Tuple with format dashboard_content, uid
    """
    uid, global_filters = _read_configs_for_dashboard(dashboard_config)
    panels = _generate_grafana_panels(dashboard_config, global_filters)
    base_file_name = dashboard_config.base_json_file_name

    base_json = json.load(
        open(os.path.join(os.path.dirname(__file__), "dashboards", base_file_name))
    )
    base_json["panels"] = panels
    # Update variables to use global_filters
    global_filters_str = ",".join(global_filters)
    variables = base_json.get("templating", {}).get("list", [])
    for variable in variables:
        if "definition" not in variable:
            continue
        variable["definition"] = variable["definition"].format(
            global_filters=global_filters_str
        )
        variable["query"]["query"] = variable["query"]["query"].format(
            global_filters=global_filters_str
        )

    tags = base_json.get("tags", []) or []
    tags.append(f"rayVersion:{ray.__version__}")
    base_json["tags"] = tags
    base_json["uid"] = uid
    # Ray metadata can be used to put arbitrary metadata
    ray_meta = base_json.get("rayMeta", []) or []
    ray_meta.append("supportsGlobalFilterOverride")
    base_json["rayMeta"] = ray_meta
    return json.dumps(base_json, indent=4), uid


def _generate_grafana_panels(
    config: DashboardConfig, global_filters: List[str]
) -> List[dict]:
    out = []
    panel_global_filters = [*config.standard_global_filters, *global_filters]
    for i, panel in enumerate(config.panels):
        template = copy.deepcopy(PANEL_TEMPLATE)
        template.update(
            {
                "title": panel.title,
                "description": panel.description,
                "id": panel.id,
                "targets": _generate_targets(panel, panel_global_filters),
            }
        )
        if panel.grid_pos:
            template["gridPos"] = asdict(panel.grid_pos)
        else:
            template["gridPos"]["y"] = i // 2
            template["gridPos"]["x"] = 12 * (i % 2)
        template["yaxes"][0]["format"] = panel.unit
        template["fill"] = panel.fill
        template["stack"] = panel.stack
        template["linewidth"] = panel.linewidth
        out.append(template)
    return out


def gen_incrementing_alphabets(length):
    assert 65 + length < 96, "we only support up to 26 targets at a time."
    # 65: ascii code of 'A'.
    return list(map(chr, range(65, 65 + length)))


def _generate_targets(panel: Panel, panel_global_filters: List[str]) -> List[dict]:
    targets = []
    for target, ref_id in zip(
        panel.targets, gen_incrementing_alphabets(len(panel.targets))
    ):
        template = copy.deepcopy(TARGET_TEMPLATE)
        template.update(
            {
                "expr": target.expr.format(
                    global_filters=",".join(panel_global_filters)
                ),
                "legendFormat": target.legend,
                "refId": ref_id,
            }
        )
        targets.append(template)
    return targets
