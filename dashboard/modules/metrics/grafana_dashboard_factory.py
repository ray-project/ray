import copy
from dataclasses import asdict
import json
import os
from typing import List, Optional

import ray
from ray.dashboard.modules.metrics.dashboards.common import DashboardConfig, Panel
from ray.dashboard.modules.metrics.dashboards.default_dashboard_panels import (
    default_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_dashboard_panels import (
    serve_dashboard_config,
)


METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")
GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")


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
    "datasource": "Prometheus",
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
            "alias": "/FINISHED|FAILED|DEAD|REMOVED/",
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


def generate_default_grafana_dashboard(override_uid: Optional[str] = None) -> str:
    panels = _generate_grafana_panels(default_dashboard_config)
    return _generate_grafana_dashboard(
        "default_grafana_dashboard_base.json", panels, override_uid=override_uid
    )


def generate_serve_grafana_dashboard(override_uid: Optional[str] = None) -> str:
    import logging

    panels = _generate_grafana_panels(serve_dashboard_config)
    logger = logging.getLogger(__name__)
    logger.info(f"panels: {panels}")
    return _generate_grafana_dashboard(
        "serve_grafana_dashboard_base.json", panels, override_uid=override_uid
    )


def _generate_grafana_dashboard(
    base_file_name: str, panels: List[dict], override_uid: Optional[str] = None
) -> str:
    base_json = json.load(
        open(os.path.join(os.path.dirname(__file__), "dashboards", base_file_name))
    )
    base_json["panels"] = panels
    tags = base_json.get("tags", []) or []
    tags.append(f"rayVersion:{ray.__version__}")
    base_json["tags"] = tags
    if override_uid:
        base_json["uid"] = override_uid
    return json.dumps(base_json, indent=4)


def _generate_grafana_panels(config: DashboardConfig) -> List[dict]:
    out = []
    for i, panel in enumerate(config.panels):
        template = copy.deepcopy(PANEL_TEMPLATE)
        template.update(
            {
                "title": panel.title,
                "description": panel.description,
                "id": panel.id,
                "targets": _generate_targets(config, panel),
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


def _generate_targets(config: DashboardConfig, panel: Panel) -> List[dict]:
    global_filters = ",".join(config.global_filters)
    targets = []
    for target, ref_id in zip(
        panel.targets, gen_incrementing_alphabets(len(panel.targets))
    ):
        template = copy.deepcopy(TARGET_TEMPLATE)
        template.update(
            {
                "expr": target.expr.format(global_filters=global_filters),
                "legendFormat": target.legend,
                "refId": ref_id,
            }
        )
        targets.append(template)
    return targets
