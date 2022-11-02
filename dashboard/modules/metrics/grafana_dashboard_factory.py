# flake8: noqa E501

import json
import os
from dataclasses import dataclass

from typing import List


@dataclass
class Target:
    expr: str
    legend: str


@dataclass
class Panel:
    title: str
    description: str
    id: int
    unit: str
    targets: List[Target]


METRICS_INPUT_ROOT = os.path.join(os.path.dirname(__file__), "export")
GRAFANA_CONFIG_INPUT_PATH = os.path.join(METRICS_INPUT_ROOT, "grafana")


GRAFANA_PANELS = [
    Panel(
        id=26,
        title="Scheduler Task State",
        description="Current number of tasks currently in a particular state.\n\nState: the task state, as described by rpc::TaskState proto in common.proto.",
        unit="tasks",
        targets=[
            Target(
                expr='sum(max_over_time(ray_tasks{State=~"FINISHED|FAILED"}[14d])) by (State) or clamp_min(sum(ray_tasks{State!~"FINISHED|FAILED"}) by (State), 0)',
                legend="{{State}}",
            )
        ],
    ),
    Panel(
        id=35,
        title="Active Tasks by Name",
        description="Current number of (live) tasks with a particular name.",
        unit="tasks",
        targets=[
            Target(
                expr='sum(ray_tasks{State!~"FINISHED|FAILED"}) by (Name)',
                legend="{{Name}}",
            )
        ],
    ),
    Panel(
        id=33,
        title="Scheduler Actor State",
    ),
    Panel(
        id=36,
        title="Active Actors by Name",
    ),
    Panel(
        id=27,
        title="Scheduler CPUs (logical slots)",
    ),
    Panel(
        id=29,
        title="Object Store Memory",
    ),
    Panel(
        id=28,
        title="Scheduler GPUs (logical slots)",
    ),
    Panel(
        id=2,
        title="Node CPU",
    ),
    Panel(
        id=8,
        title="Node GPU",
    ),
    Panel(
        id=6,
        title="Node Disk",
    ),
    Panel(
        id=32,
        title="Node Disk IO Speed",
    ),
    Panel(
        id=4,
        title="Node Memory",
    ),
    Panel(
        id=34,
        title="Node Memory by Component",
    ),
    Panel(
        id=18,
        title="Node GPU Memory",
    ),
    Panel(
        id=20,
        title="Node Network",
    ),
    Panel(
        id=24,
        title="Instance count",
    ),
]


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
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 1},
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
    "seriesOverrides": [],
    "spaceLength": 10,
    "stack": True,
    "steppedLine": False,
    "targets": [],
    "thresholds": [],
    "timeFrom": None,
    "timeRegions": [],
    "timeShift": None,
    "title": "Scheduler Task State",
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
            "format": "tasks",
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


def generate_grafana_dashboard():
    base_json = json.load(
        open(
            os.path.join(
                GRAFANA_CONFIG_INPUT_PATH, "dashboards", "grafana_dashboard_base.json"
            )
        )
    )
    base_json["panels"] = _generate_grafana_panels()
    return json.dumps(base_json, indent=4)


def _generate_grafana_panels():
    panels = []
    for panel in GRAFANA_PANELS:
        template = PANEL_TEMPLATE.copy()
        template.update(
            {
                "title": panel.title,
                "description": panel.description,
                "id": panel.id,
                "targets": _generate_targets(panel),
            }
        )
        template["yaxes"][0]["format"] = panel.unit
        panels.append(template)
    return panels


def _generate_targets(panel):
    targets = []
    for target in panel.targets:
        template = TARGET_TEMPLATE.copy()
        template.update(
            {
                "expr": target.expr,
                "legendFormat": target.legend,
            }
        )
        targets.append(template)
    return targets
