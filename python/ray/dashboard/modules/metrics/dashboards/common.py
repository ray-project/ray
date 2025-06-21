from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


@dataclass
class GridPos:
    x: int
    y: int
    w: int
    h: int


GRAPH_TARGET_TEMPLATE = {
    "exemplar": True,
    "expr": "0",
    "interval": "",
    "legendFormat": "",
    "queryType": "randomWalk",
    "refId": "A",
}

HEATMAP_TARGET_TEMPLATE = {
    "format": "heatmap",
    "fullMetaSearch": False,
    "includeNullMetadata": True,
    "instant": False,
    "range": True,
    "useBackend": False,
}


class TargetTemplate(Enum):
    GRAPH = GRAPH_TARGET_TEMPLATE
    HEATMAP = HEATMAP_TARGET_TEMPLATE


@dataclass
class Target:
    """Defines a Grafana target (time-series query) within a panel.

    A panel will have one or more targets. By default, all targets are rendered as
    stacked area charts, with the exception of legend="MAX", which is rendered as
    a blue dotted line. Any legend="FINISHED|FAILED|DEAD|REMOVED" series will also be
    rendered hidden by default.

    Attributes:
        expr: The prometheus query to evaluate.
        legend: The legend string to format for each time-series.
    """

    expr: str
    legend: str
    template: Optional[TargetTemplate] = TargetTemplate.GRAPH


HEATMAP_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {"defaults": {}, "overrides": []},
    "id": 12,
    "options": {
        "calculate": False,
        "cellGap": 1,
        "cellValues": {"unit": "none"},
        "color": {
            "exponent": 0.5,
            "fill": "dark-orange",
            "min": 0,
            "mode": "scheme",
            "reverse": False,
            "scale": "exponential",
            "scheme": "Spectral",
            "steps": 64,
        },
        "exemplars": {"color": "rgba(255,0,255,0.7)"},
        "filterValues": {"le": 1e-9},
        "legend": {"show": True},
        "rowsFrame": {"layout": "auto", "value": "Request count"},
        "tooltip": {"mode": "single", "showColorScale": False, "yHistogram": True},
        "yAxis": {"axisPlacement": "left", "reverse": False, "unit": "none"},
    },
    "pluginVersion": "11.2.0",
    "targets": [],
    "title": "<Title>",
    "type": "heatmap",
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
}

GRAPH_PANEL_TEMPLATE = {
    "aliasColors": {},
    "bars": False,
    "dashLength": 10,
    "dashes": False,
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {"defaults": {}, "overrides": []},
    # Setting height and width is important here to ensure the default panel has some size to it.
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
    "fill": 10,
    "fillGradient": 0,
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
    "nullPointMode": None,
    "options": {"alertThreshold": True},
    "percentage": False,
    "pluginVersion": "7.5.17",
    "pointradius": 2,
    "points": False,
    "renderer": "flot",
    # These series overrides are necessary to make the "MAX" and "MAX + PENDING" dotted lines
    # instead of stacked filled areas.
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

STAT_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "fieldConfig": {
        "defaults": {
            "color": {"mode": "thresholds"},
            "mappings": [],
            "min": 0,
            "thresholds": {
                "mode": "percentage",
                "steps": [
                    {"color": "super-light-yellow", "value": None},
                    {"color": "super-light-green", "value": 50},
                    {"color": "green", "value": 100},
                ],
            },
            "unit": "short",
        },
        "overrides": [],
    },
    "id": 78,
    "options": {
        "colorMode": "value",
        "graphMode": "area",
        "justifyMode": "auto",
        "orientation": "auto",
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "text": {},
        "textMode": "auto",
    },
    "pluginVersion": "7.5.17",
    "targets": [],
    "timeFrom": None,
    "timeShift": None,
    "title": "<Title>",
    "type": "stat",
    "yaxes": [
        {
            "$$hashKey": "object:628",
            "format": "Tokens",
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
}


GAUGE_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "fieldConfig": {
        "defaults": {
            "color": {"mode": "continuous-YlBl"},
            "mappings": [],
            "thresholds": {
                "mode": "percentage",
                "steps": [{"color": "rgb(230, 230, 230)", "value": None}],
            },
            "unit": "short",
        },
        "overrides": [],
    },
    "id": 10,
    "options": {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "showThresholdLabels": False,
        "showThresholdMarkers": False,
        "text": {"titleSize": 12},
    },
    "pluginVersion": "7.5.17",
    "targets": [],
    "title": "<Title>",
    "type": "gauge",
    "yaxes": [
        {
            "$$hashKey": "object:628",
            "format": "Tokens",
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
}

PIE_CHART_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {"defaults": {}, "overrides": []},
    "id": 26,
    "options": {
        "displayLabels": [],
        "legend": {
            "displayMode": "table",
            "placement": "right",
            "values": ["percent", "value"],
        },
        "pieType": "pie",
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "text": {},
    },
    "pluginVersion": "7.5.17",
    "targets": [],
    "timeFrom": None,
    "timeShift": None,
    "title": "<Title>",
    "type": "piechart",
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
}


class PanelTemplate(Enum):
    GRAPH = GRAPH_PANEL_TEMPLATE
    HEATMAP = HEATMAP_TEMPLATE
    PIE_CHART = PIE_CHART_TEMPLATE
    STAT = STAT_PANEL_TEMPLATE
    GAUGE = GAUGE_PANEL_TEMPLATE


@dataclass
class Panel:
    """Defines a Grafana panel (graph) for the Ray dashboard page.

    A panel contains one or more targets (time-series queries).

    Attributes:
        title: Short name of the graph. Note: please keep this in sync with the title
            definitions in Metrics.tsx.
        description: Long form description of the graph.
        id: Integer id used to reference the graph from Metrics.tsx.
        unit: The unit to display on the y-axis of the graph.
        targets: List of query targets.
        fill: Whether or not the graph will be filled by a color.
        stack: Whether or not the lines in the graph will be stacked.
    """

    title: str
    description: str
    id: int
    unit: str
    targets: List[Target]
    fill: int = 10
    stack: bool = True
    linewidth: int = 1
    grid_pos: Optional[GridPos] = None
    template: Optional[PanelTemplate] = PanelTemplate.GRAPH


@dataclass
class Row:
    """Defines a Grafana row that can contain multiple panels.

    Attributes:
        title: The title of the row
        panels: List of panels contained in this row
        collapsed: Whether the row should be collapsed by default
    """

    title: str
    id: int
    panels: List[Panel]
    collapsed: bool = False


@dataclass
class DashboardConfig:
    # This dashboard name is an internal key used to determine which env vars
    # to check for customization
    name: str
    # The uid of the dashboard json if not overridden by a user
    default_uid: str
    # The global filters applied to all graphs in this dashboard. Users can
    # add additional global_filters on top of this.
    standard_global_filters: List[str]
    base_json_file_name: str
    # Panels can be specified in `panels`, or nested within `rows`.
    # If both are specified, panels will be rendered before rows.
    panels: List[Panel] = field(default_factory=list)
    rows: List[Row] = field(default_factory=list)

    def __post_init__(self):
        if not self.panels and not self.rows:
            raise ValueError("At least one of panels or rows must be specified")
