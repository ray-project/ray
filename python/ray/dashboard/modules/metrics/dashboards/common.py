from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
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
    "instant": False,
    "range": True,
}

HISTOGRAM_BAR_CHART_TARGET_TEMPLATE = {
    "exemplar": True,
    "format": "heatmap",
    "instant": True,
    "range": False,
}

TABLE_TARGET_TEMPLATE = {
    "exemplar": True,
    "expr": "0",
    "format": "table",
    "instant": True,
    "range": False,
    "refId": "A",
}


@DeveloperAPI
class TargetTemplate(Enum):
    GRAPH = GRAPH_TARGET_TEMPLATE
    HEATMAP = HEATMAP_TARGET_TEMPLATE
    HISTOGRAM_BAR_CHART = HISTOGRAM_BAR_CHART_TARGET_TEMPLATE
    TABLE = TABLE_TARGET_TEMPLATE


@DeveloperAPI
@dataclass
class Target:
    """Defines a Grafana target (time-series query) within a panel.

    A panel will have one or more targets. By default, all targets are rendered as
    stacked area charts, with the following exceptions:
    - legend="MAX": rendered as a blue dotted line
    - legend="MAX + PENDING": rendered as a grey dotted line
    - legend matching "FINISHED|FAILED|DEAD|REMOVED": hidden by default

    Attributes:
        expr: The prometheus query to evaluate.
        legend: The legend string to format for each time-series.
        template: The target template to use (determines format and query type).
        instant: Whether this is an instant query (vs range query). Used for tables.
    """

    expr: str
    legend: str
    template: Optional[TargetTemplate] = TargetTemplate.GRAPH
    instant: bool = False


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
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "stat",
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
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "gauge",
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
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "piechart",
}

# Modern bar chart panel (timeseries with bars)
BAR_CHART_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {
        "defaults": {
            "unit": "short",
            "custom": {
                "drawStyle": "bars",
                "barAlignment": 0,
                "lineInterpolation": "linear",
                "fillOpacity": 100,
                "stacking": {"mode": "normal"},
                "lineWidth": 1,
                "pointSize": 5,
                "showPoints": "never",
                "spanNulls": False,
            },
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": "green", "value": None}],
            },
        },
        "overrides": [],
    },
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
    "id": 26,
    "options": {
        "legend": {
            "displayMode": "table",
            "placement": "bottom",
            "showLegend": False,
        },
        "tooltip": {"mode": "multi", "sort": "desc"},
    },
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "timeseries",
}

# Modern timeseries panel (replaces old "graph" type for time series data)
TIMESERIES_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {
        "defaults": {
            "unit": "short",
            "custom": {
                "drawStyle": "line",
                "lineInterpolation": "linear",
                "fillOpacity": 10,
                "stacking": {"mode": "none"},
                "lineWidth": 1,
                "pointSize": 5,
                "showPoints": "auto",
                "spanNulls": False,
            },
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": "green", "value": None}],
            },
        },
        "overrides": [
            # Render legend="MAX" as dotted blue line
            {
                "matcher": {"id": "byName", "options": "MAX"},
                "properties": [
                    {
                        "id": "custom.lineStyle",
                        "value": {"dash": [10, 10], "fill": "dash"},
                    },
                    {"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}},
                    {"id": "custom.fillOpacity", "value": 0},
                ],
            },
            # Render legend="MAX + PENDING" as dotted grey line
            {
                "matcher": {"id": "byName", "options": "MAX + PENDING"},
                "properties": [
                    {
                        "id": "custom.lineStyle",
                        "value": {"dash": [10, 10], "fill": "dash"},
                    },
                    {"id": "color", "value": {"mode": "fixed", "fixedColor": "grey"}},
                    {"id": "custom.fillOpacity", "value": 0},
                ],
            },
            # Hide series matching FINISHED|FAILED|DEAD|REMOVED
            {
                "matcher": {
                    "id": "byRegex",
                    "options": "/FINISHED|FAILED|DEAD|REMOVED|Failed Nodes:/",
                },
                "properties": [
                    {
                        "id": "custom.hideFrom",
                        "value": {"tooltip": False, "viz": True, "legend": False},
                    }
                ],
            },
        ],
    },
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
    "id": 1,
    "options": {
        "legend": {
            "displayMode": "list",
            "placement": "bottom",
            "showLegend": True,
        },
        "tooltip": {"mode": "multi", "sort": "desc"},
    },
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "timeseries",
}

# State timeline panel for showing state changes over time
STATE_TIMELINE_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {
        "defaults": {
            "mappings": [],
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": "green", "value": None}],
            },
            "custom": {"fillOpacity": 70},
        },
        "overrides": [],
    },
    "gridPos": {"h": 6, "w": 24, "x": 0, "y": 0},
    "id": 1,
    "options": {
        "showValue": "auto",
        "rowHeight": 0.8,
        "mergeValues": True,
        "legend": {
            "displayMode": "list",
            "placement": "bottom",
            "showLegend": True,
        },
        "tooltip": {"mode": "single"},
    },
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "state-timeline",
}

# Table panel for displaying data in tabular format
TABLE_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {
        "defaults": {},
        "overrides": [],
    },
    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
    "id": 1,
    "options": {
        "showHeader": True,
        "footer": {"show": False},
    },
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "transformations": [
        {"id": "organize", "options": {}},
    ],
    "type": "table",
}

# Histogram panel (bar chart showing value distribution)
HISTOGRAM_PANEL_TEMPLATE = {
    "datasource": r"${datasource}",
    "description": "<Description>",
    "fieldConfig": {
        "defaults": {
            "custom": {
                "fillOpacity": 80,
                "gradientMode": "none",
                "hideFrom": {"legend": False, "tooltip": False, "viz": False},
                "lineWidth": 1,
                "stacking": {"group": "A", "mode": "none"},
            },
            "mappings": [],
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": "green", "value": None}],
            },
            "unit": "short",
        },
        "overrides": [],
    },
    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
    "id": 1,
    "options": {
        "legend": {
            "displayMode": "list",
            "placement": "bottom",
            "showLegend": True,
        },
        "tooltip": {"mode": "single", "sort": "none"},
    },
    "pluginVersion": "10.0.0",
    "targets": [],
    "title": "<Title>",
    "type": "histogram",
}


@DeveloperAPI
class PanelTemplate(Enum):
    HEATMAP = HEATMAP_TEMPLATE
    PIE_CHART = PIE_CHART_TEMPLATE
    STAT = STAT_PANEL_TEMPLATE
    GAUGE = GAUGE_PANEL_TEMPLATE
    BAR_CHART = BAR_CHART_PANEL_TEMPLATE
    TIMESERIES = TIMESERIES_PANEL_TEMPLATE
    STATE_TIMELINE = STATE_TIMELINE_TEMPLATE
    TABLE = TABLE_PANEL_TEMPLATE
    HISTOGRAM = HISTOGRAM_PANEL_TEMPLATE


@DeveloperAPI
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
        linewidth: Width of the line in the graph.
        grid_pos: Position and size of the panel in the dashboard grid.
        template: The panel template to use (GRAPH, STAT, TIMESERIES, etc).
        hideXAxis: Whether to hide the X-axis.
        extra_json: Additional JSON fields to merge into the panel config.
                   Use this for panel-specific options not covered by other fields.
    """

    title: str
    description: str
    id: int
    unit: str
    targets: List[Target]
    fill: Optional[int] = None
    stack: bool = True
    linewidth: int = 1
    grid_pos: Optional[GridPos] = None
    template: Optional[PanelTemplate] = PanelTemplate.TIMESERIES
    hideXAxis: bool = False
    extra_json: dict = field(default_factory=dict)


@DeveloperAPI
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


@DeveloperAPI
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
