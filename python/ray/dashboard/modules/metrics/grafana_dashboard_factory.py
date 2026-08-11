import copy
import json
import math
import os
import re
from dataclasses import asdict, dataclass
from typing import List, Optional, Tuple

import ray
from ray._common.utils import env_bool
from ray.dashboard.modules.metrics.dashboards.common import (
    ANNOTATION_STREAM_SELECTOR_PLACEHOLDER,
    Annotation,
    DashboardConfig,
    Panel,
    PanelTemplate,
)
from ray.dashboard.modules.metrics.dashboards.data_dashboard_panels import (
    data_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.data_llm_dashboard_panels import (
    data_llm_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.default_dashboard_panels import (
    default_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_deployment_dashboard_panels import (
    serve_deployment_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_llm_dashboard_panels import (
    serve_llm_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.serve_llm_sglang_dashboard_panels import (
    serve_llm_sglang_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.train_dashboard_panels import (
    train_dashboard_config,
)
from ray.dashboard.modules.metrics.default_impl import get_serve_dashboard_config

GRAFANA_DASHBOARD_UID_OVERRIDE_ENV_VAR_TEMPLATE = "RAY_GRAFANA_{name}_DASHBOARD_UID"
GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE = (
    "RAY_GRAFANA_{name}_DASHBOARD_GLOBAL_FILTERS"
)
GRAFANA_DASHBOARD_LOG_LINK_URL_ENV_VAR_TEMPLATE = "RAY_GRAFANA_{name}_LOG_LINK_URL"

GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR = "RAY_GRAFANA_ANNOTATIONS_ENABLED"
GRAFANA_ANNOTATION_DATASOURCE_UID_ENV_VAR = "RAY_GRAFANA_ANNOTATION_DATASOURCE_UID"
GRAFANA_ANNOTATION_DATASOURCE_TYPE_ENV_VAR = "RAY_GRAFANA_ANNOTATION_DATASOURCE_TYPE"
GRAFANA_ANNOTATION_STREAM_SELECTOR_ENV_VAR = "RAY_GRAFANA_ANNOTATION_STREAM_SELECTOR"
DEFAULT_GRAFANA_ANNOTATION_DATASOURCE_TYPE = "loki"

# Names of the dashboard template variables that back the annotation queries when
# the corresponding env var is unset, so annotations can be pointed at a log
# backend from the Grafana UI without regenerating the dashboard.
ANNOTATION_DATASOURCE_VARIABLE = "annotation_datasource"
ANNOTATION_STREAM_SELECTOR_VARIABLE = "AnnotationStreamSelector"
# Default value of the stream selector variable. It only has to be broad enough
# to find the annotation lines: the queries narrow to a single session and run
# using the ``session_name``/``ray_train_run_id`` fields that Ray writes into
# every annotation record, not using the collector's stream labels.
DEFAULT_ANNOTATION_STREAM_SELECTOR = '{cluster_id=~".+"}'

# Grafana dashboard layout constants
# Dashboard uses a 24-column grid with 2-column panels
ROW_WIDTH = 24  # Full dashboard width
PANELS_PER_ROW = 2
PANEL_WIDTH = ROW_WIDTH // PANELS_PER_ROW  # Width of each panel
PANEL_HEIGHT = 8  # Height of each panel
ROW_HEIGHT = 1  # Height of row container


def _read_configs_for_dashboard(
    dashboard_config: DashboardConfig,
) -> Tuple[str, List[str], str]:
    """Reads environment variable configs for overriding uid, global_filters, and the log link URL for a given dashboard.

    Args:
        dashboard_config: The dashboard whose env-var overrides are read.
            ``dashboard_config.name`` selects the env-var suffix and
            ``default_uid`` is used as a fallback.

    Returns:
      Tuple with format uid, global_filters, log_link_url
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
    if global_filters_str == "":
        global_filters = []
    else:
        global_filters = global_filters_str.split(",")

    log_link_url = (
        os.environ.get(
            GRAFANA_DASHBOARD_LOG_LINK_URL_ENV_VAR_TEMPLATE.format(
                name=dashboard_config.name
            )
        )
        or ""
    )

    return uid, global_filters, log_link_url


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
    return _generate_grafana_dashboard(get_serve_dashboard_config())


def generate_serve_deployment_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the serve dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_deployment_dashboard_config)


def generate_serve_llm_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the serve dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_llm_dashboard_config)


def generate_serve_llm_sglang_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the SGLang Serve LLM dashboard and returns both the content
    and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_llm_sglang_dashboard_config)


def generate_data_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the data dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(data_dashboard_config)


def generate_data_llm_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the Data LLM dashboard and returns
    both the content and the uid.

    This dashboard provides vLLM metrics visibility for Ray Data LLM workloads,
    including latency (TTFT, TPOT), throughput, cache utilization, and
    prefix cache hit rate.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(data_llm_dashboard_config)


def generate_train_grafana_dashboard(
    session_name: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Generates the dashboard output for the train dashboard and returns
    both the content and the uid.

    Args:
        session_name: Session of the cluster generating the dashboard, used as
            the default of the ``SessionName`` variable. See
            :func:`_default_session_name_variable`.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(train_dashboard_config, session_name)


@dataclass
class AnnotationDatasourceConfig:
    """The log datasource that dashboard annotations are queried from.

    Both the datasource and the stream selector can either be pinned by the
    operator through env vars, or left to a dashboard template variable that is
    resolved in the Grafana UI. See :func:`read_annotation_datasource_config`.

    Attributes:
        uid: Grafana uid of the datasource to query, or a ``${var}`` reference to
            the datasource template variable.
        stream_selector: Backend-specific selector substituted for
            ``$__stream_selector`` in every annotation query, or a ``$var``
            reference to the stream selector template variable. It only has to
            be broad enough to contain the annotation lines; narrowing to a
            single session and run happens on the fields Ray emits.
        datasource_type: Grafana datasource type, e.g. ``"loki"``.
        pins_datasource: Whether ``uid`` is an operator-supplied literal rather
            than a reference to the template variable.
        pins_stream_selector: Whether ``stream_selector`` is an
            operator-supplied literal rather than a reference to the template
            variable.
    """

    uid: str
    stream_selector: str
    datasource_type: str = DEFAULT_GRAFANA_ANNOTATION_DATASOURCE_TYPE
    pins_datasource: bool = False
    pins_stream_selector: bool = False


def read_annotation_datasource_config() -> Optional[AnnotationDatasourceConfig]:
    """Resolve where dashboard annotations are queried from.

    Ray emits annotation events as JSON lines to a file in the session logs dir
    (see :class:`ray._common.observability.annotation.Annotation`) but ships no
    log collector, so rendering them requires a log collector that tails
    ``<session_dir>/logs/annotations_*.log`` and a Grafana datasource over
    whatever backend it ships to.

    Ray cannot know either of those, so each falls back to a dashboard template
    variable that the viewer resolves in the Grafana UI: a ``datasource``
    variable that Grafana auto-binds to the first datasource of the configured
    type, and a textbox holding the stream selector. That keeps annotations
    working out of the box wherever such a datasource exists, and retargetable
    without regenerating the dashboard or restarting the cluster.

    An operator that already knows both can pin them fleet-wide instead, via
    ``RAY_GRAFANA_ANNOTATION_DATASOURCE_UID`` and
    ``RAY_GRAFANA_ANNOTATION_STREAM_SELECTOR`` (plus
    ``RAY_GRAFANA_ANNOTATION_DATASOURCE_TYPE``, default ``"loki"``). Neither
    needs to identify the cluster, so both are static values.

    Returns:
        The resolved config, or ``None`` when annotations are switched off with
        ``RAY_GRAFANA_ANNOTATIONS_ENABLED=0``, in which case they are omitted
        from the generated dashboard JSON entirely.
    """
    if not env_bool(GRAFANA_ANNOTATIONS_ENABLED_ENV_VAR, True):
        return None

    # Strip surrounding whitespace from all three: these are commonly sourced
    # from a file or a k8s ConfigMap, and a trailing newline in the uid or type
    # would silently land in the dashboard JSON as an unresolvable datasource.
    uid = (os.environ.get(GRAFANA_ANNOTATION_DATASOURCE_UID_ENV_VAR) or "").strip()
    stream_selector = (
        os.environ.get(GRAFANA_ANNOTATION_STREAM_SELECTOR_ENV_VAR) or ""
    ).strip()
    datasource_type = (
        os.environ.get(GRAFANA_ANNOTATION_DATASOURCE_TYPE_ENV_VAR) or ""
    ).strip() or DEFAULT_GRAFANA_ANNOTATION_DATASOURCE_TYPE

    return AnnotationDatasourceConfig(
        uid=uid or f"${{{ANNOTATION_DATASOURCE_VARIABLE}}}",
        stream_selector=stream_selector or f"${ANNOTATION_STREAM_SELECTOR_VARIABLE}",
        datasource_type=datasource_type,
        pins_datasource=bool(uid),
        pins_stream_selector=bool(stream_selector),
    )


def generate_annotation_variables(config: AnnotationDatasourceConfig) -> List[dict]:
    """Build the template variables backing the annotation queries.

    Only the variables that the operator did not pin through an env var are
    emitted; a pinned value is substituted into the queries directly and needs
    no variable.

    Args:
        config: The resolved annotation datasource config.

    Returns:
        Template variables to add to the dashboard's ``templating.list``.
    """
    variables = []
    if not config.pins_datasource:
        variables.append(
            {
                "name": ANNOTATION_DATASOURCE_VARIABLE,
                "type": "datasource",
                "description": (
                    "Log datasource that dashboard annotations are queried from. "
                    "Ray does not provision this datasource."
                ),
                "datasource": None,
                "query": config.datasource_type,
                "refresh": 1,
                "hide": 0,
                "includeAll": False,
                "multi": False,
                "current": {"selected": False},
            }
        )
    if not config.pins_stream_selector:
        variables.append(
            {
                "name": ANNOTATION_STREAM_SELECTOR_VARIABLE,
                "type": "textbox",
                "description": (
                    "Stream selector that finds the Ray annotation log lines in the "
                    "annotation datasource. Adjust to match the labels your log "
                    "collector attaches. It does not need to identify the cluster or "
                    "the run: the queries narrow to those separately."
                ),
                "query": DEFAULT_ANNOTATION_STREAM_SELECTOR,
                "current": {
                    "selected": False,
                    "text": DEFAULT_ANNOTATION_STREAM_SELECTOR,
                    "value": DEFAULT_ANNOTATION_STREAM_SELECTOR,
                },
                "hide": 0,
            }
        )
    return variables


def generate_annotation(
    annotation: Annotation, config: AnnotationDatasourceConfig
) -> dict:
    """Expand an ``Annotation`` into the full Grafana annotation JSON.

    Args:
        annotation: The annotation to expand.
        config: The datasource to query and the stream selector to substitute for
            ``$__stream_selector`` in the annotation query.

    Returns:
        The annotation entry to append to the dashboard's ``annotations.list``.
    """
    datasource = {"type": config.datasource_type, "uid": config.uid}
    expr = annotation.expr.replace(
        ANNOTATION_STREAM_SELECTOR_PLACEHOLDER, config.stream_selector
    )

    return {
        "datasource": datasource,
        "enable": annotation.enable,
        "hide": annotation.hide,
        "iconColor": annotation.icon_color,
        "name": annotation.name,
        "expr": expr,
        "queryType": annotation.query_type,
        "tagKeys": annotation.tag_keys,
        "target": {
            "datasource": dict(datasource),
            "expr": expr,
            "queryType": annotation.query_type,
            "refId": annotation.ref_id,
        },
    }


def _default_session_name_variable(base_json: dict, session_name: str) -> None:
    """Default the ``SessionName`` variable to the session generating the dashboard.

    The variable ships as ``All``, whose ``allValue`` is ``.*``. For the Prometheus
    panels that is harmless, because a cluster's Prometheus only holds that
    cluster's metrics. For an annotation query it is not: the log backend may hold
    many clusters' annotations, and ``.*`` would render all of them onto this
    cluster's dashboard.

    A viewer can still switch sessions in the UI, and an embedded view passing
    ``var-SessionName`` in the URL still overrides this.

    Args:
        base_json: The dashboard JSON to mutate in place.
        session_name: Session of the cluster generating the dashboard.
    """
    for variable in base_json.get("templating", {}).get("list", []):
        if variable["name"] != "SessionName":
            continue
        variable["current"] = {
            "selected": True,
            "text": [session_name],
            "value": [session_name],
        }


def _generate_grafana_dashboard(
    dashboard_config: DashboardConfig, session_name: Optional[str] = None
) -> tuple[str, str]:
    """Render the Grafana dashboard JSON for the given config.

    Args:
        dashboard_config: Configuration describing the panels and base
            template JSON file to use for rendering.
        session_name: Session of the cluster generating the dashboard. Only
            needed for a dashboard carrying annotations; see
            :func:`_default_session_name_variable`.

    Returns:
      Tuple with format dashboard_content, uid
    """
    uid, global_filters, log_link_url = _read_configs_for_dashboard(dashboard_config)
    panels = _generate_grafana_panels(dashboard_config, global_filters, log_link_url)
    base_file_name = dashboard_config.base_json_file_name

    base_json = json.load(
        open(os.path.join(os.path.dirname(__file__), "dashboards", base_file_name))
    )
    base_json["panels"] = panels

    # Append config-defined annotations to the base JSON's built-in annotation list,
    # along with the template variables the annotation queries resolve through.
    annotation_config = read_annotation_datasource_config()
    if dashboard_config.annotations and annotation_config is not None:
        annotations = base_json.setdefault("annotations", {})
        annotations_list = annotations.setdefault("list", [])
        annotations_list.extend(
            generate_annotation(annotation, annotation_config)
            for annotation in dashboard_config.annotations
        )
        templating = base_json.setdefault("templating", {})
        templating.setdefault("list", []).extend(
            generate_annotation_variables(annotation_config)
        )
        if session_name:
            _default_session_name_variable(base_json, session_name)

    # Update variables to use global_filters
    global_filters_str = ",".join(global_filters)
    variables = base_json.get("templating", {}).get("list", [])
    for variable in variables:
        if "definition" not in variable:
            continue
        definition = variable["definition"].format(global_filters=global_filters_str)
        query = variable["query"]["query"].format(global_filters=global_filters_str)
        if not global_filters_str:
            definition = _clean_empty_filters(definition)
            query = _clean_empty_filters(query)
        variable["definition"] = definition
        variable["query"]["query"] = query

    tags = base_json.get("tags", []) or []
    tags.append(f"rayVersion:{ray.__version__}")
    base_json["tags"] = tags
    base_json["uid"] = uid
    # Ray metadata can be used to put arbitrary metadata
    ray_meta = base_json.get("rayMeta", []) or []
    ray_meta.append("supportsGlobalFilterOverride")
    base_json["rayMeta"] = ray_meta
    return json.dumps(base_json, indent=4), uid


def _generate_panel_template(
    panel: Panel,
    panel_global_filters: List[str],
    panel_index: int,
    base_y_position: int,
    log_link_url: str,
) -> dict:
    """
    Helper method to generate a panel template with common configuration.

    Args:
        panel: The panel configuration
        panel_global_filters: List of global filters to apply
        panel_index: The index of the panel within its row (0-based)
        base_y_position: The base y-coordinate for the row in the dashboard grid
        log_link_url: The URL to the log link for the panel

    Returns:
        dict: The configured panel template
    """
    # Create base template from panel configuration
    template = copy.deepcopy(panel.template.value)
    template.update(
        {
            "title": panel.title,
            "description": panel.description,
            "id": panel.id,
            "targets": _generate_targets(panel, panel_global_filters),
        }
    )

    # Set panel position and dimensions
    if panel.grid_pos:
        template["gridPos"] = asdict(panel.grid_pos)
    else:
        # Calculate panel position in 2-column grid layout
        # x: 0 or 12 (left or right column)
        # y: base position + (row number * panel height)
        row_number = panel_index // PANELS_PER_ROW
        template["gridPos"] = {
            "h": PANEL_HEIGHT,
            "w": PANEL_WIDTH,
            "x": PANEL_WIDTH * (panel_index % PANELS_PER_ROW),
            "y": base_y_position + (row_number * PANEL_HEIGHT),
        }

    # Set unit format for legacy graph-style panels (GRAPH, HEATMAP, STAT, GAUGE, PIE_CHART, BAR_CHART)
    if panel.template in (
        PanelTemplate.GRAPH,
        PanelTemplate.HEATMAP,
        PanelTemplate.STAT,
        PanelTemplate.GAUGE,
        PanelTemplate.PIE_CHART,
        PanelTemplate.BAR_CHART,
    ):
        template["yaxes"][0]["format"] = panel.unit

    # Set fieldConfig unit (for newer panel types with fieldConfig.defaults)
    if panel.template in (
        PanelTemplate.STAT,
        PanelTemplate.GAUGE,
        PanelTemplate.HEATMAP,
        PanelTemplate.PIE_CHART,
        PanelTemplate.BAR_CHART,
        PanelTemplate.TABLE,
        PanelTemplate.GRAPH,
    ):
        template["fieldConfig"]["defaults"]["unit"] = panel.unit

    # Set fill, stack, linewidth, nullPointMode (only for GRAPH panels)
    if panel.template == PanelTemplate.GRAPH:
        template["fill"] = panel.fill
        template["stack"] = panel.stack
        template["linewidth"] = panel.linewidth
        if panel.stack is True:
            template["nullPointMode"] = "connected"

    if panel.hideXAxis:
        template.setdefault("xaxis", {})["show"] = False

    # Handle optional panel customization fields

    # Thresholds (for panels with fieldConfig.defaults.thresholds)
    if panel.thresholds is not None:
        if panel.template in (PanelTemplate.STAT, PanelTemplate.GAUGE):
            template["fieldConfig"]["defaults"]["thresholds"][
                "steps"
            ] = panel.thresholds

    # Value mappings (for panels with fieldConfig.defaults.mappings)
    if panel.value_mappings is not None:
        if panel.template in (
            PanelTemplate.STAT,
            PanelTemplate.GAUGE,
            PanelTemplate.TABLE,
        ):
            template["fieldConfig"]["defaults"]["mappings"] = panel.value_mappings

    # Color mode (for STAT panels with options.colorMode)
    if panel.color_mode is not None:
        if panel.template == PanelTemplate.STAT:
            template["options"]["colorMode"] = panel.color_mode

    # Legend mode
    if panel.legend_mode is not None:
        if panel.template in (PanelTemplate.GRAPH, PanelTemplate.BAR_CHART):
            # For graph panels (legacy format with top-level legend object)
            template["legend"]["show"] = panel.legend_mode != "hidden"
            template["legend"]["alignAsTable"] = panel.legend_mode == "table"
        elif panel.template == PanelTemplate.PIE_CHART:
            # For PIE_CHART (options.legend.displayMode)
            template["options"]["legend"]["displayMode"] = panel.legend_mode

    # Min/max values (for panels with fieldConfig.defaults)
    if panel.min_val is not None or panel.max_val is not None:
        if panel.template in (
            PanelTemplate.STAT,
            PanelTemplate.GAUGE,
            PanelTemplate.HEATMAP,
            PanelTemplate.PIE_CHART,
            PanelTemplate.BAR_CHART,
            PanelTemplate.TABLE,
            PanelTemplate.GRAPH,
        ):
            if panel.min_val is not None:
                template["fieldConfig"]["defaults"]["min"] = panel.min_val
            if panel.max_val is not None:
                template["fieldConfig"]["defaults"]["max"] = panel.max_val

    # Reduce calculation (for panels with options.reduceOptions)
    if panel.reduce_calc is not None:
        if panel.template in (
            PanelTemplate.STAT,
            PanelTemplate.GAUGE,
            PanelTemplate.PIE_CHART,
        ):
            template["options"]["reduceOptions"]["calcs"] = [panel.reduce_calc]

    # Handle heatmap-specific options
    if panel.heatmap_color_scheme is not None:
        if panel.template == PanelTemplate.HEATMAP:
            template["options"]["color"]["scheme"] = panel.heatmap_color_scheme

    if panel.heatmap_color_reverse is not None:
        if panel.template == PanelTemplate.HEATMAP:
            template["options"]["color"]["reverse"] = panel.heatmap_color_reverse

    if panel.heatmap_yaxis_label is not None:
        if panel.template in (
            PanelTemplate.GRAPH,
            PanelTemplate.HEATMAP,
            PanelTemplate.STAT,
            PanelTemplate.GAUGE,
            PanelTemplate.PIE_CHART,
            PanelTemplate.BAR_CHART,
        ):
            template["yaxes"][0]["label"] = panel.heatmap_yaxis_label

    # Add log link if URL is provided via environment variable.
    if log_link_url:
        template["links"] = [
            {
                "targetBlank": True,
                "title": "View Logs",
                "url": log_link_url,
            }
        ]

    return template


def _create_row_panel(row: Panel, y_position: int) -> dict:
    """
    Creates a Grafana row panel that spans the full dashboard width.
    Row panels can be collapsed to hide their contained panels.

    Args:
        row: Row config with title, id, and collapse state
        y_position: Vertical position in dashboard grid

    Returns:
        Grafana row panel configuration
    """
    return {
        "collapsed": row.collapsed,
        "gridPos": {"h": ROW_HEIGHT, "w": ROW_WIDTH, "x": 0, "y": y_position},
        "id": row.id,
        "title": row.title,
        "type": "row",
        "panels": [],
    }


def _calculate_panel_heights(num_panels: int) -> int:
    """
    Calculate the total height needed for a set of panels.

    Args:
        num_panels: Number of panels to position

    Returns:
        Total height needed for the panels
    """
    rows_needed = math.ceil(num_panels / PANELS_PER_ROW)
    return rows_needed * PANEL_HEIGHT


def _generate_grafana_panels(
    config: DashboardConfig, global_filters: List[str], log_link_url: str
) -> List[dict]:
    """
    Generates Grafana panel configurations for a dashboard.

    The dashboard layout follows these rules:
    - Panels are arranged in 2 columns (12 units wide each)
    - Each panel is 8 units high
    - Rows are 1 unit high and can be collapsed
    - Panels within rows follow the same 2-column layout
    - Panel positions can be overridden via panel.grid_pos or auto-calculated

    Args:
        config: Dashboard configuration containing panels and rows
        global_filters: List of filters to apply to all panels
        log_link_url: Optional URL for panel log links. When set, each panel
            gets a "View Logs" link pointing to this URL.

    Returns:
        List of Grafana panel configurations for the dashboard
    """
    panels = []
    panel_global_filters = [*config.standard_global_filters, *global_filters]
    current_y_position = 0

    # Add top-level panels in 2-column grid
    for panel_index, panel in enumerate(config.panels):
        panel_template = _generate_panel_template(
            panel, panel_global_filters, panel_index, current_y_position, log_link_url
        )
        panels.append(panel_template)

    # Calculate space needed for top-level panels
    current_y_position += _calculate_panel_heights(len(config.panels))

    # Add rows and their panels
    if not config.rows:
        return panels

    for row in config.rows:
        # Create and add row panel
        row_panel = _create_row_panel(row, current_y_position)
        panels.append(row_panel)
        current_y_position += ROW_HEIGHT

        # Add panels within row using 2-column grid
        for panel_index, panel in enumerate(row.panels):
            panel_template = _generate_panel_template(
                panel,
                panel_global_filters,
                panel_index,
                current_y_position,
                log_link_url,
            )

            # Add panel to row if collapsed, otherwise to main dashboard
            if row.collapsed:
                row_panel["panels"].append(panel_template)
            else:
                panels.append(panel_template)

        # Update y position for next row based on actual panel positions
        # when explicit grid_pos is used, or fallback to calculated height.
        if any(p.grid_pos for p in row.panels):
            max_y_bottom = max(
                (p.grid_pos.y + p.grid_pos.h for p in row.panels if p.grid_pos),
                default=current_y_position,
            )
            current_y_position = max_y_bottom
        else:
            current_y_position += _calculate_panel_heights(len(row.panels))

    return panels


def _clean_empty_filters(expr: str) -> str:
    """Clean up malformed PromQL when global_filters is empty.

    Removes artifacts like trailing/leading commas in label matchers:
      ", ," → ","
      ", }" → "}"
      "{ ," → "{"
    """
    expr = re.sub(r",\s*,", ",", expr)
    expr = re.sub(r",\s*}", "}", expr)
    expr = re.sub(r"{\s*,", "{", expr)
    return expr


def gen_incrementing_alphabets(length):
    assert 65 + length < 96, "we only support up to 26 targets at a time."
    # 65: ascii code of 'A'.
    return list(map(chr, range(65, 65 + length)))


def _generate_targets(panel: Panel, panel_global_filters: List[str]) -> List[dict]:
    targets = []
    for target, ref_id in zip(
        panel.targets, gen_incrementing_alphabets(len(panel.targets))
    ):
        template = copy.deepcopy(target.template.value)
        global_filters_str = ",".join(panel_global_filters)
        expr = target.expr.format(global_filters=global_filters_str)
        if not global_filters_str:
            expr = _clean_empty_filters(expr)
        template.update(
            {
                "expr": expr,
                "legendFormat": target.legend,
                "refId": ref_id,
            }
        )
        targets.append(template)
    return targets
