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
from ray.dashboard.modules.metrics.dashboards.serve_llm_dashboard_panels import (
    serve_llm_dashboard_config,
)
from ray.dashboard.modules.metrics.dashboards.train_dashboard_panels import (
    train_dashboard_config,
)

GRAFANA_DASHBOARD_UID_OVERRIDE_ENV_VAR_TEMPLATE = "RAY_GRAFANA_{name}_DASHBOARD_UID"
GRAFANA_DASHBOARD_GLOBAL_FILTERS_OVERRIDE_ENV_VAR_TEMPLATE = (
    "RAY_GRAFANA_{name}_DASHBOARD_GLOBAL_FILTERS"
)


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


def generate_serve_llm_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the serve dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(serve_llm_dashboard_config)


def generate_data_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the data dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(data_dashboard_config)


def generate_train_grafana_dashboard() -> Tuple[str, str]:
    """
    Generates the dashboard output for the train dashboard and returns
    both the content and the uid.

    Returns:
      Tuple with format content, uid
    """
    return _generate_grafana_dashboard(train_dashboard_config)


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
        template = copy.deepcopy(panel.template.value)
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
        if panel.stack is True:
            # If connected is not True, any nulls will cause the stacking visualization to break
            # making the total appear much smaller than it actually is.
            template["nullPointMode"] = "connected"
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
        template = copy.deepcopy(target.template.value)
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
