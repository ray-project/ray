from ray.dashboard.modules.metrics.dashboards.common import DashboardConfig


def get_serve_dashboard_config() -> DashboardConfig:
    from ray.dashboard.modules.metrics.dashboards.serve_dashboard_panels import (
        serve_dashboard_config,
    )

    return serve_dashboard_config


# Anyscale overrides
