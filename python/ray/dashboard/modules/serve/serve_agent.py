import ray.dashboard.optional_utils as optional_utils
import ray.dashboard.utils as dashboard_utils
from ray.dashboard.modules.serve.serve_rest_api_impl import create_serve_rest_api

dashboard_agent_route_table = optional_utils.DashboardAgentRouteTable

ServeAgent = create_serve_rest_api(
    dashboard_module_superclass=dashboard_utils.DashboardAgentModule,
    dashboard_route_table=dashboard_agent_route_table,
    log_deprecation_warning=True,
)
