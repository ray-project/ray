import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.serve.serve_rest_api_impl import create_serve_rest_api


dashboard_head_route_table = optional_utils.DashboardHeadRouteTable

ServeHead = create_serve_rest_api(
    dashboard_module_superclass=dashboard_utils.DashboardHeadModule,
    dashboard_route_table=dashboard_head_route_table,
)
