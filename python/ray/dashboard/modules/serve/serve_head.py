from ray.dashboard.subprocesses.routes import SubprocessRouteTable
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.modules.serve.serve_rest_api_impl import create_serve_rest_api

ServeHead = create_serve_rest_api(
    dashboard_module_superclass=SubprocessModule,
    dashboard_route_table=SubprocessRouteTable,
)
