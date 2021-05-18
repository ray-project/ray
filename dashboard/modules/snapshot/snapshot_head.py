import ray.new_dashboard.utils as dashboard_utils

routes = dashboard_utils.ClassMethodRouteTable

class LogicalViewHead(dashboard_utils.DashboardHeadModule):
    @routes.get("/api/snapshot")
    async def snapshot(self, req):
        return rest_response(success=True, message="hello")
