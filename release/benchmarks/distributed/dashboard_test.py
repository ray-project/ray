from pydantic import BaseModel, Field

import requests
import ray

class Result(BaseModel):
    id: str = Field(..., description="The id of the driver")
    node_ip_address: str = Field(
        ..., description="The ip address of the node the driver is running on"
    )
    pid: str = Field(
        ..., description="The pid of the worker process the driver is using."
    )
    # TODO(aguo): Add node_id as a field.




@ray.remote(num_cpus=0)
class DashboardTester:
    endpoints = [
        "/logical/actors",
        "/nodes",
        "/"
        "/api/cluster_status",
        "/events",
        "/api/jobs/",
        "/log_index",
        "/api/prometheus_health",
    ]

    def __init__(self, addr: ray._private.worker.RayContext):
        self.dashboard_url = addr.dashboard_url
        self.result = {}
    
    def run(self):
        pass

    def get_result(self):
        return result


class DashboardTestAtScale:
    """This is piggybacked into existing scalability tests."""
    def __init__(self, addr: ray._private.worker.RayContext):
        # Schedule the actor on the current node (which is a head node).
        current_node_ip = ray._private.worker.global_worker.node_ip_address
        self.tester = DashboardTester.options(
            resources={f"node:{current_node_ip}": 0.001}
        ).remote(addr)

    def get_result(self):
        """Get the result from the test.
        
        If there's anything going wrong with the actor, it 
        """
        try:
            return ray.get(self.tester.get_result.remote(), timeout=15)
        except ray.exceptions.GetTimeoutError:
            return {}
