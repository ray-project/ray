from asyncio import Future

class RayletClient:
    def __init__(self, ip_address: str, port: int):
        ...

    def async_get_worker_pids(self, timeout_ms: int = 1000) -> Future[list[int]]:
        """Get the PIDs of all workers registered with the raylet."""
        ...
