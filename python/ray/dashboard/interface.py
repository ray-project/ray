import aiohttp

from abc import ABC, abstractmethod


class BaseDashboardController(ABC):
    """Set of APIs to interact with a Dashboard class and routes.

    Make sure you run start_collecting_metrics function before using
    get_[stats]_info methods.
    """

    @abstractmethod
    def get_ray_config(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_node_info(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_raylet_info(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def tune_info(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def tune_availability(self):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def launch_profiling(self, node_id, pid, duration):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def check_profiling_status(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_profiling_info(self, profiling_id):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def kill_actor(self, actor_id, ip_address, port):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_logs(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def get_errors(self, hostname, pid):
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    def start_collecting_metrics(self):
        """Start threads/processes/actors to collect metrics

        NOTE: This interface should be called only once before using
            other api calls.
        """
        raise NotImplementedError("Please implement this method.")


class BaseDashboardRouteHandler(ABC):
    """Collection of routes that should be implemented for dashboard."""

    @abstractmethod
    def get_forbidden(self, _) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def get_index(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def ray_config(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def node_info(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def raylet_info(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def tune_info(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def tune_availability(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def check_profiling_status(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def get_profiling_info(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def kill_actor(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def logs(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")

    @abstractmethod
    async def errors(self, req) -> aiohttp.web.Response:
        raise NotImplementedError("Please implement this method.")
