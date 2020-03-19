import aiohttp

from abc import ABC, abstractmethod

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
