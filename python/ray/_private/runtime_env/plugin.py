from abc import ABC, abstractstaticmethod
import logging
import os
from typing import Optional, List, Union
from ray._private.runtime_env.uri_cache import URICache

from ray.util.annotations import DeveloperAPI
from ray._private.runtime_env.context import RuntimeEnvContext


# TODO(SongGuyang): This function exists in both C++ and Python.
# We should make this logic clearly.
def encode_plugin_uri(plugin: str, uri: str) -> str:
    return plugin + "|" + uri


default_logger = logging.getLogger(__name__)


@DeveloperAPI
class RuntimeEnvPlugin(ABC):
    """Abstract base class for runtime environment plugins."""

    name: str = ""

    @abstractstaticmethod
    def validate(runtime_env_dict: dict) -> str:
        """Validate user entry and returns a URI uniquely describing resource.

        This method will be called at ``f.options(runtime_env=...)`` or
        ``ray.init(runtime_env=...)`` time and it should check the runtime env
        dictionary for any errors. For example, it can raise "TypeError:
        expected string for "conda" field".

        Args:
            runtime_env_dict(dict): the entire dictionary passed in by user.

        Returns:
            uri(str): a URI uniquely describing this resource (e.g., a hash of
              the conda spec).
        """
        raise NotImplementedError()

    def get_uri(self, runtime_env: "RuntimeEnv") -> Optional[str]:  # noqa: F821
        return None

    def create(
        self, uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
        """Create and install the runtime environment.

        Gets called in the runtime env agent at install time. The URI can be
        used as a caching mechanism.

        Args:
            uri(str): a URI uniquely describing this resource.
            runtime_env(RuntimeEnv): the runtime env protobuf.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the disk space taken up by this plugin installation for this
            environment. e.g. for working_dir, this downloads the files to the
            local node.
        """
        return 0

    def modify_context(
        self,
        uris: Optional[Union[str, List[str]]],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        """Modify context to change worker startup behavior.

        For example, you can use this to preprend "cd <dir>" command to worker
        startup, or add new environment variables.

        Args:
            uris(Union(str, List[str]): a URI or list of URIs used by this plugin.
            runtime_env(RuntimeEnv): the runtime env protobuf.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.
        """
        return

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri(str): a URI describing the resource to be deleted.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0


@DeveloperAPI
class PluginCacheManager:
    """A manager for plugins.

    This class is used to manage plugins along with a cache for plugin URIs.
    """

    def __init__(self, plugin: RuntimeEnvPlugin):
        self._plugin = plugin

        # Set the max size for the cache.  Defaults to 10 GB.
        cache_size_env_var = f"RAY_RUNTIME_ENV_{plugin.name}_CACHE_SIZE_GB".upper()
        cache_size_bytes = int(
            (1024 ** 3) * float(os.environ.get(cache_size_env_var, 10))
        )
        self._uri_cache = URICache(plugin.delete_uri, cache_size_bytes)

    async def create_if_needed(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):

        uris = self._plugin.get_uri(
            runtime_env
        )  # XXX double check uri vs uris everywhere
        if uris is None:
            return

        for uri in uris if isinstance(uris, list) else [uris]:
            if uri not in self.uri_cache:
                logger.debug(f"Cache miss for URI {uri}.")
                size_bytes = await self.plugin.create(
                    uri, runtime_env, context, logger=logger
                )
                self.uri_cache.add(uri, size_bytes, logger=logger)
            else:
                logger.debug(f"Cache hit for URI {uri}.")
                self.uri_cache.mark_used(uri, logger=logger)

        self.plugin.modify_context(uris, runtime_env, context)
