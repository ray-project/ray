import logging
import sys
from typing import List, Optional

from aiohttp import ClientSession, ClientTimeout, UnixConnector

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray.anyscale._private.constants import ANYSCALE_DATAPLANE_SERVICE_SOCKET

default_logger = logging.getLogger(__name__)


class AnyscaleImageURIPlugin(RuntimeEnvPlugin):
    """Starts worker in a container of a custom image."""

    name = "image_uri"

    @staticmethod
    def get_compatible_keys():
        return {"image_uri", "config", "env_vars", "working_dir"}

    def __init__(self, ray_tmp_dir: str):
        pass

    async def create(
        self,
        uri: Optional[str],
        runtime_env,
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> int:
        image_uri = runtime_env.image_uri()
        if not image_uri:
            return 0

        logger.info(f"Pulling image {image_uri}.")
        # Don't set timeout, and rely on RuntimeEnvAgent to cancel
        # when the runtime environment `setup_timeout_seconds` is
        # reached
        conn = UnixConnector(path=ANYSCALE_DATAPLANE_SERVICE_SOCKET)
        async with ClientSession(
            connector=conn, timeout=ClientTimeout(total=None)
        ) as session:
            async with session.post(
                "http://unix/pull_image",
                json={
                    "image_uri": image_uri,
                    "check_ray_and_python_version": True,
                },
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(await resp.text())

        logger.info(f"Pulled image {image_uri}.")
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.image_uri():
            return

        env_var_names = " ".join(runtime_env.env_vars().keys())
        shim_command = [
            sys.executable,
            "-m",
            "ray.anyscale._private.runtime_env.ray_worker_shim",
            f"--ray-worker-image-uri={runtime_env['image_uri']}",
        ]
        if env_var_names:
            shim_command.insert(3, f"--env-var-names {env_var_names}")

        context.py_executable = " ".join(shim_command)
