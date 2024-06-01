import logging
import sys
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


class AnyscaleImageURIPlugin(RuntimeEnvPlugin):
    """Starts worker in a container of a custom image."""

    name = "image_uri"

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.image_uri():
            return

        shim_command = [
            sys.executable,
            "-m",
            "ray.anyscale._private.runtime_env.ray_worker_shim",
            f"--ray-worker-image-uri={runtime_env['image_uri']}",
        ]
        context.py_executable = " ".join(shim_command)
