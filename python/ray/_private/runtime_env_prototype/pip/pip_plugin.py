import logging

from typing import List

from ray._private.runtime_env_prototype.pluggability.plugin import RuntimeEnvPlugin
from ray._private.runtime_env_prototype.pluggability.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language

default_logger = logging.getLogger(__name__)


class PipPlugin(RuntimeEnvPlugin):

    NAME = "pip"
    CONFLICTS = ["conda"]

    @staticmethod
    def validate(plugin_spec):
        raise NotImplementedError()

    @staticmethod
    def create(
        plugin_spec,
        ctx: RuntimeEnvContext,
        worker_id: str,
        job_id: str,
        worker_language: Language,
    ) -> (List[str], float, bool, bool):
        """Create and install the runtime environment.

        Returns:
            uris

            the disk space taken up by this plugin installation for this
            environment. e.g. for working_dir, this downloads the files to the
            local node.

            Workerly flag

            jobly flag
        """
        return [], 0, False, False

    @staticmethod
    def delete(uris: List[str]) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri(str): a URI uniquely describing this resource.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0
