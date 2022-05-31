from abc import ABC, abstractstaticmethod

from ray.util.annotations import DeveloperAPI
from ray._private.runtime_env.context import RuntimeEnvContext
from typing import List
from ray.core.generated.common_pb2 import Language


@DeveloperAPI
class RuntimeEnvPlugin(ABC):
    NAME = None
    CONFLICTS = []

    @abstractstaticmethod
    def validate(plugin_spec):
        raise NotImplementedError()

    @abstractstaticmethod
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

    @abstractstaticmethod
    def delete(uris: List[str]) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri(str): a URI uniquely describing this resource.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0
