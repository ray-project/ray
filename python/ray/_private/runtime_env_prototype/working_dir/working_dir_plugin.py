import logging

from typing import List

from ray._private.runtime_env_prototype.pluggability.plugin import RuntimeEnvPlugin
from ray._private.runtime_env_prototype.pluggability.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language
import hashlib

default_logger = logging.getLogger(__name__)


class WorkingDirPlugin(RuntimeEnvPlugin):

    NAME = "working_dir"

    @staticmethod
    def validate(spec) -> List[str]:
        assert isinstance(spec, str)
        hash = hashlib.sha1(spec.encode("utf-8")).hexdigest()
        return [hash]

    @staticmethod
    def create(
        uris: List[str],
        spec,
        ctx: RuntimeEnvContext,
        job_id: str,
        worker_id: str,
        worker_language: Language,
    ) -> (float, bool, bool):

        return 0, False, False

    @staticmethod
    def delete(uris: List[str]) -> float:
        return 0
