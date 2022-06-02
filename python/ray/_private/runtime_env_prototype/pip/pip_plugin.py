import logging

from typing import List

from ray._private.runtime_env_prototype.pluggability.plugin import RuntimeEnvPlugin
from ray._private.runtime_env_prototype.pluggability.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import Language
import json
import hashlib

default_logger = logging.getLogger(__name__)


class PipPlugin(RuntimeEnvPlugin):

    NAME = "pip"

    @staticmethod
    def validate(spec) -> List[str]:
        keys = ["packages", "pip_check"]
        assert isinstance(spec, dict)
        for k in spec.keys():
            assert k in keys
        serialized_pip_spec = json.dumps(spec, sort_keys=True)
        hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
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
