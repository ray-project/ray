from abc import ABC
import json
from typing import Any, Optional
from zlib import crc32

from ray.serve._private.utils import get_random_letters
from ray.serve.generated.serve_pb2 import DeploymentVersion as DeploymentVersionProto


class DeploymentVersion:
    def __init__(self, code_version: Optional[str], user_config: Optional[Any] = None):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version

        self.user_config = user_config
        # TODO(simon): make this xlang compatible
        if isinstance(user_config, bytes):
            serialized_user_config = user_config
        else:
            serialized_user_config = str.encode(json.dumps(user_config, sort_keys=True))
        self.user_config_hash = crc32(serialized_user_config)
        self._hash = crc32(serialized_user_config + self.code_version.encode("utf-8"))

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DeploymentVersion):
            return False
        return self._hash == other._hash

    def to_proto(self) -> bytes:
        # TODO(simon): enable cross language user config
        return DeploymentVersionProto(code_version=self.code_version, user_config=b"")


class VersionedReplica(ABC):
    @property
    def version(self) -> DeploymentVersion:
        pass
