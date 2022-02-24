from abc import ABC
import pickle
from typing import Any, Optional
from zlib import crc32

from ray.serve.utils import get_random_letters


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
        pickled_user_config = pickle.dumps(user_config)
        self.user_config_hash = crc32(pickled_user_config)
        self._hash = crc32(pickled_user_config + self.code_version.encode("utf-8"))

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DeploymentVersion):
            return False
        return self._hash == other._hash


class VersionedReplica(ABC):
    @property
    def version(self) -> DeploymentVersion:
        pass
