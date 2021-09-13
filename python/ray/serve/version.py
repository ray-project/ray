from abc import ABC
from collections.abc import Iterable
from ray.serve.utils import get_random_letters
from typing import Any, Optional


class BackendVersion:
    def __init__(self,
                 code_version: Optional[str],
                 user_config: Optional[Any] = None):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(
                f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version

        self.user_config = user_config
        self.user_config_hash = self._hash_user_config(user_config)
        self._hash = hash((self.code_version, self.user_config_hash))

    def _hash_user_config(self, user_config: Any) -> int:
        """Hash the user config.

        We want users to be able to pass lists and dictionaries for
        convenience, but these are not hashable types because they're mutable.

        This supports lists and dictionaries by recursively converting them
        into immutable tuples and then hashing them.
        """
        try:
            return hash(user_config)
        except TypeError:
            pass

        if isinstance(user_config, dict):
            keys = tuple(sorted(user_config))
            val_hashes = tuple(
                self._hash_user_config(user_config[k]) for k in keys)
            return hash((hash(keys), hash(val_hashes)))
        elif isinstance(user_config, Iterable):
            return hash(
                tuple(self._hash_user_config(item) for item in user_config))
        else:
            raise TypeError(
                "user_config must contain only lists, dicts, or hashable "
                f"types. Got {type(user_config)}.")

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        return self._hash == other._hash


class VersionedReplica(ABC):
    @property
    def version(self) -> BackendVersion:
        pass
