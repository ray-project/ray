from abc import ABC
from copy import deepcopy
import json
from typing import Any, Optional, Dict, List
from zlib import crc32

from ray.serve._private.utils import get_random_letters, DeploymentOptionUpdateType
from ray.serve.generated.serve_pb2 import DeploymentVersion as DeploymentVersionProto
from ray.serve.config import DeploymentConfig

import logging

logger = logging.getLogger("ray.serve")


class DeploymentVersion:
    def __init__(
        self,
        code_version: Optional[str],
        deployment_config: DeploymentConfig,
        ray_actor_options: Optional[Dict],
    ):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version

        # Options for this field may be mutated over time, so any logic that uses this
        # should access this field directly
        self.deployment_config: DeploymentConfig = deployment_config
        self.ray_actor_options: Dict = ray_actor_options
        self.compute_hashes()

    @classmethod
    def from_deployment_version(cls, deployment_version, deployment_config):
        version_copy = deepcopy(deployment_version)
        version_copy.deployment_config = deployment_config
        version_copy.compute_hashes()
        return version_copy

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DeploymentVersion):
            return False
        return self._hash == other._hash

    def requires_actor_restart(self, new_version):
        """Determines whether the new version requires actors of the current version to
        be restarted.
        """
        return (
            self.code_version != new_version.code_version
            or self.ray_actor_options_hash != new_version.ray_actor_options_hash
        )

    def requires_actor_reconfigure(self, new_version):
        """Determines whether the new version requires calling reconfigure() on the
        replica actor.
        """
        return self.reconfigure_actor_hash != new_version.reconfigure_actor_hash

    def requires_long_poll_broadcast(self, new_version):
        """Determines whether lightweightly updating an existing replica to the new
        version requires broadcasting through long poll that the running replicas has
        changed.
        """
        return (
            self.deployment_config.max_concurrent_queries
            != new_version.deployment_config.max_concurrent_queries
        )

    def compute_hashes(self):
        # If this changes, the controller will directly restart all existing replicas.
        serialized_ray_actor_options = _serialize(self.ray_actor_options or {})
        self.ray_actor_options_hash = crc32(serialized_ray_actor_options)

        # If this changes, DeploymentReplica.reconfigure() will call reconfigure on the
        # actual replica actor
        self.reconfigure_actor_hash = crc32(
            self._get_serialized_options(
                [DeploymentOptionUpdateType.NeedsActorReconfigure]
            )
        )

        # Used by __eq__ in deployment state to either reconfigure the replicas or
        # stop and restart them
        self._hash = crc32(
            self.code_version.encode("utf-8")
            + serialized_ray_actor_options
            + self._get_serialized_options(
                [
                    DeploymentOptionUpdateType.NeedsReconfigure,
                    DeploymentOptionUpdateType.NeedsActorReconfigure,
                ]
            )
        )

    def to_proto(self) -> bytes:
        # TODO(simon): enable cross language user config
        return DeploymentVersionProto(
            code_version=self.code_version,
            deployment_config=self.deployment_config.to_proto(),
            ray_actor_options=json.dumps(self.ray_actor_options),
        )

    @classmethod
    def from_proto(cls, proto: DeploymentVersionProto):
        return DeploymentVersion(
            proto.code_version,
            DeploymentConfig.from_proto(proto.deployment_config),
            json.loads(proto.ray_actor_options),
        )

    def _get_serialized_options(
        self, update_types: List[DeploymentOptionUpdateType]
    ) -> bytes:
        """Returns a serialized dictionary containing fields of a deployment config that
        should prompt a deployment version update.
        """
        reconfigure_dict = {}
        for option_name, field in self.deployment_config.__fields__.items():
            option_weight = field.field_info.extra.get("update_type")
            if option_weight in update_types:
                reconfigure_dict[option_name] = getattr(
                    self.deployment_config, option_name
                )

        if (
            isinstance(self.deployment_config.user_config, bytes)
            and "user_config" in reconfigure_dict
        ):
            del reconfigure_dict["user_config"]
            return self.deployment_config.user_config + _serialize(reconfigure_dict)

        return _serialize(reconfigure_dict)


def _serialize(json_object):
    return str.encode(json.dumps(json_object, sort_keys=True))


class VersionedReplica(ABC):
    @property
    def version(self) -> DeploymentVersion:
        pass
