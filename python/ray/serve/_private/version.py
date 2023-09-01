from abc import ABC
from copy import deepcopy
import json
from typing import Any, Optional, Dict, List
from zlib import crc32

from ray.serve._private.config import DeploymentConfig
from ray.serve._private.utils import get_random_letters, DeploymentOptionUpdateType
from ray.serve.generated.serve_pb2 import DeploymentVersion as DeploymentVersionProto

import logging

logger = logging.getLogger("ray.serve")


class DeploymentVersion:
    def __init__(
        self,
        code_version: Optional[str],
        deployment_config: DeploymentConfig,
        ray_actor_options: Optional[Dict],
        placement_group_bundles: Optional[List[Dict[str, float]]] = None,
        placement_group_strategy: Optional[str] = None,
        max_replicas_per_node: Optional[int] = None,
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
        # should access this field directly.
        self.deployment_config = deployment_config
        self.ray_actor_options = ray_actor_options
        self.placement_group_bundles = placement_group_bundles
        self.placement_group_strategy = placement_group_strategy
        self.max_replicas_per_node = max_replicas_per_node
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
            or self.placement_group_options_hash
            != new_version.placement_group_options_hash
            or self.max_replicas_per_node != new_version.max_replicas_per_node
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
        # If these change, the controller will rolling upgrade existing replicas.
        serialized_ray_actor_options = _serialize(self.ray_actor_options or {})
        self.ray_actor_options_hash = crc32(serialized_ray_actor_options)
        combined_placement_group_options = {}
        if self.placement_group_bundles is not None:
            combined_placement_group_options["bundles"] = self.placement_group_bundles
        if self.placement_group_strategy is not None:
            combined_placement_group_options["strategy"] = self.placement_group_strategy
        serialized_placement_group_options = _serialize(
            combined_placement_group_options
        )
        self.placement_group_options_hash = crc32(serialized_placement_group_options)

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
            + serialized_placement_group_options
            + str(self.max_replicas_per_node).encode("utf-8")
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
            placement_group_bundles=json.dumps(self.placement_group_bundles)
            if self.placement_group_bundles is not None
            else "",
            placement_group_strategy=self.placement_group_strategy
            if self.placement_group_strategy is not None
            else "",
            max_replicas_per_node=self.max_replicas_per_node
            if self.max_replicas_per_node is not None
            else 0,
        )

    @classmethod
    def from_proto(cls, proto: DeploymentVersionProto):
        return DeploymentVersion(
            proto.code_version,
            DeploymentConfig.from_proto(proto.deployment_config),
            json.loads(proto.ray_actor_options),
            placement_group_bundles=(
                json.loads(proto.placement_group_bundles)
                if proto.placement_group_bundles
                else None
            ),
            placement_group_version=(
                proto.placement_group_version if proto.placement_group_version else None
            ),
            max_replicas_per_node=(
                proto.max_replicas_per_node if proto.max_replicas_per_node else None
            ),
        )

    def _get_serialized_options(
        self, update_types: List[DeploymentOptionUpdateType]
    ) -> bytes:
        """Returns a serialized dictionary containing fields of a deployment config that
        should prompt a deployment version update.
        """
        reconfigure_dict = {}
        # TODO(aguo): Once we only support pydantic 2, we can remove this if check.
        # In pydantic 2.0, `__fields__` has been renamed to `model_fields`.
        fields = (
            self.deployment_config.model_fields
            if hasattr(self.deployment_config, "model_fields")
            else self.deployment_config.__fields__
        )
        for option_name, field in fields.items():
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

    def update_state(self, state):
        pass
