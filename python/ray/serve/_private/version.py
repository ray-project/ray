from abc import ABC
import json
from typing import Any, Optional, Dict
from zlib import crc32

from ray.serve._private.utils import get_random_letters
from ray.serve.generated.serve_pb2 import DeploymentVersion as DeploymentVersionProto
from ray.serve.config import DeploymentConfig, ReplicaConfig


class DeploymentVersion:
    def __init__(
        self,
        code_version: Optional[str],
        deployment_config: Optional[Any] = None,
        replica_config: Optional[Any] = None,
    ):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version

        # TODO(simon): make this xlang compatible
        if isinstance(deployment_config, bytes):
            deployment_config = DeploymentConfig.from_proto(deployment_config)
        if isinstance(replica_config, bytes):
            replica_config = ReplicaConfig.from_proto(replica_config)

        self.deployment_config: DeploymentConfig = deployment_config
        self.replica_config: ReplicaConfig = replica_config
        self.user_config = self.deployment_config.user_config

        self.user_config_hash = crc32(json.dumps(self.user_config, sort_keys=True))
        self._hash = crc32(
            get_serialized_deployment_config(self.deployment_config)
            + get_serialized_replica_config(self.replica_config)
            + self.code_version.encode("utf-8")
        )

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DeploymentVersion):
            return False
        return self._hash == other._hash

    def to_proto(self) -> bytes:
        # TODO(simon): enable cross language user config
        return DeploymentVersionProto(
            code_version=self.code_version,
            deployment_config=self.deployment_config.to_proto_bytes(),
            replica_config=self.replica_config.to_proto_bytes(),
        )


def get_serialized_deployment_config(deployment_config: DeploymentConfig) -> Dict:
    """Returns a serialized dictionary containing fields of a deployment config that
    should prompt a deployment version update.
    """
    return str.encode(
        json.dumps(
            {
                "max_concurrent_queries": deployment_config.max_concurrent_queries,
                "graceful_shutdown_timeout_s": (
                    deployment_config.graceful_shutdown_timeout_s
                ),
                "graceful_shutdown_wait_loop_s": (
                    deployment_config.graceful_shutdown_wait_loop_s
                ),
                "health_check_period_s": deployment_config.health_check_period_s,
                "health_check_timeout_s": deployment_config.health_check_timeout_s,
            },
            sort_keys=True,
        )
    )


def get_serialized_replica_config(replica_config: ReplicaConfig) -> Dict:
    """Returns a serialized dictionary containing fields of a replica config that
    should prompt a deployment version update.
    """
    return str.encode(
        json.dumps(
            {"ray_actor_options": replica_config.ray_actor_options}, sort_keys=True
        )
    )


class VersionedReplica(ABC):
    @property
    def version(self) -> DeploymentVersion:
        pass
