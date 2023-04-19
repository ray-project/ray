from abc import ABC
import json
from typing import Any, Optional, Union, Dict
from zlib import crc32

from ray.serve._private.utils import get_random_letters
from ray.serve.generated.serve_pb2 import DeploymentVersion as DeploymentVersionProto
from ray.serve.config import DeploymentConfig


class DeploymentVersion:
    def __init__(
        self,
        code_version: Optional[str],
        deployment_config: Union[DeploymentConfig, bytes],
        ray_actor_options: Optional[Union[Dict, bytes]],
    ):
        if code_version is not None and not isinstance(code_version, str):
            raise TypeError(f"code_version must be str, got {type(code_version)}.")
        if code_version is None:
            self.unversioned = True
            self.code_version = get_random_letters()
        else:
            self.unversioned = False
            self.code_version = code_version

        self.deployment_config: DeploymentConfig = deployment_config
        self.ray_actor_options: Dict = ray_actor_options
        self.compute_hashes()

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, DeploymentVersion):
            return False
        return self._hash == other._hash

    def compute_hashes(self):
        # If this changes, the controller will directly restart all existing replicas.
        serialized_ray_actor_options = _serialize(self.ray_actor_options or {})
        self.ray_actor_options_hash = crc32(serialized_ray_actor_options)

        # If this changes, DeploymentReplica.reconfigure() will call reconfigure on the
        # actual replica actor
        serialized_reconfigure_actor_options = (
            _get_serialized_reconfigure_actor_options(self.deployment_config)
        )
        self.reconfigure_actor_hash = crc32(serialized_reconfigure_actor_options)

        # Used by __eq__ in deployment state to either reconfigure the replicas or
        # stop and restart them
        self._hash = crc32(
            self.code_version.encode("utf-8")
            + _get_serialized_reconfigure_options(self.deployment_config)
            + serialized_ray_actor_options
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


def _get_serialized_reconfigure_options(deployment_config: DeploymentConfig) -> bytes:
    """Returns a serialized dictionary containing fields of a deployment config that
    should prompt a deployment version update.
    """
    if isinstance(deployment_config.user_config, bytes):
        return deployment_config.user_config + _serialize(
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
            }
        )
    else:
        return _serialize(
            {
                "user_config": deployment_config.user_config,
                "max_concurrent_queries": deployment_config.max_concurrent_queries,
                "graceful_shutdown_timeout_s": (
                    deployment_config.graceful_shutdown_timeout_s
                ),
                "graceful_shutdown_wait_loop_s": (
                    deployment_config.graceful_shutdown_wait_loop_s
                ),
                "health_check_period_s": deployment_config.health_check_period_s,
                "health_check_timeout_s": deployment_config.health_check_timeout_s,
            }
        )


def _get_serialized_reconfigure_actor_options(
    deployment_config: DeploymentConfig,
) -> bytes:
    return _serialize(
        {
            "user_config": deployment_config.user_config,
            "graceful_shutdown_wait_loop_s": (
                deployment_config.graceful_shutdown_wait_loop_s
            ),
        }
    )


def _serialize(json_object):
    return str.encode(json.dumps(json_object, sort_keys=True))


class VersionedReplica(ABC):
    @property
    def version(self) -> DeploymentVersion:
        pass
