import logging
from unittest.mock import patch

import pytest

import ray
from ray import serve
from ray.serve._private import deploy_utils
from ray.serve._private.config import ReplicaConfig


class MultiplexedModel:
    @serve.multiplexed(max_num_models_per_replica=2)
    async def get_model(self, model_id: str):
        return model_id

    async def __call__(self):
        pass


class MultiplexedIngress(MultiplexedModel):
    """Ingress-shaped class using multiplexing (invalid with direct ingress)."""

    pass


@pytest.fixture
def patch_ray_job_id():
    with patch.object(ray, "get_runtime_context") as m:
        m.return_value.get_job_id.return_value = "01000000"
        yield


def test_multiplex_warns_without_haproxy_or_strict(caplog, patch_ray_job_id):
    replica_config = ReplicaConfig.create(MultiplexedModel)
    with patch.object(deploy_utils, "RAY_SERVE_ENABLE_HA_PROXY", False):
        with patch.object(
            deploy_utils,
            "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING",
            False,
        ):
            with caplog.at_level(logging.WARNING, logger="ray.serve"):
                deploy_utils.get_deploy_args("d", replica_config, ingress=False)
    assert any(
        "future Ray Serve release" in r.message for r in caplog.records
    ), caplog.text


def test_multiplex_raises_with_strict(patch_ray_job_id):
    replica_config = ReplicaConfig.create(MultiplexedModel)
    with patch.object(
        deploy_utils,
        "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING",
        True,
    ):
        with patch.object(deploy_utils, "RAY_SERVE_ENABLE_HA_PROXY", False):
            with pytest.raises(
                ValueError, match="RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING"
            ):
                deploy_utils.get_deploy_args("d", replica_config, ingress=False)


def test_multiplex_raises_with_haproxy(patch_ray_job_id):
    replica_config = ReplicaConfig.create(MultiplexedModel)
    with patch.object(
        deploy_utils,
        "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING",
        False,
    ):
        with patch.object(deploy_utils, "RAY_SERVE_ENABLE_HA_PROXY", True):
            with pytest.raises(ValueError, match="RAY_SERVE_ENABLE_HA_PROXY"):
                deploy_utils.get_deploy_args("d", replica_config, ingress=False)


def test_multiplex_ingress_direct_ingress_error_preempts_soft_path(patch_ray_job_id):
    replica_config = ReplicaConfig.create(MultiplexedIngress)
    with patch.object(deploy_utils, "RAY_SERVE_ENABLE_DIRECT_INGRESS", True):
        with patch.object(
            deploy_utils,
            "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING",
            False,
        ):
            with patch.object(deploy_utils, "RAY_SERVE_ENABLE_HA_PROXY", False):
                with pytest.raises(ValueError, match="not supported on"):
                    deploy_utils.get_deploy_args("d", replica_config, ingress=True)


def test_multiplex_ingress_haproxy_error_when_direct_ingress_on(patch_ray_job_id):
    """HAProxy enables direct ingress; ingress multiplexing should cite HA_PROXY."""
    replica_config = ReplicaConfig.create(MultiplexedIngress)
    with patch.object(deploy_utils, "RAY_SERVE_ENABLE_DIRECT_INGRESS", True):
        with patch.object(
            deploy_utils,
            "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING",
            False,
        ):
            with patch.object(deploy_utils, "RAY_SERVE_ENABLE_HA_PROXY", True):
                with pytest.raises(ValueError, match="RAY_SERVE_ENABLE_HA_PROXY"):
                    deploy_utils.get_deploy_args("d", replica_config, ingress=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
