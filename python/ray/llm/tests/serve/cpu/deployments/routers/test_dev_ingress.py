"""Tests for DevIngress control plane endpoints.

This module tests the HTTP endpoints exposed by DevIngress:
- POST /sleep, POST /wakeup, GET /is_sleeping
- POST /pause, POST /resume, GET /is_paused
- POST /reset_prefix_cache

These tests verify:
1. Endpoints are correctly registered and accessible
2. Broadcast API correctly broadcasts to replicas
3. Sleep/wakeup and pause/resume isolation between different models
"""

import sys

import httpx
import pytest

import ray
from ray import serve
from ray.llm._internal.serve.core.ingress.dev_ingress import DEV_ENDPOINTS, DevIngress
from ray.llm._internal.serve.core.ingress.ingress import make_fastapi_ingress
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig


@pytest.fixture(scope="module")
def ray_instance():
    """Initialize Ray for the module."""
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def single_model_dev_ingress(ray_instance, disable_placement_bundles):
    """Start a Serve app with one model and DevIngress endpoints."""
    model_id = "test-model-1"
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model_id),
        runtime_env={},
        log_engine_metrics=False,
    )

    # Create LLMServer deployment with mock engine
    llm_deployment = serve.deployment(LLMServer).bind(
        llm_config, engine_cls=MockVLLMEngine
    )

    # Create DevIngress with the dev endpoints
    ingress_cls = make_fastapi_ingress(DevIngress, endpoint_map=DEV_ENDPOINTS)
    ingress_options = DevIngress.get_deployment_options([llm_config])
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[llm_deployment],
    )

    serve.run(ingress_app, name="single-model-app")
    yield model_id
    serve.delete("single-model-app", _blocking=True)


@pytest.fixture
def two_model_dev_ingress(ray_instance, disable_placement_bundles):
    """Start a Serve app with TWO model deployments to test isolation."""
    model_id_1 = "test-model-1"
    model_id_2 = "test-model-2"

    llm_config_1 = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model_id_1),
        runtime_env={},
        log_engine_metrics=False,
    )
    llm_config_2 = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id=model_id_2),
        runtime_env={},
        log_engine_metrics=False,
    )

    # Create LLMServer deployments with mock engine
    llm_deployment_1 = serve.deployment(LLMServer).bind(
        llm_config_1, engine_cls=MockVLLMEngine
    )
    llm_deployment_2 = serve.deployment(LLMServer).bind(
        llm_config_2, engine_cls=MockVLLMEngine
    )

    # Create DevIngress with the dev endpoints
    ingress_cls = make_fastapi_ingress(DevIngress, endpoint_map=DEV_ENDPOINTS)
    ingress_options = DevIngress.get_deployment_options([llm_config_1, llm_config_2])
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[llm_deployment_1, llm_deployment_2],
    )

    serve.run(ingress_app, name="two-model-app")
    yield model_id_1, model_id_2
    serve.delete("two-model-app", _blocking=True)


class TestDevIngressEndpoints:
    """Test DevIngress endpoints."""

    @pytest.mark.asyncio
    async def test_reset_prefix_cache_endpoint(self, single_model_dev_ingress):
        """Test POST /reset_prefix_cache endpoint."""
        model_id = single_model_dev_ingress

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "http://localhost:8000/reset_prefix_cache",
                json={"model": model_id},
            )
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_sleep_wakeup_cycle(self, single_model_dev_ingress):
        """Test full sleep -> is_sleeping -> wakeup -> is_sleeping cycle."""
        model_id = single_model_dev_ingress

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Initial state - should not be sleeping
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_sleeping") is False

            # Sleep the engine
            response = await client.post(
                "http://localhost:8000/sleep",
                json={"model": model_id, "options": {"level": 1}},
            )
            assert response.status_code == 200

            # Check is_sleeping - should be True
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_sleeping") is True

            # Wake up the engine
            response = await client.post(
                "http://localhost:8000/wakeup",
                json={"model": model_id, "options": {}},
            )
            assert response.status_code == 200

            # Check is_sleeping - should be False again
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_sleeping") is False

    @pytest.mark.asyncio
    async def test_pause_resume_cycle(self, single_model_dev_ingress):
        """Test full pause -> is_paused -> resume -> is_paused cycle."""
        model_id = single_model_dev_ingress

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Initial state - should not be paused
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_paused") is False

            # Pause the engine
            response = await client.post(
                "http://localhost:8000/pause",
                json={"model": model_id, "options": {"clear_cache": True}},
            )
            assert response.status_code == 200

            # Check is_paused - should be True
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_paused") is True

            # Resume the engine
            response = await client.post(
                "http://localhost:8000/resume",
                json={"model": model_id, "options": {}},
            )
            assert response.status_code == 200

            # Check is_paused - should be False again
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_id}",
            )
            assert response.status_code == 200
            assert response.json().get("is_paused") is False


class TestDevIngressModelIsolation:
    """Test that control plane operations are isolated per model."""

    @pytest.mark.asyncio
    async def test_sleep_wakeup_isolation(self, two_model_dev_ingress):
        """Test that sleeping model_1 does NOT affect model_2."""
        model_1, model_2 = two_model_dev_ingress

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Both models should start awake
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_1}",
            )
            assert response.json().get("is_sleeping") is False

            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_2}",
            )
            assert response.json().get("is_sleeping") is False

            # Sleep model_1 only
            response = await client.post(
                "http://localhost:8000/sleep",
                json={"model": model_1, "options": {"level": 1}},
            )
            assert response.status_code == 200

            # model_1 should be sleeping
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_1}",
            )
            assert response.json().get("is_sleeping") is True

            # model_2 should NOT be sleeping
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_2}",
            )
            assert response.json().get("is_sleeping") is False

            # Wake up model_1
            response = await client.post(
                "http://localhost:8000/wakeup",
                json={"model": model_1, "options": {}},
            )
            assert response.status_code == 200

            # Both should now be awake
            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_1}",
            )
            assert response.json().get("is_sleeping") is False

            response = await client.get(
                f"http://localhost:8000/is_sleeping?model={model_2}",
            )
            assert response.json().get("is_sleeping") is False

    @pytest.mark.asyncio
    async def test_pause_resume_isolation(self, two_model_dev_ingress):
        """Test that pausing model_1 does NOT affect model_2."""
        model_1, model_2 = two_model_dev_ingress

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Both models should start unpaused
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_1}",
            )
            assert response.json().get("is_paused") is False

            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_2}",
            )
            assert response.json().get("is_paused") is False

            # Pause model_1 only
            response = await client.post(
                "http://localhost:8000/pause",
                json={"model": model_1, "options": {"clear_cache": True}},
            )
            assert response.status_code == 200

            # model_1 should be paused
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_1}",
            )
            assert response.json().get("is_paused") is True

            # model_2 should NOT be paused
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_2}",
            )
            assert response.json().get("is_paused") is False

            # Resume model_1
            response = await client.post(
                "http://localhost:8000/resume",
                json={"model": model_1, "options": {}},
            )
            assert response.status_code == 200

            # Both should now be unpaused
            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_1}",
            )
            assert response.json().get("is_paused") is False

            response = await client.get(
                f"http://localhost:8000/is_paused?model={model_2}",
            )
            assert response.json().get("is_paused") is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
