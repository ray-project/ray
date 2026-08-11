import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.serve import _worker_process_setup_hook
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
)
from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    VLLMEngine,
    _set_single_engine_tcpstore_port,
)
from ray.serve.schema import ReplicaRank


class TestPDDisaggVLLMEngine:
    """Test vLLM engine under PD disagg."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kv_connector", ["NixlConnector", "LMCacheConnectorV1"])
    async def test_pd_disagg_vllm_engine(
        self,
        # llm_config is a fixture defined in serve.tests.conftest.py
        llm_config: LLMConfig,
        kv_connector: str,
        monkeypatch,
    ):
        """Test vLLM engine under PD disagg."""
        if kv_connector == "LMCacheConnectorV1":
            lmcache_mock = MagicMock()
            monkeypatch.setitem(sys.modules, "lmcache", lmcache_mock)
        llm_config = llm_config.model_copy(deep=True)
        llm_config.engine_kwargs.update(
            {
                "kv_transfer_config": dict(
                    kv_connector=kv_connector,
                    kv_role="kv_both",
                ),
            }
        )
        # In production VLLMEngine is constructed inside a Serve replica, where
        # the NIXL connector backend reads serve.get_replica_context() to derive
        # a unique side-channel port offset. Outside a replica that call raises,
        # so mock the replica context.
        replica_context = SimpleNamespace(
            rank=ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        with patch("ray.serve.get_replica_context", return_value=replica_context):
            vllm_engine = VLLMEngine(llm_config)
        assert vllm_engine is not None


def test_single_engine_tcpstore_port_uses_gpu_lane():
    parallel_config = SimpleNamespace(
        data_parallel_size=1,
        data_parallel_master_port=0,
    )
    vllm_config = SimpleNamespace(parallel_config=parallel_config)

    assert _set_single_engine_tcpstore_port(vllm_config, {"GPU": ["3"]}) == 33536
    assert parallel_config.data_parallel_master_port == 33536


def test_single_engine_tcpstore_port_leaves_data_parallel_config_unchanged():
    parallel_config = SimpleNamespace(
        data_parallel_size=2,
        data_parallel_master_port=29500,
    )
    vllm_config = SimpleNamespace(parallel_config=parallel_config)

    assert _set_single_engine_tcpstore_port(vllm_config, {"GPU": ["3"]}) is None
    assert parallel_config.data_parallel_master_port == 29500


def test_worker_setup_hook_can_disable_broken_cutedsl(monkeypatch):
    from vllm.model_executor.kernels.linear.cute_dsl import ll_bf16

    monkeypatch.setenv("RAY_SERVE_LLM_DISABLE_CUTEDSL", "1")
    monkeypatch.setattr(ll_bf16, "_cutedsl_available", None)

    _worker_process_setup_hook()

    assert ll_bf16._cutedsl_available is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
