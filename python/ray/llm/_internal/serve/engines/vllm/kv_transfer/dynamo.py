import asyncio
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray import serve
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


_DYNAMO_PUBLISHERS = []


def _get_dynamo_extra_config(kv_transfer_config: Dict[str, Any]) -> Dict[str, Any]:
    extra_config = kv_transfer_config.setdefault("kv_connector_extra_config", {})
    if not isinstance(extra_config, dict):
        raise ValueError("DynamoConnector kv_connector_extra_config must be a dict.")
    dynamo_config = extra_config.setdefault("ray_serve_dynamo", {})
    if not isinstance(dynamo_config, dict):
        raise ValueError("ray_serve_dynamo connector config must be a dict.")
    return dynamo_config


class DynamoConnectorBackend(BaseConnectorBackend):
    """Ray Serve LLM setup hook for Dynamo's vLLM KV connector."""

    def __init__(self, llm_config: "LLMConfig"):
        super().__init__(llm_config)

    def _get_replica_rank(self) -> int:
        try:
            rc = serve.get_replica_context()
            rank = getattr(getattr(rc, "rank", None), "rank", None)
            if isinstance(rank, int) and rank >= 0:
                return rank
        except Exception:
            pass
        return 0

    def _get_block_size(self, dynamo_config: Dict[str, Any]) -> int:
        block_size = dynamo_config.get("block_size")
        if isinstance(block_size, int) and block_size > 0:
            return block_size

        block_size = self.llm_config.engine_kwargs.get("block_size")
        if isinstance(block_size, int) and block_size > 0:
            return block_size

        return 16

    def _set_kv_events_config(
        self,
        dynamo_config: Dict[str, Any],
        port_offset: int,
    ) -> str:
        kv_events_base_port = int(dynamo_config.get("kv_events_base_port", 5557))
        kv_events_port = kv_events_base_port + port_offset
        bind_endpoint = dynamo_config.get(
            "kv_events_bind_endpoint", f"tcp://*:{kv_events_port}"
        )
        connect_endpoint = bind_endpoint.replace("*", "127.0.0.1")

        self.llm_config.engine_kwargs["kv_events_config"] = {
            "enable_kv_cache_events": True,
            "publisher": "zmq",
            "endpoint": bind_endpoint,
            "topic": dynamo_config.get("kv_events_topic", ""),
        }
        return connect_endpoint

    def _set_consolidator_config(
        self,
        dynamo_config: Dict[str, Any],
        port_offset: int,
        vllm_events_endpoint: str,
    ) -> str:
        kvbm_base_port = int(dynamo_config.get("kvbm_leader_zmq_pub_base_port", 56001))
        kvbm_pub_port = kvbm_base_port + port_offset
        os.environ["DYN_KVBM_LEADER_ZMQ_PUB_PORT"] = str(kvbm_pub_port)

        output_port = int(dynamo_config.get("consolidator_base_port", 57001)) + port_offset
        output_bind_endpoint = dynamo_config.get(
            "consolidator_output_bind_endpoint",
            f"tcp://0.0.0.0:{output_port}",
        )
        output_connect_endpoint = dynamo_config.get(
            "consolidator_output_connect_endpoint",
            f"tcp://127.0.0.1:{output_port}",
        )

        additional_config = self.llm_config.engine_kwargs.setdefault(
            "additional_config", {}
        )
        additional_config["consolidator_endpoints"] = [
            vllm_events_endpoint,
            output_bind_endpoint,
            output_connect_endpoint,
        ]
        return output_connect_endpoint

    def _maybe_start_event_publisher(
        self,
        dynamo_config: Dict[str, Any],
        worker_id: int,
        block_size: int,
        consolidator_endpoint: str,
    ) -> None:
        endpoint_path = dynamo_config.get("endpoint")
        if not endpoint_path:
            return

        try:
            from dynamo._core import DistributedRuntime
            from dynamo.llm import KvEventPublisher
        except ImportError:
            return

        try:
            runtime = DistributedRuntime.detached()
        except Exception:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            runtime = DistributedRuntime(
                loop,
                dynamo_config.get("discovery_backend", "mem"),
                dynamo_config.get("request_plane", "tcp"),
            )

        endpoint = runtime.endpoint(endpoint_path)
        publisher = KvEventPublisher(
            endpoint,
            worker_id=worker_id,
            kv_block_size=block_size,
            dp_rank=int(dynamo_config.get("dp_rank", 0)),
            enable_local_indexer=bool(dynamo_config.get("enable_local_indexer", True)),
            zmq_endpoint=consolidator_endpoint,
            zmq_topic=dynamo_config.get("consolidator_topic", ""),
            batching_timeout_ms=dynamo_config.get("batching_timeout_ms"),
        )
        _DYNAMO_PUBLISHERS.append(publisher)

    def setup(self) -> None:
        dynamo_config = _get_dynamo_extra_config(self.kv_transfer_config)
        replica_rank = self._get_replica_rank()
        worker_id = int(dynamo_config.get("worker_id", replica_rank))
        port_offset = int(dynamo_config.get("port_offset", replica_rank))
        block_size = self._get_block_size(dynamo_config)

        engine_id = self.kv_transfer_config.get("engine_id", "ray-serve-dynamo")
        self.kv_transfer_config["engine_id"] = f"{engine_id}-{worker_id}"
        dynamo_config["worker_id"] = worker_id
        dynamo_config["block_size"] = block_size

        vllm_events_endpoint = self._set_kv_events_config(dynamo_config, port_offset)
        consolidator_endpoint = self._set_consolidator_config(
            dynamo_config,
            port_offset,
            vllm_events_endpoint,
        )
        dynamo_config["consolidator_output_connect_endpoint"] = consolidator_endpoint

        self._maybe_start_event_publisher(
            dynamo_config,
            worker_id,
            block_size,
            consolidator_endpoint,
        )
