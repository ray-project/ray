import sys
import types
from unittest import mock

from ray.llm._internal.serve.engines.vllm.kv_transfer.dynamo import (
    DynamoConnectorBackend,
    _DYNAMO_PUBLISHERS,
)
from ray.serve.llm import LLMConfig


def test_dynamo_connector_backend_sets_worker_id_ports_and_vllm_configs():
    llm_config = LLMConfig(
        model_loading_config=dict(model_id="test-model"),
        engine_kwargs=dict(
            kv_transfer_config=dict(
                kv_connector="DynamoConnector",
                kv_role="kv_both",
                engine_id="engine",
                kv_connector_extra_config={
                    "ray_serve_dynamo": {
                        "worker_id": 7,
                        "port_offset": 3,
                        "block_size": 32,
                    }
                },
            )
        ),
    )

    backend = DynamoConnectorBackend(llm_config)
    with mock.patch.object(backend, "_maybe_start_event_publisher") as start_publisher:
        backend.setup()

    kv_transfer_config = llm_config.engine_kwargs["kv_transfer_config"]
    dynamo_config = kv_transfer_config["kv_connector_extra_config"][
        "ray_serve_dynamo"
    ]

    assert kv_transfer_config["engine_id"] == "engine-7"
    assert (
        kv_transfer_config["kv_connector_module_path"]
        == "kvbm.vllm_integration.connector"
    )
    assert dynamo_config["worker_id"] == 7
    assert dynamo_config["block_size"] == 32
    assert llm_config.engine_kwargs["kv_events_config"] == {
        "enable_kv_cache_events": True,
        "publisher": "zmq",
        "endpoint": "tcp://*:5560",
        "topic": "",
    }
    assert llm_config.engine_kwargs["additional_config"][
        "consolidator_endpoints"
    ] == [
        "tcp://127.0.0.1:5560",
        "tcp://0.0.0.0:57004",
        "tcp://127.0.0.1:57004",
    ]
    start_publisher.assert_called_once()


def test_dynamo_connector_backend_uses_serve_rank_for_worker_identity():
    llm_config = LLMConfig(
        model_loading_config=dict(model_id="test-model"),
        engine_kwargs=dict(
            kv_transfer_config=dict(
                kv_connector="DynamoConnector",
                kv_role="kv_both",
                engine_id="engine",
                kv_connector_extra_config={
                    "ray_serve_dynamo": {
                        "block_size": 32,
                    }
                },
            )
        ),
    )

    class FakeReplicaRank:
        rank = 5

    class FakeReplicaContext:
        rank = FakeReplicaRank()

    backend = DynamoConnectorBackend(llm_config)
    with (
        mock.patch(
            "ray.llm._internal.serve.engines.vllm.kv_transfer.dynamo."
            "serve.get_replica_context",
            return_value=FakeReplicaContext(),
        ),
        mock.patch.object(backend, "_maybe_start_event_publisher") as start_publisher,
    ):
        backend.setup()

    kv_transfer_config = llm_config.engine_kwargs["kv_transfer_config"]
    dynamo_config = kv_transfer_config["kv_connector_extra_config"][
        "ray_serve_dynamo"
    ]

    assert kv_transfer_config["engine_id"] == "engine-5"
    assert dynamo_config["worker_id"] == 5
    assert llm_config.engine_kwargs["kv_events_config"]["endpoint"] == "tcp://*:5562"
    assert llm_config.engine_kwargs["additional_config"][
        "consolidator_endpoints"
    ] == [
        "tcp://127.0.0.1:5562",
        "tcp://0.0.0.0:57006",
        "tcp://127.0.0.1:57006",
    ]
    start_publisher.assert_called_once()
    assert start_publisher.call_args.args[1] == 5


def test_dynamo_connector_backend_starts_kv_event_publisher(monkeypatch):
    created_publishers = []
    endpoint_paths = []

    class FakeDistributedRuntime:
        @classmethod
        def detached(cls):
            return cls()

        def endpoint(self, endpoint_path):
            endpoint_paths.append(endpoint_path)
            return f"endpoint:{endpoint_path}"

    class FakeKvEventPublisher:
        def __init__(self, endpoint, **kwargs):
            self.endpoint = endpoint
            self.kwargs = kwargs
            created_publishers.append(self)

    dynamo_module = types.ModuleType("dynamo")
    dynamo_module.__path__ = []
    core_module = types.ModuleType("dynamo._core")
    core_module.DistributedRuntime = FakeDistributedRuntime
    llm_module = types.ModuleType("dynamo.llm")
    llm_module.KvEventPublisher = FakeKvEventPublisher

    monkeypatch.setitem(sys.modules, "dynamo", dynamo_module)
    monkeypatch.setitem(sys.modules, "dynamo._core", core_module)
    monkeypatch.setitem(sys.modules, "dynamo.llm", llm_module)

    _DYNAMO_PUBLISHERS.clear()
    llm_config = LLMConfig(
        model_loading_config=dict(model_id="test-model"),
        engine_kwargs=dict(
            kv_transfer_config=dict(
                kv_connector="DynamoConnector",
                kv_role="kv_both",
                kv_connector_extra_config={
                    "ray_serve_dynamo": {
                        "endpoint": "dyn://namespace/component/endpoint",
                        "worker_id": 9,
                        "port_offset": 4,
                        "block_size": 64,
                        "dp_rank": 0,
                        "enable_local_indexer": True,
                        "consolidator_topic": "kv-events",
                        "batching_timeout_ms": 25,
                    }
                },
            )
        ),
    )

    backend = DynamoConnectorBackend(llm_config)
    backend.setup()

    kv_transfer_config = llm_config.engine_kwargs["kv_transfer_config"]
    dynamo_config = kv_transfer_config["kv_connector_extra_config"][
        "ray_serve_dynamo"
    ]

    assert kv_transfer_config["engine_id"] == "ray-serve-dynamo-9"
    assert dynamo_config["worker_id"] == 9
    assert llm_config.engine_kwargs["kv_events_config"] == {
        "enable_kv_cache_events": True,
        "publisher": "zmq",
        "endpoint": "tcp://*:5561",
        "topic": "",
    }
    assert llm_config.engine_kwargs["additional_config"][
        "consolidator_endpoints"
    ] == [
        "tcp://127.0.0.1:5561",
        "tcp://0.0.0.0:57005",
        "tcp://127.0.0.1:57005",
    ]

    assert endpoint_paths == ["dyn://namespace/component/endpoint"]
    assert len(created_publishers) == 1
    publisher = created_publishers[0]
    assert publisher.endpoint == "endpoint:dyn://namespace/component/endpoint"
    assert publisher.kwargs == {
        "worker_id": 9,
        "kv_block_size": 64,
        "dp_rank": 0,
        "enable_local_indexer": True,
        "zmq_endpoint": "tcp://127.0.0.1:57005",
        "zmq_topic": "kv-events",
        "batching_timeout_ms": 25,
    }
    assert _DYNAMO_PUBLISHERS == [publisher]
