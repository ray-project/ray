from unittest import mock

from ray.llm._internal.serve.engines.vllm.kv_transfer.dynamo import (
    DynamoConnectorBackend,
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
