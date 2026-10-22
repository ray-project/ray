import re
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.moriio import (
    _DECODE_ZMQ_RE,
    _PREFILL_ZMQ_RE,
    DEFAULT_HANDSHAKE_PORT_BASE,
    DEFAULT_NOTIFY_PORT_BASE,
    MoRIIOConnectorBackend,
    parse_peer_zmq,
    parse_zmq_address,
)
from ray.serve.llm import LLMConfig
from ray.serve.schema import ReplicaRank

_TEST_HOST = "10.0.0.5"


def _replica_context(global_rank: int) -> SimpleNamespace:
    return SimpleNamespace(
        rank=ReplicaRank(rank=global_rank, node_rank=0, local_rank=global_rank)
    )


def _make_backend(
    read_mode: bool = False,
    extra_exp: dict = None,
    dp_rank: int = None,
    dp_size: int = None,
    tp_size: int = None,
):
    extra_config = {}
    if read_mode:
        extra_config["read_mode"] = "true"
    engine_kwargs = dict(
        kv_transfer_config=dict(
            kv_connector="MoRIIOConnector",
            kv_role="kv_both",
            kv_connector_extra_config=extra_config,
        )
    )
    if dp_rank is not None:
        engine_kwargs["data_parallel_rank"] = dp_rank
    if dp_size is not None:
        engine_kwargs["data_parallel_size"] = dp_size
    if tp_size is not None:
        engine_kwargs["tensor_parallel_size"] = tp_size
    return MoRIIOConnectorBackend(
        llm_config=LLMConfig(
            model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
            engine_kwargs=engine_kwargs,
            experimental_configs=extra_exp or {},
        ),
    )


def _setup(backend, rank: int = 0):
    with (
        patch.dict("os.environ", {}, clear=True),
        patch("ray.util.get_node_ip_address", return_value=_TEST_HOST),
        patch("ray.serve.get_replica_context", return_value=_replica_context(rank)),
    ):
        backend.setup()


class TestMoRIIOConnectorBackendSetup:
    def test_setup_sets_ports_zmq_and_extra_config(self):
        backend = _make_backend()
        _setup(backend, rank=0)

        extra = backend.kv_transfer_config["kv_connector_extra_config"]
        assert extra["handshake_port"] == str(DEFAULT_HANDSHAKE_PORT_BASE)
        assert extra["notify_port"] == str(DEFAULT_NOTIFY_PORT_BASE)
        assert extra["proxy_ip"] == ""
        assert extra["proxy_ping_port"] == "0"
        assert "http_port" in extra
        assert extra["read_mode"] == "false"
        # Routable node IP passed to vLLM's MoRIIO connector (cross-node fix,
        # vllm-project/vllm#45488), matching the advertised zmq host.
        assert extra["host_ip"] == _TEST_HOST

        zmq = backend._zmq_address
        host, hs, notify = parse_zmq_address(zmq)
        assert host == _TEST_HOST
        assert host == extra["host_ip"]
        assert hs == DEFAULT_HANDSHAKE_PORT_BASE
        assert notify == DEFAULT_NOTIFY_PORT_BASE

    def test_setup_port_offset_uses_replica_rank(self):
        backend = _make_backend()
        num_devices = backend.llm_config.get_engine_config().num_devices
        _setup(backend, rank=2)
        extra = backend.kv_transfer_config["kv_connector_extra_config"]
        assert extra["handshake_port"] == str(
            DEFAULT_HANDSHAKE_PORT_BASE + 2 * num_devices
        )
        assert extra["notify_port"] == str(DEFAULT_NOTIFY_PORT_BASE + 2 * num_devices)

    def test_setup_respects_overridden_bases(self):
        backend = _make_backend(
            extra_exp={
                "MORI_HANDSHAKE_PORT_BASE": 7000,
                "MORI_NOTIFY_PORT_BASE": 62000,
            }
        )
        _setup(backend, rank=0)
        extra = backend.kv_transfer_config["kv_connector_extra_config"]
        assert extra["handshake_port"] == "7000"
        assert extra["notify_port"] == "62000"

    def test_requires_peer_binding(self):
        assert MoRIIOConnectorBackend.requires_peer_binding is True

    def test_concurrent_handoff_write_vs_read(self):
        write_backend = _make_backend(read_mode=False)
        read_backend = _make_backend(read_mode=True)
        assert write_backend.concurrent_handoff is True
        assert read_backend.concurrent_handoff is False
        assert write_backend._read_mode is False
        assert read_backend._read_mode is True

    @pytest.mark.parametrize(
        "value,expected_read",
        [
            ("true", True),
            ("True", True),
            ("1", True),
            ("false", False),
            ("0", False),
            ("", False),
        ],
    )
    def test_read_mode_parsing(self, value, expected_read):
        backend = MoRIIOConnectorBackend(
            llm_config=LLMConfig(
                model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
                engine_kwargs=dict(
                    kv_transfer_config=dict(
                        kv_connector="MoRIIOConnector",
                        kv_connector_extra_config={"read_mode": value},
                    )
                ),
            ),
        )
        assert backend._read_mode is expected_read

    def test_replica_metadata_returns_zmq(self):
        backend = _make_backend()
        _setup(backend, rank=0)
        meta = backend.replica_metadata()
        assert meta["mori_zmq_address"] == backend._zmq_address
        # Parallelism is published so the orchestrator can address remote workers.
        assert meta["dp_rank"] == 0 and meta["dp_size"] == 1 and meta["tp_size"] == 1

    def test_replica_metadata_publishes_dp_tp(self):
        backend = _make_backend(dp_rank=2, dp_size=4, tp_size=8)
        _setup(backend, rank=0)
        meta = backend.replica_metadata()
        assert meta["dp_rank"] == 2
        assert meta["dp_size"] == 4
        assert meta["tp_size"] == 8

    def test_replica_metadata_default_empty(self):
        # The default backend publishes nothing (concrete default on the base).
        assert BaseConnectorBackend.replica_metadata(None) == {}


class TestMoRIIORequestId:
    def _prepared_pair(self, backend, request, peer):
        prefill = backend.prepare_prefill_request(request=request, peer=peer)
        # prefill_response is unused in WRITE mode; pass a dummy with no params.
        decode = backend.prepare_decode_request(
            request=request,
            peer=peer,
            prefill_response=SimpleNamespace(kv_transfer_params=None),
        )
        return prefill, decode

    def _request_with_copy(self, request_id="user-req-123"):
        class _Req:
            def __init__(self, rid):
                self.request_id = rid
                self.kv_transfer_params = None
                self.max_tokens = 128
                self.max_completion_tokens = 128
                self.stream = True
                self.stream_options = {"include_usage": True}

            def model_copy(self, deep=False):
                new = _Req(self.request_id)
                return new

        return _Req(request_id)

    def test_prefill_and_decode_share_request_id_and_transfer_id(self):
        backend = _make_backend(read_mode=False)
        _setup(backend, rank=0)
        decode_zmq = backend._zmq_address
        prefill_zmq = "host:10.0.0.9,handshake:6301,notify:61005"
        peer = {"mori_zmq_address": prefill_zmq}

        req = self._request_with_copy("user-req-123")
        prefill, decode = self._prepared_pair(backend, req, peer)

        assert prefill.request_id == decode.request_id
        assert (
            prefill.kv_transfer_params["transfer_id"]
            == decode.kv_transfer_params["transfer_id"]
        )

        # Round-trips to the right peer zmq via the vLLM regexes.
        assert _PREFILL_ZMQ_RE.search(prefill.request_id).group(1) == prefill_zmq
        assert _DECODE_ZMQ_RE.search(prefill.request_id) is not None
        assert parse_peer_zmq(prefill.request_id, is_producer=False) == prefill_zmq
        assert parse_peer_zmq(prefill.request_id, is_producer=True) == decode_zmq

        # transfer_id format tx-<32hex>.
        assert re.fullmatch(
            r"tx-[0-9a-f]{32}", prefill.kv_transfer_params["transfer_id"]
        )

    def test_id_is_deterministic_across_calls(self):
        backend = _make_backend(read_mode=False)
        _setup(backend, rank=0)
        peer = {"mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005"}

        p1 = backend.prepare_prefill_request(
            request=self._request_with_copy("R"), peer=peer
        )
        p2 = backend.prepare_prefill_request(
            request=self._request_with_copy("R"), peer=peer
        )
        assert p1.request_id == p2.request_id
        assert (
            p1.kv_transfer_params["transfer_id"] == p2.kv_transfer_params["transfer_id"]
        )

    def test_prefill_kv_params_write(self):
        backend = _make_backend(read_mode=False)
        _setup(backend, rank=0)
        peer = {"mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005"}
        prefill = backend.prepare_prefill_request(
            request=self._request_with_copy(), peer=peer
        )
        assert prefill.kv_transfer_params["do_remote_decode"] is True
        assert prefill.kv_transfer_params["do_remote_prefill"] is False
        assert prefill.max_tokens == 1
        assert prefill.stream is False
        # vLLM reads "tp_size" (not "remote_tp_size"); DP defaults at dp_size=1.
        assert "remote_tp_size" not in prefill.kv_transfer_params
        assert prefill.kv_transfer_params["tp_size"] == 1
        assert prefill.kv_transfer_params["remote_dp_rank"] == 0
        assert prefill.kv_transfer_params["remote_dp_size"] == 1

    def test_decode_kv_params_write(self):
        backend = _make_backend(read_mode=False)
        _setup(backend, rank=0)
        peer = {"mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005"}
        decode = backend.prepare_decode_request(
            request=self._request_with_copy(),
            peer=peer,
            prefill_response=SimpleNamespace(kv_transfer_params=None),
        )
        assert decode.kv_transfer_params["do_remote_prefill"] is True
        assert decode.kv_transfer_params["do_remote_decode"] is False
        assert decode.kv_transfer_params["remote_block_ids"] is None
        assert "remote_tp_size" not in decode.kv_transfer_params
        assert decode.kv_transfer_params["tp_size"] == 1
        assert decode.kv_transfer_params["remote_dp_rank"] == 0
        assert decode.kv_transfer_params["remote_dp_size"] == 1

    def test_dp_routing_targets_correct_ranks(self):
        """With DP>1, the prefill request addresses the decode (this
        orchestrator) rank; the decode request addresses the selected prefill
        peer's rank (read from peer metadata)."""
        # This orchestrator is decode dp_rank=1 of a 2-way DP decode group.
        backend = _make_backend(read_mode=False, dp_rank=1, dp_size=2, tp_size=4)
        _setup(backend, rank=0)
        # The selected prefill peer is dp_rank=3 of a 4-way DP prefill group.
        peer = {
            "mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005",
            "dp_rank": 3,
            "dp_size": 4,
            "tp_size": 4,
        }
        prefill = backend.prepare_prefill_request(
            request=self._request_with_copy(), peer=peer
        )
        decode = backend.prepare_decode_request(
            request=self._request_with_copy(),
            peer=peer,
            prefill_response=SimpleNamespace(kv_transfer_params=None),
        )
        # Prefill engine's remote == this decode orchestrator (rank 1 of 2).
        assert prefill.kv_transfer_params["remote_dp_rank"] == 1
        assert prefill.kv_transfer_params["remote_dp_size"] == 2
        # Decode engine's remote == the selected prefill peer (rank 3 of 4).
        assert decode.kv_transfer_params["remote_dp_rank"] == 3
        assert decode.kv_transfer_params["remote_dp_size"] == 4
        assert decode.kv_transfer_params["tp_size"] == 4

    def test_decode_kv_params_read_forwards_prefill_params(self):
        backend = _make_backend(read_mode=True)
        _setup(backend, rank=0)
        peer = {"mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005"}
        prefill_resp = SimpleNamespace(
            kv_transfer_params={
                "remote_block_ids": [1, 2, 3],
                "remote_engine_id": "eng-7",
            }
        )
        decode = backend.prepare_decode_request(
            request=self._request_with_copy(), peer=peer, prefill_response=prefill_resp
        )
        assert decode.kv_transfer_params["do_remote_prefill"] is True
        assert decode.kv_transfer_params["remote_block_ids"] == [1, 2, 3]
        assert decode.kv_transfer_params["remote_engine_id"] == "eng-7"
        assert "transfer_id" in decode.kv_transfer_params
        # READ also stamps the remote (prefill peer) routing.
        assert decode.kv_transfer_params["remote_dp_rank"] == 0
        assert decode.kv_transfer_params["remote_dp_size"] == 1
        assert decode.kv_transfer_params["tp_size"] == 1

    def test_decode_read_fallback_when_no_remote_params(self):
        backend = _make_backend(read_mode=True)
        _setup(backend, rank=0)
        peer = {"mori_zmq_address": "host:10.0.0.9,handshake:6301,notify:61005"}
        decode = backend.prepare_decode_request(
            request=self._request_with_copy(),
            peer=peer,
            prefill_response=SimpleNamespace(kv_transfer_params=None),
        )
        assert decode.kv_transfer_params is None


class TestMoRIIOZmqValidation:
    def test_missing_peer_zmq_raises(self):
        """A missing/empty peer mori_zmq_address must raise a clear error, not
        silently build a request id containing "None"."""
        backend = _make_backend(read_mode=False)
        _setup(backend, rank=0)
        request = TestMoRIIORequestId()._request_with_copy("user-req-123")
        for peer in (None, {}, {"mori_zmq_address": ""}):
            with pytest.raises(ValueError, match="mori_zmq_address"):
                backend.prepare_prefill_request(request=request, peer=peer)


class TestMoRIIOFactory:
    def test_registered(self):
        assert KVConnectorBackendFactory.is_registered("MoRIIOConnector")

    def test_create_backend_returns_class(self):
        backend_class = KVConnectorBackendFactory.get_backend_class("MoRIIOConnector")
        assert backend_class is MoRIIOConnectorBackend
        assert issubclass(backend_class, BaseConnectorBackend)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
