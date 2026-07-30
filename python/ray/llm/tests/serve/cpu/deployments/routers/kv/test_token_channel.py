import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import zmq
from starlette.datastructures import Headers

from ray._common.network_utils import find_free_port
from ray.llm._internal.serve.core.ingress.router import LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware import token_channel
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    TokenReceiver,
    TokenSender,
    TokenStore,
    encode_prompt_token_ids,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.prompt_token_forwarding import (
    inject_prompt_token_ids,
    install_prompt_token_forwarding,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
)


def _payload(ids):
    return encode_prompt_token_ids(ids)


def _raw_request(key):
    return SimpleNamespace(headers=Headers({KV_TOKEN_KEY_HEADER: key}))


def _install_forwarding(serving, store):
    install_prompt_token_forwarding(
        SimpleNamespace(
            openai_serving_chat=None,
            openai_serving_completion=serving,
        ),
        store,
    )


async def _push_until(sender, endpoint, key, ids):
    for _ in range(100):
        if sender.push(endpoint, key, _payload(ids)):
            return
        await asyncio.sleep(0.01)
    pytest.fail("TokenSender did not connect to TokenReceiver")


async def _wait_until_staged(store, keys):
    remaining = set(keys)
    for _ in range(100):
        for key in list(remaining):
            entry = await store.pop(key)
            if entry is not None:
                await store.put(key, payload=entry.payload)
                remaining.remove(key)
        if not remaining:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"Timed out waiting for staged prompt tokens: {sorted(remaining)}")


def test_bad_endpoint_falls_back():
    """A malformed endpoint must degrade to engine tokenization, not raise."""
    sender = TokenSender()
    try:
        with patch.object(token_channel, "logger") as mock_logger:
            for _ in range(5):
                assert (
                    sender.push("not-a-valid-endpoint", "k", _payload([1, 2])) is False
                )
        # A socket that fails to connect is never cached for reuse.
        assert not sender._sockets
        # Every genuine failure is reported, not just the first.
        assert mock_logger.warning.call_count == 5
    finally:
        sender.close()


def test_send_error_discards_socket():
    """A non-EAGAIN ZMQError drops the socket instead of escaping to the caller."""
    port = find_free_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    sender = TokenSender()
    try:
        socket = sender._get_socket(endpoint)
        assert socket is not None
        # ETERM cannot be provoked without tearing down the shared ZMQ context,
        # so raise it from the send itself.
        socket.send_multipart = MagicMock(
            side_effect=zmq.ZMQError(zmq.ETERM, "context terminated")
        )

        assert sender.push(endpoint, "k", _payload([1, 2])) is False
        # Broken socket evicted so the next push redials.
        assert endpoint not in sender._sockets
    finally:
        sender.close()


def test_full_queue_keeps_socket():
    """A full send queue drops the payload; the sender stays usable afterwards."""
    port = find_free_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    sender = TokenSender(send_queue_limit=1)
    receiver_socket = zmq.Context.instance().socket(zmq.PULL)
    receiver_socket.setsockopt(zmq.LINGER, 0)
    receiver_socket.setsockopt(zmq.RCVHWM, 1)
    receiver_socket.bind(endpoint)
    try:
        # Nothing reads from receiver_socket, so the queue fills and push()
        # reports False via zmq.Again rather than raising.
        refused = 0
        with patch.object(token_channel, "logger") as mock_logger:
            for i in range(500):
                if not sender.push(endpoint, f"k{i}", _payload([i] * 512)):
                    refused += 1
        assert refused > 0
        # The socket survived the backpressure -- it was never closed.
        assert endpoint in sender._sockets
        # Backpressure is routine, so it must stay off the warning path. This
        # also pins the handler order: zmq.Again subclasses zmq.ZMQError, so
        # swapping the two except blocks would warn on every refused push.
        assert mock_logger.warning.call_count == 0
        assert mock_logger.debug.call_count == refused
    finally:
        sender.close()
        receiver_socket.close(linger=0)


@pytest.mark.asyncio
async def test_missing_key_falls_back():
    store = TokenStore()
    request = SimpleNamespace(kv_transfer_params=None)

    await inject_prompt_token_ids(request, _raw_request("missing"), store)

    assert request.kv_transfer_params is None


@pytest.mark.asyncio
async def test_keeps_tokens_separate():
    token_ids_by_key = {
        "k-a": [1, 10, 100],
        "k-b": [2, 20, 200, 2000],
        "k-c": [3, 30],
    }
    port = find_free_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    store = TokenStore()
    receiver = TokenReceiver(bind_endpoint=endpoint, store=store)
    assert await receiver.start()

    sender = TokenSender()

    class Serving:
        async def create_completion(self, request, raw_request=None):
            return "ok"

    serving = Serving()
    _install_forwarding(serving, store)
    try:
        for key, ids in token_ids_by_key.items():
            await _push_until(sender, endpoint, key, ids)
        await _wait_until_staged(store, token_ids_by_key)

        for key in ("k-b", "k-a", "k-c"):
            request = SimpleNamespace(kv_transfer_params=None)
            result = await serving.create_completion(request, _raw_request(key))

            assert result == "ok"
            assert (
                request.kv_transfer_params["prompt_token_ids"] == token_ids_by_key[key]
            )

        for key in token_ids_by_key:
            assert await store.pop(key) is None
    finally:
        sender.close()
        await receiver.close()


@pytest.mark.asyncio
async def test_routes_tokens_to_replica():
    token_ids = [101, 102, 103, 104]
    selected_port = find_free_port()
    unselected_port = find_free_port()
    selected_endpoint = f"tcp://127.0.0.1:{selected_port}"
    unselected_endpoint = f"tcp://127.0.0.1:{unselected_port}"

    selected_store = TokenStore()
    unselected_store = TokenStore()
    selected_receiver = TokenReceiver(
        bind_endpoint=selected_endpoint, store=selected_store
    )
    unselected_receiver = TokenReceiver(
        bind_endpoint=unselected_endpoint, store=unselected_store
    )
    assert await selected_receiver.start()
    assert await unselected_receiver.start()

    class Serving:
        def __init__(self):
            self.seen_prompt_token_ids = []
            self.tokenize_calls = 0

        async def create_completion(self, request, raw_request=None):
            params = getattr(request, "kv_transfer_params", None)
            token_ids = (
                params.get("prompt_token_ids") if isinstance(params, dict) else None
            )
            if token_ids is None:
                self.tokenize_calls += 1
            else:
                self.seen_prompt_token_ids.append(token_ids)
            return "ok"

    selected_serving = Serving()
    unselected_serving = Serving()
    _install_forwarding(selected_serving, selected_store)
    _install_forwarding(unselected_serving, unselected_store)

    sender = TokenSender()
    router = LLMRouter.__new__(LLMRouter)
    router._handle = MagicMock()
    router._tokenizer = MagicMock()
    router._tokenizer.tokenize = AsyncMock(return_value=token_ids)
    router._pick_replica = AsyncMock(
        return_value=("selected-host", 8000, "selected-replica", selected_endpoint)
    )
    router._token_sender = sender
    router._warned_no_token_endpoint = False

    request = MagicMock()
    request.body = AsyncMock(return_value=b'{"model": "m", "prompt": "hello"}')
    request.headers = Headers({})

    try:
        await _push_until(sender, selected_endpoint, "warmup", [0])
        await _wait_until_staged(selected_store, ["warmup"])

        response = await router.route(request)

        assert response["host"] == "selected-host"
        assert response["port"] == 8000
        assert response["replica_id"] == "selected-replica"
        assert router._pick_replica.call_args.kwargs["request_token_ids"] == token_ids

        routed_headers = response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD]
        token_key = routed_headers[KV_TOKEN_KEY_HEADER]
        await _wait_until_staged(selected_store, [token_key])

        unselected_request = SimpleNamespace(kv_transfer_params=None)
        await unselected_serving.create_completion(
            unselected_request, _raw_request(token_key)
        )
        assert unselected_request.kv_transfer_params is None

        selected_request = SimpleNamespace(kv_transfer_params=None)
        result = await selected_serving.create_completion(
            selected_request, _raw_request(token_key)
        )

        assert result == "ok"
        assert selected_request.kv_transfer_params["prompt_token_ids"] == token_ids
        assert selected_serving.seen_prompt_token_ids == [token_ids]
        assert selected_serving.tokenize_calls == 0
        assert unselected_serving.tokenize_calls == 1
        assert await selected_store.pop(token_key) is None
    finally:
        sender.close()
        await selected_receiver.close()
        await unselected_receiver.close()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
