"""Tests for the P/D coordination protocol on the KV connector backends.

Proves that NIXL, LMCache, and the default backend implement the abstract
``BaseConnectorBackend`` protocol via ``DefaultPDProtocolMixin``, that Multi delegates
the protocol to its top-most sub-connector, and that the abstract base itself cannot
be instantiated.
"""

import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
    DefaultConnectorBackend,
    DefaultPDProtocolMixin,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache import (
    LMCacheConnectorV1Backend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.multi_connector import (
    MultiConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.nixl import (
    NixlConnectorBackend,
)
from ray.serve.llm import LLMConfig


def _llm_config(kv_connector: str) -> LLMConfig:
    return LLMConfig(
        model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
        engine_kwargs=dict(
            kv_transfer_config=dict(
                kv_connector=kv_connector,
                kv_role="kv_both",
            )
        ),
    )


def test_base_connector_backend_is_abstract():
    """``BaseConnectorBackend`` is abstract: its ``prepare_*`` methods are
    abstractmethods, so direct instantiation raises ``TypeError``."""
    with pytest.raises(TypeError):
        BaseConnectorBackend(llm_config=None)


@pytest.mark.parametrize(
    "backend_factory",
    [
        lambda: NixlConnectorBackend(llm_config=_llm_config("NixlConnector")),
        lambda: LMCacheConnectorV1Backend(llm_config=_llm_config("LMCacheConnectorV1")),
        lambda: DefaultConnectorBackend(llm_config=None),
    ],
    ids=["nixl", "lmcache", "default"],
)
class TestConcreteBackendsProtocol:
    """The concrete backends expose the default P/D protocol shaping."""

    def test_is_concrete_subclass(self, backend_factory):
        be = backend_factory()
        assert isinstance(be, BaseConnectorBackend)
        assert isinstance(be, DefaultPDProtocolMixin)
        # Default flags == standard (no-peer, sequential) policy.
        assert be.requires_peer_binding is False
        assert be.concurrent_handoff is False

    def test_prepare_prefill_shaping(self, backend_factory):
        be = backend_factory()
        request = ChatCompletionRequest(
            model="test-model",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=32,
            stream=True,
            stream_options={"include_usage": True},
        )
        prefill = be.prepare_prefill_request(request=request, peer=None)
        assert prefill.kv_transfer_params["do_remote_decode"] is True
        assert prefill.kv_transfer_params["do_remote_prefill"] is False
        assert prefill.max_tokens == 1
        assert prefill.max_completion_tokens == 1
        assert prefill.stream is False
        assert prefill.stream_options is None
        # Original request untouched.
        assert request.max_completion_tokens == 32
        assert request.stream is True

    def test_prepare_decode_forwards_params(self, backend_factory):
        be = backend_factory()
        request = CompletionRequest(model="test-model", prompt="hi")
        chunk = SimpleNamespace(kv_transfer_params={"remote_engine_id": "p1"})
        decode = be.prepare_decode_request(
            request=request, peer=None, prefill_response=chunk
        )
        assert decode.kv_transfer_params == {"remote_engine_id": "p1"}

    def test_prepare_decode_none_prefill_response_no_crash(self, backend_factory):
        """Concurrent-handoff mode passes ``prefill_response=None``: must not
        crash and must leave kv_transfer_params unset (the gemini None-guard)."""
        be = backend_factory()
        request = CompletionRequest(model="test-model", prompt="hi")
        decode = be.prepare_decode_request(
            request=request, peer=None, prefill_response=None
        )
        assert getattr(decode, "kv_transfer_params", None) is None


class TestMultiConnectorDelegation:
    """MultiConnectorBackend delegates the P/D protocol to its top-most
    sub-connector rather than inheriting the default mixin."""

    def _multi_with_primary(self, primary):
        multi = MultiConnectorBackend(llm_config=_llm_config("MultiConnector"))
        multi._connector_backends = [primary]
        return multi

    def test_no_subconnectors_flags_false(self):
        multi = MultiConnectorBackend(llm_config=_llm_config("MultiConnector"))
        assert multi.requires_peer_binding is False
        assert multi.concurrent_handoff is False

    def test_delegates_prepare_to_primary(self):
        multi = self._multi_with_primary(DefaultConnectorBackend(llm_config=None))
        request = ChatCompletionRequest(
            model="test-model",
            messages=[{"role": "user", "content": "hello"}],
            max_completion_tokens=8,
        )
        prefill = multi.prepare_prefill_request(request=request, peer=None)
        assert prefill.kv_transfer_params["do_remote_decode"] is True
        assert prefill.max_tokens == 1
        chunk = SimpleNamespace(kv_transfer_params={"remote_engine_id": "p1"})
        decode = multi.prepare_decode_request(
            request=CompletionRequest(model="test-model", prompt="hi"),
            peer=None,
            prefill_response=chunk,
        )
        assert decode.kv_transfer_params == {"remote_engine_id": "p1"}

    def test_flags_follow_primary(self):
        # A primary that opts into peer binding + concurrent handoff governs the group.
        primary = SimpleNamespace(requires_peer_binding=True, concurrent_handoff=True)
        multi = self._multi_with_primary(primary)
        assert multi.requires_peer_binding is True
        assert multi.concurrent_handoff is True


@patch(
    "ray.llm._internal.serve.engines.vllm.kv_transfer.lmcache._check_lmcache_installed"
)
def test_lmcache_setup_still_works(_mock_check):
    """The P/D protocol must not break the connector-specific setup() behavior."""
    be = LMCacheConnectorV1Backend(llm_config=_llm_config("LMCacheConnectorV1"))
    be.setup()  # no-op path (no kv_connector_extra_config), must not raise


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
