"""SGLang P/D connector backend for Ray Serve LLM.

SGLang runs prefill and decode concurrently; prefill PUSHES the KV cache to
decode through a bootstrap server that lives on the prefill worker. The decode
side must know the prefill node's bootstrap (host, port) up front -> both
protocol flags are on:
  * ``concurrent_handoff = True``  (prefill pushes; decode needs nothing from
    prefill's response)
  * ``requires_peer_binding = True`` (decode binds to the selected prefill
    replica's bootstrap address before dispatch).

Modeled on ``MoRIIOConnectorBackend`` (the existing both-at-once / address-first
connector). Unlike MoRIIO it adds SGLang bootstrap fields (not
``kv_transfer_params``) and has no ``kv_transfer_config`` -> it inherits the
neutral ``BaseConnectorBackend`` directly.

``setup()`` runs on the prefill engine: it picks a free bootstrap port (SGLang's
default 8998 collides when replicas share a node) and sets the SGLang server
``host`` to the routable node IP (SGLang binds the bootstrap server to
``server_args.host``, default 127.0.0.1 -> a remote decode would get Connection
refused). The address is published via ``replica_metadata`` and the decode
orchestrator stamps it onto both requests via ``peer``.

``bootstrap_room`` is derived deterministically from the incoming request id, so
the two stateless ``prepare_*`` calls agree without per-request backend state.
"""

import hashlib
import secrets
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import ray
from ray import serve
from ray.llm._internal.serve.engines.common.kv_transfer.base import (
    BaseConnectorBackend,
    clamp_request_to_single_token,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.engines.common.kv_transfer.base import RequestType

# SGLang's default disaggregation bootstrap port. Colocated replicas collide on
# it, so setup() adds _compute_port_offset() on top of the base.
DEFAULT_BOOTSTRAP_PORT_BASE = 8998

# experimental_configs key for overriding the bootstrap port base. The builder
# shifts decode's base off prefill's default (see builder.py) so a colocated P+D
# pair on one node doesn't collide; per-replica offset is applied on top.
BOOTSTRAP_PORT_BASE_KEY = "SGLANG_BOOTSTRAP_PORT_BASE"

# bootstrap_room must fit SGLang's range (it computes room % dp_size). 62 bits
# matches the prototype's secrets.randbits(62); derived from the request id hash
# so both prepare_* calls agree.
_ROOM_BITS = 62

# Attribute the minted room is cached under when the client sent no ``rid``.
# Set on the incoming request so both prepare_* calls read the same value.
_ROOM_ATTR = "_ray_sglang_bootstrap_room"


class SGLangConnectorBackend(BaseConnectorBackend):
    """SGLang P/D connector: concurrent handoff, prefill-address-first."""

    concurrent_handoff: bool = True
    requires_peer_binding: bool = True

    # Set by setup(); published via replica_metadata().
    _bootstrap_host: Optional[str] = None
    _bootstrap_port: Optional[int] = None

    @staticmethod
    def _check_request_model_has_bootstrap_fields() -> None:
        """Fail early if the resolved OpenAI request model lacks bootstrap fields.

        Ray's ``ChatCompletionRequest`` resolves to SGLang's model only in a
        SGLang-only environment (the import chain in ``openai_api_models`` tries
        vLLM first). If vLLM is also installed, it resolves to vLLM's model,
        which has no ``bootstrap_room`` — assigning it in ``prepare_*`` then
        raises deep in request handling. Surface it at startup instead.
        """
        from ray.llm._internal.serve.core.configs.openai_api_models import (
            ChatCompletionRequest,
        )

        if "bootstrap_room" not in ChatCompletionRequest.model_fields:
            raise RuntimeError(
                "SGLang P/D requires SGLang's OpenAI request models, but the "
                "resolved ChatCompletionRequest has no 'bootstrap_room' field. "
                "This happens when vLLM is installed alongside SGLang (Ray's "
                "import chain then picks vLLM's request model). SGLang P/D needs "
                "a SGLang-only environment."
            )

    def setup(self) -> None:
        """Pick a free bootstrap port + set host to the node IP, before engine start."""
        self._check_request_model_has_bootstrap_fields()
        offset = self._compute_port_offset()
        engine_kwargs = self.llm_config.engine_kwargs

        # SGLang binds the bootstrap server to server_args.host (default
        # 127.0.0.1). The remote decode dials the node IP we advertise, so bind
        # the routable IP or it gets Connection refused.
        host = ray.util.get_node_ip_address()
        engine_kwargs["host"] = host

        # A user-pinned explicit port wins (advanced/escape hatch). Otherwise
        # compute base + per-replica offset so colocated replicas never share a
        # port. The base is overridable via experimental_configs (the builder
        # shifts decode's base off prefill's default); the offset is derived from
        # the replica rank (or DP rank), matching the MoRIIO connector.
        port = engine_kwargs.get("disaggregation_bootstrap_port")
        if port is None:
            base = int(
                self.llm_config.experimental_configs.get(
                    BOOTSTRAP_PORT_BASE_KEY, DEFAULT_BOOTSTRAP_PORT_BASE
                )
            )
            port = base + offset
            engine_kwargs["disaggregation_bootstrap_port"] = port

        # Same story for the GPUs. This deployment runs with
        # RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1 (see the SGLang P/D
        # release test), so replicas see every device on the node and pick
        # theirs via base_gpu_id instead of Serve masking one in as cuda:0.
        # Serve has no per-replica engine_kwargs, so every replica on a side
        # inherits the SAME base_gpu_id from the shared config and they would
        # all drive the same physical GPUs. Shift it by this replica's device
        # block so a multi-replica side (2P/2D, 4P/4D) spreads out; the
        # configured value stays the side's starting device.
        #
        # Computed from the replica rank directly rather than reusing the port
        # offset above: that one returns a bare DP rank in the DP case, which is
        # a port stride, not a device stride (it would collide when tp_size>1).
        engine_kwargs["base_gpu_id"] = (
            int(engine_kwargs.get("base_gpu_id", 0)) + self._compute_gpu_offset()
        )

        self._bootstrap_host = host
        self._bootstrap_port = port

    def _compute_gpu_offset(self) -> int:
        """This replica's device-block offset within its side of the P/D pair.

        Each replica occupies ``num_devices`` (tp x pp) consecutive GPUs, so
        replica ``r`` starts at ``r * num_devices``. Mirrors the replica-rank
        branch of ``_compute_port_offset``; a missing replica context raises
        rather than silently returning 0, which would double-book GPU 0.
        """
        rc = serve.get_replica_context()
        return rc.rank.rank * self.llm_config.num_devices

    def replica_metadata(self) -> Dict[str, Any]:
        """Publish this (prefill) replica's bootstrap address for the decode peer."""
        return {
            "bootstrap_host": self._bootstrap_host,
            "bootstrap_port": self._bootstrap_port,
        }

    def _peer_address(self, peer: Optional[Dict[str, Any]]) -> Tuple[str, int]:
        host = (peer or {}).get("bootstrap_host")
        port = (peer or {}).get("bootstrap_port")
        if not host or not port:
            raise ValueError(
                "SGLang peer is missing bootstrap_host/bootstrap_port: the "
                "selected prefill replica did not publish its bootstrap address "
                "(is the prefill deployment using llm_engine='SGLang' with a "
                "disaggregation_transfer_backend?)."
            )
        return host, port

    def _bootstrap_room(self, request: Any) -> int:
        """Per-request room id, shared by both prepare_* calls on this request.

        SGLang's ``rid`` is client-optional and defaults to ``None`` -- most
        OpenAI-compatible clients never set it, so hashing it would collide
        every concurrent request onto one room and let the bootstrap server
        mix KV caches. When ``rid`` is absent we mint a fresh random room and
        cache it on the request, so the prefill and decode calls agree without
        depending on object identity or per-backend request state.
        """
        rid = getattr(request, "rid", None)
        if rid is not None:
            digest = hashlib.sha256(str(rid).encode()).hexdigest()
            return int(digest, 16) & ((1 << _ROOM_BITS) - 1)

        room = getattr(request, _ROOM_ATTR, None)
        if room is None:
            room = secrets.randbits(_ROOM_BITS)
            object.__setattr__(request, _ROOM_ATTR, room)
        return room

    def _stamp(self, request: Any, peer: Optional[Dict[str, Any]]) -> Any:
        host, port = self._peer_address(peer)
        out = request.model_copy(deep=True)
        out.bootstrap_host = host
        out.bootstrap_port = port
        out.bootstrap_room = self._bootstrap_room(request)
        return out

    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        prefill_request = self._stamp(request, peer)
        # Prefill only produces the KV cache; it must emit a single,
        # non-streaming token. The decode orchestrator drains the prefill stream
        # to exhaustion inside its choose_replica context and relies on this
        # clamp to keep that bounded (same contract as MoRIIO / the default
        # mixin). Decode is NOT clamped — it generates the real output.
        clamp_request_to_single_token(prefill_request)
        return prefill_request

    def prepare_decode_request(
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        # Concurrent handoff: prefill_response is None; decode needs only the
        # SAME prefill bootstrap host/port/room to rendezvous.
        return self._stamp(request, peer)
