"""MoRIIO connector backend for Ray Serve LLM (analogue of nixl.py).

Configures a vLLM engine's ``kv_transfer_config.kv_connector_extra_config`` for
the MoRIIO connector and computes per-replica handshake/notify ports so colocated
replicas don't collide. Also builds the engine's advertised zmq address so the
P/D orchestrator can discover it via the replica-metadata hook
(``ReplicaSelection.replica_metadata``), and implements the PD connector protocol
(``requires_peer_binding`` / ``concurrent_handoff`` / ``prepare_prefill_request`` /
``prepare_decode_request``) so the decode orchestrator can address the selected
prefill peer by request id.

Unlike NIXL/LMCache, MoRIIO does NOT use ``DefaultPDProtocolMixin``: it has custom
request shaping (a dual-address request_id + transfer_id) and therefore IMPLEMENTS
the abstract ``prepare_*`` methods directly on ``BaseConnectorBackend``.

Two transfer disciplines, selected by ``read_mode``:
  * WRITE (default): prefill PUSHES KV to decode -> concurrent handoff.
  * READ: decode PULLS KV from prefill -> sequential handoff; the decode request
    forwards the ``remote_block_ids`` / ``remote_engine_id`` the prefill engine
    returned.

The dual-address request_id and the transfer_id are derived DETERMINISTICALLY
from the incoming request id (uuid5), so ``prepare_prefill_request`` and
``prepare_decode_request`` produce identical ids across their two separate calls
without per-request backend state (the backend instance is shared across
requests).

Registered with Ray's public connector registry via the factory.
"""

import logging
import re
import uuid
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import ray
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.utils.server_utils import (
    get_serve_request_id,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.engines.vllm.kv_transfer.base import RequestType

logger = logging.getLogger(__name__)

# Defaults mirror vLLM's MoRIIOConstants (DEFAULT_HANDSHAKE_PORT / NOTIFY_PORT).
# Prefill uses these bases; decode is shifted (see builder.py) so a colocated
# P+D pair on one node doesn't collide.
DEFAULT_HANDSHAKE_PORT_BASE = 6301
DEFAULT_NOTIFY_PORT_BASE = 61005

# experimental_configs keys understood by this backend.
HANDSHAKE_PORT_BASE_KEY = "MORI_HANDSHAKE_PORT_BASE"
NOTIFY_PORT_BASE_KEY = "MORI_NOTIFY_PORT_BASE"

# ---------------------------------------------------------------------------
# Dual-address request_id / zmq address encoding.
#
# These MUST stay byte-compatible with the regexes vLLM's MoRIIO connector uses
# to recover peer addresses from the request_id:
#
#   vllm/distributed/kv_transfer/kv_connector/v1/moriio/moriio_common.py
#       _PREFILL_ZMQ_RE = re.compile(r"___prefill_addr_(.+?)___decode_addr_")
#       _DECODE_ZMQ_RE  = re.compile(r"___decode_addr_(.+)_[0-9a-f]{32}(?:-.*)?$")
#       # zmq address: "host:IP,handshake:PORT,notify:PORT"
# ---------------------------------------------------------------------------

_PREFILL_PREFIX = "___prefill_addr_"
_DECODE_PREFIX = "___decode_addr_"
_TRANSFER_PREFIX = "tx"

# Copies of vLLM's regexes for local validation / round-trip tests.
_PREFILL_ZMQ_RE = re.compile(r"___prefill_addr_(.+?)___decode_addr_")
_DECODE_ZMQ_RE = re.compile(r"___decode_addr_(.+)_[0-9a-f]{32}(?:-.*)?$")

# Stable namespace for deterministic uuid5 derivation of the dual-address uid.
# Fixed so prefill & decode (and any retry) agree on the same id for a request.
_MORI_UID_NAMESPACE = uuid.UUID("d3b07384-d9a0-4f1b-9c2e-6d6f72690001")


def build_zmq_address(host: str, handshake_port: int, notify_port: int) -> str:
    """Build the MORI zmq address string ``host:IP,handshake:PORT,notify:PORT``."""
    return f"host:{host},handshake:{handshake_port},notify:{notify_port}"


def parse_zmq_address(zmq_address: str) -> Tuple[str, int, int]:
    """Inverse of :func:`build_zmq_address` -> ``(host, handshake_port, notify_port)``."""
    parts = {}
    for segment in zmq_address.split(","):
        key, _, val = segment.partition(":")
        parts[key.strip()] = val.strip()
    return parts["host"], int(parts["handshake"]), int(parts["notify"])


def parse_peer_zmq(request_id: str, is_producer: bool) -> str:
    """Recover the peer's zmq address from a request id (for tests/debugging).

    Producer (prefill) wants the *decode* address; consumer wants the *prefill*.
    """
    rex = _DECODE_ZMQ_RE if is_producer else _PREFILL_ZMQ_RE
    m = rex.search(request_id)
    if not m:
        raise ValueError(f"No peer zmq address in request_id: {request_id!r}")
    return m.group(1)


def _read_mode_enabled(extra_config: Dict[str, Any]) -> bool:
    """Mirror vLLM's ``get_moriio_mode`` parse of ``read_mode``.

    true / 1 -> READ; anything else -> WRITE (default).
    """
    return str(extra_config.get("read_mode", "false")).lower().strip() in (
        "true",
        "1",
    )


class MoRIIOConnectorBackend(BaseConnectorBackend):
    """Set up MoRIIO ports/extra_config and implement the PD connector protocol."""

    # The advertised zmq address ("host:IP,handshake:PORT,notify:PORT"),
    # computed by setup(); consumers reach it via this backend instance.
    _zmq_address: Optional[str] = None

    # MORI addresses peers by the dual-address request id, so the orchestrator
    # must bind to the selected prefill replica BEFORE dispatch.
    requires_peer_binding: bool = True

    def _extra_config(self) -> dict:
        cfg = self.kv_transfer_config.setdefault("kv_connector_extra_config", {})
        return cfg

    @property
    def _read_mode(self) -> bool:
        """True iff this engine's MoRIIO connector is configured for READ mode."""
        extra = self._extra_config()
        return _read_mode_enabled(extra)

    @property
    def concurrent_handoff(self) -> bool:
        """WRITE -> concurrent (prefill pushes); READ -> sequential (decode pulls)."""
        return not self._read_mode

    def setup(self) -> None:
        offset = self._compute_port_offset()

        handshake_base = int(
            self.llm_config.experimental_configs.get(
                HANDSHAKE_PORT_BASE_KEY, DEFAULT_HANDSHAKE_PORT_BASE
            )
        )
        notify_base = int(
            self.llm_config.experimental_configs.get(
                NOTIFY_PORT_BASE_KEY, DEFAULT_NOTIFY_PORT_BASE
            )
        )

        # NOTE: vLLM internally adds get_port_offset(dp_rank, tp_rank) on top of
        # these bases. For TP/DP>1, reserve a stride >= tp_size*pp_size when
        # shifting decode's base in the builder so the two offset schemes never
        # overlap.
        handshake_port = handshake_base + offset
        notify_port = notify_base + offset

        extra = self._extra_config()
        # Required keys for vLLM's config parser (KeyError otherwise) -- proxyless.
        extra.setdefault("proxy_ip", "")  # empty => ping/registration thread disabled
        extra.setdefault("proxy_ping_port", "0")
        # TODO: real Serve replica HTTP port. Harmless placeholder while
        # proxy_ip="" (only used to build request_address for the disabled ping).
        extra.setdefault("http_port", str(8000 + offset))
        # WRITE mode (prefill pushes). READ would be "true".
        extra.setdefault("read_mode", "false")
        extra["handshake_port"] = str(handshake_port)
        extra["notify_port"] = str(notify_port)

        # Advertise the Ray internal cluster IP as the zmq host.
        host = ray.util.get_node_ip_address()
        zmq_address = build_zmq_address(host, handshake_port, notify_port)
        # Stash so replica_metadata() can publish it; the decode
        # orchestrator reads the selected prefill replica's copy off the peer.
        self._zmq_address = zmq_address
        # TODO: cross-node correctness additionally needs MoRIIO's worker to
        # advertise the node INTERNAL IP (VLLM_HOST_IP inside every worker
        # process). That fix must live in the worker process (a vLLM general
        # plugin) -- see the follow-up worker host-IP PR.

    # ---- replica metadata (published via the PR1 hook) ----

    def replica_metadata(self) -> dict:
        """Static per-replica coordination data published to the orchestrator.

        The prefill replica publishes its MORI zmq address; the decode
        orchestrator reads it off the selected prefill replica's
        ``ReplicaSelection.replica_metadata``.
        """
        return {"mori_zmq_address": self._zmq_address}

    # ---- request shaping (PD connector protocol) ----

    def _dual_ids(
        self, request: Any, peer: Optional[Dict[str, Any]]
    ) -> Tuple[str, str]:
        """Compute the (dual-address request_id, transfer_id) for this request.

        ``prepare_prefill_request`` and ``prepare_decode_request`` are two
        independent, stateless calls for the same request, so both ids are
        derived deterministically (uuid5) from a stable per-request seed plus
        the two zmq addresses — no per-request backend state.
        """
        prefill_zmq = (peer or {}).get("mori_zmq_address")
        decode_zmq = self._zmq_address
        if not prefill_zmq:
            raise ValueError(
                "MoRIIO peer is missing 'mori_zmq_address': the selected prefill "
                "replica did not publish its address (is MoRIIOConnector "
                "configured on the prefill deployment?)."
            )
        if not decode_zmq:
            raise ValueError(
                "MoRIIO decode zmq address is not set: setup() must run on this "
                "engine before requests are shaped."
            )
        # Seed precedence: an explicitly-set request id, else the serve-context
        # id (stable across both prepare_* calls of one orchestration). The
        # id(request) last resort only applies outside a Serve request context
        # (e.g. unit tests); both calls shape the same request object.
        seed = str(
            getattr(request, "request_id", None)
            or get_serve_request_id()
            or f"mori-fallback-{id(request)}"
        )
        uid = uuid.uuid5(_MORI_UID_NAMESPACE, seed).hex
        # Wire format consumed by vLLM's MoRIIO connector: the trailing 32-hex
        # uid is what _PREFILL_ZMQ_RE / _DECODE_ZMQ_RE anchor on.
        request_id = f"{_PREFILL_PREFIX}{prefill_zmq}{_DECODE_PREFIX}{decode_zmq}_{uid}"
        transfer_id = f"{_TRANSFER_PREFIX}-{uid}"
        return request_id, transfer_id

    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        request_id, transfer_id = self._dual_ids(request, peer)
        prefill_request = request.model_copy(deep=True)
        # The dual-address id (peer zmq encoded in it) must reach the engine:
        # setting request_id explicitly makes the LLMServer pipeline preserve it
        # (not clobber it with the Serve id) and the engine copies it into the
        # X-Request-Id header that vLLM's MoRIIO connector parses.
        prefill_request.request_id = request_id
        prefill_request.kv_transfer_params = {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "transfer_id": transfer_id,
            "remote_dp_size": 1,
            "remote_tp_size": 1,
        }
        prefill_request.max_tokens = 1
        if hasattr(prefill_request, "max_completion_tokens"):
            prefill_request.max_completion_tokens = 1
        prefill_request.stream = False
        if hasattr(prefill_request, "stream_options"):
            prefill_request.stream_options = None
        return prefill_request

    def prepare_decode_request(
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        request_id, transfer_id = self._dual_ids(request, peer)
        decode_request = request.model_copy(deep=True)
        decode_request.request_id = request_id

        if not self._read_mode:
            # WRITE: prefill pushes KV; decode just needs do_remote_prefill + the
            # shared transfer_id (no block ids -- they are pushed, not pulled).
            decode_request.kv_transfer_params = {
                "do_remote_prefill": True,
                "do_remote_decode": False,
                "remote_engine_id": None,
                "remote_block_ids": None,
                "transfer_id": transfer_id,
                "remote_dp_size": 1,
                "remote_tp_size": 1,
            }
            return decode_request

        # READ: decode PULLS KV; forward the remote_block_ids / remote_engine_id
        # the prefill engine returned on its response. If absent (e.g. prompt <
        # block_size / full prefix hit), fall back to a local recompute.
        prefill_kv_params = getattr(prefill_response, "kv_transfer_params", None)
        params = dict(prefill_kv_params) if prefill_kv_params else {}
        if params.get("remote_block_ids") and params.get("remote_engine_id"):
            params.setdefault("transfer_id", transfer_id)
            params["do_remote_prefill"] = True
            params["do_remote_decode"] = False
            decode_request.kv_transfer_params = params
        else:
            logger.warning(
                "[MORI][READ] prefill returned no remote_block_ids/remote_engine_id "
                "(kv_transfer_params=%s); decode will recompute locally.",
                prefill_kv_params,
            )
            decode_request.kv_transfer_params = None
        return decode_request
