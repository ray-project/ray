import abc
import random
import string
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray import serve

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class BaseConnectorBackend(abc.ABC):
    # ---- P/D coordination protocol ----
    #
    # These class attributes and methods let the P/D orchestrator
    # (``PDOrchestratorMixin``) delegate request shaping, peer addressing, and
    # handoff discipline to the connector. The defaults reproduce the existing
    # NIXL/default flow exactly:
    #   * ``requires_peer_binding`` is False, so the orchestrator routes prefill
    #     via the standard ``handle.<method>.remote(...)`` path (no peer is
    #     resolved before dispatch).
    #   * ``concurrent_handoff`` is False, so prefill runs to its first chunk
    #     before local decode starts, and decode forwards the prefill chunk's
    #     ``kv_transfer_params``.
    # Connectors that address peers by request id (and push KV, e.g. MORI WRITE
    # mode) can opt into pre-dispatch peer binding + concurrent handoff.
    requires_peer_binding: bool = False
    concurrent_handoff: bool = False

    def __init__(self, llm_config: "LLMConfig"):
        """Base class for connector backends.

        Args:
            llm_config: The llm configuration for this engine
        """
        self.llm_config = llm_config

    @property
    def kv_transfer_config(self) -> Dict[str, Any]:
        engine_kwargs = self.llm_config.engine_kwargs
        kv_transfer_config = engine_kwargs.get("kv_transfer_config")
        assert (
            kv_transfer_config is not None
        ), "In Connector backend, kv_transfer_config is not set"
        return kv_transfer_config

    def _get_unique_suffix(self, len: int = 6) -> str:
        """Generates unique alphanumeric suffix.

        Args:
            len: Length of the suffix to generate.
        Returns:
            A unique alphanumeric suffix string of specified length.
        """
        return "".join(random.choices(string.ascii_letters + string.digits, k=len))

    def _compute_port_offset(self) -> int:
        """Compute a deterministic port offset for this replica.

        Uses data_parallel_rank if DP case, otherwise falls back to
        the replica rank assigned by Ray Serve (TP/PP case).

        For TP/PP cases, multiply by num_devices (tp × pp) to reserve
        sufficient port space, since each worker needs a unique port.
        Each TP worker adds its tp_rank (0, 1, ..., tp_size-1) to the
        base port at bind time, and PP stages also need separate ports.

        Returns:
            Non-negative integer offset to add to a base port.
        """
        # Prefer explicit DP rank when available
        dp_rank = self.llm_config.engine_kwargs.get("data_parallel_rank")
        if isinstance(dp_rank, int) and dp_rank >= 0:
            # vLLM already accounts for TP spacing in DP offset calculation
            # (data_parallel_rank × tp_size), don't multiply here
            return dp_rank

        # NOTE (jeffreywang): A missing replica context must fail loudly, not
        # silently return a 0 offset that collides colocated replicas on the
        # same NIXL side-channel port. get_replica_context() raises RayServeException
        # outside a replica.
        rc = serve.get_replica_context()
        engine_config = self.llm_config.get_engine_config()
        num_devices = engine_config.num_devices
        return rc.rank.rank * num_devices

    def prepare_prefill_request(self, request: Any, peer: Optional[Dict[str, Any]]):
        """Shape the request sent to the remote prefill engine.

        The default reproduces today's NIXL/default flow: deep-copy the request,
        stamp the standard ``kv_transfer_params`` that tell the prefill engine to
        produce KV for a remote decode, and clamp it to a single, non-streaming
        token (prefill only needs to run, not generate output).

        Args:
            request: The incoming chat/completion request.
            peer: The selected prefill replica's ``replica_metadata`` dict, or
                None when no pre-dispatch peer binding was performed (the
                default). The default implementation ignores it.

        Returns:
            A new request object to dispatch to the prefill engine.
        """
        assert (
            getattr(request, "kv_transfer_params", None) is None
        ), "kv_transfer_params should be empty before orchestrator"
        prefill_request = request.model_copy(deep=True)
        prefill_request.kv_transfer_params = {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "remote_host": None,
            "remote_port": None,
        }
        prefill_request.max_tokens = 1
        if hasattr(prefill_request, "max_completion_tokens"):
            prefill_request.max_completion_tokens = 1
        prefill_request.stream = False
        if hasattr(prefill_request, "stream_options"):
            prefill_request.stream_options = None
        return prefill_request

    def prepare_decode_request(
        self, request: Any, peer: Optional[Dict[str, Any]], prefill_response: Any
    ):
        """Shape the request run on the local decode engine.

        The default reproduces today's NIXL/default flow: deep-copy the request
        and forward the ``kv_transfer_params`` that the prefill engine returned on
        its first response chunk (``remote_block_ids``/``remote_engine_id``/...),
        so the decode engine pulls/receives the KV produced by prefill.

        Args:
            request: The incoming chat/completion request.
            peer: The selected prefill replica's ``replica_metadata`` dict, or
                None (the default). The default implementation ignores it.
            prefill_response: The captured prefill response chunk whose
                ``kv_transfer_params`` are forwarded. In concurrent-handoff mode
                this is None (no chunk is captured before decode starts).

        Returns:
            A new request object to run on the local decode engine.
        """
        decode_request = request.model_copy(deep=True)
        decode_request.kv_transfer_params = prefill_response.kv_transfer_params
        return decode_request

    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass
