import abc
import random
import string
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ray import serve

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
        CompletionRequest,
    )

    # The two OpenAI request models the P/D orchestrator shapes. Defined under
    # TYPE_CHECKING (and used as a string annotation) to avoid an import cycle
    # between this module and the config/openai-models modules.
    RequestType = Union[ChatCompletionRequest, CompletionRequest]


class BaseConnectorBackend(abc.ABC):
    # ---- P/D coordination protocol ----
    #
    # These class attributes and methods let the P/D orchestrator
    # (``PDOrchestratorMixin``) delegate request shaping, peer addressing, and
    # handoff discipline to the connector. They are connector-agnostic: a
    # connector picks a quadrant of (``requires_peer_binding``,
    # ``concurrent_handoff``) and implements ``prepare_prefill_request`` /
    # ``prepare_decode_request`` accordingly.
    #
    # ``requires_peer_binding``:
    #   * False -> the orchestrator dispatches prefill via the standard handle
    #     path; the peer (if any) is resolved post-hoc from the prefill response.
    #   * True  -> the orchestrator selects the prefill replica first
    #     (``choose_replica``) and passes its ``replica_metadata`` to the backend
    #     as ``peer`` (pre-dispatch addressing).
    #
    # ``concurrent_handoff``:
    #   * False -> prefill runs to its first chunk before local decode starts
    #     (sequential handoff).
    #   * True  -> prefill dispatch and local decode run concurrently.
    #
    # The two flags are independent. For example: a standard (pull-on-response)
    # connector is (False, False); a push-based, request-id-addressed connector
    # is (True, True); a pull-based request-id-addressed connector is
    # (True, False).
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

    @abc.abstractmethod
    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        """Shape the request sent to the remote prefill engine.

        Args:
            request: The incoming chat/completion request.
            peer: The selected prefill replica's ``replica_metadata`` dict when
                the connector opted into pre-dispatch peer binding
                (``requires_peer_binding=True``), else None.

        Returns:
            A new request object to dispatch to the prefill engine.
        """
        ...

    @abc.abstractmethod
    def prepare_decode_request(
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        """Shape the request run on the local decode engine.

        Args:
            request: The incoming chat/completion request.
            peer: The selected prefill replica's ``replica_metadata`` dict when
                the connector opted into pre-dispatch peer binding, else None.
            prefill_response: The captured prefill response chunk whose
                ``kv_transfer_params`` may be forwarded, or None when no chunk is
                captured before decode starts (concurrent-handoff mode).

        Returns:
            A new request object to run on the local decode engine.
        """
        ...

    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass


class DefaultPDProtocolMixin:
    """The default P/D protocol policy: no peer binding, sequential handoff.

    Implements ``prepare_prefill_request`` / ``prepare_decode_request`` for
    connectors that follow the standard policy: the prefill engine is told to
    produce KV for a remote decode (clamped to a single non-streaming token),
    and the decode engine forwards the ``kv_transfer_params`` that the prefill
    engine returned on its first response chunk.

    Mix this in *before* ``BaseConnectorBackend`` in a backend's bases so its
    concrete methods satisfy the abstract methods.
    """

    def prepare_prefill_request(
        self, *, request: "RequestType", peer: Optional[Dict[str, Any]]
    ) -> "RequestType":
        """Shape the prefill request under the default P/D protocol policy.

        Deep-copies the request, stamps the standard ``kv_transfer_params`` that
        tell the prefill engine to produce KV for a remote decode, and clamps it
        to a single, non-streaming token. ``peer`` is ignored.
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
        self,
        *,
        request: "RequestType",
        peer: Optional[Dict[str, Any]],
        prefill_response: Optional[Any],
    ) -> "RequestType":
        """Shape the decode request under the default P/D protocol policy.

        Deep-copies the request and, only when a prefill response chunk was
        captured, forwards its ``kv_transfer_params`` so the decode engine
        pulls/receives the KV produced by prefill. In concurrent-handoff mode
        ``prefill_response`` is None and the request is left unmodified. ``peer``
        is ignored.
        """
        decode_request = request.model_copy(deep=True)
        if prefill_response is not None:
            decode_request.kv_transfer_params = prefill_response.kv_transfer_params
        return decode_request


class DefaultConnectorBackend(DefaultPDProtocolMixin, BaseConnectorBackend):
    """Concrete connector backend using the default P/D protocol policy.

    Used as the factory fallback for connectors that are not registered with a
    dedicated backend class: they get a no-op ``setup()`` and the default
    request-shaping policy. ``BaseConnectorBackend`` is abstract, so the factory
    must return a concrete class like this one.
    """

    pass
