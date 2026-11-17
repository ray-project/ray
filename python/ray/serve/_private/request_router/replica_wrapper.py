import asyncio
import logging
import pickle
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, cast

import ray
from ray._private.grpc_utils import init_grpc_channel
from ray.actor import ActorHandle
from ray.serve._private.common import (
    ActorHandleNotFoundError,
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import (
    RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_result import (
    ActorReplicaResult,
    ReplicaResult,
    gRPCReplicaResult,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.serialization import RPCSerializer
from ray.serve._private.utils import JavaActorHandleProxy
from ray.serve.generated.serve_pb2 import (
    ASGIRequest,
    RequestMetadata as RequestMetadataProto,
)
from ray.serve.generated.serve_pb2_grpc import ASGIServiceStub
from ray.util.annotations import PublicAPI
from ray.util.tracing.tracing_helper import (
    _DictPropagator,
    _is_tracing_enabled,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ReplicaWrapper(ABC):
    """This is used to abstract away details of the transport layer
    when communicating with the replica.
    """

    @abstractmethod
    def send_request_java(self, pr: PendingRequest) -> ReplicaResult:
        """Send request to Java replica."""

    @abstractmethod
    def send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> ReplicaResult:
        """Send request to Python replica.

        If sending request with rejection, the replica will yield a
        system message (ReplicaQueueLengthInfo) before executing the
        actual request. This can cause it to reject the request. The
        result will *always* be a generator, so for non-streaming
        requests it's up to the caller to resolve it to its first (and
        only) ObjectRef.
        """


class ActorReplicaWrapper(ReplicaWrapper):
    def __init__(self, actor_handle):
        self._actor_handle = actor_handle

    def send_request_java(self, pr: PendingRequest) -> ActorReplicaResult:
        """Send the request to a Java replica.
        Does not currently support streaming.
        """
        if pr.metadata.is_streaming:
            raise RuntimeError("Streaming not supported for Java.")

        if len(pr.args) != 1:
            raise ValueError("Java handle calls only support a single argument.")

        return ActorReplicaResult(
            self._actor_handle.handle_request.remote(
                RequestMetadataProto(
                    request_id=pr.metadata.request_id,
                    # Default call method in java is "call," not "__call__" like Python.
                    call_method="call"
                    if pr.metadata.call_method == "__call__"
                    else pr.metadata.call_method,
                ).SerializeToString(),
                pr.args,
            ),
            pr.metadata,
        )

    def send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> ActorReplicaResult:
        """Send the request to a Python replica."""
        if with_rejection:
            # Call a separate handler that may reject the request.
            # This handler is *always* a streaming call and the first message will
            # be a system message that accepts or rejects.
            method = self._actor_handle.handle_request_with_rejection.options(
                num_returns="streaming"
            )
        elif pr.metadata.is_streaming:
            method = self._actor_handle.handle_request_streaming.options(
                num_returns="streaming"
            )
        else:
            method = self._actor_handle.handle_request

        obj_ref_gen = method.remote(pickle.dumps(pr.metadata), *pr.args, **pr.kwargs)
        return ActorReplicaResult(
            obj_ref_gen, pr.metadata, with_rejection=with_rejection
        )


class gRPCReplicaWrapper(ReplicaWrapper):
    def __init__(self, stub, actor_id):
        self._stub = stub
        self._actor_id = actor_id
        self._loop = asyncio.get_running_loop()

    def send_request_java(self, pr: PendingRequest):
        raise RuntimeError("gRPC requests not supported for Java.")

    def send_request_python(
        self, pr: PendingRequest, *, with_rejection: bool
    ) -> gRPCReplicaResult:
        """Send the request to a Python replica."""

        # Get serialization options from request metadata
        request_serialization = pr.metadata.request_serialization
        response_serialization = pr.metadata.response_serialization

        # Get cached serializer for this request to avoid per-request instantiation overhead
        serializer = RPCSerializer.get_cached_serializer(
            request_serialization, response_serialization
        )

        # When using gRPC transport, requests go over the network rather than through
        # Ray's actor RPC. Ray's tracing decorators inject _ray_trace_ctx for actor
        # calls, but that doesn't apply here. We must manually inject the current
        # trace context so it propagates to the replica (matching the actor path).
        if _is_tracing_enabled():
            pr.kwargs["_ray_trace_ctx"] = _DictPropagator.inject_current_context()

        asgi_request = ASGIRequest(
            pickled_request_metadata=pickle.dumps(pr.metadata),
            request_args=serializer.dumps_request(pr.args),
            request_kwargs=serializer.dumps_request(pr.kwargs),
        )
        if with_rejection and pr.metadata.is_streaming:
            # Call a separate handler that may reject the request.
            # This handler is *always* a streaming call and the first message will
            # be a system message that accepts or rejects.
            call = self._stub.HandleRequestWithRejectionStreaming(asgi_request)
        elif with_rejection and not pr.metadata.is_streaming:
            # Call a separate handler that may reject the request.
            # This handler is *always* a unary call and the first message will
            # be a system message that accepts or rejects.
            call = self._stub.HandleRequestWithRejection(asgi_request)
        elif pr.metadata.is_streaming:
            call = self._stub.HandleRequestStreaming(asgi_request)
        else:
            call = self._stub.HandleRequest(asgi_request)

        return gRPCReplicaResult(
            call,
            pr.metadata,
            self._actor_id,
            loop=self._loop,
            with_rejection=with_rejection,
        )


@PublicAPI(stability="alpha")
class RunningReplica:
    """Contains info on a running replica.
    Also defines the interface for a request router to talk to a replica.

    Actor-handle resolution is lazy: the underlying ``ray.get_actor()`` call
    does not happen at construction time but on the first method that needs
    it (``get_queue_len``, ``try_send_request``, ``reserve_slot``,
    ``push_proxy_handle``, or the ``actor_id`` property).  This moves the
    GCS lookup off the Serve-router update path, which previously caused an
    O(Routers * Replicas) GCS storm under high-concurrency workloads
    (see issue #65782).

    The resolved handle is cached on the instance after first use, and the
    per-process cache in :class:`RunningReplicaInfo` ensures that multiple
    ``RunningReplica`` instances in the same worker share a single handle.
    """

    def __init__(self, replica_info: RunningReplicaInfo):
        self._replica_info = replica_info
        self._multiplexed_model_ids = set(replica_info.multiplexed_model_ids)

        # ------------------------------------------------------------------
        # Lazy actor-handle resolution
        # ------------------------------------------------------------------
        # Do NOT eagerly call replica_info.get_actor_handle() here.  That was
        # the original O(Routers * Replicas) GCS storm source.  Instead we
        # resolve on first use and cache.
        self._actor_handle: ActorHandle | JavaActorHandleProxy | None = None
        self._handle_resolved: bool = False
        self._handle_resolve_lock = asyncio.Lock()

        # Lazily created
        self._channel = None
        self._stub = None

        # Replica wrappers (also lazy: they need a resolved handle)
        self._actor_replica_wrapper: ActorReplicaWrapper | None = None
        self._grpc_replica_wrapper: gRPCReplicaWrapper | None = None

    # ------------------------------------------------------------------
    # Lazy handle resolution helpers
    # ------------------------------------------------------------------

    async def _ensure_actor_handle(self) -> ActorHandle | JavaActorHandleProxy:
        """Async: resolve and cache the actor handle.  Uses the
        single-flight, thread-pool-backed ``get_actor_handle_async`` so it
        never blocks the event loop.

        If a subclass has already populated ``_actor_handle`` directly (e.g.
        test doubles), this method returns it as-is without re-resolving.
        """
        if self._actor_handle is not None:
            # Either previously resolved, or injected by a subclass (e.g.
            # FakeRunningReplicaForSlotReservation).  Trust it.
            self._handle_resolved = True
            return self._actor_handle

        async with self._handle_resolve_lock:
            # Re-check inside the lock: another coroutine may have resolved
            # the handle while we were waiting, or a subclass may have
            # populated it directly.
            if self._actor_handle is not None:
                self._handle_resolved = True
                return self._actor_handle

            try:
                handle = await self._replica_info.get_actor_handle_async()
            except Exception:
                # If resolution fails (actor died, GCS error, ...), clear
                # whatever partial state we may have cached and re-raise.
                self._replica_info.invalidate_actor_handle(
                    self._replica_info.actor_name
                )
                self._handle_resolved = False
                self._actor_handle = None
                raise

            if self._replica_info.is_cross_language:
                self._actor_handle = JavaActorHandleProxy(handle)
            else:
                self._actor_handle = handle
            self._handle_resolved = True

        return cast(ActorHandle | JavaActorHandleProxy, self._actor_handle)

    def _get_actor_handle_sync(self) -> ActorHandle | JavaActorHandleProxy:
        """Sync: resolve and cache the actor handle.  Uses the process-level
        cached ``get_actor_handle`` (which itself is thread-safe).

        Only called from paths that cannot be async (e.g. ``actor_id`` property,
        ``push_proxy_handle``'s caller-side fire-and-forget).

        If a subclass has already populated ``_actor_handle`` directly, this
        method returns it as-is.
        """
        if self._actor_handle is not None:
            self._handle_resolved = True
            return self._actor_handle

        try:
            handle = self._replica_info.get_actor_handle()
        except Exception:
            # If resolution fails (actor died, GCS error, ...), clear
            # whatever partial state we may have cached and re-raise.
            self._replica_info.invalidate_actor_handle(self._replica_info.actor_name)
            self._handle_resolved = False
            self._actor_handle = None
            raise

        if self._replica_info.is_cross_language:
            self._actor_handle = JavaActorHandleProxy(handle)
        else:
            self._actor_handle = handle
        self._handle_resolved = True

        return cast(ActorHandle | JavaActorHandleProxy, self._actor_handle)

    async def _get_or_create_actor_replica_wrapper(self) -> ActorReplicaWrapper:
        """Lazily create and cache the ``ActorReplicaWrapper``.  Requires a
        resolved actor handle.
        """
        if self._actor_replica_wrapper is not None:
            return self._actor_replica_wrapper
        handle = await self._ensure_actor_handle()
        self._actor_replica_wrapper = ActorReplicaWrapper(handle)
        return self._actor_replica_wrapper

    def invalidate(self) -> None:
        """Drop cached actor handle and wrappers.  Called when the replica
        is known to have died so that subsequent lookups go back to GCS.
        """
        self._actor_handle = None
        self._handle_resolved = False
        self._actor_replica_wrapper = None
        self._grpc_replica_wrapper = None
        # Also drop the process-level cache entry so a fresh ray.get_actor
        # is forced next time.
        self._replica_info.invalidate_actor_handle(self._replica_info.actor_name)

    def update_replica_info(self, replica_info: RunningReplicaInfo) -> None:
        """Update mutable fields from a new RunningReplicaInfo.

        Called when reusing an existing wrapper in _update_running_replicas.
        Replicas dynamically load/unload models via record_multiplexed_model_ids,
        which triggers a broadcast with updated RunningReplicaInfo. Without this
        update, the router would use stale multiplexed_model_ids and break
        multiplexed model routing.

        Because we reassign _replica_info, any property that reads from it
        (including max_ongoing_requests, node_id, availability_zone, etc.)
        will reflect the new values. Fields that are cached separately
        (e.g., _actor_handle) are NOT refreshed here because they are tied
        to the replica's identity and should never change for a live replica.
        """
        self._replica_info = replica_info
        self._multiplexed_model_ids = set(replica_info.multiplexed_model_ids)

    @property
    def replica_id(self) -> ReplicaID:
        """ID of this replica."""
        return self._replica_info.replica_id

    @property
    def actor_id(self) -> ray.ActorID:
        """Actor ID of this replica.

        This property is occasionally accessed synchronously (e.g. from the
        gRPC wrapper constructor before any await).  Resolves the actor
        handle synchronously with the process-level cache to keep the fast
        path cheap.
        """
        handle = self._get_actor_handle_sync()
        return handle._actor_id

    @property
    def node_id(self) -> str:
        """Node ID of the node this replica is running on."""
        # NOTE: `RunningReplicaInfo.node_id` is `Optional[str]`, so this can
        # actually return `None` despite the declared return type.
        return self._replica_info.node_id  # type: ignore[return-value]

    @property
    def availability_zone(self) -> str | None:
        """Availability zone of the node this replica is running on."""
        return self._replica_info.availability_zone

    @property
    def multiplexed_model_ids(self) -> set[str]:
        """Set of model IDs on this replica."""
        return self._multiplexed_model_ids

    @property
    def routing_stats(self) -> dict[str, Any]:
        """Dictionary of routing stats."""
        return self._replica_info.routing_stats

    @property
    def replica_metadata(self) -> dict[str, Any]:
        """Static per-replica metadata captured once when the replica became ready."""
        # Return a copy so callers can't mutate the RunningReplicaInfo's dict.
        return self._replica_info.replica_metadata.copy()

    @property
    def max_ongoing_requests(self) -> int:
        """Max concurrent requests that can be sent to this replica."""
        return self._replica_info.max_ongoing_requests

    @property
    def is_cross_language(self) -> bool:
        """Whether this replica is cross-language (Java)."""
        return self._replica_info.is_cross_language

    @property
    def backend_http_endpoint(self) -> tuple[str, int] | None:
        """Return (host, port) of the replica's backend HTTP server."""
        port = self._replica_info.backend_http_port
        host = self._replica_info.node_ip
        if host is not None and port is not None:
            return (host, port)
        return None

    @property
    def stub(self):
        if self._stub is None:
            # Created via the shared helper so that Ray's authentication client
            # interceptors and TLS credentials are applied; a raw insecure
            # channel would be rejected by the replica's authenticated server.
            self._channel = init_grpc_channel(
                f"{self._replica_info.node_ip}:{self._replica_info.port}",
                options=[
                    (
                        "grpc.max_receive_message_length",
                        RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH,
                    )
                ],
                asynchronous=True,
            )
            self._stub = ASGIServiceStub(self._channel)

        return self._stub

    async def _get_replica_wrapper(self, pr: PendingRequest) -> ReplicaWrapper:
        """Return the appropriate replica wrapper for this request.

        Resolves the actor handle lazily on demand (async).
        """
        if self._grpc_replica_wrapper is None:
            # Do NOT use the ``actor_id`` property here: it calls
            # ``_get_actor_handle_sync`` which performs a blocking
            # ``ray.get_actor`` on the first invocation, stalling the
            # event loop.  Go through the async resolver instead.
            actor_handle = await self._ensure_actor_handle()
            self._grpc_replica_wrapper = gRPCReplicaWrapper(
                self.stub, actor_handle._actor_id
            )

        if pr.metadata._by_reference:
            return await self._get_or_create_actor_replica_wrapper()
        return self._grpc_replica_wrapper

    def push_proxy_handle(self, handle: ActorHandle):
        """When on proxy, push proxy's self handle to replica.

        Called eagerly by proxy actors right after a new replica appears.
        Uses the sync handle resolver (with process-level caching) since
        callers don't await this fire-and-forget RPC.

        If the replica actor has already died (``ActorHandleNotFoundError``
        from the lazy handle resolver, which wraps the raw ``ValueError``
        from ``ray.get_actor``), silently skip — the controller will send
        a new replica update on the next tick.  Any other error from handle
        resolution is re-raised so the caller knows something went wrong.
        """
        try:
            actor_handle = self._get_actor_handle_sync()
        except ActorHandleNotFoundError:
            # Replica actor is gone (e.g. died between controller listing
            # and our update tick).  ``get_actor_handle`` in common.py
            # converts the raw ``ValueError`` from ``ray.get_actor`` into
            # this narrower exception so we don't accidentally swallow
            # unrelated ``ValueError`` sources (e.g. Java-call argument
            # mistakes).  Nothing more we can do here; the controller will
            # notice and send a corrected replica list.
            return
        actor_handle.push_proxy_handle.remote(handle)

    async def get_queue_len(self, *, deadline_s: float) -> int:
        """Returns current queue len for the replica.
        `deadline_s` is passed to verify backoff for testing.

        Resolves the actor handle lazily on first call.
        """
        # NOTE(edoakes): the `get_num_ongoing_requests` method name is shared by
        # the Python and Java replica implementations. If you change it, you need to
        # change both (or introduce a branch here).
        actor_handle = await self._ensure_actor_handle()
        obj_ref = actor_handle.get_num_ongoing_requests.remote()
        try:
            return await obj_ref
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            raise

    async def try_send_request(
        self, pr: PendingRequest, with_rejection: bool
    ) -> ReplicaResult:
        """Try to send the request to this replica. It may be rejected.

        Resolves the actor handle lazily on first call (async, single-flight,
        thread-pool backed so the event loop stays free).
        """
        wrapper = await self._get_replica_wrapper(pr)
        if self._replica_info.is_cross_language:
            assert not with_rejection, "Request rejection not supported for Java."
            return wrapper.send_request_java(pr)

        return wrapper.send_request_python(pr, with_rejection=with_rejection)

    async def reserve_slot(
        self, request_metadata: RequestMetadata
    ) -> tuple[str, ReplicaQueueLengthInfo]:
        """Reserve a slot on this replica for an upcoming request.

        Returns a unique token that can be used to release the slot later.
        This is used in the choose_replica/dispatch pattern to track
        reservations that haven't been dispatched yet.

        Resolves the actor handle lazily on first call.
        """
        if self._replica_info.is_cross_language:
            raise RuntimeError("Slot reservation not supported for Java.")

        actor_handle = await self._ensure_actor_handle()
        slot_token = str(uuid.uuid4())
        obj_ref = actor_handle.reserve_slot.remote(request_metadata, slot_token)
        try:
            accepted, num_ongoing_requests = await obj_ref
        except asyncio.CancelledError:
            ray.cancel(obj_ref)
            actor_handle.release_slot.remote(slot_token)
            raise
        except Exception:
            # The actor may have reserved the slot before the reply was lost
            # (e.g. ActorUnavailableError). `release_slot` is idempotent for unknown
            # tokens, so this is safe even when the reservation never actually happened.
            actor_handle.release_slot.remote(slot_token)
            raise

        return slot_token, ReplicaQueueLengthInfo(
            accepted=accepted,
            num_ongoing_requests=num_ongoing_requests,
        )

    async def release_slot(self, slot_token: str) -> int:
        """Release a previously reserved slot.

        This should be called if a request is not dispatched after
        reserving a slot (e.g., due to an error or cancellation).

        Returns the replica's reported num_ongoing_requests after the release.

        Resolves the actor handle lazily on first call.
        """
        if self._replica_info.is_cross_language:
            raise RuntimeError("Slot reservation not supported for Java.")

        actor_handle = await self._ensure_actor_handle()
        _, num_ongoing_requests = await actor_handle.release_slot.remote(slot_token)
        return num_ongoing_requests


@dataclass
class ReplicaSelection:
    """Represents a selected replica, holding information for dispatch or coordination.

    This class is returned by the choose_replica() context manager.
    The slot reservation lifecycle is managed by the context manager.
    """

    # Public, user-accessible fields
    replica_id: str
    """Unique identifier for the selected replica."""

    node_ip: str
    """IP address of the node running this replica."""

    port: int | None
    """Port number for direct communication (if configured)."""

    node_id: str
    """Ray node ID where the replica is running."""

    availability_zone: str | None
    """Cloud availability zone of the replica's node."""

    replica_metadata: dict[str, Any]
    """Static, immutable per-replica metadata published by the deployment's
    ``record_replica_metadata`` hook (captured once when the replica became
    ready). Empty dict if the deployment does not define the hook."""

    # Internal fields (not part of public API)
    _replica: RunningReplica
    _deployment_id: DeploymentID | None
    _request_metadata: RequestMetadata
    _method_name: str
    # Token to be used for replica reservation;
    # Can be None when created via the pick-only path
    _slot_token: str | None
    _dispatched: bool = field(
        default=False, init=False
    )  # Tracks if dispatch was called

    # Set by dispatch once the result's done-callback is wired up. Read by
    # choose_replica's finally to decide whether to fire on_request_completed
    # manually (only one of the two paths should fire it).
    _completion_callback_registered: bool = field(default=False, init=False)

    @property
    def address(self) -> str:
        """Returns the replica address in host:port format."""
        if self.port:
            return f"{self.node_ip}:{self.port}"
        return self.node_ip

    def to_dict(self) -> dict[str, Any]:
        """Serialize public fields to a dictionary."""
        return {
            "replica_id": self.replica_id,
            "node_ip": self.node_ip,
            "port": self.port,
            "node_id": self.node_id,
            "availability_zone": self.availability_zone,
            "replica_metadata": self.replica_metadata,
        }

    def _mark_dispatched(self) -> None:
        """Internal: Mark this selection as dispatched (slot consumed).

        Raises:
            RuntimeError: If the selection has already been dispatched.
        """
        if self._dispatched:
            raise RuntimeError(
                f"ReplicaSelection for {self.replica_id} has already been dispatched. "
                "Each selection can only be dispatched once."
            )
        self._dispatched = True

    async def _release_slot(self, *, force: bool = False) -> int | None:
        """Internal: Release the reserved slot.

        Returns the replica's reported num_ongoing_requests after the release,
        or None if dispatch already consumed the slot (and ``force`` is False),
        or None if this selection was created without a reservation.
        """
        if self._slot_token is None:
            return  # type: ignore[return-value]
        if self._dispatched and not force:
            return  # type: ignore[return-value]

        return await self._replica.release_slot(self._slot_token)
