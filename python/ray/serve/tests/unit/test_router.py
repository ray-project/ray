import asyncio
import concurrent.futures
import random
import sys
import threading
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set, Tuple
from unittest.mock import Mock, patch

import pytest

import ray
from ray._common.test_utils import async_wait_for_condition, wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray.exceptions import ActorDiedError, ActorUnavailableError
from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    RAY_SERVE_METRICS_EXPORT_INTERVAL_MS,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.request_router import (
    PendingRequest,
    RequestRouter,
    RunningReplica,
)
from ray.serve._private.request_router.common import ReplicaQueueLengthCache
from ray.serve._private.router import (
    QUEUED_REQUESTS_KEY,
    AsyncioRouter,
    RouterMetricsManager,
    SingletonThreadRouter,
)
from ray.serve._private.test_utils import FakeCounter, FakeGauge, MockTimer
from ray.serve._private.utils import get_random_string
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import BackPressureError


class FakeReplicaResult(ReplicaResult):
    def __init__(
        self,
        replica_id,
        is_generator_object: bool,
        queue_len_info: Optional[ReplicaQueueLengthInfo] = None,
    ):
        self._replica_id = replica_id
        self._is_generator_object = is_generator_object
        self._queue_len_info = queue_len_info
        self.cancelled = False

    async def get_rejection_response(self):
        return self._queue_len_info

    def get(self, timeout_s: Optional[float]):
        raise NotImplementedError

    async def get_async(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    async def __anext__(self):
        raise NotImplementedError

    def add_done_callback(self, callback: Callable):
        pass

    def cancel(self):
        self.cancelled = True

    def to_object_ref(self, timeout_s: Optional[float]) -> ray.ObjectRef:
        raise NotImplementedError

    async def to_object_ref_async(self) -> ray.ObjectRef:
        raise NotImplementedError

    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        raise NotImplementedError


class FakeReplica(RunningReplica):
    def __init__(
        self,
        replica_id: ReplicaID,
        *,
        queue_len_info: Optional[ReplicaQueueLengthInfo] = None,
        is_cross_language: bool = False,
        error: Optional[Exception] = None,
    ):
        self._replica_id = replica_id
        self._is_cross_language = is_cross_language
        self._queue_len_info = queue_len_info
        self._error = error

    @property
    def replica_id(self) -> ReplicaID:
        return self._replica_id

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    def get_queue_len(self, *, deadline_s: float) -> int:
        raise NotImplementedError

    def try_send_request(
        self, pr: PendingRequest, with_rejection: bool
    ) -> FakeReplicaResult:
        if with_rejection:
            if self._error:
                raise self._error

            assert (
                not self.is_cross_language
            ), "Rejection not supported for cross language."
            assert (
                self._queue_len_info is not None
            ), "Must set queue_len_info to use `send_request_with_rejection`."

            return FakeReplicaResult(
                self._replica_id,
                is_generator_object=True,
                queue_len_info=self._queue_len_info,
            )
        else:
            if pr.metadata.is_streaming:
                return FakeReplicaResult(self._replica_id, is_generator_object=True)
            else:
                return FakeReplicaResult(self._replica_id, is_generator_object=False)


class FakeRequestRouter(RequestRouter):
    def __init__(self, use_queue_len_cache: bool):
        self._block_requests = False
        self._blocked_requests: List[asyncio.Event] = []
        self._replica_to_return: Optional[FakeReplica] = None
        self._replica_to_return_on_retry: Optional[FakeReplica] = None
        self._replica_queue_len_cache = ReplicaQueueLengthCache()
        self._dropped_replicas: Set[ReplicaID] = set()
        self._use_queue_len_cache = use_queue_len_cache
        self.on_request_routed_called = False

    def create_replica_wrapper(self, replica_info: RunningReplicaInfo):
        return FakeReplica(replica_info)

    @property
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        return self._replica_queue_len_cache

    @property
    def dropped_replicas(self) -> Set[ReplicaID]:
        return self._dropped_replicas

    @property
    def curr_replicas(self) -> Dict[ReplicaID, RunningReplica]:
        replicas = {}
        if self._replica_to_return is not None:
            replicas[self._replica_to_return.replica_id] = self._replica_to_return
        if self._replica_to_return_on_retry is not None:
            replicas[
                self._replica_to_return_on_retry.replica_id
            ] = self._replica_to_return_on_retry

        return replicas

    def update_replicas(self, replicas: List[RunningReplica]):
        pass

    def on_replica_actor_died(self, replica_id: ReplicaID):
        self._dropped_replicas.add(replica_id)

    def on_new_queue_len_info(
        self, replica_id: ReplicaID, queue_len_info: ReplicaQueueLengthInfo
    ):
        if self._use_queue_len_cache:
            self._replica_queue_len_cache.update(
                replica_id, queue_len_info.num_ongoing_requests
            )

    def on_send_request(self, replica_id: ReplicaID):
        if self._use_queue_len_cache:
            num_ongoing_requests = self._replica_queue_len_cache.get(replica_id) or 0
            self._replica_queue_len_cache.update(replica_id, num_ongoing_requests + 1)

    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        self._replica_queue_len_cache.invalidate_key(replica_id)

    def set_should_block_requests(self, block_requests: bool):
        self._block_requests = block_requests

    def set_replica_to_return(self, replica: FakeReplica):
        self._replica_to_return = replica

    def set_replica_to_return_on_retry(self, replica: FakeReplica):
        self._replica_to_return_on_retry = replica

    def unblock_requests(self, num: int):
        assert self._block_requests and len(self._blocked_requests) >= num
        for _ in range(num):
            self._blocked_requests.pop(0).set()

    async def _choose_replica_for_request(
        self, pr: PendingRequest, *, is_retry: bool = False
    ) -> FakeReplica:
        if self._block_requests:
            event = asyncio.Event()
            self._blocked_requests.append(event)
            await event.wait()

        if is_retry:
            assert (
                self._replica_to_return_on_retry is not None
            ), "Set a replica to return on retry."
            replica = self._replica_to_return_on_retry
        else:
            assert self._replica_to_return is not None, "Set a replica to return."
            replica = self._replica_to_return

        return replica

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        pass

    def on_request_routed(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        self.on_request_routed_called = True


@pytest.fixture
@pytest.mark.asyncio
def setup_router(request) -> Tuple[AsyncioRouter, FakeRequestRouter]:
    if not hasattr(request, "param"):
        request.param = {}

    fake_request_router = FakeRequestRouter(
        request.param.get("enable_queue_len_cache", False)
    )
    router = AsyncioRouter(
        # TODO(edoakes): refactor to make a better fake controller or not depend on it.
        controller_handle=Mock(),
        deployment_id=DeploymentID(name="test-deployment"),
        handle_id="test-handle-id",
        self_actor_id="test-node-id",
        handle_source=DeploymentHandleSource.UNKNOWN,
        event_loop=get_or_create_event_loop(),
        enable_strict_max_ongoing_requests=request.param.get(
            "enable_strict_max_ongoing_requests", False
        ),
        request_router=fake_request_router,
        node_id="test-node-id",
        availability_zone="test-az",
        prefer_local_node_routing=False,
        _request_router_initialized_event=asyncio.Event(),
    )
    return router, fake_request_router


def dummy_request_metadata(is_streaming: bool = False) -> RequestMetadata:
    return RequestMetadata(
        request_id="test-request-1",
        internal_request_id="test-internal-request-1",
        is_streaming=is_streaming,
    )


@pytest.mark.asyncio
class TestAssignRequest:
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_basic(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        is_streaming: bool,
    ):
        router, fake_request_router = setup_router

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
            is_streaming=is_streaming,
        )
        replica_result = await router.assign_request(request_metadata)
        if is_streaming:
            assert replica_result._is_generator_object
            assert replica_result._replica_id == r1_id
        else:
            assert not replica_result._is_generator_object
            assert replica_result._replica_id == r1_id

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": False,
            },
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_basic_with_rejection(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        is_streaming: bool,
    ):
        router, fake_request_router = setup_router

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(
            r1_id,
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=True, num_ongoing_requests=10
            ),
        )
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
            is_streaming=is_streaming,
        )
        replica_result = await router.assign_request(request_metadata)
        assert replica_result._is_generator_object
        assert replica_result._replica_id == r1_id

        if router._request_router._use_queue_len_cache:
            assert fake_request_router.replica_queue_len_cache.get(r1_id) == 10

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": False,
            },
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_retry_with_rejection(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        is_streaming: bool,
    ):
        router, fake_request_router = setup_router

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica1 = FakeReplica(
            r1_id,
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=False, num_ongoing_requests=10
            ),
        )
        fake_request_router.set_replica_to_return(replica1)

        r2_id = ReplicaID(
            unique_id="test-replica-2", deployment_id=DeploymentID(name="test")
        )
        replica2 = FakeReplica(
            r2_id,
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=True, num_ongoing_requests=20
            ),
        )
        fake_request_router.set_replica_to_return_on_retry(replica2)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
            is_streaming=is_streaming,
        )
        replica_result = await router.assign_request(request_metadata)
        assert replica_result._is_generator_object
        assert replica_result._replica_id == r2_id

        if router._request_router._use_queue_len_cache:
            assert fake_request_router.replica_queue_len_cache.get(r1_id) == 10
            assert fake_request_router.replica_queue_len_cache.get(r2_id) == 20

    @pytest.mark.parametrize(
        "setup_router",
        [{"enable_strict_max_ongoing_requests": True}],
        indirect=True,
    )
    async def test_cross_lang_no_rejection(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id, is_cross_language=True)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )
        replica_result = await router.assign_request(request_metadata)
        assert not replica_result._is_generator_object
        assert replica_result._replica_id == r1_id

    async def test_max_queued_requests_no_limit(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router
        fake_request_router.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=-1))

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        # Queued a bunch of tasks. None should error because there's no limit.
        assign_request_tasks = [
            asyncio.ensure_future(router.assign_request(request_metadata))
            for _ in range(100)
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Unblock the requests, now they should all get routed.
        fake_request_router.unblock_requests(100)
        assert all(
            [
                not replica_result._is_generator_object
                and replica_result._replica_id == r1_id
                for replica_result in await asyncio.gather(*assign_request_tasks)
            ]
        )

    async def test_max_queued_requests_limited(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router
        fake_request_router.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=5))

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        # Queued `max_queued_requests` tasks. None should fail.
        assign_request_tasks = [
            asyncio.ensure_future(router.assign_request(request_metadata))
            for _ in range(5)
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Try to queue more tasks, they should fail immediately.
        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        # Unblock a request.
        fake_request_router.unblock_requests(1)
        done, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(done) == 1
        replica_result = await done.pop()
        assert not replica_result._is_generator_object
        assert replica_result._replica_id == r1_id

        # One more task should be allowed to be queued.
        assign_request_tasks = list(pending) + [
            asyncio.ensure_future(router.assign_request(request_metadata))
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Unblock the requests, now they should all get routed.
        fake_request_router.unblock_requests(5)
        assert all(
            [
                not replica_result._is_generator_object
                and replica_result._replica_id == r1_id
                for replica_result in await asyncio.gather(*assign_request_tasks)
            ]
        )

    async def test_max_queued_requests_updated(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router
        fake_request_router.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=5))

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        # Queued `max_queued_requests` tasks. None should fail.
        assign_request_tasks = [
            asyncio.ensure_future(router.assign_request(request_metadata))
            for _ in range(5)
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Try to queue more tasks, they should fail immediately.
        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        # Dynamically increase `max_queued_requests`, more tasks should be allowed to
        # be queued.
        router.update_deployment_config(DeploymentConfig(max_queued_requests=10))
        assign_request_tasks.extend(
            [
                asyncio.ensure_future(router.assign_request(request_metadata))
                for _ in range(5)
            ]
        )

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Try to queue more tasks, they should fail immediately.
        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        # Dynamically decrease `max_queued_requests`, the existing tasks should remain
        # queued but the limit should apply to new tasks.
        router.update_deployment_config(DeploymentConfig(max_queued_requests=5))
        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        fake_request_router.unblock_requests(5)
        done, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == 5
        assert all(
            [
                not replica_result._is_generator_object
                and replica_result._replica_id == r1_id
                for replica_result in await asyncio.gather(*done)
            ]
        )
        assign_request_tasks = list(pending)

        # Try to queue more tasks, they should fail immediately if the new limit is
        # respected.
        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        # Unblock the requests, now they should all get routed.
        fake_request_router.unblock_requests(5)
        assert all(
            [
                not replica_result._is_generator_object
                and replica_result._replica_id == r1_id
                for replica_result in await asyncio.gather(*assign_request_tasks)
            ]
        )

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    async def test_replica_actor_died(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router
        d_id = DeploymentID(name="test")
        r1_id = ReplicaID(unique_id="r1", deployment_id=d_id)
        r2_id = ReplicaID(unique_id="r2", deployment_id=d_id)

        fake_request_router.set_replica_to_return(
            FakeReplica(r1_id, error=ActorDiedError())
        )
        fake_request_router.set_replica_to_return_on_retry(
            FakeReplica(
                r2_id,
                queue_len_info=ReplicaQueueLengthInfo(
                    accepted=True, num_ongoing_requests=5
                ),
            )
        )
        await router.assign_request(dummy_request_metadata())
        assert r1_id in fake_request_router.dropped_replicas

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_ongoing_requests": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    async def test_replica_actor_unavailable(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ):
        router, fake_request_router = setup_router
        # Two replicas
        d_id = DeploymentID(name="test")
        r1_id = ReplicaID(unique_id="r1", deployment_id=d_id)
        r2_id = ReplicaID(unique_id="r2", deployment_id=d_id)

        # First request is sent to r1, cache should be populated with r1:5
        fake_request_router.set_replica_to_return(
            FakeReplica(
                r1_id,
                queue_len_info=ReplicaQueueLengthInfo(
                    accepted=True, num_ongoing_requests=5
                ),
            )
        )
        replica_result = await router.assign_request(dummy_request_metadata())
        assert replica_result._replica_id == r1_id
        # Cache should have R1:5
        assert fake_request_router.replica_queue_len_cache.get(r1_id) == 5
        assert fake_request_router.replica_queue_len_cache.get(r2_id) is None

        # Second request is sent to r2, cache should be populated with r2:10
        fake_request_router.set_replica_to_return(
            FakeReplica(
                r2_id,
                queue_len_info=ReplicaQueueLengthInfo(
                    accepted=True, num_ongoing_requests=10
                ),
            )
        )
        replica_result = await router.assign_request(dummy_request_metadata())
        assert replica_result._replica_id == r2_id
        # Cache should have R1:5, R2:10
        assert fake_request_router.replica_queue_len_cache.get(r1_id) == 5
        assert fake_request_router.replica_queue_len_cache.get(r2_id) == 10

        # Third request is sent to r1 again, but system message yields
        # an ActorUnavailableError
        fake_request_router.set_replica_to_return(
            FakeReplica(
                r1_id,
                error=ActorUnavailableError(error_message="unavailable", actor_id=None),
            )
        )
        fake_request_router.set_replica_to_return_on_retry(
            FakeReplica(
                r2_id,
                queue_len_info=ReplicaQueueLengthInfo(
                    accepted=True, num_ongoing_requests=15
                ),
            )
        )
        await router.assign_request(dummy_request_metadata())
        # R1 should be REMOVED from cache, cache should now be R2:15
        assert fake_request_router.replica_queue_len_cache.get(r1_id) is None
        assert fake_request_router.replica_queue_len_cache.get(r2_id) == 15

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_ongoing_requests": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_on_request_routed(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        is_streaming: bool,
    ):
        """Test that on_request_routed is called when a request is routed."""
        router, fake_request_router = setup_router

        # Before the request is routed, on_request_routed_called should be False.
        assert fake_request_router.on_request_routed_called is False

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(
            r1_id,
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=True, num_ongoing_requests=1
            ),
        )
        fake_request_router.set_replica_to_return(replica)

        replica_result = await router.assign_request(dummy_request_metadata())
        assert replica_result._replica_id == r1_id

        # After the request is routed, on_request_routed_called should be False.
        assert fake_request_router.on_request_routed_called is True


def running_replica_info(replica_id: ReplicaID) -> RunningReplicaInfo:
    return RunningReplicaInfo(
        replica_id=replica_id,
        node_id="node_id",
        node_ip="node_ip",
        availability_zone="some-az",
        actor_name=replica_id.to_full_id_str(),
        max_ongoing_requests=1,
    )


class TestRouterMetricsManager:
    @pytest.mark.asyncio
    async def test_num_router_requests(self):
        tags = {
            "deployment": "a",
            "application": "b",
            "route": "/alice",
            "handle": "random_handle",
            "actor_id": "random_actor",
        }
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random_handle",
            "random_actor",
            DeploymentHandleSource.UNKNOWN,
            Mock(),
            FakeCounter(
                tag_keys=("deployment", "route", "application", "handle", "actor_id")
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )
        assert metrics_manager.num_router_requests.get_count(tags) is None

        n = random.randint(1, 10)
        for _ in range(n):
            metrics_manager.inc_num_total_requests(route="/alice")

        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)
        assert metrics_manager.num_router_requests.get_count(tags) == n

    @pytest.mark.asyncio
    async def test_num_queued_requests_gauge(self):
        tags = {
            "deployment": "a",
            "application": "b",
            "handle": "random_handle",
            "actor_id": "random_actor",
        }
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random_handle",
            "random_actor",
            DeploymentHandleSource.UNKNOWN,
            Mock(),
            FakeCounter(
                tag_keys=("deployment", "route", "application", "handle", "actor_id")
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )
        assert metrics_manager.num_queued_requests_gauge.get_value(tags) == 0

        n, m = random.randint(0, 10), random.randint(0, 5)
        for _ in range(n):
            metrics_manager.inc_num_queued_requests()
        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)
        assert metrics_manager.num_queued_requests_gauge.get_value(tags) == n
        for _ in range(m):
            metrics_manager.dec_num_queued_requests()

        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)
        assert metrics_manager.num_queued_requests_gauge.get_value(tags) == n - m

    @pytest.mark.asyncio
    async def test_track_requests_sent_to_replicas(self):
        d_id = DeploymentID(name="a", app_name="b")
        metrics_manager = RouterMetricsManager(
            d_id,
            "random",
            "random_actor",
            DeploymentHandleSource.UNKNOWN,
            Mock(),
            FakeCounter(
                tag_keys=("deployment", "route", "application", "handle", "actor_id")
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )

        # r1: number requests -> 0, removed from list of running replicas -> prune
        # r2: number requests -> 0, remains on list of running replicas -> don't prune
        # r3: number requests > 0, removed from list of running replicas -> don't prune
        # r4: number requests > 0, remains on list of running replicas -> don't prune
        replica_ids = [
            ReplicaID(unique_id=f"test-replica-{i}", deployment_id=d_id)
            for i in range(1, 5)
        ]
        r1, r2, r3, r4 = replica_ids

        # ri has i requests
        for i in range(4):
            for _ in range(i + 1):
                metrics_manager.inc_num_running_requests_for_replica(replica_ids[i])
        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)

        # All 4 replicas should have a positive number of requests
        for i, r in enumerate(replica_ids):
            assert metrics_manager.num_requests_sent_to_replicas[r] == i + 1
        assert (
            metrics_manager.num_running_requests_gauge.get_value(
                {
                    "deployment": "a",
                    "application": "b",
                    "handle": "random",
                    "actor_id": "random_actor",
                }
            )
            == 10
        )

        # Requests at r1 and r2 drop to 0
        for _ in range(1):
            metrics_manager.dec_num_running_requests_for_replica(r1)
        for _ in range(2):
            metrics_manager.dec_num_running_requests_for_replica(r2)
        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)
        assert metrics_manager.num_requests_sent_to_replicas[r1] == 0
        assert metrics_manager.num_requests_sent_to_replicas[r2] == 0

        # 3 requests finished processing
        assert (
            metrics_manager.num_running_requests_gauge.get_value(
                {
                    "deployment": "a",
                    "application": "b",
                    "handle": "random",
                    "actor_id": "random_actor",
                }
            )
            == 7
        )

        # Running replicas reduces to [r2, r4]
        metrics_manager._update_running_replicas(
            [
                running_replica_info(r2),
                running_replica_info(r4),
            ]
        )
        await asyncio.sleep(RAY_SERVE_METRICS_EXPORT_INTERVAL_MS * 2 / 1000)

        # Only r1 should be pruned, the rest should still be tracked.
        assert r1 not in metrics_manager.num_requests_sent_to_replicas
        assert r2 in metrics_manager.num_requests_sent_to_replicas
        assert r3 in metrics_manager.num_requests_sent_to_replicas
        assert r4 in metrics_manager.num_requests_sent_to_replicas

    @pytest.mark.asyncio
    async def test_should_send_scaled_to_zero_optimized_push(self):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            "random_actor",
            DeploymentHandleSource.UNKNOWN,
            Mock(),
            FakeCounter(
                tag_keys=("deployment", "route", "application", "handle", "actor_id")
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )

        # Not an autoscaling deployment, should not push metrics
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(0)

        # No queued requests at the handle, should not push metrics
        metrics_manager._deployment_config = DeploymentConfig(
            autoscaling_config=AutoscalingConfig()
        )
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(0)

        # Current number of replicas is non-zero, should not push metrics
        metrics_manager.inc_num_queued_requests()
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(1)

        # All 3 conditions satisfied, should push metrics
        assert metrics_manager.should_send_scaled_to_zero_optimized_push(0)

    @pytest.mark.asyncio
    @patch(
        "ray.serve._private.router.RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1"
    )
    async def test_push_autoscaling_metrics_to_controller(self):
        timer = MockTimer()
        start = random.randint(50, 100)
        timer.reset(start)
        deployment_id = DeploymentID(name="a", app_name="b")
        handle_id = "random"
        self_actor_id = "abc"
        mock_controller_handle = Mock()

        with patch("time.time", new=timer.time):
            metrics_manager = RouterMetricsManager(
                deployment_id,
                handle_id,
                self_actor_id,
                DeploymentHandleSource.PROXY,
                mock_controller_handle,
                FakeCounter(
                    tag_keys=(
                        "deployment",
                        "route",
                        "application",
                        "handle",
                        "actor_id",
                    )
                ),
                FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
                FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
                event_loop=asyncio.get_event_loop(),
            )
            metrics_manager._deployment_config = DeploymentConfig(
                autoscaling_config=AutoscalingConfig()
            )

            # Set up some requests
            n = random.randint(0, 5)
            replica_ids = [
                ReplicaID(get_random_string(), DeploymentID("d", "a")) for _ in range(3)
            ]
            running_requests = defaultdict(int)
            for _ in range(n):
                metrics_manager.inc_num_queued_requests()
            for _ in range(20):
                r = random.choice(replica_ids)
                running_requests[r] += 1
                metrics_manager.inc_num_running_requests_for_replica(r)

            # Check metrics are pushed correctly
            metrics_manager.push_autoscaling_metrics_to_controller()
            handle_metric_report = metrics_manager._get_metrics_report()
            mock_controller_handle.record_autoscaling_metrics_from_handle.remote.assert_called_with(
                handle_metric_report
            )

    @pytest.mark.skipif(
        not RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
        reason="Tests handle metrics behavior.",
    )
    @pytest.mark.asyncio
    @patch(
        "ray.serve._private.router.RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S",
        0.01,
    )
    async def test_memory_cleared(self):
        deployment_id = DeploymentID(name="a", app_name="b")
        metrics_manager = RouterMetricsManager(
            deployment_id,
            "some_handle",
            "some_actor",
            DeploymentHandleSource.PROXY,
            Mock(),
            FakeCounter(
                tag_keys=(
                    "deployment",
                    "route",
                    "application",
                    "handle",
                    "actor_id",
                )
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )
        metrics_manager.update_deployment_config(
            deployment_config=DeploymentConfig(
                autoscaling_config=AutoscalingConfig(
                    metrics_interval_s=0.005, look_back_period_s=0.01
                )
            ),
            curr_num_replicas=0,
        )

        r1 = ReplicaID("r1", deployment_id)
        r2 = ReplicaID("r2", deployment_id)
        r3 = ReplicaID("r3", deployment_id)

        def check_database(expected: Set[ReplicaID]):
            assert set(metrics_manager.metrics_store.data) == expected
            return True

        # r1: 1
        metrics_manager.inc_num_running_requests_for_replica(r1)
        await async_wait_for_condition(
            check_database, expected={r1, QUEUED_REQUESTS_KEY}
        )

        # r1: 1, r2: 0
        metrics_manager.inc_num_running_requests_for_replica(r2)
        await async_wait_for_condition(
            check_database, expected={r1, r2, QUEUED_REQUESTS_KEY}
        )
        metrics_manager.dec_num_running_requests_for_replica(r2)

        # r1: 1, r2: 0, r3: 0
        metrics_manager.inc_num_running_requests_for_replica(r3)
        await async_wait_for_condition(
            check_database, expected={r1, r2, r3, QUEUED_REQUESTS_KEY}
        )
        metrics_manager.dec_num_running_requests_for_replica(r3)

        # update running replicas {r2}
        metrics_manager._update_running_replicas([running_replica_info(r2)])
        await async_wait_for_condition(
            check_database, expected={r1, r2, QUEUED_REQUESTS_KEY}
        )

    @pytest.mark.asyncio
    @patch(
        "ray.serve._private.router.RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1"
    )
    @patch("ray.serve._private.router.MetricsPusher")
    async def test_update_deployment_config(self, metrics_pusher_mock):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            "random_actor",
            DeploymentHandleSource.UNKNOWN,
            Mock(),
            FakeCounter(
                tag_keys=("deployment", "route", "application", "handle", "actor_id")
            ),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            FakeGauge(tag_keys=("deployment", "application", "handle", "actor_id")),
            event_loop=asyncio.get_event_loop(),
        )

        # Without autoscaling config, do nothing
        metrics_manager.update_deployment_config(DeploymentConfig(), 0)
        metrics_manager.metrics_pusher.register_or_update_task.assert_not_called()

        # With autoscaling config, register or update task should be called
        metrics_manager.update_deployment_config(
            DeploymentConfig(autoscaling_config=AutoscalingConfig()), 0
        )
        metrics_manager.metrics_pusher.register_or_update_task.assert_called()


class TestSingletonThreadRouter:
    @pytest.fixture
    def setup_singleton_thread_router(
        self, setup_router: Tuple[AsyncioRouter, FakeRequestRouter]
    ) -> SingletonThreadRouter:
        asyncio_router, fake_request_router = setup_router

        # Mock ray.get_runtime_context() to avoid triggering Ray initialization
        with patch("ray.get_runtime_context") as mock_context:
            mock_context.return_value.get_actor_id.return_value = "test-actor-id"

            router = SingletonThreadRouter(
                controller_handle=Mock(),
                deployment_id=DeploymentID(name="test", app_name="test"),
                handle_id="test",
                self_actor_id="test",
                handle_source="test",
                request_router=fake_request_router,
                enable_strict_max_ongoing_requests=False,
                resolve_request_arg_func=Mock(),
                node_id="test-node-id",
                availability_zone="test-az",
                prefer_local_node_routing=False,
            )
        router._asyncio_router = asyncio_router
        return router

    def test_request_assignment(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        setup_singleton_thread_router: SingletonThreadRouter,
    ):
        _, fake_request_router = setup_router
        thread_router = setup_singleton_thread_router

        r1_id = ReplicaID(
            unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
        )
        replica = FakeReplica(r1_id)
        fake_request_router.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        future = thread_router.assign_request(request_metadata)
        assert isinstance(future, concurrent.futures.Future)
        assert future.result()._replica_id == r1_id

    @pytest.mark.asyncio
    async def test_cancellation_propagation(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        setup_singleton_thread_router: SingletonThreadRouter,
    ):
        fake_router, _ = setup_router
        thread_router = setup_singleton_thread_router

        loop = thread_router._get_singleton_asyncio_loop(component="unknown")

        async def init_events():
            return asyncio.Event()

        f = asyncio.run_coroutine_threadsafe(init_events(), loop)
        lock_acquire_event = f.result()
        cancel_block_event = threading.Event()

        fake_replica_result = FakeReplicaResult(
            replica_id=ReplicaID(
                unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
            ),
            is_generator_object=False,
        )

        async def mock_assign_request(*args, **kwargs):
            try:
                await lock_acquire_event.wait()
                return fake_replica_result
            except asyncio.CancelledError:
                cancel_block_event.wait()
                fake_replica_result.cancel()
                raise

        fake_router.assign_request = mock_assign_request

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        assign_request_future = thread_router.assign_request(request_metadata)
        assign_request_future.cancel()

        assert fake_replica_result.cancelled is False

        cancel_block_event.set()

        def release_lock():
            lock_acquire_event.set()

        loop.call_soon_threadsafe(release_lock)

        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.result()
        assert assign_request_future.cancelled() is True
        wait_for_condition(lambda: fake_replica_result.cancelled is True)

    @pytest.mark.asyncio
    async def test_replica_result_cancellation(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        setup_singleton_thread_router: SingletonThreadRouter,
    ):
        fake_router, _ = setup_router
        thread_router = setup_singleton_thread_router

        fake_replica_result = FakeReplicaResult(
            replica_id=ReplicaID(
                unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
            ),
            is_generator_object=False,
        )

        event = threading.Event()

        async def mock_assign_request(*args, **kwargs):
            # this is a blocking call that will block the asyncio loop
            event.wait()
            return fake_replica_result

        fake_router.assign_request = mock_assign_request

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )
        assign_request_future = thread_router.assign_request(request_metadata)
        assign_request_future.cancel()

        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.result()

        assert assign_request_future.cancelled() is True
        assert fake_replica_result.cancelled is False

        event.set()
        wait_for_condition(lambda: fake_replica_result.cancelled is True)

    @pytest.mark.asyncio
    async def test_assign_request_with_exception(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        setup_singleton_thread_router: SingletonThreadRouter,
    ):
        fake_router, _ = setup_router
        thread_router = setup_singleton_thread_router

        async def mock_assign_request(*args, **kwargs):
            raise Exception("test exception")

        fake_router.assign_request = mock_assign_request

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )
        assign_request_future = thread_router.assign_request(request_metadata)

        assert assign_request_future.exception() is not None
        assert str(assign_request_future.exception()) == "test exception"

    @pytest.mark.asyncio
    async def test_assign_request_with_exception_during_cancellation(
        self,
        setup_router: Tuple[AsyncioRouter, FakeRequestRouter],
        setup_singleton_thread_router: SingletonThreadRouter,
    ):
        fake_router, _ = setup_router
        thread_router = setup_singleton_thread_router

        mock_replica_result = FakeReplicaResult(
            replica_id=ReplicaID(
                unique_id="test-replica-1", deployment_id=DeploymentID(name="test")
            ),
            is_generator_object=False,
        )
        loop = thread_router._get_singleton_asyncio_loop(component="unknown")

        async def init_events():
            return asyncio.Event(), asyncio.Event()

        f = asyncio.run_coroutine_threadsafe(init_events(), loop)
        lock_acquire_event, cancel_block_event = f.result()

        async def mock_assign_request(*args, **kwargs):
            try:
                await lock_acquire_event.wait()
                return mock_replica_result
            except asyncio.CancelledError:
                cancel_block_event.set()
                raise Exception("test exception")

        fake_router.assign_request = mock_assign_request

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            internal_request_id="test-internal-request-1",
        )

        assign_request_future = thread_router.assign_request(request_metadata)
        assign_request_future.cancel()

        async def coro():
            await cancel_block_event.wait()

        asyncio.run_coroutine_threadsafe(coro(), loop).result()

        def release_lock():
            lock_acquire_event.set()

        loop.call_soon_threadsafe(release_lock)

        assert assign_request_future.cancelled() is True
        with pytest.raises(concurrent.futures.CancelledError):
            assign_request_future.exception()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
