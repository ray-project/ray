import asyncio
import random
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union
from unittest.mock import Mock, patch

import pytest

from ray._private.utils import get_or_create_event_loop
from ray.serve._private.common import (
    DeploymentID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.replica_scheduler import (
    PendingRequest,
    ReplicaScheduler,
    ReplicaWrapper,
)
from ray.serve._private.replica_scheduler.pow_2_scheduler import ReplicaQueueLengthCache
from ray.serve._private.router import Router, RouterMetricsManager
from ray.serve._private.test_utils import FakeCounter, FakeGauge, MockTimer
from ray.serve._private.utils import get_random_string
from ray.serve.config import AutoscalingConfig
from ray.serve.exceptions import BackPressureError


class FakeObjectRefOrGen:
    def __init__(self, replica_id: str):
        self._replica_id = replica_id

    @property
    def replica_id(self) -> str:
        return self._replica_id


class FakeObjectRef(FakeObjectRefOrGen):
    def __await__(self):
        raise NotImplementedError


class FakeObjectRefGen(FakeObjectRefOrGen):
    def __anext__(self):
        raise NotImplementedError


class FakeReplica(ReplicaWrapper):
    def __init__(
        self,
        replica_id: str,
        *,
        queue_len_info: Optional[ReplicaQueueLengthInfo] = None,
        is_cross_language: bool = False
    ):
        self._replica_id = replica_id
        self._is_cross_language = is_cross_language
        self._queue_len_info = queue_len_info

    @property
    def replica_id(self) -> str:
        return self._replica_id

    @property
    def is_cross_language(self) -> bool:
        return self._is_cross_language

    def send_request(
        self, pr: PendingRequest
    ) -> Union[FakeObjectRef, FakeObjectRefGen]:
        obj_ref_or_gen = None
        if pr.metadata.is_streaming:
            obj_ref_or_gen = FakeObjectRefGen(self._replica_id)
        else:
            obj_ref_or_gen = FakeObjectRef(self._replica_id)

        return obj_ref_or_gen

    async def send_request_with_rejection(
        self,
        pr: PendingRequest,
    ) -> Tuple[Optional[FakeObjectRefGen], ReplicaQueueLengthInfo]:
        assert not self.is_cross_language, "Rejection not supported for cross language."
        assert (
            self._queue_len_info is not None
        ), "Must set queue_len_info to use `send_request_with_rejection`."

        return FakeObjectRefGen(self._replica_id), self._queue_len_info


class FakeReplicaScheduler(ReplicaScheduler):
    def __init__(self, **kwargs):
        self._block_requests = False
        self._blocked_requests: List[asyncio.Event] = []
        self._replica_to_return: Optional[FakeReplica] = None
        self._replica_to_return_on_retry: Optional[FakeReplica] = None
        self._replica_queue_len_cache = ReplicaQueueLengthCache()

    def set_should_block_requests(self, block_requests: bool):
        self._block_requests = block_requests

    @property
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        return self._replica_queue_len_cache

    @property
    def curr_replicas(self) -> Dict[str, ReplicaWrapper]:
        replicas = {}
        if self._replica_to_return is not None:
            replicas[self._replica_to_return.replica_id] = self._replica_to_return
        if self._replica_to_return_on_retry is not None:
            replicas[
                self._replica_to_return_on_retry.replica_id
            ] = self._replica_to_return_on_retry

        return replicas

    def update_replicas(self, replicas: List[ReplicaWrapper]):
        raise NotImplementedError

    def set_replica_to_return(self, replica: FakeReplica):
        self._replica_to_return = replica

    def set_replica_to_return_on_retry(self, replica: FakeReplica):
        self._replica_to_return_on_retry = replica

    def unblock_requests(self, num: int):
        assert self._block_requests and len(self._blocked_requests) >= num
        for _ in range(num):
            self._blocked_requests.pop(0).set()

    async def choose_replica_for_request(
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


@pytest.fixture
@pytest.mark.asyncio
def setup_router(request) -> Tuple[Router, FakeReplicaScheduler]:
    if not hasattr(request, "param"):
        request.param = {}

    router = Router(
        # TODO(edoakes): refactor to make a better fake controller or not depend on it.
        controller_handle=Mock(),
        deployment_id=DeploymentID(name="test-deployment"),
        handle_id="test-handle-id",
        self_node_id="test-node-id",
        self_actor_id="test-node-id",
        self_availability_zone="test-az",
        event_loop=get_or_create_event_loop(),
        _prefer_local_node_routing=False,
        # TODO(edoakes): just pass a class instance here.
        _router_cls="ray.serve.tests.unit.test_router.FakeReplicaScheduler",
        enable_queue_len_cache=request.param.get("enable_queue_len_cache", False),
        enable_strict_max_concurrent_queries=request.param.get(
            "enable_strict_max_concurrent_queries", False
        ),
    )
    return router, router._replica_scheduler


@pytest.mark.asyncio
class TestAssignRequest:
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_basic(
        self, setup_router: Tuple[Router, FakeReplicaScheduler], is_streaming: bool
    ):
        router, fake_replica_scheduler = setup_router

        replica = FakeReplica("test-replica-1")
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
            is_streaming=is_streaming,
        )
        obj_ref = await router.assign_request(request_metadata)
        if is_streaming:
            assert isinstance(obj_ref, FakeObjectRefGen)
        else:
            assert isinstance(obj_ref, FakeObjectRef)
        assert obj_ref.replica_id == "test-replica-1"

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_concurrent_queries": True,
                "enable_queue_len_cache": False,
            },
            {
                "enable_strict_max_concurrent_queries": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_basic_with_rejection(
        self, setup_router: Tuple[Router, FakeReplicaScheduler], is_streaming: bool
    ):
        router, fake_replica_scheduler = setup_router

        replica = FakeReplica(
            "test-replica-1",
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=True, num_ongoing_requests=10
            ),
        )
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
            is_streaming=is_streaming,
        )
        obj_ref_gen = await router.assign_request(request_metadata)
        assert isinstance(obj_ref_gen, FakeObjectRefGen)
        assert obj_ref_gen.replica_id == "test-replica-1"

        if router._enable_queue_len_cache:
            assert (
                fake_replica_scheduler.replica_queue_len_cache.get("test-replica-1")
                == 10
            )

    @pytest.mark.parametrize(
        "setup_router",
        [
            {
                "enable_strict_max_concurrent_queries": True,
                "enable_queue_len_cache": False,
            },
            {
                "enable_strict_max_concurrent_queries": True,
                "enable_queue_len_cache": True,
            },
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("is_streaming", [False, True])
    async def test_retry_with_rejection(
        self, setup_router: Tuple[Router, FakeReplicaScheduler], is_streaming: bool
    ):
        router, fake_replica_scheduler = setup_router

        replica1 = FakeReplica(
            "test-replica-1",
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=False, num_ongoing_requests=10
            ),
        )
        fake_replica_scheduler.set_replica_to_return(replica1)
        replica2 = FakeReplica(
            "test-replica-2",
            queue_len_info=ReplicaQueueLengthInfo(
                accepted=True, num_ongoing_requests=20
            ),
        )
        fake_replica_scheduler.set_replica_to_return_on_retry(replica2)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
            is_streaming=is_streaming,
        )
        obj_ref = await router.assign_request(request_metadata)
        assert isinstance(obj_ref, FakeObjectRefGen)
        assert obj_ref.replica_id == "test-replica-2"

        if router._enable_queue_len_cache:
            assert (
                fake_replica_scheduler.replica_queue_len_cache.get("test-replica-1")
                == 10
            )
            assert (
                fake_replica_scheduler.replica_queue_len_cache.get("test-replica-2")
                == 20
            )

    @pytest.mark.parametrize(
        "setup_router",
        [
            {"enable_strict_max_concurrent_queries": True},
        ],
        indirect=True,
    )
    async def test_cross_lang_no_rejection(
        self, setup_router: Tuple[Router, FakeReplicaScheduler]
    ):
        router, fake_replica_scheduler = setup_router

        replica = FakeReplica("test-replica-1", is_cross_language=True)
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
        )
        obj_ref = await router.assign_request(request_metadata)
        assert isinstance(obj_ref, FakeObjectRef)
        assert obj_ref.replica_id == "test-replica-1"

    async def test_max_queued_requests_no_limit(
        self, setup_router: Tuple[Router, FakeReplicaScheduler]
    ):
        router, fake_replica_scheduler = setup_router
        fake_replica_scheduler.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=-1))

        replica = FakeReplica("test-replica-1")
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
        )

        # Queued a bunch of tasks. None should error because there's no limit.
        assign_request_tasks = [
            asyncio.ensure_future(router.assign_request(request_metadata))
            for _ in range(100)
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Unblock the requests, now they should all get scheduled.
        fake_replica_scheduler.unblock_requests(100)
        assert all(
            [
                isinstance(obj_ref, FakeObjectRef)
                and obj_ref.replica_id == "test-replica-1"
                for obj_ref in await asyncio.gather(*assign_request_tasks)
            ]
        )

    async def test_max_queued_requests_limited(
        self, setup_router: Tuple[Router, FakeReplicaScheduler]
    ):
        router, fake_replica_scheduler = setup_router
        fake_replica_scheduler.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=5))

        replica = FakeReplica("test-replica-1")
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
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
        fake_replica_scheduler.unblock_requests(1)
        done, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(done) == 1
        obj_ref = await done.pop()
        assert isinstance(obj_ref, FakeObjectRef)
        assert obj_ref.replica_id == "test-replica-1"

        # One more task should be allowed to be queued.
        assign_request_tasks = list(pending) + [
            asyncio.ensure_future(router.assign_request(request_metadata))
        ]

        _, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == len(assign_request_tasks)

        # Unblock the requests, now they should all get scheduled.
        fake_replica_scheduler.unblock_requests(5)
        assert all(
            [
                isinstance(obj_ref, FakeObjectRef)
                and obj_ref.replica_id == "test-replica-1"
                for obj_ref in await asyncio.gather(*assign_request_tasks)
            ]
        )

    async def test_max_queued_requests_updated(
        self, setup_router: Tuple[Router, FakeReplicaScheduler]
    ):
        router, fake_replica_scheduler = setup_router
        fake_replica_scheduler.set_should_block_requests(True)
        router.update_deployment_config(DeploymentConfig(max_queued_requests=5))

        replica = FakeReplica("test-replica-1")
        fake_replica_scheduler.set_replica_to_return(replica)

        request_metadata = RequestMetadata(
            request_id="test-request-1",
            endpoint="",
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

        fake_replica_scheduler.unblock_requests(5)
        done, pending = await asyncio.wait(assign_request_tasks, timeout=0.01)
        assert len(pending) == 5
        assert all(
            [
                isinstance(obj_ref, FakeObjectRef)
                and obj_ref.replica_id == "test-replica-1"
                for obj_ref in await asyncio.gather(*done)
            ]
        )
        assign_request_tasks = list(pending)

        # Try to queue more tasks, they should fail immediately if the new limit is
        # respected.
        for _ in range(10):
            with pytest.raises(BackPressureError):
                await router.assign_request(request_metadata)

        # Unblock the requests, now they should all get scheduled.
        fake_replica_scheduler.unblock_requests(5)
        assert all(
            [
                isinstance(obj_ref, FakeObjectRef)
                and obj_ref.replica_id == "test-replica-1"
                for obj_ref in await asyncio.gather(*assign_request_tasks)
            ]
        )


def running_replica_info(replica_tag: str) -> RunningReplicaInfo:
    return RunningReplicaInfo(
        deployment_name="f",
        replica_tag=replica_tag,
        node_id="node_id",
        availability_zone="some-az",
        actor_handle=Mock(),
        max_concurrent_queries=1,
    )


class TestRouterMetricsManager:
    def test_num_router_requests(self):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            get_or_create_event_loop(),
            Mock(),
            FakeCounter(tag_keys=("deployment", "route", "application")),
            FakeGauge(tag_keys=("deployment", "application")),
        )
        assert metrics_manager.num_router_requests.get_count() == 0

        n = random.randint(0, 10)
        for _ in range(n):
            metrics_manager.inc_num_total_requests(route="/alice")
        assert metrics_manager.num_router_requests.get_count() == n
        assert metrics_manager.num_router_requests.get_tags() == {
            "deployment": "a",
            "application": "b",
            "route": "/alice",
        }

    def test_num_queued_requests_gauge(self):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            get_or_create_event_loop(),
            Mock(),
            FakeCounter(tag_keys=("deployment", "route", "application")),
            FakeGauge(tag_keys=("deployment", "application")),
        )
        assert metrics_manager.num_queued_requests_gauge.get_value() == 0

        n, m = random.randint(0, 10), random.randint(0, 5)
        for _ in range(n):
            metrics_manager.inc_num_queued_requests()
        assert metrics_manager.num_queued_requests_gauge.get_value() == n
        for _ in range(m):
            metrics_manager.dec_num_queued_requests()
        assert metrics_manager.num_queued_requests_gauge.get_value() == n - m
        assert metrics_manager.num_queued_requests_gauge.get_tags() == {
            "deployment": "a",
            "application": "b",
        }

    def test_track_requests_sent_to_replicas(self):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            get_or_create_event_loop(),
            Mock(),
            FakeCounter(tag_keys=("deployment", "route", "application")),
            FakeGauge(tag_keys=("deployment", "application")),
        )

        # r1: number requests -> 0, removed from list of running replicas -> prune
        # r2: number requests -> 0, remains on list of running replicas -> don't prune
        # r3: number requests > 0, removed from list of running replicas -> don't prune
        # r4: number requests > 0, remains on list of running replicas -> don't prune
        replica_tags = [get_random_string() for _ in range(4)]
        r1, r2, r3, r4 = replica_tags

        # ri has i requests
        for i in range(4):
            for _ in range(i + 1):
                metrics_manager.inc_num_running_requests_for_replica(replica_tags[i])

        # All 4 replicas should have a positive number of requests
        for i, r in enumerate(replica_tags):
            assert metrics_manager.num_requests_sent_to_replicas[r] == i + 1

        # Requests at r1 and r2 drop to 0
        for _ in range(1):
            metrics_manager.process_finished_request(r1, None)
        for _ in range(2):
            metrics_manager.process_finished_request(r2, None)
        assert metrics_manager.num_requests_sent_to_replicas[r1] == 0
        assert metrics_manager.num_requests_sent_to_replicas[r2] == 0

        # Running replicas reduces to [r2, r4]
        metrics_manager.update_running_replicas(
            [
                running_replica_info(r2),
                running_replica_info(r4),
            ]
        )

        # Only r1 should be pruned, the rest should still be tracked.
        assert r1 not in metrics_manager.num_requests_sent_to_replicas
        assert r2 in metrics_manager.num_requests_sent_to_replicas
        assert r3 in metrics_manager.num_requests_sent_to_replicas
        assert r4 in metrics_manager.num_requests_sent_to_replicas

    def test_should_send_scaled_to_zero_optimized_push(self):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            get_or_create_event_loop(),
            Mock(),
            FakeCounter(tag_keys=("deployment", "route", "application")),
            FakeGauge(tag_keys=("deployment", "application")),
        )

        # Not an autoscaling deployment, should not push metrics
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(0)

        # No queued requests at the handle, should not push metrics
        metrics_manager.deployment_config = DeploymentConfig(
            autoscaling_config=AutoscalingConfig()
        )
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(0)

        # Current number of replicas is non-zero, should not push metrics
        metrics_manager.inc_num_queued_requests()
        assert not metrics_manager.should_send_scaled_to_zero_optimized_push(1)

        # All 3 conditions satisfied, should push metrics
        assert metrics_manager.should_send_scaled_to_zero_optimized_push(0)

    @patch(
        "ray.serve._private.router.RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1"
    )
    def test_push_autoscaling_metrics_to_controller(self):
        timer = MockTimer()
        start = random.randint(50, 100)
        timer.reset(start)
        deployment_id = DeploymentID(name="a", app_name="b")
        handle_id = "random"
        mock_controller_handle = Mock()

        with patch("time.time", new=timer.time):
            metrics_manager = RouterMetricsManager(
                deployment_id,
                handle_id,
                get_or_create_event_loop(),
                mock_controller_handle,
                FakeCounter(tag_keys=("deployment", "route", "application")),
                FakeGauge(tag_keys=("deployment", "application")),
            )
            metrics_manager.deployment_config = DeploymentConfig(
                autoscaling_config=AutoscalingConfig()
            )

            # Set up some requests
            n = random.randint(0, 5)
            replica_tags = [get_random_string() for _ in range(3)]
            running_requests = defaultdict(int)
            for _ in range(n):
                metrics_manager.inc_num_queued_requests()
            for _ in range(20):
                r = random.choice(replica_tags)
                running_requests[r] += 1
                metrics_manager.inc_num_running_requests_for_replica(r)

            # Check metrics are pushed correctly
            metrics_manager.push_autoscaling_metrics_to_controller()
            mock_controller_handle.record_handle_metrics.remote.assert_called_with(
                deployment_id=deployment_id,
                handle_id=handle_id,
                queued_requests=n,
                running_requests=running_requests,
                send_timestamp=start,
            )

    @patch(
        "ray.serve._private.router.RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE", "1"
    )
    @patch("ray.serve._private.router.MetricsPusher")
    def test_update_deployment_config(self, metrics_pusher_mock):
        metrics_manager = RouterMetricsManager(
            DeploymentID(name="a", app_name="b"),
            "random",
            get_or_create_event_loop(),
            Mock(),
            FakeCounter(tag_keys=("deployment", "route", "application")),
            FakeGauge(tag_keys=("deployment", "application")),
        )

        # Without autoscaling config, do nothing
        metrics_manager.update_deployment_config(DeploymentConfig(), 0)
        metrics_manager.metrics_pusher.register_or_update_task.assert_not_called()

        # With autoscaling config, register or update task should be called
        metrics_manager.update_deployment_config(
            DeploymentConfig(autoscaling_config=AutoscalingConfig()), 0
        )
        metrics_manager.metrics_pusher.register_or_update_task.assert_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
