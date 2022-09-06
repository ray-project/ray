from collections import defaultdict
from typing import List, Optional, Dict, Callable

from dataclasses import dataclass

import ray.actor
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

from ray.air.execution.impl.train.train_result import (
    TrainTrainingEvent,
    TrainInitEvent,
    TrainStartEvent,
    TrainSetupIPEvent,
)
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.event import FutureResult, MultiFutureResult, ExecutionEvent
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.train import BackendConfig
from ray.train._internal.backend_executor import TrainBackendError
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.train._internal.session import get_session, init_session
from ray.train._internal.worker_group import WorkerGroup, RayTrainWorker


@dataclass
class _StaticWorker:
    actor: ray.actor.ActorHandle


class _StaticWorkerGroup(WorkerGroup):
    def __init__(self, actors: List[ray.actor.ActorHandle], resources_per_worker: Dict):
        self.workers = [_StaticWorker(actor) for actor in actors]
        self.num_workers = len(self.workers)
        self.resources_per_worker = resources_per_worker

    @property
    def num_cpus_per_worker(self):
        return self.resources_per_worker.get("CPU", 0)

    @property
    def num_gpus_per_worker(self):
        return self.resources_per_worker.get("GPU", 0)

    def start(self):
        raise NotImplementedError

    def shutdown(self, patience_s: float = 5):
        raise NotImplementedError


def _get_ip():
    return ray.util.get_node_ip_address()


def _initialize_session(
    train_func,
    world_rank,
    local_rank,
    world_size,
    trial_info,
    checkpoint,
    dataset_shard,
    encode_data_fn,
    use_detailed_autofilled_metrics,
):
    print("I AM STARTING A SESSION OH YEAH")
    try:
        init_session(
            training_func=train_func,
            world_rank=world_rank,
            local_rank=local_rank,
            world_size=world_size,
            trial_info=trial_info,
            dataset_shard=dataset_shard,
            checkpoint=checkpoint,
            encode_data_fn=encode_data_fn,
            detailed_autofilled_metrics=use_detailed_autofilled_metrics,
        )
    except ValueError:
        raise TrainBackendError(
            "Attempting to start training but a "
            "previous training run is still ongoing. "
            "You must call `finish_training` before "
            "calling `start_training` again."
        )


def _start_training():
    session = get_session()
    session.start()


def _get_session(method_name: str):
    # Get the session for this worker.
    session = get_session()
    if not session:
        # Session is not initialized yet.
        raise TrainBackendError(
            f"`{method_name}` has been called "
            "before `start_training`. Please call "
            "`start_training` before "
            f"`{method_name}`."
        )
    return session


def _get_next_result():
    session = _get_session("get_next_results")
    try:
        result = session.get_next()
    except RuntimeError:
        # Training thread has not been started yet.
        raise TrainBackendError(
            "`get_next_results` has been called "
            "before `start_training`. Please call "
            "`start_training` before "
            "`get_next_results`."
        )

    return result


class TrainController(Controller):
    def __init__(
        self,
        train_fn: Callable,
        backend_config: BackendConfig,
        dataset_spec: Optional[RayDatasetSpec] = None,
        num_workers: int = 2,
        resources_per_worker: Optional[Dict] = None,
        resource_manager: Optional[ResourceManager] = None,
    ):
        self._actor_manager = ActorManager(
            resource_manager=resource_manager or FixedResourceManager({"CPU": 8})
        )

        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

        self._train_fn = train_fn
        self._num_workers = num_workers
        self._resources_per_worker = resources_per_worker or {"CPU": 1}

        self._live_actors: List[ray.actor.ActorHandle] = []
        self._actors_to_ip: Dict[ray.actor.ActorHandle, str] = {}
        self._actors_requested: bool = False
        self._training_started: bool = False

        self._dataset_shards = None
        self._dataset_spec = dataset_spec or RayDatasetSpec(None)

    def is_finished(self) -> bool:
        return self._training_started and not self._live_actors

    def next_event(self) -> ExecutionEvent:
        return self._actor_manager.next_event()

    def on_step_begin(self) -> None:
        self._create_training_actors()

    def _create_training_actors(self):
        if self._actors_requested:
            return None

        for _ in range(self._num_workers):
            self._actor_manager.add_actor(
                ActorRequest(
                    cls=RayTrainWorker,
                    kwargs={},
                    resources=ResourceRequest(bundles=[self._resources_per_worker]),
                )
            )

        self._actors_requested = True

    def actor_started(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register actor start. Return immediate decision."""
        self._live_actors.append(actor)

        self._actor_manager.track_future(
            actor=actor,
            future=actor._RayTrainWorker__execute.remote(_get_ip),
            cls=TrainSetupIPEvent,
        )

    def actor_failed(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo, exception: Exception
    ) -> None:
        """Register actor failure."""
        self._live_actors.remove(actor)
        print("Actor failed:", exception)
        for other_actor in self._live_actors:
            if other_actor is actor:
                continue

            self._actor_manager.remove_actor(other_actor)

    def actor_stopped(self, actor: ray.actor.ActorHandle, actor_info: ActorInfo):
        self._live_actors.remove(actor)

    def future_result(self, result: FutureResult):
        if isinstance(result, TrainSetupIPEvent):
            self._handle_ip_result(actor=result.actor, result=result)
        else:
            raise ValueError(
                f"Result type not allowed for async result: {type(result)}"
            )

    def multi_future_result(self, result: MultiFutureResult):
        first_result = result.results[0]

        if isinstance(first_result, TrainInitEvent):
            self._stage_start_thread()
        elif isinstance(first_result, TrainStartEvent):
            self._stage_training()
        elif isinstance(first_result, TrainTrainingEvent):
            self._handle_training_results(results=result.results)
        else:
            raise ValueError(
                f"Result type not allowed for sync result: {type(first_result)}"
            )

    def _handle_ip_result(
        self, actor: ray.actor.ActorHandle, result: TrainSetupIPEvent
    ):
        self._actors_to_ip[actor] = result.ip

        if len(self._actors_to_ip) == self._num_workers:
            self._stage_init_session()

    def _stage_init_session(self):
        worker_group = _StaticWorkerGroup(
            self._live_actors,
            resources_per_worker=self._resources_per_worker,
        )
        self._backend.on_start(
            worker_group=worker_group, backend_config=self._backend_config
        )

        # create_local_rank_map
        local_rank_map = {}
        ip_dict = defaultdict(int)
        for world_rank in range(self._num_workers):
            actor_info = self._live_actors[world_rank]
            node_ip = self._actors_to_ip[actor_info]
            local_rank_map[world_rank] = ip_dict[node_ip]
            ip_dict[node_ip] += 1

        if self._dataset_shards is None:
            self.dataset_shards = self._dataset_spec.get_dataset_shards(
                self._live_actors
            )

        self._actor_manager.track_sync_futures(
            {
                actor: actor._RayTrainWorker__execute.remote(
                    _initialize_session,
                    world_rank=index,
                    local_rank=local_rank_map[index],
                    world_size=self._num_workers,
                    trial_info=None,
                    train_func=self._train_fn,
                    dataset_shard=self.dataset_shards[index],
                    checkpoint=None,
                    encode_data_fn=self._backend.encode_data,
                    use_detailed_autofilled_metrics=False,
                )
                for index, actor in enumerate(self._live_actors)
            },
            cls=TrainInitEvent,
        )

        self._training_started = True

    def _stage_start_thread(self):
        self._actor_manager.track_sync_futures(
            {
                actor: actor._RayTrainWorker__execute.remote(_start_training)
                for actor in self._live_actors
            },
            cls=TrainStartEvent,
        )

    def _stage_training(self):
        self._actor_manager.track_sync_futures(
            {
                actor: actor._RayTrainWorker__execute.remote(_get_next_result)
                for actor in self._live_actors
            },
            cls=TrainTrainingEvent,
        )

    def _handle_training_results(self, results: List[TrainTrainingEvent]):
        first_results = results[0]
        done = first_results.result is None

        if done:
            print("I am done.")
            for live_actor in self._live_actors:
                self._actor_manager.remove_actor(live_actor)
        else:
            result_data = self._backend.decode_data(first_results.result.data)
            print("Event data", result_data)

            self._stage_training()
