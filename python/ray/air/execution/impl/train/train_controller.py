from collections import defaultdict
from typing import List, Optional, Dict, Callable

import ray.actor
from ray.air.execution import action
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

from ray.air.execution.future import TypedFuture
from ray.air.execution.impl.train.train_result import (
    TrainTrainingResult,
    TrainInitResult,
    TrainStartResult,
    TrainSetupIPResult,
)
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.result import ExecutionResult
from ray.train import BackendConfig
from ray.train._internal.backend_executor import TrainBackendError
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.train._internal.session import get_session, init_session
from ray.train._internal.worker_group import WorkerGroup, RayTrainWorker


class StaticWorkerGroup(WorkerGroup):
    def __init__(self, actors: List[ray.actor.ActorHandle], resources_per_worker: Dict):
        self.workers = actors
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
    ):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

        self._train_fn = train_fn
        self._num_workers = num_workers
        self._resources_per_worker = resources_per_worker or {"CPU": 1}

        self._live_actors = []
        self._actors_to_ip = {}
        self._actors_requested = False
        self._training_started = False

        self._actions = defaultdict(list)

        self._dataset_shards = None
        self._dataset_spec = dataset_spec or RayDatasetSpec(None)

    def is_finished(self) -> bool:
        return self._training_started and not self._live_actors

    def get_actor_requests(self) -> List[ActorRequest]:
        if self._training_started or self._actors_requested:
            return []

        requests = [
            ActorRequest(
                cls=RayTrainWorker,
                kwargs={},
                resources=ResourceRequest(bundles=[self._resources_per_worker]),
            )
            for _ in range(self._num_workers)
        ]

        self._actors_requested = True
        return requests

    def actor_started(self, actor_info: ActorInfo) -> None:
        """Register actor start. Return immediate decision."""
        self._live_actors.append(actor_info)
        self._actions[actor_info].append(
            action.Continue(
                futures=[
                    TypedFuture(
                        future=actor_info.actor._RayTrainWorker__execute.remote(
                            _get_ip
                        ),
                        cls=TrainSetupIPResult,
                    )
                ]
            )
        )

    def actor_failed(self, actor_info: ActorInfo, exception: Exception) -> None:
        """Register actor failure."""
        self._live_actors.remove(actor_info)
        print("Actor failed:", exception)
        self._actions = {
            ai: [action.Stop()] for ai in self._live_actors if ai is not actor_info
        }

    def actor_stopped(self, actor_info: ActorInfo):
        self._live_actors.remove(actor_info)

    def actor_results(
        self, actor_infos: List[ActorInfo], results: List[ExecutionResult]
    ):
        """Handle result."""
        # main_result = results[0]
        #
        # if isinstance(main_result, TrainTrainingResult):
        #     self._handle_training_result(main_result)
        #     return

        for actor_info, result in zip(actor_infos, results):
            if isinstance(result, TrainSetupIPResult):
                self._handle_ip_result(actor_info=actor_info, result=result)
            elif isinstance(result, TrainTrainingResult):
                self._handle_training_result(actor_info=actor_info, result=result)

    def _handle_ip_result(self, actor_info: ActorInfo, result: TrainSetupIPResult):
        self._actors_to_ip[actor_info] = result.ip

    def _handle_training_result(
        self, actor_info: ActorInfo, result: TrainTrainingResult
    ):
        done = result.result is None

        if done:
            self._actions = {ai: [action.Stop()] for ai in self._live_actors}
        else:
            result_data = self._backend.decode_data(result.result.data)
            print("Result data", result_data)
            self._actions[actor_info].append(
                action.Continue(
                    futures=[
                        TypedFuture(
                            future=actor_info.actor._RayTrainWorker__execute.remote(
                                _get_next_result
                            ),
                            cls=TrainTrainingResult,
                        )
                    ]
                )
            )

    def get_actions(self) -> Dict[ActorInfo, List[action.Action]]:
        actions = self._actions
        self._actions = defaultdict(list)

        if len(self._actors_to_ip) == self._num_workers and not self._training_started:
            worker_group = StaticWorkerGroup(
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
                actors = [actor_info.actor for actor_info in self._live_actors]
                self.dataset_shards = self._dataset_spec.get_dataset_shards(actors)

            # Create init futures
            for index, actor_info in enumerate(self._live_actors):
                actions[actor_info].append(
                    action.Continue(
                        futures=[
                            TypedFuture(
                                future=actor_info.actor._RayTrainWorker__execute.remote(
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
                                ),
                                cls=TrainInitResult,
                                n_args=0,
                            ),
                            TypedFuture(
                                future=actor_info.actor._RayTrainWorker__execute.remote(
                                    _start_training
                                ),
                                cls=TrainStartResult,
                                n_args=0,
                            ),
                        ]
                    )
                )

            for actor_info in self._live_actors:
                actions[actor_info].append(
                    action.Continue(
                        futures=[
                            TypedFuture(
                                future=actor_info.actor._RayTrainWorker__execute.remote(
                                    _get_next_result
                                ),
                                cls=TrainTrainingResult,
                            )
                        ]
                    )
                )

            self._training_started = True

        return actions
