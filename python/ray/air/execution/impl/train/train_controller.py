from collections import defaultdict
from typing import List, Optional, Dict, Callable

import ray.actor
from ray.air.execution import action
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

from ray.air.execution.future import TypedFuture
from ray.air.execution.impl.train.train_result import (
    TrainTrainingResult,
)
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.result import ExecutionResult
from ray.train import BackendConfig
from ray.train._internal.worker_group import WorkerGroup, RayTrainWorker


class StaticWorkerGroup(WorkerGroup):
    def __init__(self, actors: List[ray.actor.ActorHandle]):
        self.workers = actors
        self.num_workers = len(self.workers)

    def start(self):
        raise NotImplementedError

    def shutdown(self, patience_s: float = 5):
        raise NotImplementedError


class TrainController(Controller):
    def __init__(
        self,
        train_fn: Callable,
        backend_config: BackendConfig,
        num_workers: int = 2,
        resources_per_worker: Optional[Dict] = None,
    ):
        self._backend_config = backend_config
        self._backend = backend_config.backend_cls()

        self._train_fn = train_fn
        self._num_workers = num_workers
        self._resources_per_worker = resources_per_worker

        self._live_actors = []
        self._training_started = False

        self._actions = defaultdict(list)

    def is_finished(self) -> bool:
        return self._training_started and not self._live_actors

    def get_actor_requests(self) -> List[ActorRequest]:
        if self._training_started:
            return []

        requests = [
            ActorRequest(
                cls=RayTrainWorker,
                kwargs={},
                resources=ResourceRequest(bundles=[self._resources_per_worker]),
            )
        ]

        return requests

    def actor_started(self, actor_info: ActorInfo) -> None:
        """Register actor start. Return immediate decision."""
        self._live_actors.append(actor_info)

        if len(self._live_actors) == self._num_workers:
            worker_group = StaticWorkerGroup([ai.actor for ai in self._live_actors])
            self._backend.on_start(
                worker_group=worker_group, backend_config=self._backend_config
            )

            self._actions = {
                ai: [
                    action.Continue(
                        futures=[
                            TypedFuture(
                                future=ai.actor.train.remote(), cls=TrainTrainingResult
                            )
                        ]
                    )
                ]
                for ai in self._live_actors
            }

        return None

    def actor_failed(self, actor_info: ActorInfo, exception: Exception) -> None:
        """Register actor failure."""
        self._live_actors.remove(actor_info)
        self._actions = {
            ai: [action.Stop()] for ai in self._live_actors if ai is not actor_info
        }

    def actor_stopped(self, actor_info: ActorInfo):
        self._live_actors.remove(actor_info)

    def actor_results(
        self, actor_infos: List[ActorInfo], results: List[ExecutionResult]
    ):
        """Handle result."""
        main_result = results[0]

        if isinstance(ExecutionResult, TrainTrainingResult):
            self._handle_training_result(main_result)

    def _handle_training_result(self, result: TrainTrainingResult):
        done = result.metrics is None

        if done:
            self._actions = {ai: [action.Stop()] for ai in self._live_actors}
        else:
            self._actions = {
                ai: [
                    action.Continue(
                        futures=[
                            TypedFuture(
                                future=ai.actor.train.remote(), cls=TrainTrainingResult
                            )
                        ]
                    )
                ]
                for ai in self._live_actors
            }

    def get_actions(self) -> Dict[ActorInfo, List[action.Action]]:
        actions = self._actions
        self._actions = defaultdict(list)
        return actions
