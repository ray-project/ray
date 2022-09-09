from collections import defaultdict
from typing import Optional, Type

import ray.actor
from ray import tune
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

# Legacy tune
from ray.air.execution.impl.tune.tune_result import (
    TuneTrainingEvent,
    TuneSavingEvent,
    TuneRestoringEvent,
)
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.event import (
    FutureResult,
    FutureFailed,
    ExecutionEvent,
    MultiFutureResult,
)
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.tune.callback import CallbackList
from ray.tune.result import RESULT_DUPLICATE, DONE, SHOULD_CHECKPOINT
from ray.tune.trainable import Trainable
from ray.tune.stopper import Stopper
from ray.tune.experiment import Experiment, Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import SearchAlgorithm, BasicVariantGenerator
from ray.tune.stopper import NoopStopper
from ray.tune.utils.callback import _create_default_callbacks


class TuneController(Controller):
    def __init__(
        self,
        trainable_cls: Type[Trainable],
        param_space: dict,
        num_samples: int = 1,
        search_alg: Optional[SearchAlgorithm] = None,
        scheduler: Optional[TrialScheduler] = None,
        resource_manager: Optional[ResourceManager] = None,
    ):
        self._actor_manager = ActorManager(
            resource_manager=resource_manager or FixedResourceManager({"CPU": 8})
        )

        self._param_space = param_space

        self._all_trials = []
        self._buffered_actor_requests = []
        self._pending_actor_requests = {}
        self._live_actors = {}
        self._actions = defaultdict(list)
        self._actors_to_pause = set()
        self._actors_to_terminate = set()

        self._searcher: SearchAlgorithm = search_alg or BasicVariantGenerator()
        self._stopper: Stopper = NoopStopper()
        self._scheduler: TrialScheduler = scheduler or FIFOScheduler()
        self._callbacks = CallbackList(
            _create_default_callbacks([], sync_config=tune.SyncConfig())
        )

        self._searcher.add_configurations(
            Experiment(
                name="some_test",
                run=trainable_cls,
                config=self._param_space,
                num_samples=num_samples,
            )
        )

    def is_finished(self) -> bool:
        return (
            self._searcher.is_finished() and not self._live_actors
        ) or self._stopper.stop_all()

    def next_event(self) -> ExecutionEvent:
        return self._actor_manager.next_event()

    def on_step_begin(self) -> None:
        self._create_new_trials()

    def on_step_end(self) -> None:
        # Todo: checkpoint etc
        pass

    def _create_new_trials(self) -> None:
        for actor_request in self._buffered_actor_requests:
            self._actor_manager.add_actor(actor_request)

        self._buffered_actor_requests = []

        trial = self._searcher.next_trial()
        while trial:
            trial.set_status(Trial.PENDING)
            trial.init_logdir()

            actor_request = ActorRequest(
                cls=trial.get_trainable_cls(),
                kwargs={
                    "config": trial.config,
                    "remote_checkpoint_dir": trial.remote_checkpoint_dir,
                },
                resources=ResourceRequest(
                    bundles=trial.placement_group_factory.bundles
                ),
            )
            self._pending_actor_requests[actor_request] = trial
            self._all_trials.append(trial)
            self._actor_manager.add_actor(actor_request)

            trial = self._searcher.next_trial()

    def actor_started(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register actor start. Return immediate decision."""
        trial = self._pending_actor_requests.pop(actor_info.actor_request)
        self._live_actors[actor] = trial

        trial.set_status(Trial.RUNNING)
        # Todo: Let's get rid of trial.runner completely
        trial.set_runner(actor)

        self._callbacks.on_trial_start(
            iteration=0, trials=self._all_trials, trial=trial
        )

        if trial.checkpoint and trial.checkpoint.dir_or_data:
            self._actor_manager.track_future(
                actor=actor,
                future=actor.restore.remote(
                    trial.checkpoint.dir_or_data, trial.checkpoint.node_ip
                ),
                cls=TuneRestoringEvent,
            )

        self._actor_manager.track_future(
            actor=actor, future=actor.train.remote(), cls=TuneTrainingEvent
        )

    def actor_failed(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo, exception: Exception
    ) -> None:
        """Register actor failure. Return immediate decision."""
        trial = self._live_actors.pop(actor)

        self._actors_to_pause.discard(actor)
        self._actors_to_terminate.discard(actor)

        trial.set_status(Trial.ERROR)
        # Todo: Let's get rid of trial.runner completely
        trial.set_runner(None)

        self._scheduler.on_trial_error(None, trial)
        self._searcher.on_trial_complete(trial.trial_id, error=True)
        self._callbacks.on_trial_error(
            iteration=0, trials=self._all_trials, trial=trial
        )

    def actor_stopped(self, actor: ray.actor.ActorHandle, actor_info: ActorInfo):
        trial = self._live_actors.pop(actor)
        trial.set_runner(None)

        if actor in self._actors_to_pause:
            self._actors_to_pause.remove(actor)
            self._pending_actor_requests[actor_info.actor_request] = trial
            self._buffered_actor_requests.append(actor_info.actor_request)
            trial.set_status(Trial.PAUSED)
        elif actor in self._actors_to_terminate:
            self._actors_to_terminate.remove(actor)
            trial.set_status(Trial.TERMINATED)

    def future_result(self, result: FutureResult):
        if isinstance(result, FutureFailed):
            self._actor_manager.remove_actor(
                actor=result.actor, resolve_futures=False, exception=result.exception
            )
            return

        if isinstance(result, TuneTrainingEvent):
            self._handle_training_result(result=result)
        elif isinstance(result, TuneSavingEvent):
            self._handle_saving_result(result=result)

    def _handle_training_result(self, result: TuneTrainingEvent):
        actor = result.actor
        trial = self._live_actors[actor]

        done = result.metrics.get(DONE, False) or result.metrics.get(
            RESULT_DUPLICATE, False
        )
        trial.last_result = result.metrics.copy()

        if result.metrics.get(SHOULD_CHECKPOINT, False):
            future = actor.save.remote()
            trial.saving_to = _TrackedCheckpoint(
                dir_or_data=future,
                storage_mode=CheckpointStorage.PERSISTENT,
                metrics=trial.last_result,
            )
            self._actor_manager.track_future(
                actor=actor, future=future, cls=TuneSavingEvent
            )

        if done:
            self._scheduler.on_trial_complete(None, trial=trial, result=result.metrics)
            self._searcher.on_trial_complete(
                trial_id=trial.trial_id, result=result.metrics, error=False
            )
            self._callbacks.on_trial_complete(
                iteration=0, trials=self._all_trials, trial=trial
            )
            decision = TrialScheduler.STOP
        else:
            decision = self._scheduler.on_trial_result(
                None, trial=trial, result=result.metrics
            )
            self._searcher.on_trial_result(
                trial_id=trial.trial_id, result=result.metrics
            )
            self._callbacks.on_trial_result(
                iteration=0, trials=self._all_trials, trial=trial, result=result.metrics
            )

        if decision == TrialScheduler.STOP:
            self._actors_to_terminate.add(actor)
            self._actor_manager.remove_actor(actor=actor, resolve_futures=True)
        elif decision == TrialScheduler.PAUSE:
            self._actors_to_pause.add(actor)
            self._actor_manager.remove_actor(actor=actor, resolve_futures=True)
        elif decision == TrialScheduler.NOOP:
            pass
        else:
            self._actor_manager.track_future(
                actor=actor, future=actor.train.remote(), cls=TuneTrainingEvent
            )

    def _handle_saving_result(self, result: TuneSavingEvent):
        actor = result.actor
        trial = self._live_actors[actor]
        trial.saving_to.dir_or_data = result.dir_or_data
        trial.on_checkpoint(trial.saving_to)

    def multi_future_result(self, result: MultiFutureResult):
        pass
