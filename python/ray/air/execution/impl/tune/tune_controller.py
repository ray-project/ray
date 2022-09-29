from collections import defaultdict
from typing import Optional, List, Dict, Any

import ray.actor
from ray import tune
from ray._private.dict import flatten_dict
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
from ray.air.execution.impl.tune.utils import get_max_pending_trials
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.event import (
    FutureResult,
    FutureFailed,
    ExecutionEvent,
    MultiFutureResult,
)
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.tune.callback import CallbackList, Callback
from ray.tune.result import RESULT_DUPLICATE, SHOULD_CHECKPOINT
from ray.tune.stopper import Stopper
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import SearchAlgorithm
from ray.tune.stopper import NoopStopper
from ray.tune.utils import warn_if_slow
from ray.tune.utils.callback import _create_default_callbacks


class TuneController(Controller):
    def __init__(
        self,
        search_alg: SearchAlgorithm,
        scheduler: Optional[TrialScheduler] = None,
        resource_manager: Optional[ResourceManager] = None,
        callbacks: Optional[List[Callback]] = None,
    ):
        self._actor_manager = ActorManager(
            resource_manager=resource_manager or FixedResourceManager({"CPU": 8})
        )

        self._all_trials = []
        self._buffered_actor_requests = []
        self._pending_actor_requests = {}
        self._live_actors = {}
        self._actions = defaultdict(list)
        self._actors_to_pause = set()
        self._actors_to_terminate = set()

        self._searcher: SearchAlgorithm = search_alg
        self._stopper: Stopper = NoopStopper()
        self._scheduler: TrialScheduler = scheduler or FIFOScheduler()
        self._callbacks = CallbackList(
            _create_default_callbacks(callbacks or [], sync_config=tune.SyncConfig())
        )

        self._max_pending_trials = get_max_pending_trials(self._searcher)

        self._iteration = 0

    def is_finished(self) -> bool:
        return (
            self._searcher.is_finished()
            and not self._live_actors
            and not self._pending_actor_requests
            and not self._buffered_actor_requests
        ) or self._stopper.stop_all()

    def next_event(self) -> ExecutionEvent:
        return self._actor_manager.next_event()

    def on_step_begin(self) -> None:
        self._callbacks.on_step_begin(
            iteration=self._iteration, trials=self._all_trials
        )
        self._iteration += 1
        self._create_new_trials()

    def on_step_end(self) -> None:
        # Todo: checkpoint etc
        self._callbacks.on_step_end(iteration=self._iteration, trials=self._all_trials)

    def _create_new_trials(self) -> None:
        for actor_request in self._buffered_actor_requests:
            if self._actor_manager.num_actor_requests >= self._max_pending_trials:
                break
            self._actor_manager.add_actor(actor_request)

        self._buffered_actor_requests = []

        if self._actor_manager.num_actor_requests >= self._max_pending_trials:
            return

        trial = self._searcher.next_trial()
        while (
            trial and self._actor_manager.num_actor_requests < self._max_pending_trials
        ):
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

            self._scheduler.on_trial_complete(
                None, trial=trial, result=trial.last_result
            )
            self._searcher.on_trial_complete(
                trial_id=trial.trial_id, result=trial.last_result, error=False
            )
            self._callbacks.on_trial_complete(
                iteration=0, trials=self._all_trials, trial=trial
            )

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

        metrics = result.metrics.copy()
        metrics.update(trial_id=trial.trial_id)

        # SHOULD_CHECKPOINT is reported when the function runner observed a
        # checkpoint - then our next future should be trainable.save.remote()
        should_checkpoint = metrics.get(SHOULD_CHECKPOINT, False)

        # RESULT_DUPLICATE is reported when the function runner thread exits.
        # Then we should re-use the last available result.
        is_duplicate = RESULT_DUPLICATE in metrics
        if is_duplicate:
            metrics = trial.last_result
            # This will lead `_get_decision_from_metrics` to return STOP
            metrics.update(done=True)

        # Update last result
        trial.last_result = result.metrics.copy()

        # For our stoppers, we use the flat metrics to be able to specify stopping
        # conditions better
        flat_metrics = flatten_dict(metrics)

        # Todo: validate results metrics

        decision = self._get_decision_from_metrics(
            trial=trial, flat_metrics=flat_metrics
        )

        # If the scheduler wants to stop, update the metrics
        if decision == TrialScheduler.STOP:
            metrics.update(done=True)
        else:
            # Only updating search alg if the trial is not to be stopped.
            # The scheduler has already been informed in
            # `self._get_decision_from_metrics`
            with warn_if_slow("search_alg.on_trial_result"):
                self._searcher.on_trial_result(trial.trial_id, flat_metrics)

        # Inform the callbacks if this is not a duplicate.
        if not is_duplicate:
            self._callbacks.on_trial_result(
                iteration=self._iteration,
                trials=self._all_trials,
                trial=trial,
                result=metrics,
            )

        if should_checkpoint:
            self._schedule_save(actor=actor)

        self._act_on_decision(actor=actor, decision=decision)

    def _get_decision_from_metrics(
        self, trial: Trial, flat_metrics: Dict[str, Any]
    ) -> str:
        if self._stopper(trial.trial_id, flat_metrics) or trial.should_stop(
            flat_metrics
        ):
            # If a stopping condition is met, stop.
            decision = TrialScheduler.STOP
        else:
            # Otherwise, ask the scheduler what to do
            with warn_if_slow("scheduler.on_trial_result"):
                decision = self._scheduler.on_trial_result(self, trial, flat_metrics)

        return decision

    def _act_on_decision(self, actor: ray.actor.ActorHandle, decision: str):
        trial = self._live_actors[actor]

        if decision == TrialScheduler.STOP:
            self._schedule_stop(actor=actor)
        elif decision == TrialScheduler.PAUSE:
            self._schedule_pause(actor=actor)
        elif decision == TrialScheduler.NOOP:
            pass
        elif not trial.is_saving:
            self._schedule_train(actor=actor)

    def _schedule_train(self, actor: ray.actor.ActorHandle):
        self._actor_manager.track_future(
            actor=actor, future=actor.train.remote(), cls=TuneTrainingEvent
        )

    def _schedule_save(self, actor: ray.actor.ActorHandle):
        trial = self._live_actors[actor]

        future = actor.save.remote()
        trial.saving_to = _TrackedCheckpoint(
            dir_or_data=future,
            storage_mode=CheckpointStorage.PERSISTENT,
            metrics=trial.last_result,
        )
        self._actor_manager.track_future(
            actor=actor, future=future, cls=TuneSavingEvent
        )

    def _schedule_stop(self, actor: ray.actor.ActorHandle):
        self._actors_to_terminate.add(actor)
        self._actor_manager.remove_actor(actor=actor, resolve_futures=True)

    def _schedule_pause(self, actor: ray.actor.ActorHandle):
        self._actors_to_pause.add(actor)
        self._actor_manager.remove_actor(actor=actor, resolve_futures=True)

    def _handle_saving_result(self, result: TuneSavingEvent):
        actor = result.actor
        trial = self._live_actors[actor]
        trial.saving_to.dir_or_data = result.dir_or_data
        trial.on_checkpoint(trial.saving_to)

    def multi_future_result(self, result: MultiFutureResult):
        pass
