from collections import defaultdict
from typing import List, Type, Optional, Dict

from ray import tune
from ray.air.execution import action
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

# Legacy tune
from ray.air.execution.future import TypedFuture
from ray.air.execution.impl.tune.tune_result import TuneTrainingResult
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.result import ExecutionResult
from ray.tune.callback import CallbackList
from ray.tune.result import RESULT_DUPLICATE, DONE
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
        search_alg: Optional[SearchAlgorithm] = None,
        scheduler: Optional[TrialScheduler] = None,
    ):
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
                num_samples=8,
            )
        )

    def is_finished(self) -> bool:
        return (
            self._searcher.is_finished() and not self._live_actors
        ) or self._stopper.stop_all()

    def _create_trial(self) -> Optional[Trial]:
        return self._searcher.next_trial()

    def get_actor_requests(self) -> List[ActorRequest]:
        buffered_requests = self._buffered_actor_requests
        self._buffered_actor_requests = []

        trial = self._create_trial()
        if not trial:
            return buffered_requests

        trial.set_status(Trial.PENDING)
        requests = []
        while trial:
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
            requests.append(actor_request)
            self._pending_actor_requests[actor_request] = trial
            self._all_trials.append(trial)
            trial = self._create_trial()

        return requests + buffered_requests

    def actor_started(self, actor_info: ActorInfo) -> None:
        """Register actor start. Return immediate decision."""
        trial = self._pending_actor_requests.pop(actor_info.request)
        self._live_actors[actor_info] = trial

        trial.set_status(Trial.RUNNING)
        # Todo: Let's get rid of trial.runner completely
        trial.set_runner(actor_info.actor)

        self._callbacks.on_trial_start(
            iteration=0, trials=self._all_trials, trial=trial
        )
        self._actions[actor_info].append(
            action.Continue(
                futures=[
                    TypedFuture(
                        future=actor_info.actor.train.remote(), cls=TuneTrainingResult
                    )
                ]
            )
        )

    def actor_failed(self, actor_info: ActorInfo, exception: Exception) -> None:
        """Register actor failure. Return immediate decision."""
        trial = self._live_actors.pop(actor_info)

        self._actors_to_pause.discard(actor_info)
        self._actors_to_terminate.discard(actor_info)

        trial.set_status(Trial.ERROR)
        # Todo: Let's get rid of trial.runner completely
        trial.set_runner(None)

        self._scheduler.on_trial_error(None, trial)
        self._searcher.on_trial_complete(trial.trial_id, error=True)
        self._callbacks.on_trial_error(
            iteration=0, trials=self._all_trials, trial=trial
        )

    def actor_stopped(self, actor_info: ActorInfo):
        trial = self._live_actors.pop(actor_info)
        trial.set_runner(None)

        if actor_info in self._actors_to_pause:
            self._actors_to_pause.remove(actor_info)
            self._pending_actor_requests[actor_info.request] = trial
            self._buffered_actor_requests.append(actor_info.request)
            trial.set_status(Trial.PAUSED)
        elif actor_info in self._actors_to_terminate:
            self._actors_to_terminate.remove(actor_info)
            trial.set_status(Trial.TERMINATED)

    def actor_results(
        self, actor_infos: List[ActorInfo], results: List[ExecutionResult]
    ):
        """Handle result."""
        for actor_info, result in zip(actor_infos, results):
            if isinstance(result, TuneTrainingResult):
                self._handle_training_result(actor_info, result)

    def _handle_training_result(
        self, actor_info: ActorInfo, result: TuneTrainingResult
    ):
        trial = self._live_actors[actor_info]

        done = result.metrics.get(DONE, False) or result.metrics.get(
            RESULT_DUPLICATE, False
        )
        trial.last_result = result.metrics.copy()

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
            self._actors_to_terminate.add(actor_info)
            act = action.Stop()
        elif decision == TrialScheduler.PAUSE:
            self._actors_to_pause.add(actor_info)
            act = action.Stop()
        elif decision == TrialScheduler.NOOP:
            act = None
        else:
            act = action.Continue(
                futures=[
                    TypedFuture(
                        future=actor_info.actor.train.remote(), cls=TuneTrainingResult
                    )
                ]
            )

        if act:
            self._actions[actor_info].append(act)

    def get_actions(self) -> Dict[ActorInfo, List[action.Action]]:
        actions = self._actions
        self._actions = defaultdict(list)
        return actions
