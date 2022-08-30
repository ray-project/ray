from collections import defaultdict
from typing import List, Type, Optional, Dict

from ray import tune
from ray.air.execution import action
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.controller import Controller

# Legacy tune
from ray.air.execution.impl.tune.tune_result import TuneTrainingResult
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.result import ExecutionResult
from ray.tune.callback import CallbackList
from ray.tune.trainable import Trainable
from ray.tune.stopper import Stopper
from ray.tune.experiment import Experiment, Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import SearchAlgorithm, BasicVariantGenerator
from ray.tune.stopper import NoopStopper
from ray.tune.utils.callback import _create_default_callbacks


class TuneController(Controller):
    def __init__(self, trainable_cls: Type[Trainable], param_space: dict):
        self._param_space = param_space

        self._all_trials = []
        self._pending_actor_requests = {}
        self._live_actors = {}
        self._actions = defaultdict(list)

        self._searcher: SearchAlgorithm = BasicVariantGenerator(max_concurrent=4)
        self._stopper: Stopper = NoopStopper()
        self._scheduler = FIFOScheduler()
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
        return self._searcher.is_finished() or self._stopper.stop_all()

    def _create_trial(self) -> Optional[Trial]:
        return self._searcher.next_trial()

    def get_actor_requests(self) -> List[ActorRequest]:
        trial = self._create_trial()
        trial.set_status(Trial.PENDING)
        requests = []
        while trial:
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

        return requests

    def actor_started(self, actor_info: ActorInfo) -> action.Action:
        """Register actor start. Return immediate decision."""
        trial = self._pending_actor_requests.pop(actor_info.request)
        self._live_actors[actor_info] = trial

        trial.set_status(Trial.RUNNING)

        self._callbacks.on_trial_start(
            iteration=0, trials=self._all_trials, trial=trial
        )
        return action.Continue(futures=[actor_info.actor.train.remote()])

    def actor_failed(self, actor_info: ActorInfo) -> action.Action:
        """Register actor failure. Return immediate decision."""
        trial = self._live_actors.pop(actor_info)

        trial.set_status(Trial.ERROR)

        self._scheduler.on_trial_error(None, trial)
        self._searcher.on_trial_complete(trial.trial_id, error=True)
        self._callbacks.on_trial_error(
            iteration=0, trials=self._all_trials, trial=trial
        )
        return action.Stop()

    def actor_results(
        self, actor_infos: List[ActorInfo], results: List[ExecutionResult]
    ):
        """Handle result."""
        for actor_info, result in zip(actor_infos, results):
            # Todo: hardcoded training result, adjust trainable
            training_result = TuneTrainingResult(metrics=result)

            if isinstance(result, TuneTrainingResult):
                self._handle_training_result(actor_info, training_result)

    def _handle_training_result(
        self, actor_info: ActorInfo, result: TuneTrainingResult
    ):
        trial = self._live_actors[actor_info]

        done = result.metrics.get("done", False)

        if done:
            decision = self._scheduler.on_trial_complete(
                None, trial=trial, result=result.metrics
            )
            self._searcher.on_trial_complete(
                trial_id=trial.trial_id, result=result.metrics, error=False
            )
            self._callbacks.on_trial_complete(
                iteration=0, trials=self._all_trials, trial=trial
            )
        else:
            decision = self._scheduler.on_trial_result(
                None, trial=trial, result=result.metrics
            )
            self._searcher.on_trial_result(
                trial_id=trial.trial_id, result=result.metrics
            )
            self._callbacks.on_trial_result(
                iteration=0, trials=self._all_trials, trial=trial
            )

        if decision == TrialScheduler.STOP:
            trial.set_status(Trial.PAUSED)
            act = action.Stop()
        elif decision == TrialScheduler.PAUSE:
            trial.set_status(Trial.TERMINATED)
            self._pending_actor_requests[actor_info.request] = trial
            act = action.Stop()
        elif decision == TrialScheduler.NOOP:
            act = None
        else:
            act = action.Continue(futures=[actor_info.actor.train.remote()])

        if act:
            self._actions[trial].append(act)

    def get_actions(self) -> Dict[ActorInfo, List[action.Action]]:
        actions = self._actions
        self._actions = defaultdict(list)
        return actions
