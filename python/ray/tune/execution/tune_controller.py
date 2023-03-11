import copy
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Union, Tuple, Set

import logging
import os

import ray
from ray.air import Checkpoint
from ray.air.config import CheckpointConfig
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.air.execution import ResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal import RayActorManager, TrackedActor
from ray.tune.error import _AbortTrialExecution
from ray.tune.execution.ray_trial_executor import _noop_logger_creator, _class_cache
from ray.tune.execution.trial_runner import _TuneControllerBase, TrialRunnerWrapper
from ray.tune.experiment.trial import _change_working_directory, _TrialInfo, _Location
from ray.tune.result import TRIAL_INFO, STDOUT_FILE, STDERR_FILE
from ray.tune.trainable import TrainableUtil
from ray.tune import TuneError
from ray.tune.callback import Callback
from ray.tune.schedulers import TrialScheduler
from ray.tune.stopper import Stopper
from ray.tune.search import SearchAlgorithm
from ray.tune.syncer import SyncConfig
from ray.tune.experiment import Trial
from ray.tune.utils import warn_if_slow
from ray.tune.utils.object_cache import _ObjectCache
from ray.util.debug import log_once


logger = logging.getLogger(__name__)


class TuneController(_TuneControllerBase):
    def __init__(
        self,
        *,
        search_alg: Optional[SearchAlgorithm] = None,
        placeholder_resolvers: Optional[Dict[Tuple, Any]] = None,
        scheduler: Optional[TrialScheduler] = None,
        local_checkpoint_dir: Optional[str] = None,
        sync_config: Optional[SyncConfig] = None,
        experiment_dir_name: Optional[str] = None,
        stopper: Optional[Stopper] = None,
        resume: Union[str, bool] = False,
        server_port: Optional[int] = None,
        fail_fast: bool = False,
        checkpoint_period: Union[str, int] = None,
        callbacks: Optional[List[Callback]] = None,
        metric: Optional[str] = None,
        trial_checkpoint_config: Optional[CheckpointConfig] = None,
        chdir_to_trial_dir: bool = False,
        reuse_actors: bool = False,
        resource_manager_factory: Optional[Callable[[], ResourceManager]] = None,
    ):
        super().__init__(
            search_alg=search_alg,
            placeholder_resolvers=placeholder_resolvers,
            scheduler=scheduler,
            local_checkpoint_dir=local_checkpoint_dir,
            sync_config=sync_config,
            experiment_dir_name=experiment_dir_name,
            stopper=stopper,
            resume=resume,
            server_port=server_port,
            fail_fast=fail_fast,
            checkpoint_period=checkpoint_period,
            callbacks=callbacks,
            metric=metric,
            trial_checkpoint_config=trial_checkpoint_config,
        )

        if resource_manager_factory:
            self._resource_manager = resource_manager_factory()
        else:
            self._resource_manager = PlacementGroupResourceManager()

        self._actor_manager = RayActorManager(resource_manager=self._resource_manager)

        # Actor <-> Trial mappings
        self._actor_to_trial: Dict[TrackedActor, Trial] = {}
        self._trial_to_actor: Dict[Trial, TrackedActor] = {}

        # Keep track of actor states
        self._pending_trials: Set[Trial] = set()
        self._pending_trials_list: List[Trial] = []

        self._running_trials: Set[Trial] = set()

        self._paused_trials: Set[Trial] = set()
        self._paused_trials_list: List[Trial] = []

        self._stopped_trials: Set[Trial] = set()
        self._failed_trials: Set[Trial] = set()

        self._resetting_trials: Set[Trial] = set()

        self._staged_trials: Set[Trial] = set()

        # Reuse actors
        self._reuse_actors = False  # reuse_actors
        self._actor_cache = _ObjectCache(may_keep_one=True)

        # General trial behavior
        self._chdir_to_trial_dir = chdir_to_trial_dir

        # TRAINING
        self._buffer_length = int(os.getenv("TUNE_RESULT_BUFFER_LENGTH", 1))
        self._buffer_min_time_s = float(os.getenv("TUNE_RESULT_BUFFER_MIN_TIME_S", 0.0))
        self._buffer_max_time_s = float(
            os.getenv("TUNE_RESULT_BUFFER_MAX_TIME_S", 100.0)
        )

    def _wrapped(self):
        return TrialRunnerWrapper(
            self,
            trial_executor=_FakeRayTrialExecutor(self),
            runner_whitelist_attr={"search_alg"},
        )

    def _used_resources_string(self):
        return "TODO"

    def on_step_begin(self):
        pass

    def on_step_end(self):
        pass

    def step(self):
        if self.is_finished():
            raise TuneError("Called step when all trials finished?")

        with warn_if_slow("on_step_begin"):
            self.on_step_begin()

        with warn_if_slow("callbacks.on_step_begin"):
            self._callbacks.on_step_begin(
                iteration=self._iteration, trials=self._trials
            )

        self._maybe_update_trial_queue()

        self._maybe_add_actors()

        self._actor_manager.next(timeout=1)

        self._stop_experiment_if_needed()

        try:
            self.checkpoint()
        except Exception as e:
            logger.warning(f"Trial controller checkpointing failed: {str(e)}")

        self._iteration += 1

        if self._server:
            with warn_if_slow("server"):
                self._process_stop_requests()

            if self.is_finished():
                self._server.shutdown()

        with warn_if_slow("on_step_end"):
            self.on_step_end()
        with warn_if_slow("callbacks.on_step_end"):
            self._callbacks.on_step_end(iteration=self._iteration, trials=self._trials)

    def _set_trial_status(self, trial: Trial, status: str):
        current_status = trial.status

        if current_status == status:
            logger.debug(f"Trial {trial} already has status {status}. Skipping update.")
            return

        status_str_map = {
            Trial.PENDING: self._pending_trials,
            Trial.RUNNING: self._running_trials,
            Trial.PAUSED: self._paused_trials,
            Trial.TERMINATED: self._stopped_trials,
            Trial.ERROR: self._failed_trials,
        }

        logger.debug(
            f"Setting status for trial {trial} from {current_status} to {status}"
        )

        assert trial in status_str_map[current_status], (trial, current_status)
        assert trial not in status_str_map[status], (trial, status)

        status_str_map[current_status].remove(trial)
        status_str_map[status].add(trial)

        # We keep a log for paused/pending trials for FIFO scheduling.
        # We do not need to remove from this list as we will just discard
        # items that are in this list but not in the respective set.
        if status == Trial.PAUSED:
            self._paused_trials_list.append(trial)
        if status == Trial.PENDING:
            self._pending_trials_list.append(trial)

        trial.set_status(status)

    def _get_trial_checkpoints(self) -> Dict[str, str]:
        return {}

    def _mark_trial_to_checkpoint(self, trial: Trial):
        pass

    ###
    # UPDATE TRIALS
    def add_trial(self, trial: Trial):
        super().add_trial(trial)

        status_str_map = {
            Trial.PENDING: self._pending_trials,
            Trial.RUNNING: self._running_trials,
            Trial.PAUSED: self._paused_trials,
            Trial.TERMINATED: self._stopped_trials,
            Trial.ERROR: self._failed_trials,
        }

        status_str_map[trial.status].add(trial)

    def _maybe_update_trial_queue(self):
        if self._search_alg.is_finished():
            return

        dont_wait_for_trial = (
            self._pending_trials or self._running_trials or self._paused_trials
        )

        while len(self._pending_trials) < self._max_pending_trials:
            if not self._update_trial_queue(blocking=not dont_wait_for_trial):
                break
            dont_wait_for_trial = True

    def _cleanup_trials(self):
        # Todo: Remove all
        pass

    ###
    # ADD ACTORS
    def _maybe_add_actors(self):
        with warn_if_slow("choose_trial_to_run"):
            trial_to_run = self._scheduler_alg.choose_trial_to_run(self._wrapped())

        if trial_to_run:
            if trial_to_run not in self._staged_trials:
                self._staged_trials.add(trial_to_run)
                self._actor_cache.increase_max(trial_to_run.placement_group_factory)
                self._schedule_trial_actor(trial_to_run)

        def _maybe_add_actors(candidates: List[Trial]):
            while candidates:
                if len(self._staged_trials) >= self._max_pending_trials:
                    break

                trial = candidates.pop(0)

                if trial not in (self._pending_trials | self._paused_trials):
                    continue

                if trial in self._staged_trials:
                    continue

                self._staged_trials.add(trial)
                self._actor_cache.increase_max(trial.placement_group_factory)
                self._schedule_trial_actor(trial)

        _maybe_add_actors(self._pending_trials_list)
        _maybe_add_actors(self._paused_trials_list)

    def _schedule_trial_actor(self, trial: Trial):
        self._set_trial_status(trial, Trial.PENDING)

        trial.init_logdir()
        # We checkpoint metadata here to try mitigating logdir duplication
        self._mark_trial_to_checkpoint(trial)
        logger_creator = partial(
            _noop_logger_creator,
            logdir=trial.logdir,
            should_chdir=self._chdir_to_trial_dir,
        )

        resource_request = trial.placement_group_factory
        if self._actor_cache.has_cached_object(resource_request):
            cached_actor = self._actor_cache.pop_cached_object(resource_request)
            logger.debug(f"Reusing ACTOR for trial {trial}: {cached_actor}")

            self._trial_to_actor[trial] = cached_actor
            self._actor_to_trial[cached_actor] = trial

            # Todo: get rid of Trial.runner
            ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
                cached_actor
            ][0]
            trial.set_runner(ray_actor)

            self._resetting_trials.add(trial)
            self._schedule_trial_reset(
                trial, trial.config, trial.experiment_tag, logger_creator
            )
            return

        trainable_cls = trial.get_trainable_cls()
        if not trainable_cls:
            raise _AbortTrialExecution(
                f"Invalid trainable: {trial.trainable_name}. If you passed "
                f"a string, make sure the trainable was registered before."
            )
        _actor_cls = _class_cache.get(trainable_cls)

        trial.set_location(_Location())
        trial_config = copy.deepcopy(trial.config)
        trial_config[TRIAL_INFO] = _TrialInfo(trial)
        stdout_file, stderr_file = trial.log_to_file
        trial_config[STDOUT_FILE] = stdout_file
        trial_config[STDERR_FILE] = stderr_file

        with _change_working_directory(trial):
            tracked_actor = self._actor_manager.add_actor(
                cls=_actor_cls,
                resource_request=trial.placement_group_factory,
                kwargs={
                    "config": trial_config,
                    "logger_creator": logger_creator,
                    "remote_checkpoint_dir": trial.remote_checkpoint_dir,
                    "sync_config": trial.sync_config,
                },
                on_start=self._actor_started,
                on_stop=self._actor_stopped,
                on_error=self._actor_failed,
            )
            self._trial_to_actor[trial] = tracked_actor
            self._actor_to_trial[tracked_actor] = trial

        logger.debug(f"Scheduled new ACTOR for trial {trial}: {tracked_actor}")

    def _unstage_trial_with_resources(self, trial: Trial):
        # Case 1: The trial we started was staged. Just remove it
        if trial in self._staged_trials:
            self._staged_trials.remove(trial)
            self._actor_cache.decrease_max(trial.placement_group_factory)
            return

        # Case 2: We staged a trial "A" with the same resources, but our trial "B"
        # was selected by the scheduler to run. The resource manager does not care
        # about "trials", it just cares about resources being available. Thus we
        # look for a staged trial with the same resource requirements and remove it

        resource_request = trial.placement_group_factory
        # Remove staged trial with same resource requirements
        candidate_trial = None
        for staged_trial in self._staged_trials:
            staged_resources = staged_trial.placement_group_factory
            if staged_resources == resource_request:
                candidate_trial = staged_trial
                break

        if candidate_trial:
            self._staged_trials.remove(candidate_trial)
            self._actor_cache.decrease_max(candidate_trial.placement_group_factory)
            return

        raise RuntimeError(
            "Started a trial with resources requested by a different trial, but "
            "this trial was lost. This is an error in Ray Tune's execution "
            "logic. Please raise a GitHub issue at "
            "https://github.com/ray-project/ray/issues"
        )

    def _maybe_cache_trial_actor(self, trial: Trial) -> bool:
        """Cache trial actor for reuse, if needed.

        We will only cache as many actors as are needed to fulfill any pending
        resource requests for actors with the same resource requirements.
        E.g. if we have 6 running trials and 4 additional staged actors, we will only
        cache up to 4 of the running trial actors when they finish.

        One exception is the case when we have no cached actors, yet. In that case,
        we will always cache the actor in this method.

        Later, in `_cleanup_cached_actors`, we will check again if we need this cached
        actor. That method will keep the actor if we don't have any staged trials,
        because we don't know at that point if the next trial might require the same
        resources. But because there is no staged trial, it is safe to keep the actor
        around, as it won't occupy resources needed by another trial until it's staged.
        """
        if not self._reuse_actors:
            return False

        tracked_actor = self._trial_to_actor[trial]

        if not self._actor_cache.cache_object(
            trial.placement_group_factory, tracked_actor
        ):
            logger.debug(
                f"Could not cache actor of trial {trial} for "
                "reuse, as there are no pending trials "
                "requiring its resources."
            )
            return False

        logger.debug(f"Caching actor of trial {trial} for re-use")

        tracked_actor = self._trial_to_actor.pop(trial)
        self._actor_to_trial.pop(tracked_actor)

        trial.set_runner(None)

        return True

    def _actor_started(self, tracked_actor: TrackedActor):
        trial = self._actor_to_trial[tracked_actor]

        self._unstage_trial_with_resources(trial)

        ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
            tracked_actor
        ][0]
        trial.set_runner(ray_actor)

        if not self._schedule_trial_restore(trial):
            self._set_trial_status(trial, Trial.RUNNING)
            self._schedule_trial_train(trial)

    def _actor_stopped(self, tracked_actor: TrackedActor):
        trial = self._actor_to_trial.pop(tracked_actor)
        self._trial_to_actor.pop(trial)

        trial.set_runner(None)

    def _actor_failed(self, tracked_actor: TrackedActor, exception: Exception):
        self._actor_stopped(tracked_actor)

    def _schedule_trial_task(
        self,
        trial: Trial,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        on_result: Optional[Callable[[Trial, Any], None]] = None,
        on_error: Optional[Callable[[Trial, Exception], None]] = None,
        _return_future: bool = False,
    ) -> Optional[ray.ObjectRef]:

        tracked_actor = self._trial_to_actor[trial]

        _on_result = None
        _on_error = None

        if on_result:

            def _on_result(tracked_actor: TrackedActor, *args, **kwargs):
                assert trial == self._actor_to_trial[tracked_actor]
                logger.debug(
                    f"Future {method_name.upper()} RESOLVED for trial {trial}: "
                    f"{args}, {kwargs}"
                )
                on_result(trial, *args, **kwargs)

        if on_error:

            def _on_error(tracked_actor: TrackedActor, exception: Exception):
                assert trial == self._actor_to_trial[tracked_actor]
                logger.debug(
                    f"Future {method_name.upper()} FAILED for trial {trial}: "
                    f"{exception}"
                )
                on_result(trial, *args, **kwargs)

        logger.debug(f"Future {method_name.upper()} SCHEDULED for trial {trial}")

        with _change_working_directory(trial):
            future = self._actor_manager.schedule_actor_task(
                tracked_actor=tracked_actor,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
                on_result=_on_result,
                on_error=_on_error,
                _return_future=_return_future,
            )
            if _return_future:
                return future

    ###
    # Failure
    def _trial_task_failure(self, trial: Trial, exception: Exception):
        if self._fail_fast == self.RAISE:
            raise exception
        else:
            if self._print_trial_errors:
                logger.exception("Trial task failed", exc=exception)
            self._process_trial_failure(trial, exception=exception)

    def _schedule_trial_stop(self, trial: Trial, exception: Optional[Exception] = None):
        trial.saving_to = None
        trial.restoring_from = None

        self._set_trial_status(trial, Trial.ERROR if exception else Trial.TERMINATED)
        trial.set_location(_Location())

        if not exception and self._maybe_cache_trial_actor(trial):
            # Trial runner has been cached
            return

        tracked_actor = self._trial_to_actor[trial]

        self._actor_manager.remove_actor(tracked_actor)

    def _schedule_trial_pause(self, trial: Trial, should_checkpoint: bool = True):
        if should_checkpoint:
            self._schedule_trial_save(trial, storage=CheckpointStorage.MEMORY)
        self._schedule_trial_stop(trial)
        self._set_trial_status(Trial.PAUSED)

    ###
    # TRAIN

    def _schedule_trial_train(self, trial: Trial):
        args = ()
        method_name = "train"

        buffer_length, buffer_time_s = self._maybe_buffer_training(trial)

        if buffer_length > 1:
            method_name = "train_buffered"
            args = (buffer_length, buffer_time_s)

        logger.debug(f"Scheduling future {method_name.upper()} for trial {trial}")

        self._schedule_trial_task(
            trial=trial,
            method_name=method_name,
            args=args,
            on_result=self._on_training_result,
            on_error=self._trial_task_failure,
        )

    def _maybe_buffer_training(self, trial: Trial) -> Tuple[int, float]:
        buffer_time_s = max(
            self._buffer_min_time_s,
            min(self._buffer_max_time_s, self._actor_manager.num_actor_tasks // 10),
        )
        buffer_length = self._buffer_length

        if buffer_length > 1 and trial.checkpoint_at_end:
            # If a trial checkpoint can be triggered externally,
            # it is not safe to buffer results.
            if log_once("trial_executor_buffer_checkpoint"):
                logger.warning(
                    "Disabling buffered training as you passed "
                    "`checkpoint_at_end` to `air.CheckpointConfig()`."
                )
            return 1, buffer_time_s

        if buffer_length > 1 and trial.checkpoint_freq > 0:
            return min(buffer_length, trial.checkpoint_freq), buffer_time_s

        return buffer_length, buffer_time_s

    ###
    # SAVE
    def _schedule_trial_save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TrackedCheckpoint:
        result = result or trial.last_result

        if storage == CheckpointStorage.MEMORY:
            future = self._schedule_trial_task(
                trial=trial,
                method_name="save_to_object",
                on_result=None,
                on_error=self._trial_task_failure,
                _return_future=True,
            )
            checkpoint = _TrackedCheckpoint(
                dir_or_data=future, storage_mode=storage, metrics=result
            )
        else:
            future = self._schedule_trial_task(
                trial=trial,
                method_name="save",
                on_result=self._on_saving_result,
                on_error=self._trial_task_failure,
                _return_future=True,
            )
            checkpoint = _TrackedCheckpoint(
                dir_or_data=future,
                storage_mode=storage,
                metrics=result,
                local_to_remote_path_fn=partial(
                    TrainableUtil.get_remote_storage_path,
                    logdir=trial.logdir,
                    remote_checkpoint_dir=trial.remote_checkpoint_dir,
                )
                if trial.uses_cloud_checkpointing
                else None,
            )
            trial.saving_to = checkpoint

        return checkpoint

    ###
    # RESTORE
    def _schedule_trial_restore(self, trial: Trial) -> bool:
        checkpoint = trial.checkpoint

        if checkpoint.dir_or_data is None:
            logger.debug(f"Not restoring trial {trial}: No checkpoint found.")
            return False

        kwargs = {}

        if checkpoint.storage_mode == CheckpointStorage.MEMORY:
            method_name = "restore_from_object"
            args = (checkpoint.dir_or_data,)
        elif (
            trial.uses_cloud_checkpointing
            or not trial.sync_on_checkpoint
            or not os.path.exists(checkpoint.dir_or_data)
        ):
            fallback_to_latest = bool(
                int(os.environ.get("TUNE_FALLBACK_TO_LATEST_CHECKPOINT", "1"))
            )

            method_name = "restore"
            args = (checkpoint.dir_or_data,)
            kwargs = {
                "checkpoint_node_ip": checkpoint.node_ip,
                "fallback_to_latest": fallback_to_latest,
            }
        elif trial.sync_on_checkpoint:
            checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint.dir_or_data)
            obj = Checkpoint.from_directory(checkpoint_path).to_bytes()

            method_name = "restore_from_object"
            args = (obj,)
        else:
            raise _AbortTrialExecution(
                "Pass in `sync_on_checkpoint=True` for driver-based trial"
                "restoration. Pass in an `upload_dir` for remote "
                "storage-based restoration"
            )

        trial.restoring_from = checkpoint
        self._schedule_trial_task(
            trial=trial,
            method_name=method_name,
            args=args,
            kwargs=kwargs,
            on_result=self._on_restore_result,
            on_error=self._trial_task_failure,
        )
        return True

    def _on_restore_result(self, trial: Trial, result: Any):
        self._process_trial_restore(trial)

    ###
    # EXPORT
    def _schedule_trial_export(self, trial: Trial):
        if not trial.export_formats or len(trial.export_formats) <= 0:
            return

        self._schedule_trial_task(
            trial=trial,
            method_name="export_model",
            args=(trial.export_formats,),
            on_result=None,
            on_error=self._trial_task_failure,
        )

    ###
    # RESET
    def _schedule_trial_reset(
        self,
        trial: Trial,
        new_config: Dict,
        new_experiment_tag: str,
        logger_creator: Optional[Callable[[Dict], "ray.tune.Logger"]] = None,
    ):
        trial.set_experiment_tag(new_experiment_tag)
        trial.set_config(new_config)

        # Pass magic variables
        extra_config = copy.deepcopy(new_config)
        extra_config[TRIAL_INFO] = _TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        extra_config[STDOUT_FILE] = stdout_file
        extra_config[STDERR_FILE] = stderr_file

        self._schedule_trial_task(
            trial=trial,
            method_name="reset",
            args=(extra_config,),
            kwargs={
                "logger_creator": logger_creator,
                "remote_checkpoint_dir": trial.remote_checkpoint_dir,
            },
            on_result=self._on_trial_reset,
            on_error=self._trial_task_failure,
        )

    def _on_trial_reset(self, trial: Trial, success: bool):
        if not trial:
            exception = _AbortTrialExecution(
                "Trainable runner reuse requires reset_config() to be "
                "implemented and return True."
            )
            return self._process_trial_failure(trial=trial, exception=exception)

        # Todo: continue

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("_resource_manager")
        state.pop("_actor_manager")
        return state


class _FakeRayTrialExecutor:
    def __init__(self, tune_controller: TuneController):
        self._tune_controller = tune_controller

    def pause_trial(self, trial: Trial, should_checkpoint: bool = True):
        return self._tune_controller._schedule_trial_pause(
            trial, should_checkpoint=should_checkpoint
        )

    def save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TrackedCheckpoint:
        return self._tune_controller._schedule_trial_save(
            trial=trial, storage=storage, result=result
        )

    def has_resources_for_trial(self, trial: Trial):
        return True
