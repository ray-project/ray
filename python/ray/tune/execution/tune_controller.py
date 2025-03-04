import copy
import json
import logging
import os
import time
import traceback
import warnings
from collections import defaultdict, deque
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.air import ResourceRequest
from ray.air.constants import TIME_THIS_ITER_S
from ray.air.execution import PlacementGroupResourceManager, ResourceManager
from ray.air.execution._internal import RayActorManager, TrackedActor
from ray.exceptions import RayActorError, RayTaskError
from ray.tune import CheckpointConfig
from ray.train._internal.session import _FutureTrainingResult, _TrainingResult
from ray.train._internal.storage import StorageContext
from ray.tune.callback import Callback, CallbackList
from ray.tune.error import TuneError, _AbortTrialExecution, _TuneStopTrialError
from ray.tune.execution.class_cache import _ActorClassCache
from ray.tune.execution.experiment_state import (
    _ExperimentCheckpointManager,
    _find_newest_experiment_checkpoint,
)
from ray.tune.execution.insufficient_resources_manager import (
    _InsufficientResourcesManager,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment import Experiment, Trial
from ray.tune.experiment.trial import (
    _change_working_directory,
    _get_trainable_kwargs,
    _Location,
    _noop_logger_creator,
    _TrialInfo,
)
from ray.tune.result import (
    DEBUG_METRICS,
    DEFAULT_METRIC,
    DONE,
    RESULT_DUPLICATE,
    SHOULD_CHECKPOINT,
    STDERR_FILE,
    STDOUT_FILE,
    TRIAL_INFO,
)
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import BasicVariantGenerator, SearchAlgorithm
from ray.tune.stopper import NoopStopper, Stopper
from ray.tune.tune_config import ResumeConfig
from ray.tune.utils import flatten_dict, warn_if_slow
from ray.tune.utils.log import Verbosity, _dedup_logs, has_verbosity
from ray.tune.utils.object_cache import _ObjectCache
from ray.tune.utils.resource_updater import _ResourceUpdater
from ray.tune.utils.serialization import TuneFunctionDecoder, TuneFunctionEncoder
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@DeveloperAPI
class TuneController:
    CKPT_FILE_TMPL = "experiment_state-{}.json"
    RAISE = "RAISE"

    def __init__(
        self,
        *,
        search_alg: Optional[SearchAlgorithm] = None,
        placeholder_resolvers: Optional[Dict[Tuple, Any]] = None,
        scheduler: Optional[TrialScheduler] = None,
        stopper: Optional[Stopper] = None,
        resume_config: Optional[ResumeConfig] = None,
        fail_fast: bool = False,
        checkpoint_period: Union[str, int] = None,
        callbacks: Optional[List[Callback]] = None,
        metric: Optional[str] = None,
        trial_checkpoint_config: Optional[CheckpointConfig] = None,
        storage: Optional[StorageContext] = None,
        reuse_actors: bool = False,
        resource_manager_factory: Optional[Callable[[], ResourceManager]] = None,
        _trainer_api: bool = False,
    ):
        if resource_manager_factory:
            resource_manager = resource_manager_factory()
        else:
            resource_manager = PlacementGroupResourceManager()

        self._actor_manager = RayActorManager(resource_manager=resource_manager)

        self._class_cache = _ActorClassCache()

        # Resource status
        self._resource_updater = _ResourceUpdater(None)

        # Actor <-> Trial mappings
        self._actor_to_trial: Dict[TrackedActor, Trial] = {}
        self._trial_to_actor: Dict[Trial, TrackedActor] = {}

        # Resources <-> Trial
        self._resources_to_pending_trials: Dict[
            ResourceRequest, Set[Trial]
        ] = defaultdict(set)

        # Keep track of actor states
        self._pending_trials: Set[Trial] = set()
        self._pending_trials_list: List[Trial] = []

        self._running_trials: Set[Trial] = set()

        self._paused_trials: Set[Trial] = set()

        self._stopped_trials: Set[Trial] = set()
        self._failed_trials: Set[Trial] = set()

        self._resetting_trials: Set[Trial] = set()
        self._staged_trials: Set[Trial] = set()

        # Removed actors
        self._started_actors: Set[TrackedActor] = set()

        # Map of tracked actors -> timestamp
        # The timestamp is when we requested the stop.
        # We track these actors here to force a
        # cleanup after some time (as they might be hanging).
        # Todo: This timeout logic should be moved into the actor manager.
        # This map is populated whenever we request an actor stop:
        #  - Regular STOP decision
        #  - Removing an actor because its trial REUSEs a different trial's actor
        #  - Removing a cached actor because it's not needed anymore
        # Actors are only tracked in this map if they actually started (not if they
        # were only requested but never started).
        # Actors are removed from this map:
        #  - When the STOP resolved and the actor actually stopped
        #  - When they are forcefully cleaned up after the timeout.
        self._stopping_actors: Dict[TrackedActor, float] = {}
        self._earliest_stopping_actor: float = float("inf")
        self._actor_cleanup_timeout: int = int(
            os.environ.get("TUNE_FORCE_TRIAL_CLEANUP_S", "600")
        )
        self._actor_force_cleanup_timeout: int = 10

        # Reuse actors
        self._reuse_actors = reuse_actors
        self._actor_cache = _ObjectCache(may_keep_one=True)

        # Trial metadata for experiment checkpoints
        self._trials_to_cache: Set[Trial] = set()
        self._trial_metadata: Dict[str, str] = {}

        # TRAINING
        self._buffer_length = int(os.getenv("TUNE_RESULT_BUFFER_LENGTH", 1))
        self._buffer_min_time_s = float(os.getenv("TUNE_RESULT_BUFFER_MIN_TIME_S", 0.0))
        self._buffer_max_time_s = float(
            os.getenv("TUNE_RESULT_BUFFER_MAX_TIME_S", 100.0)
        )

        # Legacy TrialRunner init
        self._search_alg = search_alg or BasicVariantGenerator()
        self._placeholder_resolvers = placeholder_resolvers
        self._scheduler_alg = scheduler or FIFOScheduler()
        self._callbacks = CallbackList(callbacks or [])
        self._insufficient_resources_manager = _InsufficientResourcesManager(
            for_train=_trainer_api
        )
        self._pending_trial_queue_times = {}

        self._max_pending_trials = _get_max_pending_trials(self._search_alg)

        self._storage = storage
        self._metric = metric

        self._total_time = 0
        self._iteration = 0
        self._has_errored = False
        self._fail_fast = fail_fast
        if isinstance(self._fail_fast, str):
            self._fail_fast = self._fail_fast.upper()
            if self._fail_fast == self.RAISE:
                warnings.warn(
                    "fail_fast='raise' detected. Be careful when using this "
                    "mode as resources (such as Ray processes, "
                    "file descriptors, and temporary files) may not be "
                    "cleaned up properly. To use "
                    "a safer mode, use fail_fast=True."
                )
            else:
                raise ValueError(
                    "fail_fast must be one of {bool, RAISE}. " f"Got {self._fail_fast}."
                )

        self._print_trial_errors = bool(
            int(os.environ.get("TUNE_PRINT_ALL_TRIAL_ERRORS", "1"))
        )

        self._trials: List[Trial] = []
        self._live_trials: Set[Trial] = set()  # Set of non-terminated trials
        self._cached_trial_decisions = {}
        self._queued_trial_decisions = {}

        self._stop_queue = []
        self._should_stop_experiment = False  # used by TuneServer

        self._stopper = stopper or NoopStopper()

        self._start_time = time.time()

        self._session_str = datetime.fromtimestamp(self._start_time).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )

        if checkpoint_period is None:
            checkpoint_period = os.getenv("TUNE_GLOBAL_CHECKPOINT_S", "auto")

        self._checkpoint_period = checkpoint_period
        self._trial_checkpoint_config = trial_checkpoint_config or CheckpointConfig()
        self._checkpoint_manager = self._create_checkpoint_manager()

        self._resumed = False

        if resume_config is not None:
            # Use the metadata file to restore TuneController state
            try:
                self.resume(resume_config=resume_config)
                self._resumed = True
            except Exception as e:
                if has_verbosity(Verbosity.V3_TRIAL_DETAILS):
                    logger.error(str(e))
                logger.exception("Failed to restore the run state.")
                if self._fail_fast:
                    raise
                logger.info("Restarting experiment.")
        else:
            logger.debug("Starting a new experiment.")

    def _wrapped(self):
        """Return wrapped tune controller to be passed to scheduler/searchers."""
        return TrialRunnerWrapper(
            self,
            trial_executor=_FakeRayTrialExecutor(self),
            runner_whitelist_attr={
                "search_alg",
                "get_trials",
                "get_live_trials",
                "_set_trial_status",
                "pause_trial",
                "stop_trial",
                "_schedule_trial_save",
            },
            executor_whitelist_attr={
                "has_resources_for_trial",
                "pause_trial",
                "save",
                "_resource_updater",
            },
        )

    @property
    def resumed(self):
        return self._resumed

    @property
    def search_alg(self):
        return self._search_alg

    @property
    def scheduler_alg(self):
        return self._scheduler_alg

    def setup_experiments(
        self, experiments: List[Experiment], total_num_samples: int
    ) -> None:
        """Obtains any necessary information from experiments.

        Mainly used to setup callbacks.

        Args:
            experiments: List of Experiments
                to use.
            total_num_samples: Total number of samples
                factoring in grid search samplers.
        """
        experiment = experiments[0]
        spec = experiment.public_spec if experiment else {}
        spec["total_num_samples"] = total_num_samples
        self._callbacks.setup(**spec)

    def end_experiment_callbacks(self) -> None:
        """Calls ``on_experiment_end`` method in callbacks."""
        self._callbacks.on_experiment_end(trials=self._trials)

    @property
    def experiment_state_file_name(self) -> str:
        return self.CKPT_FILE_TMPL.format(self._session_str)

    @property
    def experiment_state_path(self) -> str:
        """Returns the local experiment checkpoint path."""
        return Path(
            self._storage.experiment_driver_staging_path,
            self.experiment_state_file_name,
        ).as_posix()

    @property
    def experiment_path(self) -> str:
        return self._storage.experiment_fs_path

    def _create_checkpoint_manager(self):
        return _ExperimentCheckpointManager(
            storage=self._storage,
            checkpoint_period=self._checkpoint_period,
            sync_every_n_trial_checkpoints=self._trial_checkpoint_config.num_to_keep,
        )

    def save_to_dir(self):
        """Save TuneController state to the local staging experiment directory.

        This includes:
        - trial states
        - TuneController internal state (all the serializable attributes)
        - the searcher state
        - the callback states
        """
        # Get state from trial executor and runner
        runner_state = {
            # Trials
            "trial_data": list(self._get_trial_checkpoints().values()),
            # Experiment data
            "runner_data": self.__getstate__(),
            # Metadata
            "stats": {"start_time": self._start_time},
        }

        driver_staging_path = self._storage.experiment_driver_staging_path
        os.makedirs(driver_staging_path, exist_ok=True)
        with open(
            Path(driver_staging_path, self.experiment_state_file_name),
            "w",
        ) as f:
            json.dump(runner_state, f, cls=TuneFunctionEncoder)

        self._search_alg.save_to_dir(driver_staging_path, session_str=self._session_str)
        self._callbacks.save_to_dir(driver_staging_path, session_str=self._session_str)

    def checkpoint(self, force: bool = False, wait: bool = False):
        self._checkpoint_manager.sync_up_experiment_state(
            save_fn=self.save_to_dir, force=force, wait=wait
        )

    def _requeue_restored_trials(
        self, trials: List[Trial], resume_config: ResumeConfig
    ):
        # Set trial statuses according to the resume configuration
        for trial in sorted(
            trials, key=lambda t: t.run_metadata.last_result_time, reverse=True
        ):
            if trial.status == Trial.ERROR:
                resume_type = resume_config.errored
            elif trial.status == Trial.TERMINATED:
                resume_type = resume_config.finished
            else:  # Unfinished (PENDING, RUNNING, PAUSED)
                resume_type = resume_config.unfinished

            trial_to_add = None
            if resume_type == ResumeConfig.ResumeType.RESUME:
                # Keep trial ID on resume
                trial_to_add = trial
                trial_to_add.run_metadata.error_filename = None
                trial_to_add.run_metadata.pickled_error_filename = None
                trial_to_add.set_status(Trial.PENDING)
            elif resume_type == ResumeConfig.ResumeType.RESTART:
                trial_to_add = trial.reset()
                trial_to_add.restore_path = None
            elif resume_type == ResumeConfig.ResumeType.SKIP:
                trial_to_add = trial
                if trial_to_add.status != Trial.ERROR:
                    # Set the status to terminated to skip it.
                    # Keep errored trial status as ERROR.
                    trial_to_add.set_status(Trial.TERMINATED)
            else:
                raise ValueError(f"Unknown resume type: {resume_type}")
            assert trial_to_add is not None

            self.add_trial(trial_to_add)

    def _restore_trials(self, experiment_state: Dict) -> List[Trial]:
        trials = []
        for trial_json_state, trial_runtime_metadata in experiment_state["trial_data"]:
            trial = Trial.from_json_state(trial_json_state)
            trial.restore_run_metadata(trial_runtime_metadata)

            # The following properties may be updated on restoration
            # Ex: moved local/cloud experiment directory

            # Propagate updated storage ctx properties to the trial's restored copy.
            new_storage = copy.copy(trial.storage)
            new_storage.storage_filesystem = self._storage.storage_filesystem
            new_storage.storage_fs_path = self._storage.storage_fs_path
            new_storage.experiment_dir_name = self._storage.experiment_dir_name

            # ATTN: `trial.set_storage` is used intentionally, since it
            # also updates the absolute paths and filesystem of tracked checkpoints.
            trial.set_storage(new_storage)

            # Avoid creating logdir in client mode for returned trial results,
            # since the dir might not be creatable locally.
            # TODO(ekl) this is kind of a hack.
            if not ray.util.client.ray.is_connected():
                trial.init_local_path()  # Create logdir if it does not exist

            trials.append(trial)

        # NOTE: The restored run should reuse the same driver staging directory.
        self._storage._timestamp = trials[0].storage._timestamp

        return trials

    def resume(self, resume_config: ResumeConfig):
        """Resumes all checkpointed trials from previous run.

        Requires user to manually re-register their objects. Also stops
        all ongoing trials.
        """
        # 1. Restore TuneController state
        # Find newest state file
        newest_state_path = _find_newest_experiment_checkpoint(
            self._storage.experiment_fs_path, fs=self._storage.storage_filesystem
        )

        if newest_state_path is None:
            raise ValueError(
                f"Tried to resume experiment from directory "
                f"'{self._storage.experiment_fs_path}', but no "
                f"experiment state file of the form '{TuneController.CKPT_FILE_TMPL}' "
                "was found. This is expected if you are launching a new experiment."
            )

        logger.info(
            "Restoring the run from the latest experiment state file: "
            f"{Path(newest_state_path).name}"
        )
        with self._storage.storage_filesystem.open_input_stream(newest_state_path) as f:
            experiment_state = json.loads(f.readall(), cls=TuneFunctionDecoder)

        self.__setstate__(experiment_state["runner_data"])

        # 2. Get the trial states that the run left off at.
        trials = self._restore_trials(experiment_state)

        # 3. Restore search algorithm and callback state
        # Download the search algorithm and callback state to the driver staging dir.
        self._checkpoint_manager.sync_down_experiment_state()

        driver_staging_dir = self._storage.experiment_driver_staging_path
        if self._search_alg.has_checkpoint(driver_staging_dir):
            self._search_alg.restore_from_dir(driver_staging_dir)

        if self._callbacks.can_restore(driver_staging_dir):
            self._callbacks.restore_from_dir(driver_staging_dir)

        # 4. Re-queue trials as needed, depending on their status.
        self._requeue_restored_trials(trials, resume_config)

    def update_max_pending_trials(self, max_pending_trials: Optional[int] = None):
        self._max_pending_trials = max_pending_trials or _get_max_pending_trials(
            self._search_alg
        )

    def update_pending_trial_resources(
        self, resources: Union[dict, PlacementGroupFactory]
    ):
        """Update trial resources when resuming from checkpoint.

        Only updating the pending ones.
        """
        assert resources
        if isinstance(resources, dict) and "gpu" not in resources:
            resources["gpu"] = 0
        for trial in self._trials:
            if trial.status == Trial.PENDING:
                trial.update_resources(resources=resources)

    def is_finished(self):
        """Returns whether all trials have finished running."""
        # The checks here are partly redundant but optimized for quick
        # evaluation. Specifically, if there are live trials, we check
        # these live trials first. Only if none of the live trials is
        # live anymore do we loop over all trials for a final check.
        trials_done = (
            len(self._live_trials) == 0
            or all(trial.is_finished() for trial in self._live_trials)
        ) and all(trial.is_finished() for trial in self._trials)
        return trials_done and self._search_alg.is_finished()

    def get_trial(self, tid):
        trial = [t for t in self._trials if t.trial_id == tid]
        return trial[0] if trial else None

    def get_trials(self):
        """Returns the list of trials managed by this TrialRunner.

        Note that the caller usually should not mutate trial state directly.
        """
        return self._trials

    def get_live_trials(self):
        """Returns the set of trials that are not in Trial.TERMINATED state."""
        return self._live_trials

    def add_trial(self, trial: Trial):
        """Adds a new trial to this TrialRunner.

        Trials may be added at any time.

        Args:
            trial: Trial to queue.
        """
        # If the config map has had all the references replaced with placeholders,
        # resolve them before adding the trial.
        if self._placeholder_resolvers:
            trial.resolve_config_placeholders(self._placeholder_resolvers)

        # With trial.config resolved, create placement group factory if needed.
        trial.create_placement_group_factory()

        self._trials.append(trial)
        if trial.status != Trial.TERMINATED:
            self._live_trials.add(trial)
        with warn_if_slow("scheduler.on_trial_add"):
            self._scheduler_alg.on_trial_add(self._wrapped(), trial)
        self._mark_trial_to_checkpoint(trial)

        logger.debug(f"Adding trial {trial} with status {trial.status}")

        status_str_map = {
            Trial.PENDING: self._pending_trials,
            Trial.RUNNING: self._running_trials,
            Trial.PAUSED: self._paused_trials,
            Trial.TERMINATED: self._stopped_trials,
            Trial.ERROR: self._failed_trials,
        }

        status_str_map[trial.status].add(trial)

        if trial.status == Trial.PENDING:
            self._pending_trials_list.append(trial)
            self._resources_to_pending_trials[trial.placement_group_factory].add(trial)

    def _update_trial_queue(self, blocking: bool = False, timeout: int = 600) -> bool:
        """Adds next trials to queue if possible.

        Note that the timeout is currently unexposed to the user.

        Args:
            blocking: Blocks until either a trial is available
                or is_finished (timeout or search algorithm finishes).
            timeout: Seconds before blocking times out.

        Returns:
            Boolean indicating if a new trial was created or not.
        """
        trial = self._search_alg.next_trial()
        if blocking and not trial:
            start = time.time()
            # Checking `is_finished` instead of _search_alg.is_finished
            # is fine because blocking only occurs if all trials are
            # finished and search_algorithm is not yet finished
            while (
                not trial and not self.is_finished() and time.time() - start < timeout
            ):
                logger.debug("Blocking for next trial...")
                trial = self._search_alg.next_trial()
                time.sleep(1)

        if trial:
            self.add_trial(trial)
            return True

        return False

    def _used_resources_string(self) -> str:
        allocated_resources = self._actor_manager.get_live_actors_resources()

        return self._resource_updater.debug_string(allocated_resources)

    def on_step_begin(self):
        self._resource_updater.update_avail_resources()

    def on_step_end(self):
        self._cleanup_cached_actors(force_all=False)
        self._cleanup_stopping_actors(force_all=False)

    def _cleanup_cached_actors(self, force_all: bool = False):
        if (
            self._search_alg.is_finished()
            and not self._staged_trials
            and self._actor_cache.total_max_objects == 0
        ):
            # If there are no more trials coming in, no trials are pending execution,
            # and we don't explicitly want to cache objects, we can evict the full
            # cache.
            force_all = True

        for tracked_actor in self._actor_cache.flush_cached_objects(
            force_all=force_all
        ):
            logger.debug(f"Cleaning up cached actor: {tracked_actor}")
            # Unset termination callbacks as no trial is associated
            tracked_actor.set_on_stop(None)
            tracked_actor.set_on_error(None)
            self._remove_actor(tracked_actor=tracked_actor)

    def _cleanup_stopping_actors(self, force_all: bool = False):
        now = time.monotonic()

        if (
            not force_all
            and now - self._earliest_stopping_actor <= self._actor_cleanup_timeout
        ):
            # If the earliest actor to timeout has not reached the timeout, return
            return

        # This is a bit costly, so we want to avoid running it too often
        times = deque(
            sorted(
                [
                    (timestamp, tracked_actor)
                    for tracked_actor, timestamp in self._stopping_actors.items()
                ],
                key=lambda item: item[0],
            )
        )

        while times and (
            force_all or time.monotonic() - times[0][0] > self._actor_cleanup_timeout
        ):
            if (
                time.monotonic() - times[0][0] < self._actor_force_cleanup_timeout
            ) and self._actor_manager.is_actor_started(tracked_actor=times[0][1]):
                # Even if force_all=True, we give the actors time to clean up
                self._actor_manager.next(timeout=1)
                continue

            _, tracked_actor = times.popleft()

            if tracked_actor not in self._stopping_actors:
                # Actor stopping has been handled by the block above
                continue

            if self._actor_manager.is_actor_started(tracked_actor=tracked_actor):
                logger.debug(f"Forcefully killing actor: {tracked_actor}")
                self._actor_manager.remove_actor(tracked_actor=tracked_actor, kill=True)
            self._stopping_actors.pop(tracked_actor)

        if times:
            self._earliest_stopping_actor = times[0][0]
        else:
            self._earliest_stopping_actor = float("inf")

    def step(self):
        if self.is_finished():
            raise TuneError("Called step when all trials finished?")

        with warn_if_slow("on_step_begin"):
            self.on_step_begin()

        with warn_if_slow("callbacks.on_step_begin"):
            self._callbacks.on_step_begin(
                iteration=self._iteration, trials=self._trials
            )

        # Ask searcher for more trials
        self._maybe_update_trial_queue()

        # Start actors for added trials
        self._maybe_add_actors()

        # Handle one event
        if not self._actor_manager.next(timeout=0.1):
            # If there are no actors running, warn about potentially
            # insufficient resources
            if not self._actor_manager.num_live_actors:
                self._insufficient_resources_manager.on_no_available_trials(
                    self.get_trials()
                )

        # Maybe stop whole experiment
        self._stop_experiment_if_needed()

        # Maybe save experiment state
        try:
            self.checkpoint()
        except Exception as e:
            logger.warning(f"Trial controller checkpointing failed: {str(e)}")
            raise e

        self._iteration += 1

        with warn_if_slow("on_step_end"):
            self.on_step_end()
        with warn_if_slow("callbacks.on_step_end"):
            self._callbacks.on_step_end(iteration=self._iteration, trials=self._trials)

    def _set_trial_status(self, trial: Trial, status: str):
        """Set trial to a specific status.

        This will keep track of trials with specific statuses in sets.

        For PENDING and PAUSED trials we also keep a list of trials to be able
        to retain FIFO ordering. See ``_maybe_add_actors`` for details.

        Lastly we also keep a mapping from resources to pending/paused trials
        to be able to efficiently start trials for cached actors.
        """
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

        # We keep a log for pending trials for FIFO scheduling.
        # We do not need to remove from this list as we will just discard
        # items that are in this list but not in the respective set.
        if status == Trial.PENDING:
            self._pending_trials_list.append(trial)
            self._resources_to_pending_trials[trial.placement_group_factory].add(trial)
        else:
            self._resources_to_pending_trials[trial.placement_group_factory].discard(
                trial
            )

        trial.set_status(status)

    def _get_trial_checkpoints(self) -> Dict[str, str]:
        for trial in self._trials_to_cache:
            self._trial_metadata[trial.trial_id] = trial.get_json_state()
        self._trials_to_cache.clear()
        return self._trial_metadata

    def _mark_trial_to_checkpoint(self, trial: Trial):
        self._trials_to_cache.add(trial)

    ###
    # UPDATE TRIALS
    def _maybe_update_trial_queue(self):
        """Ask the searcher for more trials."""
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
        logger.debug("CLEANING UP all trials")

        for tracked_actor in list(self._actor_to_trial):
            trial = self._actor_to_trial[tracked_actor]
            logger.debug(
                f"Scheduling trial stop at end of experiment (trial {trial}): "
                f"{tracked_actor}"
            )
            self._schedule_trial_stop(trial)

        # Clean up cached actors now
        self._cleanup_cached_actors(force_all=True)

        start = time.monotonic()
        while time.monotonic() - start < 5 and self._actor_manager.num_total_actors:
            if _dedup_logs("actor_manager_cleanup", str(start)):
                logger.debug(
                    "Waiting for actor manager to clean up final state [dedup]"
                )
            self._actor_manager.next(timeout=1)

        logger.debug("Force cleanup of remaining actors")
        self._cleanup_stopping_actors(force_all=True)

        self._actor_manager.cleanup()

    def _remove_actor(self, tracked_actor: TrackedActor):
        stop_future = self._actor_manager.schedule_actor_task(
            tracked_actor, "stop", _return_future=True
        )
        now = time.monotonic()

        if self._actor_manager.remove_actor(
            tracked_actor, kill=False, stop_future=stop_future
        ):
            # If the actor was previously alive, track
            self._stopping_actors[tracked_actor] = now
            self._earliest_stopping_actor = min(self._earliest_stopping_actor, now)

    ###
    # ADD ACTORS
    def _maybe_add_actors(self) -> None:
        """Add actors for pending and paused trials.

        For actors that have not been staged, yet, we request an actor.

        For actors that have been staged, already, we try to reuse a cached actor.

        First, we handle the trial that the scheduler chooses to run.

        Then, we handle all trials that are pending.

        Lastly, we see if we have cached actors that we can assign to a pending or
        paused trial. This can be the case when a trial has not been staged, yet,
        for instance because the number of staging trials was too large.
        """

        ###
        # 1: Start trial that the scheduler wants to run
        with warn_if_slow("choose_trial_to_run"):
            trial_to_run = self._scheduler_alg.choose_trial_to_run(self._wrapped())

        if trial_to_run:
            if _dedup_logs("trial_to_run_chosen", trial_to_run.trial_id):
                logger.debug(
                    f"Chose trial to run from scheduler: {trial_to_run} [dedup]"
                )
            if (
                trial_to_run not in self._staged_trials
                and trial_to_run not in self._trial_to_actor
            ):
                logger.debug(f"Staging trial to run: {trial_to_run}")
                self._set_trial_status(trial_to_run, Trial.PENDING)
                self._staged_trials.add(trial_to_run)
                self._actor_cache.increase_max(trial_to_run.placement_group_factory)
                # schedule_trial_actor also potentially uses cached actors
                self._schedule_trial_actor(trial_to_run)
            else:
                # Otherwise, only try to use the cached actor
                if _dedup_logs("trial_to_run_reuse", trial_to_run.trial_id):
                    logger.debug(
                        f"Trying to re-use actor for trial to run: {trial_to_run} "
                        f"[dedup]"
                    )
                self._maybe_reuse_cached_actor(trial_to_run)

        ###
        # 2: Start trials that are PENDING
        def _maybe_add_actors(candidates: List[Trial]):
            new_candidates = []

            while candidates:
                if self._actor_manager.num_pending_actors >= self._max_pending_trials:
                    break

                trial = candidates.pop(0)

                # If the trial is part of the list, but not of the set,
                # we just ignore it. Removing it from the list on status
                # change is too expensive.
                if trial not in self._pending_trials:
                    continue

                if trial in self._trial_to_actor:
                    new_candidates.append(trial)
                    continue

                if trial in self._staged_trials:
                    self._maybe_reuse_cached_actor(trial)
                    continue

                logger.debug(f"Scheduling actor for enqueued trial: {trial}")
                self._staged_trials.add(trial)
                self._actor_cache.increase_max(trial.placement_group_factory)
                self._schedule_trial_actor(trial)

            return new_candidates + candidates

        self._pending_trials_list = _maybe_add_actors(self._pending_trials_list)

        ###
        # 3: Start any trial that can be started with a cached actor
        if self._actor_cache.num_cached_objects:
            for resource in self._resources_to_pending_trials:
                if not self._resources_to_pending_trials[resource]:
                    continue

                if not self._actor_cache.has_cached_object(resource):
                    continue

                start_trial = self._resources_to_pending_trials[resource].pop()
                logger.debug(
                    f"Trying to re-use actor for enqueued trial: {start_trial}"
                )
                if not self._maybe_reuse_cached_actor(start_trial):
                    self._resources_to_pending_trials[resource].add(start_trial)
                else:
                    if start_trial not in self._staged_trials:
                        self._staged_trials.add(start_trial)
                        self._actor_cache.increase_max(
                            start_trial.placement_group_factory
                        )

    def _maybe_reuse_cached_actor(self, trial: Trial) -> bool:
        """Maybe reuse a cached actor for a trial.

        If an actor has been scheduled for the trial already,
        this will remove the original actor.
        """
        if trial in self._resetting_trials:
            return True

        resource_request = trial.placement_group_factory

        if not self._actor_cache.has_cached_object(resource_request):
            return False

        cached_actor = self._actor_cache.pop_cached_object(resource_request)
        logger.debug(f"Reusing ACTOR for trial {trial}: {cached_actor}")

        if trial in self._trial_to_actor:
            original_actor = self._trial_to_actor.pop(trial)
            self._actor_to_trial.pop(original_actor)

            logger.debug(f"Removing ORIGINAL ACTOR for trial {trial}: {original_actor}")
            self._remove_actor(tracked_actor=original_actor)

        self._trial_to_actor[trial] = cached_actor
        self._actor_to_trial[cached_actor] = trial

        # Todo: get rid of Trial.runner
        ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
            cached_actor
        ][0]
        trial.set_ray_actor(ray_actor)

        self._schedule_trial_reset(trial, trial.config, trial.experiment_tag)

        return True

    def _schedule_trial_actor(self, trial: Trial):
        """Schedule an actor for a trial.

        If a cached actor is available, use it. Otherwise, request a
        new actor.
        """
        logger.debug(f"Trying to schedule new ACTOR for trial {trial}")

        assert trial.status == Trial.PENDING

        trial.init_local_path()
        # We checkpoint metadata here to try mitigating logdir duplication
        self._mark_trial_to_checkpoint(trial)

        if self._maybe_reuse_cached_actor(trial):
            return

        # Safeguard
        if trial in self._trial_to_actor:
            raise RuntimeError(
                f"Tried to request a new actor for trial {trial}, but an old "
                f"actor still exists. This can lead to leaked resources. The old "
                f"actor should be removed first. "
                f"This is an internal problem in Ray Tune. If you encounter this "
                f"error, please raise an issue on "
                f"https://github.com/ray-project/ray/issues"
            )

        trainable_cls = trial.get_trainable_cls()
        if not trainable_cls:
            exception = _AbortTrialExecution(
                f"Invalid trainable: {trial.trainable_name}. If you passed "
                f"a string, make sure the trainable was registered before."
            )
            trial.handle_error(exception)
            self._schedule_trial_stop(trial, exception=exception)
            return

        _actor_cls = self._class_cache.get(trainable_cls)

        trial.set_location(_Location())
        trainable_kwargs = _get_trainable_kwargs(trial=trial)

        with _change_working_directory(trial):
            tracked_actor = self._actor_manager.add_actor(
                cls=_actor_cls,
                resource_request=trial.placement_group_factory,
                kwargs=trainable_kwargs,
                on_start=self._actor_started,
                on_stop=self._actor_stopped,
                on_error=self._actor_failed,
            )
            self._trial_to_actor[trial] = tracked_actor
            self._actor_to_trial[tracked_actor] = trial

        logger.debug(
            f"Scheduled new ACTOR for trial {trial}: {tracked_actor}. "
            f"Resources: {trial.placement_group_factory}"
        )

    def _unstage_trial_with_resources(self, trial: Trial):
        """Unstage trial, or one with the same resources as ``trial``."""
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

        if self._search_alg.is_finished() and not self._staged_trials:
            logger.debug(
                f"Not caching actor of trial {trial} as the search is over "
                f"and no more trials are staged."
            )
            return False

        tracked_actor = self._trial_to_actor[trial]

        if (
            not self._actor_manager.is_actor_started(tracked_actor)
            or self._actor_manager.is_actor_failed(tracked_actor)
            or tracked_actor not in self._started_actors
        ):
            logger.debug(
                f"Not caching actor of trial {trial} as it has not been started, yet: "
                f"{tracked_actor}"
            )
            return False

        if not self._actor_cache.cache_object(
            trial.placement_group_factory, tracked_actor
        ):
            logger.debug(
                f"Could not cache actor of trial {trial} for "
                "reuse, as there are no pending trials "
                "requiring its resources."
            )
            return False

        logger.debug(f"Caching actor of trial {trial} for re-use: {tracked_actor}")

        tracked_actor = self._trial_to_actor.pop(trial)
        self._actor_to_trial.pop(tracked_actor)

        trial.set_ray_actor(None)

        return True

    def _actor_started(self, tracked_actor: TrackedActor, log: str = "STARTED"):
        self._started_actors.add(tracked_actor)

        trial = self._actor_to_trial[tracked_actor]

        logger.debug(f"Actor {log} for trial {trial}: {tracked_actor}")

        self._unstage_trial_with_resources(trial)

        ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
            tracked_actor
        ][0]
        trial.set_ray_actor(ray_actor)

        self._callbacks.on_trial_start(
            iteration=self._iteration, trials=self._trials, trial=trial
        )

        self._set_trial_status(trial, Trial.RUNNING)

        self._mark_trial_to_checkpoint(trial)

        if not self._schedule_trial_restore(trial):
            self._schedule_trial_train(trial)

    def _actor_stopped(self, tracked_actor: TrackedActor):
        if tracked_actor in self._actor_to_trial:
            trial = self._actor_to_trial.pop(tracked_actor)
            logger.debug(f"Actor STOPPED for trial {trial}: {tracked_actor}")
            self._trial_to_actor.pop(trial)
            trial.set_ray_actor(None)

        logger.debug(f"Actor STOPPED: {tracked_actor}")

        self._stopping_actors.pop(tracked_actor, None)
        self._started_actors.discard(tracked_actor)

    def _actor_failed(self, tracked_actor: TrackedActor, exception: Exception):
        trial = self._actor_to_trial[tracked_actor]

        logger.debug(
            f"Actor FAILED for trial {trial}: {tracked_actor}. "
            f"Exception: {exception}"
        )

        if trial in (self._pending_trials | self._paused_trials):
            # First, set to running (needed downstream in _process_trial_failure)
            self._set_trial_status(trial, Trial.RUNNING)

            logger.debug(
                f"Trial {trial} failed in its creation task. Unstaging "
                f"to allow it to be re-scheduled."
            )

            self._unstage_trial_with_resources(trial)
            self._trial_task_failure(trial, exception=exception)

        self._actor_manager.clear_actor_task_futures(tracked_actor)

        # Clean up actor
        tracked_actor.set_on_stop(None)
        tracked_actor.set_on_error(None)
        self._actor_manager.remove_actor(tracked_actor, kill=False)

        # Trigger actor stopped callback
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
        """Schedule an actor task future for a trial.

        This is a wrapper around ``ActorManager.schedule_actor_task``. This method
        retrieves the tracked actor for a trial to kick off the task.

        It also wraps around the callbacks, retrieving the trial object given the
        tracked actor.
        """

        tracked_actor = self._trial_to_actor[trial]

        _on_result = None
        _on_error = None

        args = args or tuple()
        kwargs = kwargs or {}

        if on_result:

            def _on_result(tracked_actor: TrackedActor, *args, **kwargs):
                assert trial == self._actor_to_trial[tracked_actor]
                logger.debug(
                    f"Future {method_name.upper()} RESOLVED for trial {trial}: "
                    f"{args}, {kwargs}"
                )
                try:
                    on_result(trial, *args, **kwargs)
                except Exception as e:
                    logger.debug(
                        f"Error handling {method_name.upper()} result "
                        f"for trial {trial}: {e}"
                    )
                    if e is TuneError or self._fail_fast == self.RAISE:
                        raise e
                    else:
                        raise TuneError(traceback.format_exc())

        if on_error:

            def _on_error(tracked_actor: TrackedActor, exception: Exception):
                # If the actor failed, it has already been cleaned up.
                if tracked_actor not in self._actor_to_trial:
                    assert isinstance(exception, RayActorError), type(exception)
                else:
                    assert trial == self._actor_to_trial[tracked_actor]

                logger.debug(
                    f"Future {method_name.upper()} FAILED for trial {trial}: "
                    f"{exception}"
                )
                try:
                    on_error(trial, exception)
                except Exception as e:
                    logger.debug(
                        f"Error handling {method_name.upper()} failure "
                        f"for trial {trial}: {e}"
                    )
                    if e is TuneError or self._fail_fast == self.RAISE:
                        raise e
                    else:
                        raise TuneError(traceback.format_exc())

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

    def _queue_decision(self, trial, decision):
        # Get old decision, setting it to the current decision if it isn't set
        old_decision = self._queued_trial_decisions.setdefault(trial.trial_id, decision)

        # Stopping always takes precedence. If we decided to stop, just quit
        if old_decision is TrialScheduler.STOP:
            return

        # The old decision wasn't STOP. We update the decision only if it is
        # STOP or PAUSE. The action will only be CONTINUE if it was set by
        # the first received result and was never updated after that.
        if decision is TrialScheduler.STOP or decision is TrialScheduler.PAUSE:
            self._queued_trial_decisions[trial.trial_id] = decision

    def _execute_action(self, trial: Trial, decision: str, after_save: bool = False):
        """Executes action based on decision.

        Args:
            trial: Trial to act on.
            decision: Scheduling decision to undertake.
        """
        if decision == TrialScheduler.CONTINUE:
            self._schedule_trial_train(trial)
        elif decision == TrialScheduler.PAUSE:
            self.pause_trial(trial, should_checkpoint=not after_save)
        elif decision == TrialScheduler.STOP:
            self.stop_trial(trial)
        elif decision == TrialScheduler.NOOP:
            pass
        else:
            raise ValueError("Invalid decision: {}".format(decision))

    def _maybe_execute_queued_decision(self, trial: Trial, after_save: bool = False):
        # `self._queued_trial_decisions` now contains a final decision
        # based on all results
        final_decision = self._queued_trial_decisions.pop(trial.trial_id, None)
        if final_decision:
            logger.debug(
                f"Executing final queued decision for {trial}: {final_decision}"
            )
            self._execute_action(trial, final_decision, after_save=after_save)

    def _stop_experiment_if_needed(self):
        """Stops all trials."""
        fail_fast = self._fail_fast and self._has_errored
        if self._stopper.stop_all() or fail_fast or self._should_stop_experiment:
            self._search_alg.set_finished()
            [
                self._schedule_trial_stop(t)
                for t in self._trials
                if t.status not in {Trial.ERROR, Trial.TERMINATED}
            ]

    ###
    # Failure
    def _trial_task_failure(self, trial: Trial, exception: Exception):
        if self._fail_fast == self.RAISE:
            raise exception
        else:
            if self._print_trial_errors:
                logger.error(f"Trial task failed for trial {trial}", exc_info=exception)
            self._process_trial_failure(trial, exception=exception)

    def _process_trial_failure(
        self,
        trial: Trial,
        exception: Union[TuneError, RayTaskError, RayActorError],
    ):
        """Handle trial failure.

        Attempt trial recovery if possible, clean up state otherwise.

        Args:
            trial: Failed trial.
            exception: Exception prior to invoking this method.
        """
        self._has_errored = True
        trial.handle_error(exception)
        if trial.status == Trial.RUNNING and trial.should_recover():
            self._try_recover(trial, exc=exception)
            self._callbacks.on_trial_recover(
                iteration=self._iteration, trials=self._trials, trial=trial
            )
        elif trial.status in {Trial.RUNNING, Trial.PENDING}:
            self._scheduler_alg.on_trial_error(self, trial)
            self._search_alg.on_trial_complete(trial.trial_id, error=True)
            self._schedule_trial_stop(trial, exception=exception)
            self._callbacks.on_trial_error(
                iteration=self._iteration, trials=self._trials, trial=trial
            )

    def _schedule_trial_stop(self, trial: Trial, exception: Optional[Exception] = None):
        if trial.status == Trial.ERROR:
            logger.debug(f"Not requesting trial STOP as it is ERROR already: {trial}")
            return

        logger.debug(f"Requesting to STOP actor for trial {trial}")

        if trial.is_saving:
            logger.debug(
                f"Trial {trial} is currently saving/pausing. Scheduling STOP after "
                f"save resolved."
            )
            self._cached_trial_decisions[trial.trial_id] = TrialScheduler.STOP

        trial.temporary_state.saving_to = None
        trial.temporary_state.restoring_from = None

        self._set_trial_status(trial, Trial.ERROR if exception else Trial.TERMINATED)
        trial.set_location(_Location())

        if trial not in self._trial_to_actor:
            logger.debug(f"Will not STOP trial actor as it is not live: {trial}")
            return

        tracked_actor = self._trial_to_actor[trial]

        self._actor_manager.clear_actor_task_futures(tracked_actor=tracked_actor)

        self._mark_trial_to_checkpoint(trial)

        if not exception and self._maybe_cache_trial_actor(trial):
            # Trial runner has been cached
            return

        logger.debug(f"Terminating actor for trial {trial}: {tracked_actor}")

        tracked_actor = self._trial_to_actor.pop(trial)
        self._actor_to_trial.pop(tracked_actor)

        trial.set_ray_actor(None)

        self._remove_actor(tracked_actor=tracked_actor)

    def stop_trial(self, trial):
        """The canonical implementation of stopping a trial.

        Trials may be in any external status when this function is called.
        If trial is in state PENDING or PAUSED, calls `on_trial_remove` for
        scheduler and `on_trial_complete()` for search_alg.
        If trial is in state RUNNING, calls `on_trial_complete` for scheduler
        and search_alg if RUNNING. Caller to ensure that there is no
        outstanding future to be handled for the trial. If there is, the future
        would be discarded.
        """
        try:
            if trial.status in [Trial.ERROR, Trial.TERMINATED]:
                return
            elif trial.status in [Trial.PENDING, Trial.PAUSED]:
                self._scheduler_alg.on_trial_remove(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id)
            elif trial.status is Trial.RUNNING:
                # By this time trial.last_result should have been
                # updated already.
                self._scheduler_alg.on_trial_complete(
                    self, trial, flatten_dict(trial.last_result)
                )
                self._search_alg.on_trial_complete(
                    trial.trial_id, result=flatten_dict(trial.last_result)
                )
            self._callbacks.on_trial_complete(
                iteration=self._iteration, trials=self._trials, trial=trial
            )
            self._schedule_graceful_trial_stop(trial)
            self._live_trials.discard(trial)
        except Exception as e:
            logger.exception("Trial %s: Error stopping trial.", trial)
            if self._fail_fast == self.RAISE:
                raise
            if isinstance(e, TuneError):
                self._process_trial_failure(trial, exception=e)
            else:
                self._process_trial_failure(
                    trial, _TuneStopTrialError(traceback.format_exc())
                )

    def _schedule_graceful_trial_stop(self, trial: Trial):
        self._schedule_trial_export(trial)
        if trial.status != "ERROR":
            self._schedule_trial_stop(trial)

    def _schedule_trial_pause(self, trial: Trial, should_checkpoint: bool = True):
        if trial not in self._trial_to_actor:
            logger.debug(
                f"Trial PAUSE requested for trial {trial} but trial is already "
                f"stopping. Ignoring."
            )
            return

        if should_checkpoint:
            self._cached_trial_decisions[trial.trial_id] = TrialScheduler.PAUSE
            self._schedule_trial_save(trial=trial)
        else:
            self._schedule_trial_stop(trial)
            self._set_trial_status(trial, Trial.PAUSED)

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
                    "`checkpoint_at_end` to `tune.CheckpointConfig()`."
                )
            return 1, buffer_time_s

        if buffer_length > 1 and trial.checkpoint_freq > 0:
            return min(buffer_length, trial.checkpoint_freq), buffer_time_s

        return buffer_length, buffer_time_s

    ###
    # RESULT

    def _on_training_result(self, trial, result):
        if not isinstance(result, list):
            result = [result]
        with warn_if_slow("process_trial_result"):
            self._process_trial_results(trial, result)
        self._maybe_execute_queued_decision(trial, after_save=False)

    def _process_trial_results(self, trial, results):
        logger.debug(f"Processing trial results for trial {trial}: {results}")
        with warn_if_slow(
            "process_trial_results",
            message="Processing trial results took {duration:.3f} s, "
            "which may be a performance bottleneck. Please consider "
            "reporting results less frequently to Ray Tune.",
        ):
            for i, result in enumerate(results):
                with warn_if_slow("process_trial_result"):
                    decision = self._process_trial_result(trial, result)
                if decision is None:
                    # If we didn't get a decision, this means a
                    # non-training future (e.g. a save) was scheduled.
                    # We do not allow processing more results then.
                    if i < len(results) - 1:
                        if log_once("tune_controller_buffer_checkpoint"):
                            logger.warning(
                                f"Trial {trial} has a non-training future "
                                f"scheduled but {len(results) - i} results "
                                f"left to process. This means that a "
                                f"checkpoint was requested, but buffered "
                                f"training was continued before it was "
                                f"saved. Consider using non-buffered "
                                f"training by setting the env variable "
                                f"`TUNE_RESULT_BUFFER_LENGTH=1`."
                            )
                elif decision == TrialScheduler.STOP:
                    # If the decision is to stop the trial,
                    # ignore all results that came after that.
                    break

    def _process_trial_result(self, trial, result):
        result.update(trial_id=trial.trial_id)
        is_duplicate = RESULT_DUPLICATE in result
        force_checkpoint = result.get(SHOULD_CHECKPOINT, False)
        # TrialScheduler and SearchAlgorithm still receive a
        # notification because there may be special handling for
        # the `on_trial_complete` hook.
        if is_duplicate:
            logger.debug("Trial finished without logging 'done'.")
            result = trial.last_result
            result.update(done=True)

        self._total_time += result.get(TIME_THIS_ITER_S, 0)

        flat_result = flatten_dict(result)
        self._validate_result_metrics(flat_result)

        if self._stopper(trial.trial_id, result) or trial.should_stop(flat_result):
            decision = TrialScheduler.STOP
        else:
            with warn_if_slow("scheduler.on_trial_result"):
                decision = self._scheduler_alg.on_trial_result(
                    self._wrapped(), trial, flat_result
                )
        if decision == TrialScheduler.STOP:
            result.update(done=True)
        else:
            # Only updating search alg if the trial is not to be stopped.
            with warn_if_slow("search_alg.on_trial_result"):
                self._search_alg.on_trial_result(trial.trial_id, flat_result)

        # If this is not a duplicate result, the callbacks should
        # be informed about the result.
        if not is_duplicate:
            with warn_if_slow("callbacks.on_trial_result"):
                self._callbacks.on_trial_result(
                    iteration=self._iteration,
                    trials=self._trials,
                    trial=trial,
                    result=result.copy(),
                )
            trial.update_last_result(result)
            # Include in next experiment checkpoint
            self._mark_trial_to_checkpoint(trial)

        # Checkpoints to disk. This should be checked even if
        # the scheduler decision is STOP or PAUSE. Note that
        # PAUSE only checkpoints to memory and does not update
        # the global checkpoint state.
        if decision != TrialScheduler.PAUSE:
            # TODO(justinvyu): This is a temporary hack to fix pausing trials.
            # We already schedule a save task in `pause_trial`, so no need
            # to do it again here.
            self._checkpoint_trial_if_needed(trial, force=force_checkpoint)

        if trial.is_saving:
            logger.debug(f"Caching trial decision for trial {trial}: {decision}")
            # Cache decision to execute on after the save is processed.
            # This prevents changing the trial's state or kicking off
            # another training step prematurely.
            if not self._cached_trial_decisions.get(trial.trial_id) or decision in {
                TrialScheduler.PAUSE,
                TrialScheduler.STOP,
            }:
                # If already set, only overwrite if it's a PAUSE or STOP. This is
                # to avoid that CONTINUE decisions from a training step that resolve
                # late overwrite PAUSE/STOP decision.
                self._cached_trial_decisions[trial.trial_id] = decision
            return None
        else:
            self._queue_decision(trial, decision)
            return decision

    def _validate_result_metrics(self, result):
        """
        Check if any of the required metrics was not reported
        in the last result. If the only items are ``done`` or any of
        DEBUG_METRICS, this means that no result was ever received and
        the trial just returned. This is also okay and will not raise
        an error.

        This will ignore checking for the DEFAULT_METRIC.
        """
        if int(os.environ.get("TUNE_DISABLE_STRICT_METRIC_CHECKING", 0)) != 1 and (
            len({k for k in result if k not in list(DEBUG_METRICS) + [DONE]}) > 1
        ):
            base_metric = self._metric if self._metric != DEFAULT_METRIC else None
            scheduler_metric = (
                self._scheduler_alg.metric
                if self._scheduler_alg.metric != DEFAULT_METRIC
                else None
            )
            search_metrics = (
                self._search_alg.metric
                if self._search_alg.metric != DEFAULT_METRIC
                else None
            )

            if isinstance(search_metrics, str):
                search_metrics = [search_metrics]

            if base_metric and base_metric not in result:
                report_metric = base_metric
                location = "tune.TuneConfig()"
            elif scheduler_metric and scheduler_metric not in result:
                report_metric = scheduler_metric
                location = type(self._scheduler_alg).__name__
            elif search_metrics and any(
                search_metric not in result for search_metric in search_metrics
            ):
                report_metric = list(
                    filter(
                        lambda search_metric: search_metric not in result,
                        search_metrics,
                    )
                )
                if len(report_metric) == 1:
                    report_metric = report_metric[0]
                location = type(self._search_alg).__name__
            else:
                report_metric = None
                location = None

            if report_metric:
                raise ValueError(
                    "Trial returned a result which did not include the "
                    "specified metric(s) `{}` that `{}` expects. "
                    "Make sure your calls to `tune.report()` include the "
                    "metric, or set the "
                    "TUNE_DISABLE_STRICT_METRIC_CHECKING "
                    "environment variable to 1. Result: {}".format(
                        report_metric, location, result
                    )
                )

    ###
    # SAVE
    def _schedule_trial_save(
        self,
        trial: Trial,
        result: Optional[Dict] = None,
    ) -> Optional[_FutureTrainingResult]:
        if trial not in self._trial_to_actor:
            logger.debug(
                f"Trial SAVE requested for trial {trial} but trial is already "
                f"stopping. Ignoring."
            )
            return None

        result = result or trial.last_result

        future = self._schedule_trial_task(
            trial=trial,
            method_name="save",
            on_result=self._on_saving_result,
            on_error=self._trial_task_failure,
            _return_future=True,
        )
        # TODO(justinvyu): `trial.saving_to` (and trial.is_saving) is needed
        # in order to prevent a done=True result from executing a STOP decision
        # (which clears all futures) before the save gets processed.
        # Keep this in for now while `train` and `save` are 2 separate steps.
        trial.temporary_state.saving_to = _FutureTrainingResult(future)

        # `trial.saving_to` holds a future training result -- this is only used
        # in the case of PBT to block until the checkpoint is ready.
        # In all other situations, the checkpoint future is processed by the
        # actor event manager when it is ready.
        return trial.temporary_state.saving_to

    def _on_saving_result(self, trial, checkpoint_value: _TrainingResult):
        with warn_if_slow("process_trial_save"):
            self._process_trial_save(trial, checkpoint_value)

        with warn_if_slow("callbacks.on_trial_save"):
            self._callbacks.on_trial_save(
                iteration=self._iteration, trials=self._trials, trial=trial
            )

        self._maybe_execute_queued_decision(trial, after_save=True)

    def _process_trial_save(self, trial: Trial, checkpoint_value: _TrainingResult):
        """Processes a trial save.

        Acts on the decision cached during the last `_process_trial` call.

        Args:
            trial: Trial being saved.
        """
        logger.debug("Trial %s: Processing trial save.", trial)

        try:
            if not checkpoint_value.checkpoint:
                logger.debug(f"Got empty checkpoint for trial {trial}")
            else:
                try:
                    self._callbacks.on_checkpoint(
                        iteration=self._iteration,
                        trials=self._trials,
                        trial=trial,
                        checkpoint=checkpoint_value.checkpoint,
                    )
                except Exception:
                    logger.warning(
                        "Error encountered during processing of callbacks. "
                        "Ray Train/Tune recently changed the checkpoint interface "
                        "that is passed to callbacks. If you implemented your own "
                        "callback with an `on_checkpoint` handler, please review "
                        "the checkpoint interface and adjust your code "
                        "accordingly."
                    )
                    raise

                trial.on_checkpoint(checkpoint_value)

                self._checkpoint_manager.on_trial_checkpoint(trial)

                self._mark_trial_to_checkpoint(trial)
        except Exception:
            logger.exception(
                "Trial %s: Error handling checkpoint %s", trial, checkpoint_value
            )

        trial.temporary_state.saving_to = None
        decision = self._cached_trial_decisions.pop(trial.trial_id, None)
        if decision and checkpoint_value:
            self._queue_decision(trial, decision)

    def _checkpoint_trial_if_needed(self, trial, force=False):
        """Checkpoints trial based off trial.last_result."""
        if trial.should_checkpoint() or force:
            # Save trial runtime if possible.
            if trial.temporary_state.ray_actor:
                self._schedule_trial_save(trial)

    ###
    # RESTORE
    def _schedule_trial_restore(self, trial: Trial) -> bool:
        checkpoint_result = trial.latest_checkpoint_result

        if not checkpoint_result:
            logger.debug(f"Not restoring trial {trial}: No checkpoint found.")
            return False

        # TODO(justinvyu): Is this really needed?
        trial.temporary_state.restoring_from = checkpoint_result

        method_name = "restore"
        args = (checkpoint_result,)
        self._schedule_trial_task(
            trial=trial,
            method_name=method_name,
            args=args,
            kwargs={},
            on_result=self._on_restoring_result,
            on_error=self._trial_task_failure,
        )
        return True

    def _on_restoring_result(self, trial: Trial, result: Any):
        self._process_trial_restore(trial)

    def _process_trial_restore(self, trial: Trial):
        """Processes a trial restore.

        Args:
            trial: Trial being restored.
        """
        logger.debug("Trial %s: Processing trial restore.", trial)
        trial.on_restore()
        logger.debug("Trial %s: Restore processed successfully", trial)
        self._set_trial_status(trial, Trial.RUNNING)
        self._schedule_trial_train(trial)
        self._live_trials.add(trial)

    def _try_recover(
        self, trial: Trial, exc: Union[TuneError, RayTaskError, RayActorError]
    ):
        """Tries to recover trial.

        Notifies SearchAlgorithm and Scheduler if failure to recover.

        Args:
            trial: Trial to recover.
            exc: Exception prior to invoking this method.
        """
        self._cached_trial_decisions.pop(trial.trial_id, None)
        # Resetting this, in case that the trial is in saving status when it crashes.
        if trial.is_saving:
            trial.temporary_state.saving_to = None
        self._schedule_trial_stop(trial, exception=exc)

        logger.debug("Trial %s: Notifying Scheduler and requeueing.", trial)
        self._requeue_trial(trial)

    def _requeue_trial(self, trial):
        """Notification to TrialScheduler and requeue trial.

        This does not notify the SearchAlgorithm because the function
        evaluation is still in progress.

        """
        self._scheduler_alg.on_trial_error(self, trial)
        self._set_trial_status(trial, status=Trial.PENDING)

        # TODO(rliaw): Right now, this pushes the trial to the end of queue
        # because restoration can be expensive. However, this is not
        # ideal since it just hides the issue - a better fix would
        # be to use an actor table to detect the IP of the Trainable
        # and rsync the files there.
        # See https://github.com/ray-project/ray/issues/5168
        self._trials.pop(self._trials.index(trial))
        self._trials.append(trial)
        self._live_trials.add(trial)

        with warn_if_slow("scheduler.on_trial_add"):
            self._scheduler_alg.on_trial_add(self._wrapped(), trial)

    ###
    # EXPORT
    def _schedule_trial_export(self, trial: Trial):
        if not trial.export_formats or len(trial.export_formats) <= 0:
            return

        # Todo: We are waiting here synchronously until the task resolved.
        # Instead, we should schedule the trial stop after the export resolved.
        # This requires changes in TrialRunner, which we can remove once the
        # legacy execution path has been removed.
        future = self._schedule_trial_task(
            trial=trial,
            method_name="export_model",
            args=(trial.export_formats,),
            on_result=None,
            on_error=self._trial_task_failure,
            _return_future=True,
        )
        self._actor_manager._actor_task_events.resolve_future(future)

    ###
    # RESET
    def _schedule_trial_reset(
        self,
        trial: Trial,
        new_config: Dict,
        new_experiment_tag: str,
    ):
        trial.set_experiment_tag(new_experiment_tag)
        trial.set_config(new_config)

        # Pass magic variables
        extra_config = copy.deepcopy(new_config)
        extra_config[TRIAL_INFO] = _TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        extra_config[STDOUT_FILE] = stdout_file
        extra_config[STDERR_FILE] = stderr_file

        logger_creator = partial(
            _noop_logger_creator, logdir=trial.storage.trial_working_directory
        )

        self._resetting_trials.add(trial)
        self._schedule_trial_task(
            trial=trial,
            method_name="reset",
            args=(extra_config,),
            kwargs={
                "logger_creator": logger_creator,
                "storage": trial.storage,
            },
            on_result=self._on_trial_reset,
            on_error=self._trial_task_failure,
        )

    def _on_trial_reset(self, trial: Trial, success: bool):
        self._resetting_trials.remove(trial)

        if not success:
            info = (
                "Trainable runner reuse requires reset_config() to be "
                "implemented and return True."
            )

            logger.error(f"Could not re-use actor for trial {trial}: {info}")

            exception = _AbortTrialExecution(info)

            trial.handle_error(exception)
            self._schedule_trial_stop(trial, exception=exception)
            return

        tracked_actor = self._trial_to_actor[trial]

        self._actor_started(tracked_actor, log="REUSED")

    def request_stop_trial(self, trial):
        self._stop_queue.append(trial)

    def request_stop_experiment(self):
        self._should_stop_experiment = True

    def _process_stop_requests(self):
        while self._stop_queue:
            t = self._stop_queue.pop()
            self.stop_trial(t)

    def pause_trial(self, trial: Trial, should_checkpoint: bool = True):
        """Pause a trial and reset the necessary state variables for resuming later.

        Args:
            trial: Trial to pause.
            should_checkpoint: Whether or not an in-memory checkpoint should be created
                for this paused trial. Defaults to True.
        """
        # NOTE: The cached trial decision is not needed since we will overrule this
        # decision with PAUSE.
        self._cached_trial_decisions.pop(trial.trial_id, None)
        self._schedule_trial_pause(trial, should_checkpoint=should_checkpoint)

    def cleanup(self):
        """Cleanup trials and callbacks."""
        self._cleanup_trials()
        self.end_experiment_callbacks()

    def __getstate__(self):
        """Gets state for trial.

        Note that this is not used as a pickling override as
        does not have all fields.
        """
        state = self.__dict__.copy()
        for k in [
            "_trials",
            "_live_trials",
            "_stop_queue",
            "_search_alg",
            "_placeholder_resolvers",
            "_scheduler_alg",
            "_pending_trial_queue_times",
            "_callbacks",
            "_checkpoint_manager",
            "_storage",
            "_insufficient_resources_manager",
            "_actor_manager",
            "_class_cache",
            "_resource_updater",
            "_trials_to_cache",
            "_trial_metadata",
            "_actor_to_trial",
            "_trial_to_actor",
            "_resources_to_pending_trials",
            "_pending_trials",
            "_pending_trials_list",
            "_running_trials",
            "_paused_trials",
            "_stopped_trials",
            "_failed_trials",
            "_resetting_trials",
            "_started_actors",
            "_stopping_actors",
            "_staged_trials",
            "_actor_cache",
        ]:
            del state[k]
        return state

    def __setstate__(self, state):
        # Use session_str from previous checkpoint if does not exist
        session_str = state.pop("_session_str")
        self.__dict__.setdefault("_session_str", session_str)
        # Use start_time from previous checkpoint if does not exist
        start_time = state.pop("_start_time")
        self.__dict__.setdefault("_start_time", start_time)

        self.__dict__.update(state)
        self._checkpoint_manager = self._create_checkpoint_manager()


class _TrialExecutorWrapper:
    """Wraps around TrialExecutor class, intercepts API calls and warns users
    of restricted API access.

    This is meant to facilitate restricting
    the current API exposure of TrialExecutor by TrialScheduler.
    """

    def __init__(
        self,
        trial_executor: "_FakeRayTrialExecutor",
        whitelist_attr: Optional[set] = None,
    ):
        self._trial_executor = trial_executor
        self._whitelist_attr = whitelist_attr or set()

        for attr in self._whitelist_attr:
            assert hasattr(self._trial_executor, attr)

    def __getattr__(self, attr):
        if attr not in self._whitelist_attr:
            if log_once("restrict_accessing_trial_executor"):
                logger.warning(
                    f"You are trying to access {attr} interface of "
                    f"TrialExecutor in TrialScheduler, which is being "
                    f"restricted. If you believe it is reasonable for "
                    f"your scheduler to access this TrialExecutor API, "
                    f"please reach out to Ray team on GitHub. A more "
                    f"strict API access pattern would be enforced "
                    f"starting 1.12.0"
                )
        return getattr(self._trial_executor, attr)


@DeveloperAPI
class TrialRunnerWrapper:
    """Wraps around TrialRunner class, intercepts API calls and warns users
    of restricted API access.

    This is meant to facilitate restricting
    the current API exposure of TrialRunner by TrialScheduler.
    """

    _EXECUTOR_ATTR = "trial_executor"

    def __init__(
        self,
        tune_controller: TuneController,
        trial_executor: Any,
        runner_whitelist_attr: Optional[set] = None,
        executor_whitelist_attr: Optional[set] = None,
    ):
        self._tune_controller = tune_controller
        self._trial_executor = _TrialExecutorWrapper(
            trial_executor, executor_whitelist_attr
        )
        self._runner_whitelist_attr = runner_whitelist_attr or set()

        for attr in self._runner_whitelist_attr:
            assert hasattr(self, attr)

    def __getattr__(self, attr):
        if attr == self._EXECUTOR_ATTR:
            return self._trial_executor
        if attr not in self._runner_whitelist_attr:
            if log_once("restrict_accessing_tune_controller"):
                logger.warning(
                    f"You are trying to access {attr} interface of "
                    f"TrialRunner in TrialScheduler, which is being "
                    f"restricted. If you believe it is reasonable for "
                    f"your scheduler to access this TrialRunner API, "
                    f"please reach out to Ray team on GitHub. A more "
                    f"strict API access pattern would be enforced "
                    f"starting 1.12s.0"
                )
        return getattr(self._tune_controller, attr)


def _get_max_pending_trials(search_alg: SearchAlgorithm) -> int:
    max_pending_trials = os.getenv("TUNE_MAX_PENDING_TRIALS_PG", "auto")

    if max_pending_trials != "auto":
        return int(max_pending_trials)

    # Else, auto detect.

    # Only BasicVariantGenerator supports > 1 pending trials.
    # This is because we don't want to generate too many trials
    # before we fit the searcher model.
    if not isinstance(search_alg, BasicVariantGenerator):
        return 1

    # Allow up to at least 200 pending trials to trigger fast autoscaling
    min_autoscaling_rate = 200

    # Allow more pending trials for larger clusters (based on number of CPUs)
    cluster_cpus = ray.cluster_resources().get("CPU", 1.0)
    max_pending_trials = max(min_autoscaling_rate, int(cluster_cpus * 1.1))

    if max_pending_trials > min_autoscaling_rate:
        logger.warning(
            f"The maximum number of pending trials has been "
            f"automatically set to the number of available "
            f"cluster CPUs, which is high "
            f"({max_pending_trials} CPUs/pending trials). "
            f"If you're running an experiment with a large number "
            f"of trials, this could lead to scheduling overhead. "
            f"In this case, consider setting the "
            f"`TUNE_MAX_PENDING_TRIALS_PG` environment variable "
            f"to the desired maximum number of concurrent pending trials."
        )

    return max_pending_trials


class _FakeRayTrialExecutor:
    """The TuneController does not use a RayTrialExecutor anymore.

    Instead, we pass this fake executor for searchers/schedulers to use
    as an interface.

    In the future, we should have the searchers/schedulers either interact with
    the tune controller, or define a different API for more fine-grained scheduler
    control.
    """

    def __init__(self, tune_controller: TuneController):
        self._tune_controller = tune_controller

    def pause_trial(self, trial: Trial, should_checkpoint: bool = True):
        return self._tune_controller._schedule_trial_pause(
            trial, should_checkpoint=should_checkpoint
        )

    def save(
        self,
        trial: Trial,
        result: Optional[Dict] = None,
    ) -> Optional[_FutureTrainingResult]:
        return self._tune_controller._schedule_trial_save(trial=trial, result=result)

    def has_resources_for_trial(self, trial: Trial):
        return True

    @property
    def _resource_updater(self):
        return self._tune_controller._resource_updater

    def force_reconcilation_on_next_step_end(self):
        pass
