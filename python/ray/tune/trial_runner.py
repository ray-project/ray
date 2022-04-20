from typing import Any, List, Mapping, Optional, Union

import click
from datetime import datetime
import json
import logging
import os
import time
import traceback
import warnings

import ray
from ray.exceptions import RayTaskError
from ray.tune.error import TuneStopTrialError
from ray.tune.impl.out_of_band_serialize_dataset import out_of_band_serialize_dataset
from ray.util import get_node_ip_address
from ray.tune import TuneError
from ray.tune.callback import CallbackList, Callback
from ray.tune.experiment import Experiment
from ray.tune.insufficient_resources_manager import InsufficientResourcesManager
from ray.tune.ray_trial_executor import (
    RayTrialExecutor,
    ExecutorEventType,
    ExecutorEvent,
)
from ray.tune.result import (
    DEBUG_METRICS,
    DEFAULT_METRIC,
    DONE,
    TIME_THIS_ITER_S,
    RESULT_DUPLICATE,
    SHOULD_CHECKPOINT,
)
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.stopper import NoopStopper, Stopper
from ray.tune.suggest import BasicVariantGenerator, SearchAlgorithm
from ray.tune.syncer import CloudSyncer, get_cloud_syncer, SyncConfig
from ray.tune.trial import _TuneCheckpoint, Trial
from ray.tune.utils import warn_if_slow, flatten_dict
from ray.tune.utils.log import Verbosity, has_verbosity
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.tune.utils.serialization import TuneFunctionDecoder, TuneFunctionEncoder
from ray.tune.web_server import TuneServer
from ray.util.debug import log_once

MAX_DEBUG_TRIALS = 20

logger = logging.getLogger(__name__)


def find_newest_experiment_checkpoint(ckpt_dir) -> Optional[str]:
    """Returns path to most recently modified checkpoint."""
    full_paths = [
        os.path.join(ckpt_dir, fname)
        for fname in os.listdir(ckpt_dir)
        if fname.startswith("experiment_state") and fname.endswith(".json")
    ]
    if not full_paths:
        return None
    return max(full_paths)


def load_trials_from_experiment_checkpoint(
    experiment_checkpoint: Mapping[str, Any], stub: bool = False
) -> List[Trial]:
    """Create trial objects from experiment checkpoint.

    Given an experiment checkpoint (TrialRunner state dict), return
    list of trials."""
    checkpoints = [
        json.loads(cp, cls=TuneFunctionDecoder) if isinstance(cp, str) else cp
        for cp in experiment_checkpoint["checkpoints"]
    ]

    trials = []
    for trial_cp in checkpoints:
        new_trial = Trial(
            trial_cp["trainable_name"], stub=stub, _setup_default_resource=False
        )
        new_trial.__setstate__(trial_cp)
        trials.append(new_trial)

    return trials


class _ExperimentCheckpointManager:
    """Helper class for managing experiment-level checkpoints.

    This class implements the ``checkpoint()`` method used to checkpoint
    experiment state. When called, this will serialize and write to disk
    the state of the trial runner, trial executor, and search algorithm, to
    a specified checkpoint file.

    The checkpoint period is automatically adjusted to
    ``max(10, time_per_checkpoint * 19)``. This means that at most 5% of the
    time (1/20) will be used for writing checkpoints, while 95% of the time
    (19/20) will be used to handle the rest of the training loop.

    """

    def __init__(
        self,
        checkpoint_dir: str,
        checkpoint_period: Union[int, float, str],
        start_time: float,
        session_str: str,
        syncer: CloudSyncer,
        sync_trial_checkpoints: bool = True,
    ):
        self._checkpoint_dir = checkpoint_dir
        self._auto_checkpoint_enabled = checkpoint_period == "auto"
        if self._auto_checkpoint_enabled:
            self._checkpoint_period = 10.0  # Initial value
        else:
            self._checkpoint_period = float(checkpoint_period)

        self._start_time = start_time
        self._session_str = session_str

        self._syncer = syncer
        self._sync_trial_checkpoints = sync_trial_checkpoints

        self._last_checkpoint_time = 0.0

    @property
    def auto_checkpoint_enabled(self):
        return self._auto_checkpoint_enabled

    def checkpoint(
        self,
        checkpoint_file: str,
        trial_runner: "TrialRunner",
        trial_executor: RayTrialExecutor,
        search_alg: SearchAlgorithm,
        force: bool = False,
    ):
        """Saves execution state to `self._local_checkpoint_dir`.

        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.

        Also automatically saves the search algorithm to the local
        checkpoint dir.

        Args:
            force: Forces a checkpoint despite checkpoint_period.
        """
        if not self._checkpoint_dir:
            return

        now = time.time()
        if now - self._last_checkpoint_time < self._checkpoint_period and (not force):
            return

        def _serialize_and_write():
            runner_state = {
                "checkpoints": list(trial_executor.get_checkpoints().values()),
                "runner_data": trial_runner.__getstate__(),
                "stats": {
                    "start_time": self._start_time,
                    "timestamp": self._last_checkpoint_time,
                },
            }
            tmp_file_name = os.path.join(self._checkpoint_dir, ".tmp_checkpoint")
            with open(tmp_file_name, "w") as f:
                json.dump(runner_state, f, indent=2, cls=TuneFunctionEncoder)

            os.replace(tmp_file_name, checkpoint_file)
            search_alg.save_to_dir(self._checkpoint_dir, session_str=self._session_str)

        checkpoint_time_start = time.monotonic()
        with out_of_band_serialize_dataset():
            _serialize_and_write()

        if self._sync_trial_checkpoints:
            exclude = None
        else:
            exclude = ["*/checkpoint_*"]

        if force:
            # Wait until previous sync command finished
            self._syncer.wait()
            self._syncer.sync_up(exclude=exclude)
        else:
            self._syncer.sync_up_if_needed(exclude=exclude)
        checkpoint_time_taken = time.monotonic() - checkpoint_time_start

        if self._auto_checkpoint_enabled:
            # Multiplying this time by 19 means we spend ~5% of the time
            # writing global checkpoints and 95% of the time processing trials
            self._checkpoint_period = max(10.0, checkpoint_time_taken * 19)
            logger.debug(
                f"Global experiment checkpointing took "
                f"{checkpoint_time_taken:.2f} seconds. "
                f"Adjusting checkpoint period to "
                f"{self._checkpoint_period:.2f} seconds."
            )

        self._last_checkpoint_time = time.time()
        return self._checkpoint_dir


class TrialRunner:
    """A TrialRunner implements the event loop for scheduling trials on Ray.

    .. code-block: python

        runner = TrialRunner()
        runner.add_trial(Trial(...))
        runner.add_trial(Trial(...))
        while not runner.is_finished():
            runner.step()
            print(runner.debug_string())

    The main job of TrialRunner is scheduling trials to efficiently use cluster
    resources, without overloading the cluster.

    While Ray itself provides resource management for tasks and actors, this is
    not sufficient when scheduling trials that may instantiate multiple actors.
    This is because if insufficient resources are available, concurrent trials
    could deadlock waiting for new resources to become available. Furthermore,
    oversubscribing the cluster could degrade training performance, leading to
    misleading benchmark results.

    Args:
        search_alg: SearchAlgorithm for generating
            Trial objects.
        scheduler: Defaults to FIFOScheduler.
        local_checkpoint_dir: Path where
            global checkpoints are stored and restored from.
        remote_checkpoint_dir: Remote path where
            global checkpoints are stored and restored from. Used
            if `resume` == REMOTE.
        sync_config: See `tune.py:run`.
        stopper: Custom class for stopping whole experiments. See
            ``Stopper``.
        resume: see `tune.py:run`.
        server_port: Port number for launching TuneServer.
        fail_fast: Finishes as soon as a trial fails if True.
            If fail_fast='raise' provided, Tune will automatically
            raise the exception received by the Trainable. fail_fast='raise'
            can easily leak resources and should be used with caution.
        checkpoint_period: Trial runner checkpoint periodicity in
            seconds. Defaults to ``"auto"``, which adjusts checkpointing
            time so that at most 5% of the time is spent on writing
            checkpoints.
        trial_executor: Defaults to RayTrialExecutor.
        callbacks: List of callbacks that will be called at different
            times in the training loop. Must be instances of the
            ``ray.tune.trial_runner.Callback`` class.
        metric: Metric used to check received results. If a result is
            reported without this metric, an error will be raised. The error
            can be omitted by not providing a metric or by setting the env
            variable ``TUNE_DISABLE_STRICT_METRIC_CHECKING=0``

    """

    CKPT_FILE_TMPL = "experiment_state-{}.json"
    VALID_RESUME_TYPES = [True, "LOCAL", "REMOTE", "PROMPT", "ERRORED_ONLY", "AUTO"]
    RAISE = "RAISE"

    def __init__(
        self,
        search_alg: Optional[SearchAlgorithm] = None,
        scheduler: Optional[TrialScheduler] = None,
        local_checkpoint_dir: Optional[str] = None,
        remote_checkpoint_dir: Optional[str] = None,
        sync_config: Optional[SyncConfig] = None,
        stopper: Optional[Stopper] = None,
        resume: Union[str, bool] = False,
        server_port: Optional[int] = None,
        fail_fast: bool = False,
        checkpoint_period: Union[str, int] = None,
        trial_executor: Optional[RayTrialExecutor] = None,
        callbacks: Optional[List[Callback]] = None,
        metric: Optional[str] = None,
        # Deprecate on next refactor
        driver_sync_trial_checkpoints: bool = False,
    ):
        self._search_alg = search_alg or BasicVariantGenerator()
        self._scheduler_alg = scheduler or FIFOScheduler()
        self.trial_executor = trial_executor or RayTrialExecutor()
        self._insufficient_resources_manager = InsufficientResourcesManager()
        self._pending_trial_queue_times = {}

        # Set the number of maximum pending trials
        max_pending_trials = os.getenv("TUNE_MAX_PENDING_TRIALS_PG", "auto")
        if max_pending_trials == "auto":
            # Auto detect
            if isinstance(self._search_alg, BasicVariantGenerator):
                # Use a minimum of 16 to trigger fast autoscaling
                # Scale up to at most the number of available cluster CPUs
                cluster_cpus = ray.cluster_resources().get("CPU", 1.0)
                self._max_pending_trials = max(16, int(cluster_cpus * 1.1))

                if self._max_pending_trials > 128:
                    logger.warning(
                        f"The maximum number of pending trials has been "
                        f"automatically set to the number of available "
                        f"cluster CPUs, which is high "
                        f"({self._max_pending_trials} CPUs/pending trials). "
                        f"If you're running an experiment with a large number "
                        f"of trials, this could lead to scheduling overhead. "
                        f"In this case, consider setting the "
                        f"`TUNE_MAX_PENDING_TRIALS_PG` environment variable "
                        f"to the desired maximum number of concurrent trials."
                    )
            else:
                self._max_pending_trials = 1
        else:
            # Manual override
            self._max_pending_trials = int(max_pending_trials)
        self.trial_executor.set_max_pending_trials(self._max_pending_trials)

        self._metric = metric

        if "TRIALRUNNER_WALLTIME_LIMIT" in os.environ:
            raise ValueError(
                "The TRIALRUNNER_WALLTIME_LIMIT environment variable is "
                "deprecated. "
                "Use `tune.run(time_budget_s=limit)` instead."
            )

        self._total_time = 0
        self._iteration = 0
        self._has_errored = False
        self._fail_fast = fail_fast
        if isinstance(self._fail_fast, str):
            self._fail_fast = self._fail_fast.upper()
            if self._fail_fast == TrialRunner.RAISE:
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

        self._server = None
        self._server_port = server_port
        if server_port is not None:
            self._server = TuneServer(self, self._server_port)

        self._trials = []
        self._live_trials = set()  # Set of non-terminated trials
        self._cached_trial_decisions = {}
        self._queued_trial_decisions = {}

        self._stop_queue = []
        self._should_stop_experiment = False  # used by TuneServer
        self._local_checkpoint_dir = local_checkpoint_dir

        if self._local_checkpoint_dir:
            os.makedirs(self._local_checkpoint_dir, exist_ok=True)

        sync_config = sync_config or SyncConfig()
        self._remote_checkpoint_dir = remote_checkpoint_dir
        self._syncer = get_cloud_syncer(
            local_checkpoint_dir, remote_checkpoint_dir, sync_config.syncer
        )
        self._stopper = stopper or NoopStopper()
        self._resumed = False

        if self._validate_resume(
            resume_type=resume,
            driver_sync_trial_checkpoints=driver_sync_trial_checkpoints,
        ):
            errored_only = False
            if isinstance(resume, str):
                errored_only = resume.upper() == "ERRORED_ONLY"
            try:
                self.resume(run_errored_only=errored_only)
                self._resumed = True
            except Exception as e:
                if has_verbosity(Verbosity.V3_TRIAL_DETAILS):
                    logger.error(str(e))
                logger.exception("Runner restore failed.")
                if self._fail_fast:
                    raise
                logger.info("Restarting experiment.")
        else:
            logger.debug("Starting a new experiment.")

        self._start_time = time.time()
        self._last_checkpoint_time = -float("inf")

        self._session_str = datetime.fromtimestamp(self._start_time).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
        self.checkpoint_file = None
        if self._local_checkpoint_dir:
            self.checkpoint_file = os.path.join(
                self._local_checkpoint_dir,
                TrialRunner.CKPT_FILE_TMPL.format(self._session_str),
            )

        self._callbacks = CallbackList(callbacks or [])

        if checkpoint_period is None:
            checkpoint_period = os.getenv("TUNE_GLOBAL_CHECKPOINT_S", "auto")

        self._checkpoint_period = checkpoint_period
        self._checkpoint_manager = self._create_checkpoint_manager(
            driver_sync_trial_checkpoints
        )

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

    def _create_checkpoint_manager(self, sync_trial_checkpoints: bool = True):
        return _ExperimentCheckpointManager(
            checkpoint_dir=self._local_checkpoint_dir,
            checkpoint_period=self._checkpoint_period,
            start_time=self._start_time,
            session_str=self._session_str,
            syncer=self._syncer,
            sync_trial_checkpoints=sync_trial_checkpoints,
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

    def _validate_resume(self, resume_type, driver_sync_trial_checkpoints=True):
        """Checks whether to resume experiment.

        Args:
            resume_type: One of True, "REMOTE", "LOCAL",
                "PROMPT", "ERRORED_ONLY", "AUTO".
            driver_sync_trial_checkpoints: Boolean indicating if the driver
                should sync trial checkpoints from the driver node to cloud.
        """
        # TODO: Consider supporting ERRORED_ONLY+REMOTE?
        if not resume_type:
            return False
        assert (
            resume_type in self.VALID_RESUME_TYPES
        ), "resume_type {} is not one of {}".format(
            resume_type, self.VALID_RESUME_TYPES
        )
        # Not clear if we need this assertion, since we should always have a
        # local checkpoint dir.
        assert self._local_checkpoint_dir or self._remote_checkpoint_dir

        if resume_type == "AUTO":
            if self._remote_checkpoint_dir:
                logger.info(
                    f"Trying to find and download experiment checkpoint at "
                    f"{self._remote_checkpoint_dir}"
                )
                # Todo: This syncs the entire experiment including trial
                # checkpoints. We should exclude these in the future.
                try:
                    self._syncer.sync_down_if_needed()
                    self._syncer.wait()
                except TuneError as e:
                    logger.warning(
                        f"Got error when trying to sync down: {e} "
                        f"\nPlease check this error message for potential "
                        f"access problems - if a directory was not found, "
                        f"that is expected at this stage when you're starting "
                        f"a new experiment."
                    )
                    logger.info(
                        "No remote checkpoint was found or an error occurred "
                        "when trying to download the experiment checkpoint. "
                        "Please check the previous warning message for more "
                        "details. "
                        "Ray Tune will now start a new experiment."
                    )
                    return False
                if not self.checkpoint_exists(self._local_checkpoint_dir):
                    logger.warning(
                        "A remote checkpoint was fetched, but no checkpoint "
                        "data was found. This can happen when e.g. the cloud "
                        "bucket exists but does not contain any data. "
                        "Ray Tune will start a new, fresh run."
                    )
                    return False
                logger.info(
                    "A remote experiment checkpoint was found and will be "
                    "used to restore the previous experiment state."
                )
                return True
            elif not self.checkpoint_exists(self._local_checkpoint_dir):
                logger.info(
                    "No local checkpoint was found. "
                    "Ray Tune will now start a new experiment."
                )
                return False
            logger.info(
                "A local experiment checkpoint was found and will be used "
                "to restore the previous experiment state."
            )
            return True

        if resume_type in [True, "LOCAL", "PROMPT", "ERRORED_ONLY"]:
            if not self.checkpoint_exists(self._local_checkpoint_dir):
                raise ValueError(
                    f"You called resume ({resume_type}) when no checkpoint "
                    f"exists in local directory "
                    f"({self._local_checkpoint_dir}). If you want to start "
                    f'a new experiment, use `resume="AUTO"` or '
                    f"`resume=None`. If you expected an experiment to "
                    f"already exist, check if you supplied the correct "
                    f"`local_dir` to `tune.run()`."
                )
            elif resume_type == "PROMPT":
                if click.confirm(
                    f"Resume from local directory? " f"({self._local_checkpoint_dir})"
                ):
                    return True

        if resume_type in ["REMOTE", "PROMPT"]:
            if resume_type == "PROMPT" and not click.confirm(
                f"Try downloading from remote directory? "
                f"({self._remote_checkpoint_dir})"
            ):
                return False
            if not self._remote_checkpoint_dir:
                raise ValueError(
                    "Called resume from remote without remote directory. "
                    "Fix this by passing a `SyncConfig` object with "
                    "`upload_dir` set to `tune.run(sync_config=...)`."
                )

            # Try syncing down the upload directory.
            logger.info(
                f"Downloading experiment checkpoint from "
                f"{self._remote_checkpoint_dir}"
            )
            if driver_sync_trial_checkpoints:
                exclude = None
            else:
                exclude = ["*/checkpoint_*"]

            try:
                self._syncer.sync_down_if_needed(exclude=exclude)
                self._syncer.wait()
            except TuneError as e:
                raise RuntimeError(
                    "Syncing the remote experiment checkpoint to the driver "
                    "failed. Please check the error message. If you want to "
                    'start a new experiment, use `resume="AUTO"` or '
                    "`resume=None`. If you expected an experiment to "
                    "already exist, check if you supplied the correct "
                    "`upload_dir` to the `tune.SyncConfig` passed to "
                    "`tune.run()`."
                ) from e

            if not self.checkpoint_exists(self._local_checkpoint_dir):
                raise ValueError(
                    "Called resume when no checkpoint exists "
                    "in remote or local directory."
                )
        return True

    @classmethod
    def checkpoint_exists(cls, directory):
        if not os.path.exists(directory):
            return False
        return any(
            (fname.startswith("experiment_state") and fname.endswith(".json"))
            for fname in os.listdir(directory)
        )

    def checkpoint(self, force: bool = False):
        """Saves execution state to `self._local_checkpoint_dir`.

        Overwrites the current session checkpoint, which starts when self
        is instantiated. Throttle depends on self._checkpoint_period.

        Also automatically saves the search algorithm to the local
        checkpoint dir.

        Args:
            force: Forces a checkpoint despite checkpoint_period.
        """
        with warn_if_slow(
            "experiment_checkpoint",
            message="Checkpointing the experiment state took "
            "{duration:.3f} s, which may be a performance "
            "bottleneck. Please ensure the "
            "`TUNE_GLOBAL_CHECKPOINT_S` environment variable is "
            "something significantly higher than this duration "
            "to ensure compute time is mostly spent on the main "
            "training loop.",
            # No backlog warning if forced checkpoint as we wait
            # for previous sync to finish.
            disable=self._checkpoint_manager.auto_checkpoint_enabled or force,
        ):

            self._checkpoint_manager.checkpoint(
                checkpoint_file=self.checkpoint_file,
                trial_runner=self,
                trial_executor=self.trial_executor,
                search_alg=self._search_alg,
                force=force,
            )

    def resume(self, run_errored_only=False):
        """Resumes all checkpointed trials from previous run.

        Requires user to manually re-register their objects. Also stops
        all ongoing trials.
        """
        newest_ckpt_path = find_newest_experiment_checkpoint(self._local_checkpoint_dir)

        if not newest_ckpt_path:
            raise ValueError(
                f"Tried to resume from checkpoint dir "
                f"`{self._local_checkpoint_dir}`, but no "
                f"experiment checkpoint data was found."
            )

        with open(newest_ckpt_path, "r") as f:
            runner_state = json.load(f, cls=TuneFunctionDecoder)
            self.checkpoint_file = newest_ckpt_path

        logger.warning(
            "".join(
                [
                    "Attempting to resume experiment from {}. ".format(
                        self._local_checkpoint_dir
                    ),
                    "This will ignore any new changes to the specification.",
                ]
            )
        )

        self.__setstate__(runner_state["runner_data"])
        if self._search_alg.has_checkpoint(self._local_checkpoint_dir):
            self._search_alg.restore_from_dir(self._local_checkpoint_dir)

        trials = load_trials_from_experiment_checkpoint(runner_state)
        for trial in sorted(trials, key=lambda t: t.last_update_time, reverse=True):
            if run_errored_only and trial.status == Trial.ERROR:
                new_trial = trial.reset()
                self.add_trial(new_trial)
            else:
                self.add_trial(trial)

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

    def _update_trial_queue_and_get_next_trial(self) -> Optional[Trial]:
        """Adding suggested trials to the live queue of trials (they start as PENDING trials).

        Returns:
            next_trial: Trial
        """
        wait_for_trial = True  # wait for new trials when all trials are finished
        num_pending_trials = 0
        for trial in self._live_trials:
            if not trial.is_finished():
                wait_for_trial = False
                if trial.status == Trial.PENDING:
                    num_pending_trials += 1

        if not self._search_alg.is_finished():
            # Create pending trials until it fails.
            while num_pending_trials < self._max_pending_trials:
                if not self._update_trial_queue(blocking=wait_for_trial):
                    break
                wait_for_trial = False  # wait at most one trial
                num_pending_trials += 1

        with warn_if_slow("choose_trial_to_run"):
            return self._scheduler_alg.choose_trial_to_run(self)

    def _wait_and_handle_event(self, next_trial: Optional[Trial]):
        try:
            # Single wait of entire tune loop.
            event = self.trial_executor.get_next_executor_event(
                self._live_trials, next_trial is not None
            )
            if event.type == ExecutorEventType.PG_READY:
                self._on_pg_ready(next_trial)
            elif event.type == ExecutorEventType.NO_RUNNING_TRIAL_TIMEOUT:
                self._insufficient_resources_manager.on_no_available_trials(
                    self.get_trials()
                )
            elif event.type == ExecutorEventType.YIELD:
                pass
            else:
                trial = event.trial
                result = event.result
                if event.type == ExecutorEventType.ERROR:
                    self._on_executor_error(trial, result[ExecutorEvent.KEY_EXCEPTION])
                elif event.type == ExecutorEventType.RESTORING_RESULT:
                    self._on_restoring_result(trial)
                else:
                    assert event.type in (
                        ExecutorEventType.SAVING_RESULT,
                        ExecutorEventType.TRAINING_RESULT,
                    ), f"Unexpected future type - {event.type}"
                    if event.type == ExecutorEventType.TRAINING_RESULT:
                        self._on_training_result(
                            trial, result[ExecutorEvent.KEY_FUTURE_RESULT]
                        )
                    else:
                        self._on_saving_result(
                            trial, result[ExecutorEvent.KEY_FUTURE_RESULT]
                        )
                    self._post_process_on_training_saving_result(trial)
        except Exception as e:
            if e is TuneError or self._fail_fast == TrialRunner.RAISE:
                raise e
            else:
                raise TuneError(traceback.format_exc())

    def step(self):
        """Runs one step of the trial event loop.

        Callers should typically run this method repeatedly in a loop. They
        may inspect or modify the runner's state in between calls to step().
        """
        if self.is_finished():
            raise TuneError("Called step when all trials finished?")
        with warn_if_slow("on_step_begin"):
            self.trial_executor.on_step_begin(self.get_trials())
        with warn_if_slow("callbacks.on_step_begin"):
            self._callbacks.on_step_begin(
                iteration=self._iteration, trials=self._trials
            )

        next_trial = self._update_trial_queue_and_get_next_trial()
        if next_trial:
            logger.debug(f"Running trial {next_trial}")

        self._wait_and_handle_event(next_trial)

        self._stop_experiment_if_needed()

        try:
            self.checkpoint()
        except Exception as e:
            logger.warning(f"Trial Runner checkpointing failed: {str(e)}")
        self._iteration += 1

        if self._server:
            with warn_if_slow("server"):
                self._process_stop_requests()

            if self.is_finished():
                self._server.shutdown()

        self._reconcile_live_trials()

        with warn_if_slow("on_step_end"):
            self.trial_executor.on_step_end(self.get_trials())
        with warn_if_slow("callbacks.on_step_end"):
            self._callbacks.on_step_end(iteration=self._iteration, trials=self._trials)

    def _on_pg_ready(self, next_trial: Optional[Trial]):
        def _start_trial(trial: Trial) -> bool:
            """Helper function to start trial and call callbacks"""
            with warn_if_slow("start_trial"):
                if self.trial_executor.start_trial(trial):
                    self._callbacks.on_trial_start(
                        iteration=self._iteration, trials=self._trials, trial=trial
                    )
                    return True
                return False

        assert next_trial is not None
        logger.info(f"starting {next_trial}")
        if not _start_trial(next_trial) and next_trial.status != Trial.ERROR:
            # Only try to start another trial if previous trial startup
            # did not error (e.g. it just didn't start because its
            # placement group is not ready, yet).
            # Without this clause, this test fails:
            # test_trial_runner_pg.py::
            # TrialRunnerPlacementGroupHeterogeneousTest::
            # testResourceDeadlock
            next_trial = self.trial_executor.get_staged_trial()
            if next_trial is not None:
                # Must be able to start.
                assert _start_trial(next_trial)
            else:
                logger.info(f"reconciling {self.get_trials()}")
                self.trial_executor._pg_manager.reconcile_placement_groups(
                    self.get_trials()
                )

    def _on_saving_result(self, trial, checkpoint_value: Union[ray.ObjectRef, str]):
        with warn_if_slow("process_trial_save") as _profile:
            self._process_trial_save(trial, checkpoint_value)
        with warn_if_slow("callbacks.on_trial_save"):
            self._callbacks.on_trial_save(
                iteration=self._iteration, trials=self._trials, trial=trial
            )
        if _profile.too_slow and trial.sync_on_checkpoint:
            # TODO(ujvl): Suggest using cloud checkpointing once
            #  API has converged.

            msg = (
                "Consider turning off forced head-worker trial "
                "checkpoint syncs by setting sync_on_checkpoint=False"
                ". Note that this may result in faulty trial "
                "restoration if a failure occurs while the checkpoint "
                "is being synced from the worker to the head node."
            )

            if trial.location.hostname and (
                trial.location.hostname != get_node_ip_address()
            ):
                if log_once("tune_head_worker_checkpoint"):
                    logger.warning(msg)

    def _on_restoring_result(self, trial):
        with warn_if_slow("process_trial_restore"):
            self._process_trial_restore(trial)
        with warn_if_slow("callbacks.on_trial_restore"):
            self._callbacks.on_trial_restore(
                iteration=self._iteration, trials=self._trials, trial=trial
            )

    def _on_training_result(self, trial, result):
        if not isinstance(result, list):
            result = [result]
        with warn_if_slow("process_trial_result"):
            self._process_trial_results(trial, result)

    def _post_process_on_training_saving_result(self, trial):
        # `self._queued_trial_decisions` now contains a final decision
        # based on all results
        if trial not in self._cached_trial_decisions:
            final_decision = self._queued_trial_decisions.pop(trial.trial_id, None)
            if final_decision:
                self._execute_action(trial, final_decision)

    def _on_executor_error(self, trial, e: Union[RayTaskError, TuneError]):
        error_msg = f"Trial {trial}: Error processing event."
        if self._fail_fast == TrialRunner.RAISE:
            logger.error(error_msg)
            raise e
        else:
            logger.exception(error_msg)
            self._process_trial_failure(trial, exc=e)

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
        self._trials.append(trial)
        if trial.status != Trial.TERMINATED:
            self._live_trials.add(trial)
        with warn_if_slow("scheduler.on_trial_add"):
            self._scheduler_alg.on_trial_add(
                TrialRunnerWrapper(self, runner_whitelist_attr={"search_alg"}), trial
            )
        self.trial_executor.mark_trial_to_checkpoint(trial)

    def debug_string(self, delim="\n"):
        from ray.tune.progress_reporter import trial_progress_str

        result_keys = [list(t.last_result) for t in self.get_trials() if t.last_result]
        metrics = set().union(*result_keys)
        messages = [
            self._scheduler_alg.debug_string(),
            self.trial_executor.debug_string(),
            trial_progress_str(self.get_trials(), metrics, force_table=True),
        ]
        return delim.join(messages)

    def _stop_experiment_if_needed(self):
        """Stops all trials."""
        fail_fast = self._fail_fast and self._has_errored
        if self._stopper.stop_all() or fail_fast or self._should_stop_experiment:
            self._search_alg.set_finished()
            [
                self.trial_executor.stop_trial(t)
                for t in self._trials
                if t.status is not Trial.ERROR
            ]

    def _process_trial_results(self, trial, results):
        logger.debug(f"process_trial_results {results}")
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
                        if log_once("trial_runner_buffer_checkpoint"):
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
                decision = self._scheduler_alg.on_trial_result(self, trial, flat_result)
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
            self.trial_executor.mark_trial_to_checkpoint(trial)

        # Checkpoints to disk. This should be checked even if
        # the scheduler decision is STOP or PAUSE. Note that
        # PAUSE only checkpoints to memory and does not update
        # the global checkpoint state.
        self._checkpoint_trial_if_needed(trial, force=force_checkpoint)

        if trial.is_saving:
            logger.debug(f"caching trial decision {trial}")
            # Cache decision to execute on after the save is processed.
            # This prevents changing the trial's state or kicking off
            # another training step prematurely.
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
                location = "tune.run()"
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

    def _process_trial_save(
        self, trial: Trial, checkpoint_value: Union[ray.ObjectRef, str]
    ):
        """Processes a trial save.

        Acts on the decision cached during the last `_process_trial` call.

        Args:
            trial: Trial being saved.
        """
        logger.debug("Trial %s: Processing trial save.", trial)

        try:
            trial.saving_to.value = checkpoint_value
            self._callbacks.on_checkpoint(
                iteration=self._iteration,
                trials=self._trials,
                trial=trial,
                checkpoint=trial.saving_to,
            )
            trial.on_checkpoint(trial.saving_to)
            if trial.checkpoint.storage != _TuneCheckpoint.MEMORY:
                self.trial_executor.mark_trial_to_checkpoint(trial)
        except Exception:
            logger.exception(
                "Trial %s: Error handling checkpoint %s", trial, checkpoint_value
            )
            if self._fail_fast == TrialRunner.RAISE:
                raise

        trial.saving_to = None
        decision = self._cached_trial_decisions.pop(trial.trial_id, None)
        if decision and checkpoint_value:
            self._queue_decision(trial, decision)

    def _process_trial_restore(self, trial: Trial):
        """Processes a trial restore.

        Args:
            trial: Trial being restored.
        """
        logger.debug("Trial %s: Processing trial restore.", trial)
        trial.on_restore()
        logger.debug("Trial %s: Restore processed successfully", trial)
        self.trial_executor.set_status(trial, Trial.RUNNING)
        self.trial_executor.continue_training(trial)
        self._live_trials.add(trial)

    def _process_trial_failure(
        self, trial: Trial, exc: Optional[Union[TuneError, RayTaskError]] = None
    ):
        """Handle trial failure.

        Attempt trial recovery if possible, clean up state otherwise.

        Args:
            trial: Failed trial.
            exc: Exception prior to invoking this method.
        """
        self._has_errored = True
        if trial.status == Trial.RUNNING:
            if trial.should_recover():
                self._try_recover(trial, exc=exc)
            else:
                self._scheduler_alg.on_trial_error(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id, error=True)
                self._callbacks.on_trial_error(
                    iteration=self._iteration, trials=self._trials, trial=trial
                )
                self.trial_executor.stop_trial(trial, exc=exc)

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

    def _execute_action(self, trial: Trial, decision: str):
        """Executes action based on decision.

        Args:
            trial: Trial to act on.
            decision: Scheduling decision to undertake.
        """
        if decision == TrialScheduler.CONTINUE:
            self.trial_executor.continue_training(trial)
        elif decision == TrialScheduler.PAUSE:
            self.trial_executor.pause_trial(trial)
        elif decision == TrialScheduler.STOP:
            self.stop_trial(trial)
        elif decision == TrialScheduler.NOOP:
            pass
        else:
            raise ValueError("Invalid decision: {}".format(decision))

    def _checkpoint_trial_if_needed(self, trial, force=False):
        """Checkpoints trial based off trial.last_result."""
        if trial.should_checkpoint() or force:
            # Save trial runtime if possible.
            if trial.runner:
                self.trial_executor.save(trial, storage=_TuneCheckpoint.PERSISTENT)

    def _try_recover(self, trial: Trial, exc: Union[TuneError, RayTaskError]):
        """Tries to recover trial.

        Notifies SearchAlgorithm and Scheduler if failure to recover.

        Args:
            trial: Trial to recover.
            exc: Exception prior to invoking this method.
        """
        self._cached_trial_decisions.pop(trial.trial_id, None)
        # Resetting this, in case that the trial is in saving status when it crashes.
        if trial.is_saving:
            trial.saving_to = None
        if trial.is_restoring:
            # Restore was unsuccessful, try again without checkpoint.
            trial.clear_checkpoint()
        self.trial_executor.stop_trial(trial, error=exc is not None, exc=exc)
        if self.trial_executor.has_resources_for_trial(trial):
            requeue_trial = False
            logger.info(
                "Trial %s: Attempting to restore trial state from last checkpoint.",
                trial,
            )
            # TODO(xwjiang): For better consistency, consider not starting
            #  trials here. Instead rely on requeuing the trial.
            started = self.trial_executor.start_trial(trial)
            if not started:
                requeue_trial = True
            elif trial.status == Trial.ERROR:
                logger.exception(
                    "Trial %s: Error restoring trial from checkpoint, abort.", trial
                )
                if started:
                    # Clean up again if an actor was launched
                    self.trial_executor.stop_trial(trial, error=True)
                self._scheduler_alg.on_trial_error(self, trial)
                self._search_alg.on_trial_complete(trial.trial_id, error=True)
                self._callbacks.on_trial_error(
                    iteration=self._iteration, trials=self._trials, trial=trial
                )
            else:
                logger.debug("Trial %s: Restore dispatched correctly.", trial)
        else:
            requeue_trial = True

        if requeue_trial:
            logger.debug("Trial %s: Notifying Scheduler and requeueing.", trial)
            self._requeue_trial(trial)

    def _requeue_trial(self, trial):
        """Notification to TrialScheduler and requeue trial.

        This does not notify the SearchAlgorithm because the function
        evaluation is still in progress.

        """
        self._scheduler_alg.on_trial_error(self, trial)
        self.trial_executor.set_status(trial, Trial.PENDING)

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
            self._scheduler_alg.on_trial_add(
                TrialRunnerWrapper(self, runner_whitelist_attr={"search_alg"}), trial
            )

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
                logger.info("Blocking for next trial...")
                trial = self._search_alg.next_trial()
                time.sleep(1)

        if trial:
            self.add_trial(trial)
            return True

        return False

    def request_stop_trial(self, trial):
        self._stop_queue.append(trial)

    def request_stop_experiment(self):
        self._should_stop_experiment = True

    def _process_stop_requests(self):
        while self._stop_queue:
            t = self._stop_queue.pop()
            self.stop_trial(t)

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
            self.trial_executor.export_trial_if_needed(trial)
            self.trial_executor.stop_trial(trial)
            self._live_trials.discard(trial)
        except Exception as e:
            logger.exception("Trial %s: Error stopping trial.", trial)
            if self._fail_fast == TrialRunner.RAISE:
                raise
            if isinstance(e, TuneError):
                self._process_trial_failure(trial, exc=e)
            else:
                self._process_trial_failure(
                    trial, TuneStopTrialError(traceback.format_exc())
                )

    def cleanup_trials(self):
        self.trial_executor.cleanup(self.get_trials())

    def cleanup(self):
        """Cleanup trials and callbacks."""
        self.cleanup_trials()
        self.end_experiment_callbacks()

    def _reconcile_live_trials(self):
        """Loop through live trials and remove if terminated"""
        for trial in list(self._live_trials):
            # Only for TERMINATED trials. ERRORed trials might be retried.
            if trial.status == Trial.TERMINATED:
                self._live_trials.remove(trial)

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
            "_server",
            "_search_alg",
            "_scheduler_alg",
            "_pending_trial_queue_times",
            "trial_executor",
            "_syncer",
            "_callbacks",
            "_checkpoint_manager",
        ]:
            del state[k]
        state["launch_web_server"] = bool(self._server)
        return state

    def __setstate__(self, state):
        launch_web_server = state.pop("launch_web_server")

        # Use session_str from previous checkpoint if does not exist
        session_str = state.pop("_session_str")
        self.__dict__.setdefault("_session_str", session_str)
        # Use start_time from previous checkpoint if does not exist
        start_time = state.pop("_start_time")
        self.__dict__.setdefault("_start_time", start_time)

        self.__dict__.update(state)
        self._checkpoint_manager = self._create_checkpoint_manager()

        if launch_web_server:
            self._server = TuneServer(self, self._server_port)


class TrialExecutorWrapper(RayTrialExecutor):
    """Wraps around TrialExecutor class, intercepts API calls and warns users
    of restricted API access.

    This is meant to facilitate restricting
    the current API exposure of TrialExecutor by TrialScheduler.
    """

    def __init__(
        self, trial_executor: RayTrialExecutor, whitelist_attr: Optional[set] = None
    ):
        self._trial_executor = trial_executor
        self._whitelist_attr = whitelist_attr or set()

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


class TrialRunnerWrapper(TrialRunner):
    """Wraps around TrialRunner class, intercepts API calls and warns users
    of restricted API access.

    This is meant to facilitate restricting
    the current API exposure of TrialRunner by TrialScheduler.
    """

    _EXECUTOR_ATTR = "trial_executor"

    def __init__(
        self,
        trial_runner: TrialRunner,
        runner_whitelist_attr: Optional[set] = None,
        executor_whitelist_attr: Optional[set] = None,
    ):
        self._trial_runner = trial_runner
        self._trial_executor = TrialExecutorWrapper(
            trial_runner.trial_executor, executor_whitelist_attr
        )
        self._runner_whitelist_attr = runner_whitelist_attr or set()

    def __getattr__(self, attr):
        if attr == self._EXECUTOR_ATTR:
            return self._trial_executor
        if attr not in self._runner_whitelist_attr:
            if log_once("restrict_accessing_trial_runner"):
                logger.warning(
                    f"You are trying to access {attr} interface of "
                    f"TrialRunner in TrialScheduler, which is being "
                    f"restricted. If you believe it is reasonable for "
                    f"your scheduler to access this TrialRunner API, "
                    f"please reach out to Ray team on GitHub. A more "
                    f"strict API access pattern would be enforced "
                    f"starting 1.12s.0"
                )
        return getattr(self._trial_runner, attr)
