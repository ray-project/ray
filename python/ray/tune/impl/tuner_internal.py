import copy
import os
import math
import logging
import warnings
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Type, Union, TYPE_CHECKING, Tuple
import urllib.parse

import ray
import ray.cloudpickle as pickle
from ray.util import inspect_serializability
from ray.air._internal.remote_storage import download_from_uri, is_non_local_path_uri
from ray.air.config import RunConfig, ScalingConfig
from ray.tune import Experiment, TuneError, ExperimentAnalysis
from ray.tune.execution.trial_runner import _ResumeConfig
from ray.tune.registry import is_function_trainable
from ray.tune.result_grid import ResultGrid
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.tune.tune_config import TuneConfig

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.util.queue import Queue


_TRAINABLE_PKL = "trainable.pkl"
_TUNER_PKL = "tuner.pkl"
_TRAINABLE_KEY = "_trainable"
_CONVERTED_TRAINABLE_KEY = "_converted_trainable"
_PARAM_SPACE_KEY = "_param_space"
_EXPERIMENT_ANALYSIS_KEY = "_experiment_analysis"

logger = logging.getLogger(__name__)

TrainableType = Union[str, Callable, Type[Trainable]]
TrainableTypeOrTrainer = Union[TrainableType, "BaseTrainer"]


class TunerInternal:
    """The real implementation behind external facing ``Tuner``.

    The external facing ``Tuner`` multiplexes between local Tuner and remote Tuner
    depending on whether in Ray client mode.

    In Ray client mode, external ``Tuner`` wraps ``TunerInternal`` into a remote actor,
    which is guaranteed to be placed on head node.

    ``TunerInternal`` can be constructed from fresh, in which case, ``trainable`` needs
    to be provided, together with optional ``param_space``, ``tune_config`` and
    ``run_config``.

    It can also be restored from a previous failed run (given ``restore_path``).

    Args:
        restore_path: The path from where the Tuner can be restored. If provided, None
            of the rest args are needed.
        resume_config: Resume config to configure which trials to continue.
        trainable: The trainable to be tuned.
        param_space: Search space of the tuning job.
            One thing to note is that both preprocessor and dataset can be tuned here.
        tune_config: Tuning algorithm specific configs.
            Refer to ray.tune.tune_config.TuneConfig for more info.
        run_config: Runtime configuration that is specific to individual trials.
            If passed, this will overwrite the run config passed to the Trainer,
            if applicable. Refer to ray.air.config.RunConfig for more info.
    """

    def __init__(
        self,
        restore_path: str = None,
        resume_config: Optional[_ResumeConfig] = None,
        trainable: Optional[TrainableTypeOrTrainer] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_config: Optional[TuneConfig] = None,
        run_config: Optional[RunConfig] = None,
        _tuner_kwargs: Optional[Dict] = None,
    ):
        from ray.train.trainer import BaseTrainer

        # If no run config was passed to Tuner directly, use the one from the Trainer,
        # if available
        if not run_config and isinstance(trainable, BaseTrainer):
            run_config = trainable.run_config

        self._tune_config = tune_config or TuneConfig()
        self._run_config = run_config or RunConfig()

        self._missing_params_error_message = None

        # Restore from Tuner checkpoint.
        if restore_path:
            self._restore_from_path_or_uri(
                path_or_uri=restore_path,
                resume_config=resume_config,
                overwrite_trainable=trainable,
            )
            return

        # Start from fresh
        if not trainable:
            raise TuneError("You need to provide a trainable to tune.")

        self._is_restored = False
        self.trainable = trainable
        self._resume_config = None

        self._tuner_kwargs = copy.deepcopy(_tuner_kwargs) or {}
        self._experiment_checkpoint_dir = self._setup_create_experiment_checkpoint_dir(
            self._run_config
        )

        self._experiment_analysis = None

        # Not used for restored Tuner.
        self._param_space = param_space or {}
        self._process_scaling_config()

        # This needs to happen before `tune.run()` is kicked in.
        # This is because currently tune does not exit gracefully if
        # run in ray client mode - if crash happens, it just exits immediately
        # without allowing for checkpointing tuner and trainable.
        # Thus this has to happen before tune.run() so that we can have something
        # to restore from.
        experiment_checkpoint_path = Path(self._experiment_checkpoint_dir)
        with open(experiment_checkpoint_path / _TUNER_PKL, "wb") as fp:
            pickle.dump(self, fp)

        try:
            with open(experiment_checkpoint_path / _TRAINABLE_PKL, "wb") as fp:
                pickle.dump(self.trainable, fp)
        except TypeError as e:
            inspect_serializability(self.trainable)
            msg = (
                "The provided trainable is not serializable, which is a requirement "
                "since the trainable is serialized and deserialized when transferred "
                "to remote workers. See above for a trace of the non-serializable "
                "objects that were found in your trainable."
            )
            raise TypeError(msg) from e

        self._maybe_warn_resource_contention()

    def get_run_config(self) -> RunConfig:
        return self._run_config

    # For Jupyter output with Ray Client
    def set_run_config_and_remote_string_queue(
        self, run_config: RunConfig, string_queue: "Queue"
    ):
        self._run_config = run_config
        self._tuner_kwargs["_remote_string_queue"] = string_queue

    def clear_remote_string_queue(self):
        self._tuner_kwargs.pop("_remote_string_queue", None)

    def _expected_utilization(self, cpus_per_trial, cpus_total):
        num_samples = self._tune_config.num_samples
        if num_samples < 0:  # TODO: simplify this in Tune
            num_samples = math.inf
        concurrent_trials = self._tune_config.max_concurrent_trials or 0
        if concurrent_trials < 1:  # TODO: simplify this in Tune
            concurrent_trials = math.inf

        actual_concurrency = min(
            (
                (cpus_total // cpus_per_trial) if cpus_per_trial else 0,
                num_samples,
                concurrent_trials,
            )
        )
        return (actual_concurrency * cpus_per_trial) / (cpus_total + 0.001)

    def _maybe_warn_resource_contention(self):
        if not ray.is_initialized():
            return

        trainable = self.converted_trainable

        # This may not be precise, but we don't have a great way of
        # accessing the actual scaling config if it is being tuned.
        scaling_config = None
        get_scaling_config = getattr(trainable, "base_scaling_config", None)
        if callable(get_scaling_config):
            scaling_config = get_scaling_config()

        if scaling_config is None or scaling_config._max_cpu_fraction_per_node:
            return

        has_base_dataset = getattr(trainable, "has_base_dataset", False)

        cpus_per_trial = scaling_config.total_resources.get("CPU", 0)
        cpus_left = ray.available_resources().get("CPU", 0)  # avoid div by 0
        # TODO(amogkam): Remove this warning after _max_cpu_fraction_per_node is no
        # longer experimental.
        if (
            has_base_dataset
            and self._expected_utilization(cpus_per_trial, cpus_left) > 0.8
        ):
            warnings.warn(
                "Executing `.fit()` may leave less than 20% of CPUs in "
                "this cluster for Dataset execution, which can lead to "
                "resource contention or hangs. To avoid this, "
                "reserve at least 20% of node CPUs for Dataset execution by "
                "setting `_max_cpu_fraction_per_node = 0.8` in the Trainer "
                "scaling_config. See "
                "https://docs.ray.io/en/master/data/dataset-internals.html"
                "#datasets-and-tune for more info.",
                stacklevel=4,
            )

    def _validate_overwrite_trainable(
        self,
        original_trainable: TrainableTypeOrTrainer,
        overwrite_trainable: Optional[TrainableTypeOrTrainer],
    ):
        """Determines whether the new `overwrite_trainable` is compatible
        with the restored experiment with some basic sanity checks
        (ensuring same type and name as the original trainable).
        """

        # Check if the trainable was wrapped with `tune.with_parameters`,
        # Set the Tuner to fail on fit if the trainable is not re-specified.
        trainable_wrapped_params = getattr(
            original_trainable, "_attached_param_names", None
        )
        if trainable_wrapped_params and not overwrite_trainable:
            self._missing_params_error_message = (
                "The original trainable cannot be used to resume training, since "
                "`tune.with_parameters` attached references to objects "
                "in the Ray object store that may not exist anymore. "
                "You must re-supply the trainable with the same parameters "
                f"{trainable_wrapped_params} attached:\n\n"
                "from ray import tune\n\n"
                "# Reconstruct the trainable with the same parameters\n"
                "trainable_with_params = tune.with_parameters(trainable, ...)\n"
                "tuner = tune.Tuner.restore(\n"
                "    ..., overwrite_trainable=trainable_with_params\n"
                ")\n\nSee https://docs.ray.io/en/master/tune/api_docs/trainable.html"
                "#tune-with-parameters for more details."
            )
        if not overwrite_trainable:
            return

        error_message = (
            "Usage of `overwrite_trainable` is limited to re-specifying the "
            "same trainable that was passed to `Tuner`, in the case "
            "that the trainable is not serializable (e.g. it holds object references)."
        )

        if type(original_trainable) != type(overwrite_trainable):
            raise ValueError(
                f"{error_message}\n"
                f"Got new trainable of type {type(overwrite_trainable)} "
                f"but expected {type(original_trainable)}."
            )

        from ray.train.trainer import BaseTrainer

        if isinstance(overwrite_trainable, BaseTrainer):
            if overwrite_trainable.run_config != original_trainable.run_config:
                warnings.warn(
                    "Overwriting the AIR Trainer with a new `RunConfig` is not "
                    "supported - the restored experiment will continue with the old "
                    "config. To avoid this warning, revert changes made to `RunConfig`."
                )
                overwrite_trainable.run_config = original_trainable.run_config
        else:
            original_name = Experiment.get_trainable_name(original_trainable)
            overwrite_name = Experiment.get_trainable_name(overwrite_trainable)
            if original_name != overwrite_name:
                raise ValueError(
                    f"{error_message}\nGot new trainable with identifier "
                    f"{overwrite_name} but expected {original_name}."
                )

        logger.warning(
            "The trainable will be overwritten - this should be done with caution: "
            "it's possible to supply an incompatible trainable, and there are "
            "no guarantees that the resumed experiment will continue successfully. "
            "If you encounter errors during training, ensure that you are passing "
            "in the same trainable that was passed into the initial `Tuner` object."
        )

    def _restore_from_path_or_uri(
        self,
        path_or_uri: str,
        resume_config: Optional[_ResumeConfig],
        overwrite_trainable: Optional[TrainableTypeOrTrainer],
    ):
        # Sync down from cloud storage if needed
        synced, experiment_checkpoint_dir = self._maybe_sync_down_tuner_state(
            path_or_uri
        )
        experiment_checkpoint_path = Path(experiment_checkpoint_dir)

        if (
            not (experiment_checkpoint_path / _TRAINABLE_PKL).exists()
            or not (experiment_checkpoint_path / _TUNER_PKL).exists()
        ):
            raise RuntimeError(
                f"Could not find Tuner state in restore directory. Did you pass"
                f"the correct path (including experiment directory?) Got: "
                f"{path_or_uri}"
            )

        # Load trainable and tuner state
        with open(experiment_checkpoint_path / _TRAINABLE_PKL, "rb") as fp:
            trainable = pickle.load(fp)

        with open(experiment_checkpoint_path / _TUNER_PKL, "rb") as fp:
            tuner = pickle.load(fp)
            self.__dict__.update(tuner.__dict__)

        self._validate_overwrite_trainable(trainable, overwrite_trainable)
        if overwrite_trainable:
            trainable = overwrite_trainable

        self._is_restored = True
        self.trainable = trainable
        self._resume_config = resume_config

        if not synced:
            # If we didn't sync, use the restore_path local dir
            self._experiment_checkpoint_dir = os.path.abspath(
                os.path.expanduser(path_or_uri)
            )

            # Update local_dir to use the parent of the experiment path
            # provided to `Tuner.restore`
            experiment_path = Path(self._experiment_checkpoint_dir)
            self._run_config.local_dir = str(experiment_path.parent)
            self._run_config.name = experiment_path.name
        else:
            # Set the experiment `name` and `upload_dir` according to the URI
            parsed_uri = urllib.parse.urlparse(path_or_uri)
            remote_path = Path(os.path.normpath(parsed_uri.netloc + parsed_uri.path))
            upload_dir = parsed_uri._replace(
                netloc="", path=str(remote_path.parent)
            ).geturl()

            self._run_config.name = remote_path.name
            self._run_config.sync_config.upload_dir = upload_dir

            # If we synced, `experiment_checkpoint_dir` will contain a temporary
            # directory. Create an experiment checkpoint dir instead and move
            # our data there.
            new_exp_path = Path(
                self._setup_create_experiment_checkpoint_dir(self._run_config)
            )
            for file_dir in experiment_checkpoint_path.glob("*"):
                file_dir.replace(new_exp_path / file_dir.name)
            shutil.rmtree(experiment_checkpoint_path)
            self._experiment_checkpoint_dir = str(new_exp_path)

        try:
            self._experiment_analysis = ExperimentAnalysis(
                self._experiment_checkpoint_dir,
                default_metric=self._tune_config.metric,
                default_mode=self._tune_config.mode,
            )
        except Exception:
            self._experiment_analysis = None

    def _maybe_sync_down_tuner_state(self, restore_path: str) -> Tuple[bool, str]:
        """Sync down trainable state from remote storage.

        Returns:
            Tuple of (downloaded from remote, local_dir)
        """
        if not is_non_local_path_uri(restore_path):
            return False, os.path.expanduser(restore_path)

        tempdir = Path(tempfile.mkdtemp("tmp_experiment_dir"))

        path = Path(restore_path)
        download_from_uri(str(path / _TRAINABLE_PKL), str(tempdir / _TRAINABLE_PKL))
        download_from_uri(str(path / _TUNER_PKL), str(tempdir / _TUNER_PKL))
        return True, str(tempdir)

    def _process_scaling_config(self) -> None:
        """Converts ``self._param_space["scaling_config"]`` to a dict.

        The dict is converted back to a dataclass by the Trainer, after the
        Tune search specification is resolved.
        """
        # TODO: introduce `ray.tune.sample.TuneableDataclass` and allow Tune to
        # natively resolve specs with dataclasses.
        scaling_config = self._param_space.get("scaling_config")
        if not isinstance(scaling_config, ScalingConfig):
            return
        self._param_space["scaling_config"] = scaling_config.__dict__.copy()

    def _setup_create_experiment_checkpoint_dir(
        self, run_config: Optional[RunConfig]
    ) -> str:
        """Sets up experiment checkpoint dir before actually running the experiment."""
        path = Experiment.get_experiment_checkpoint_dir(
            self.converted_trainable,
            run_config.local_dir,
            run_config.name,
        )
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        return path

    # This has to be done through a function signature (@property won't do).
    def get_experiment_checkpoint_dir(self) -> str:
        return self._experiment_checkpoint_dir

    @property
    def trainable(self) -> TrainableTypeOrTrainer:
        return self._trainable

    @property
    def converted_trainable(self) -> TrainableType:
        return self._converted_trainable

    @trainable.setter
    def trainable(self, trainable: TrainableTypeOrTrainer):
        self._trainable = trainable
        self._converted_trainable = self._convert_trainable(trainable)

    def _convert_trainable(self, trainable: TrainableTypeOrTrainer) -> TrainableType:
        """Converts an AIR Trainer to a Tune trainable and saves the converted
        trainable. If not using an AIR Trainer, this leaves the trainable as is."""
        from ray.train.trainer import BaseTrainer

        return (
            trainable.as_trainable()
            if isinstance(trainable, BaseTrainer)
            else trainable
        )

    def fit(self) -> ResultGrid:
        trainable = self.converted_trainable
        assert self._experiment_checkpoint_dir
        if not self._is_restored:
            param_space = copy.deepcopy(self._param_space)
            analysis = self._fit_internal(trainable, param_space)
        else:
            analysis = self._fit_resume(trainable)

        self._experiment_analysis = analysis

        return ResultGrid(self._experiment_analysis)

    def get_results(self) -> ResultGrid:
        if not self._experiment_analysis:
            raise RuntimeError(
                "Can't return results as experiment has not been run, yet. "
                "Call `Tuner.fit()` to run the experiment first."
            )
        return ResultGrid(self._experiment_analysis)

    def _get_tune_run_arguments(self, trainable: TrainableType) -> Dict[str, Any]:
        """Get tune.run arguments common for both new and resumed runs."""
        checkpoint_freq = self._run_config.checkpoint_config.checkpoint_frequency
        checkpoint_at_end = self._run_config.checkpoint_config.checkpoint_at_end

        if checkpoint_freq:
            # Function trainables (and thus most of our trainers) usually don't handle
            # this argument.
            handle_checkpoint_freq = getattr(
                trainable, "_handles_checkpoint_freq", None
            )
            if handle_checkpoint_freq is False:
                # If we specifically know this trainable doesn't support the
                # argument, raise an error
                raise ValueError(
                    f"You passed `checkpoint_frequency={checkpoint_freq}` to your "
                    "CheckpointConfig, but this trainer does not support "
                    "this argument. If you passed in an AIR trainer that takes in a "
                    "custom training loop, you will need to "
                    "report a checkpoint every `checkpoint_frequency` iterations "
                    "within your training loop using "
                    "`ray.air.session.report(metrics=..., checkpoint=...)` "
                    "to get this behavior."
                )
            elif handle_checkpoint_freq is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's bookkeeping.
                checkpoint_freq = 0
            # Otherwise, the trainable is not an AIR trainer and we just keep the
            # user-supplied value.
            # Function trainables will raise a runtime error later if set > 0
        if checkpoint_at_end is not None:
            # Again, function trainables usually don't handle this argument.
            handle_cp_at_end = getattr(trainable, "_handles_checkpoint_at_end", None)
            if handle_cp_at_end is False:
                # If we specifically know we don't support it, raise an error.
                raise ValueError(
                    f"You passed `checkpoint_at_end={checkpoint_at_end}` to your "
                    "CheckpointConfig, but this trainer does not support "
                    "this argument. If you passed in an AIR trainer that takes in a "
                    "custom training loop, you should include one last call to "
                    "`ray.air.session.report(metrics=..., checkpoint=...)` "
                    "at the end of your training loop to get this behavior."
                )
            elif handle_cp_at_end is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's internal bookkeeping.
                checkpoint_at_end = False
            # If this is a user-defined trainable, just keep the value
            # Function trainables will raise a runtime error later if set to True
        else:
            # Set default to False for function trainables and True for everything else
            if is_function_trainable(trainable):
                checkpoint_at_end = False
            else:
                checkpoint_at_end = True

        return dict(
            local_dir=self._run_config.local_dir,
            mode=self._tune_config.mode,
            metric=self._tune_config.metric,
            callbacks=self._run_config.callbacks,
            sync_config=self._run_config.sync_config,
            stop=self._run_config.stop,
            max_failures=self._run_config.failure_config.max_failures,
            keep_checkpoints_num=self._run_config.checkpoint_config.num_to_keep,
            checkpoint_score_attr=(
                self._run_config.checkpoint_config._tune_legacy_checkpoint_score_attr
            ),
            checkpoint_freq=checkpoint_freq,
            checkpoint_at_end=checkpoint_at_end,
            _experiment_checkpoint_dir=self._experiment_checkpoint_dir,
            raise_on_failed_trial=False,
            fail_fast=(self._run_config.failure_config.fail_fast),
            progress_reporter=self._run_config.progress_reporter,
            verbose=self._run_config.verbose,
            reuse_actors=self._tune_config.reuse_actors,
            max_concurrent_trials=self._tune_config.max_concurrent_trials,
            time_budget_s=self._tune_config.time_budget_s,
            trial_name_creator=self._tune_config.trial_name_creator,
            trial_dirname_creator=self._tune_config.trial_dirname_creator,
            chdir_to_trial_dir=self._tune_config.chdir_to_trial_dir,
        )

    def _fit_internal(
        self, trainable: TrainableType, param_space
    ) -> ExperimentAnalysis:
        """Fitting for a fresh Tuner."""
        args = {
            **self._get_tune_run_arguments(trainable),
            **dict(
                run_or_experiment=trainable,
                config={**param_space},
                num_samples=self._tune_config.num_samples,
                search_alg=self._tune_config.search_alg,
                scheduler=self._tune_config.scheduler,
                name=self._run_config.name,
                log_to_file=self._run_config.log_to_file,
            ),
            **self._tuner_kwargs,
        }
        analysis = run(
            **args,
        )
        self.clear_remote_string_queue()
        return analysis

    def _fit_resume(self, trainable: TrainableType) -> ExperimentAnalysis:
        """Fitting for a restored Tuner."""
        if self._missing_params_error_message:
            raise ValueError(self._missing_params_error_message)

        resume = "AUTO"

        if self._resume_config:
            if not self._resume_config.resume_unfinished:
                if self._resume_config.resume_errored:
                    resume += "+ERRORED_ONLY"
                elif self._resume_config.restart_errored:
                    resume += "+RESTART_ERRORED_ONLY"
            else:
                if self._resume_config.resume_errored:
                    resume += "+ERRORED"
                elif self._resume_config.restart_errored:
                    resume += "+RESTART_ERRORED"

        args = {
            **self._get_tune_run_arguments(trainable),
            **dict(
                run_or_experiment=trainable,
                resume=resume,
                search_alg=self._tune_config.search_alg,
                scheduler=self._tune_config.scheduler,
            ),
            **self._tuner_kwargs,
        }
        analysis = run(**args)
        self.clear_remote_string_queue()
        return analysis

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_tuner_kwargs"] = state["_tuner_kwargs"].copy()
        state["_tuner_kwargs"].pop("_remote_string_queue", None)
        state.pop(_TRAINABLE_KEY, None)
        state.pop(_CONVERTED_TRAINABLE_KEY, None)
        state.pop(_PARAM_SPACE_KEY, None)
        state.pop(_EXPERIMENT_ANALYSIS_KEY, None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
