import copy
import io
import os
import math
import logging
import shutil
import tempfile
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
    TYPE_CHECKING,
    Tuple,
)

import pyarrow.fs

import ray.cloudpickle as pickle
from ray.util import inspect_serializability
from ray.air._internal.remote_storage import download_from_uri, is_non_local_path_uri
from ray.air._internal.uri_utils import URI
from ray.air._internal.usage import AirEntrypoint
from ray.air.config import RunConfig, ScalingConfig
from ray.train._internal.storage import (
    _use_storage_context,
    StorageContext,
    get_fs_and_path,
)
from ray.tune import Experiment, TuneError, ExperimentAnalysis
from ray.tune.analysis.experiment_analysis import LegacyExperimentAnalysis
from ray.tune.execution.experiment_state import _ResumeConfig
from ray.tune.tune import _Config
from ray.tune.registry import is_function_trainable
from ray.tune.result import _get_defaults_results_dir
from ray.tune.result_grid import ResultGrid
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.tune.tune_config import TuneConfig
from ray.tune.utils import flatten_dict

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer
    from ray.util.queue import Queue


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
            if applicable. Refer to ray.train.RunConfig for more info.
    """

    def __init__(
        self,
        restore_path: str = None,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
        resume_config: Optional[_ResumeConfig] = None,
        trainable: Optional[TrainableTypeOrTrainer] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_config: Optional[TuneConfig] = None,
        run_config: Optional[RunConfig] = None,
        _tuner_kwargs: Optional[Dict] = None,
        _entrypoint: AirEntrypoint = AirEntrypoint.TUNER,
    ):
        from ray.train.trainer import BaseTrainer

        if isinstance(trainable, BaseTrainer):
            run_config = self._choose_run_config(
                tuner_run_config=run_config,
                trainer=trainable,
                param_space=param_space,
            )

        self._tune_config = tune_config or TuneConfig()
        self._run_config = run_config or RunConfig()
        self._entrypoint = _entrypoint

        # Restore from Tuner checkpoint.
        if restore_path:
            self._restore_from_path_or_uri(
                path_or_uri=restore_path,
                trainable=trainable,
                overwrite_param_space=param_space,
                resume_config=resume_config,
                storage_filesystem=storage_filesystem,
            )
            return

        # Start from fresh
        if not trainable:
            raise TuneError("You need to provide a trainable to tune.")

        self.trainable = trainable
        assert self.converted_trainable
        self._validate_trainable(self.converted_trainable)

        self.param_space = param_space

        self._resume_config = None
        self._is_restored = False
        self._tuner_kwargs = copy.deepcopy(_tuner_kwargs) or {}
        (
            self._legacy_experiment_checkpoint_dir,
            self._experiment_dir_name,
        ) = self.setup_create_experiment_checkpoint_dir(
            self.converted_trainable, self._run_config
        )
        self._experiment_analysis = None

        # This needs to happen before `tune.run()` is kicked in.
        # This is because currently tune does not exit gracefully if
        # run in ray client mode - if crash happens, it just exits immediately
        # without allowing for checkpointing tuner and trainable.
        # Thus this has to happen before tune.run() so that we can have something
        # to restore from.
        experiment_checkpoint_path = Path(self._legacy_experiment_checkpoint_dir)
        with open(experiment_checkpoint_path / _TUNER_PKL, "wb") as fp:
            pickle.dump(self.__getstate__(), fp)

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

    def _validate_trainable(
        self, trainable: TrainableType, required_trainable_name: Optional[str] = None
    ):
        """Determines whether or not the trainable is valid.

        This includes checks on the serializability of the trainable, as well
        asserting that the trainable name is as expected on restoration.

        This trainable name validation is needed due to an implementation detail
        where the trainable name (which is differently generated depending on
        the trainable type) is saved in the Trial metadata and needs to match
        upon restoration. This does not affect the typical path, since `Tuner.restore`
        expects the exact same trainable (which will have the same name).

        Raises:
            ValueError: if the trainable name does not match or if the trainable
                is not serializable.
        """
        try:
            pickle.dumps(trainable)
        except TypeError as e:
            sio = io.StringIO()
            inspect_serializability(trainable, print_file=sio)
            msg = (
                "The provided trainable is not serializable, which is a requirement "
                "since the trainable is serialized and deserialized when transferred "
                "to remote workers. See below for a trace of the non-serializable "
                "objects that were found in your trainable:\n"
                f"{sio.getvalue()}"
            )
            raise TypeError(msg) from e

        if not required_trainable_name:
            return

        trainable_name = Experiment.get_trainable_name(trainable)

        if trainable_name != required_trainable_name:
            raise ValueError(
                "Invalid `trainable` input to `Tuner.restore()`. To fix this error, "
                "pass in the same trainable that was used to initialize the Tuner. "
                "Got a trainable with identifier "
                f"'{trainable_name}' but expected '{required_trainable_name}'."
            )

    def _set_trainable_on_restore(
        self, trainable: TrainableType, old_trainable_name: Optional[str]
    ):
        from ray.train.base_trainer import BaseTrainer

        self.trainable = trainable
        assert self.converted_trainable
        self._validate_trainable(
            trainable=self.converted_trainable,
            required_trainable_name=old_trainable_name,
        )

        if isinstance(self.trainable, BaseTrainer):
            # Log a warning in case the user tries to modify the
            # `RunConfig` from the Trainer
            trainer: BaseTrainer = self.trainable

            # Only log if the Trainer has a non-default RunConfig
            if trainer.run_config != RunConfig():
                logger.warning(
                    "The Tune experiment will restore using the original run's "
                    "`RunConfig`. If you made any changes to the `RunConfig` "
                    "within the Trainer you passed into `Tuner.restore`, "
                    "they will be ignored in the resumed run."
                )

            trainer.run_config = self._run_config

    def _validate_param_space_on_restore(
        self,
        new_param_space: Dict[str, Any],
        flattened_param_space_keys: Optional[List[str]],
    ):
        """Determines whether the (optionally) re-specified `param_space` is valid.

        This method performs very loose validation on the new param_space to
        prevent users from trying to specify new hyperparameters to tune over.

        Raises:
            ValueError: if not all keys match the original param_space.
        """
        if flattened_param_space_keys is None:
            # Backwards compatibility: skip validation
            return

        keys = sorted(flatten_dict(new_param_space).keys())
        if keys != flattened_param_space_keys:
            raise ValueError(
                "Invalid `param_space` input to `Tuner.restore()`. To fix this error, "
                "pass in the same `param_space` that was used to initialize the Tuner. "
                "Only re-specify the `param_space` to refresh Ray object references "
                "that no longer exist due to restoring from a new Ray cluster session. "
                "It should not be used to introduce new hyperparameters to tune."
                f"\n\nGot: {keys}\nExpected: {flattened_param_space_keys}"
            )

    def _set_param_space_on_restore(
        self,
        param_space: Optional[Dict[str, Any]],
        flattened_param_space_keys: Optional[List[str]],
    ):
        self.param_space = param_space

        if self.param_space is not None:
            # param_space = None -> use the original param_space
            self._validate_param_space_on_restore(
                new_param_space=self.param_space,
                flattened_param_space_keys=flattened_param_space_keys,
            )

    def _load_tuner_state(
        self, tuner_state: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[List[str]]]:
        """Loads Tuner state from the previously saved `tuner.pkl`.

        Args:
            tuner_pkl_path: pathlib.Path of the `tuner.pkl` file saved during the
                original Tuner initialization.

        Returns:
            tuple: of `(old_trainable_name, flattened_param_space_keys)` used for
                validating the re-specified `trainable` and `param_space`.
        """
        # NOTE: These are magic keys used for validating restore args.
        old_trainable_name = tuner_state.pop("__trainable_name", None)
        flattened_param_space_keys = tuner_state.pop(
            "__flattened_param_space_keys", None
        )

        self.__setstate__(tuner_state)

        return old_trainable_name, flattened_param_space_keys

    def _restore_from_path_or_uri(
        self,
        path_or_uri: str,
        trainable: TrainableTypeOrTrainer,
        overwrite_param_space: Optional[Dict[str, Any]],
        resume_config: _ResumeConfig,
        storage_filesystem: Optional[pyarrow.fs.FileSystem],
    ):
        if _use_storage_context():
            fs, fs_path = get_fs_and_path(path_or_uri, storage_filesystem)
            with fs.open_input_file(os.path.join(fs_path, _TUNER_PKL)) as f:
                tuner_state = pickle.loads(f.readall())

            restoring_from_cloud = None
            local_experiment_checkpoint_dir = None
            experiment_checkpoint_path = None
        else:
            # Sync down from cloud storage if needed
            (
                restoring_from_cloud,
                local_experiment_checkpoint_dir,
            ) = self._maybe_sync_down_tuner_state(path_or_uri)
            experiment_checkpoint_path = Path(local_experiment_checkpoint_dir)

            with open(experiment_checkpoint_path / _TUNER_PKL, "rb") as fp:
                tuner_state = pickle.load(fp)

        old_trainable_name, flattened_param_space_keys = self._load_tuner_state(
            tuner_state
        )

        # Perform validation and set the re-specified `trainable` and `param_space`
        self._set_trainable_on_restore(
            trainable=trainable, old_trainable_name=old_trainable_name
        )
        self._set_param_space_on_restore(
            param_space=overwrite_param_space,
            flattened_param_space_keys=flattened_param_space_keys,
        )

        # Update RunConfig to reflect changes in the experiment directory
        path_or_uri_obj = URI(path_or_uri)

        # Infer the `storage_path` and run `name` of the restored run using the
        # experiment directory.
        # Ex: ~/ray_results/exp_name -> ~/ray_results, exp_name
        # Ex: s3://bucket/exp_name -> s3://bucket, exp_name
        self._run_config.name = path_or_uri_obj.name
        self._run_config.storage_path = str(path_or_uri_obj.parent)

        if _use_storage_context():
            _, self._experiment_dir_name = self.setup_create_experiment_checkpoint_dir(
                self.converted_trainable, self._run_config
            )

            self._legacy_experiment_checkpoint_dir = None
        else:
            # Set the experiment directory
            if not restoring_from_cloud:
                self._legacy_experiment_checkpoint_dir = local_experiment_checkpoint_dir
            else:
                # If we synced, `experiment_checkpoint_dir` will contain a temporary
                # directory. Create an experiment checkpoint dir instead and move
                # our data there.
                (
                    new_exp_path,
                    new_exp_name,
                ) = self.setup_create_experiment_checkpoint_dir(
                    self.converted_trainable, self._run_config
                )
                new_exp_path = Path(new_exp_path)
                for file_dir in experiment_checkpoint_path.glob("*"):
                    file_dir.replace(new_exp_path / file_dir.name)
                shutil.rmtree(experiment_checkpoint_path)
                self._legacy_experiment_checkpoint_dir = str(new_exp_path)
                self._experiment_dir_name = str(new_exp_name)

        # Load the experiment results at the point where it left off.
        try:
            if _use_storage_context():
                self._experiment_analysis = ExperimentAnalysis(
                    experiment_checkpoint_path=path_or_uri,
                    default_metric=self._tune_config.metric,
                    default_mode=self._tune_config.mode,
                )
            else:
                self._experiment_analysis = LegacyExperimentAnalysis(
                    experiment_checkpoint_path=path_or_uri,
                    default_metric=self._tune_config.metric,
                    default_mode=self._tune_config.mode,
                )
        except Exception:
            self._experiment_analysis = None

        self._resume_config = resume_config
        self._is_restored = True

    def _maybe_sync_down_tuner_state(self, restore_path: str) -> Tuple[bool, str]:
        """Sync down trainable state from remote storage.

        Returns:
            Tuple of (downloaded from remote, local_dir)
        """
        if not is_non_local_path_uri(restore_path):
            return False, Path(restore_path).expanduser().absolute().as_posix()

        tempdir = Path(tempfile.mkdtemp("tmp_experiment_dir"))

        restore_uri = URI(restore_path)
        download_from_uri(str(restore_uri / _TUNER_PKL), str(tempdir / _TUNER_PKL))
        return True, str(tempdir)

    def _choose_run_config(
        self,
        tuner_run_config: Optional[RunConfig],
        trainer: "BaseTrainer",
        param_space: Optional[Dict[str, Any]],
    ) -> RunConfig:
        """Chooses which `RunConfig` to use when multiple can be passed in
        through a Trainer or the Tuner itself.

        Args:
            tuner_run_config: The run config passed into the Tuner constructor.
            trainer: The Trainer instance to use with Tune, which may have
                a RunConfig specified by the user.
            param_space: The param space passed to the Tuner.

        Raises:
            ValueError: if the `run_config` is specified as a hyperparameter.
        """
        if param_space and "run_config" in param_space:
            raise ValueError(
                "`RunConfig` cannot be tuned as part of the `param_space`! "
                "Move the run config to be a parameter of the `Tuner`: "
                "Tuner(..., run_config=RunConfig(...))"
            )

        # Both Tuner RunConfig + Trainer RunConfig --> prefer Tuner RunConfig
        if tuner_run_config and trainer.run_config != RunConfig():
            logger.info(
                "A `RunConfig` was passed to both the `Tuner` and the "
                f"`{trainer.__class__.__name__}`. The run config passed to "
                "the `Tuner` is the one that will be used."
            )
            return tuner_run_config

        # No Tuner RunConfig -> pass the Trainer config through
        # This returns either a user-specified config, or the default RunConfig
        # if nothing was provided to both the Trainer or Tuner.
        if not tuner_run_config:
            return trainer.run_config

        # Tuner RunConfig + No Trainer RunConfig --> Use the Tuner config
        return tuner_run_config

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

    @classmethod
    def setup_create_experiment_checkpoint_dir(
        cls, trainable: TrainableType, run_config: Optional[RunConfig]
    ) -> Tuple[str, str]:
        """Sets up and creates the local experiment checkpoint dir.
        This is so that the `tuner.pkl` file gets stored in the same directory
        and gets synced with other experiment results.

        Returns:
            Tuple: (experiment_path, experiment_dir_name)
        """
        if _use_storage_context():
            experiment_dir_name = (
                run_config.name or StorageContext.get_experiment_dir_name(trainable)
            )
            storage_local_path = _get_defaults_results_dir()
            experiment_path = (
                Path(storage_local_path).joinpath(experiment_dir_name).as_posix()
            )
        else:
            experiment_path = Experiment.get_experiment_checkpoint_dir(
                trainable,
                run_config.storage_path,
                run_config.name,
            )
            experiment_dir_name = os.path.basename(experiment_path)

        os.makedirs(experiment_path, exist_ok=True)
        return experiment_path, experiment_dir_name

    # This has to be done through a function signature (@property won't do).
    def get_experiment_checkpoint_dir(self) -> str:
        return self._legacy_experiment_checkpoint_dir

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

    @property
    def param_space(self) -> Optional[Dict[str, Any]]:
        return self._param_space

    @param_space.setter
    def param_space(self, param_space: Optional[Dict[str, Any]]):
        # Handle any configs that adhere to the `to_dict` interface.
        # Ex: AlgorithmConfig from RLlib
        if isinstance(param_space, _Config):
            param_space = param_space.to_dict()

        if not isinstance(param_space, dict) and param_space is not None:
            raise ValueError(
                "The `param_space` passed to the `Tuner` must be a dict. "
                f"Got '{type(param_space)}' instead."
            )

        self._param_space = param_space

        if param_space:
            self._process_scaling_config()

    def _convert_trainable(self, trainable: TrainableTypeOrTrainer) -> TrainableType:
        """Converts a Trainer to a Tune trainable and saves the converted
        trainable. If not using a Trainer, this leaves the trainable as is."""
        from ray.train.trainer import BaseTrainer

        return (
            trainable.as_trainable()
            if isinstance(trainable, BaseTrainer)
            else trainable
        )

    def fit(self) -> ResultGrid:
        trainable = self.converted_trainable
        param_space = copy.deepcopy(self.param_space)
        if not self._is_restored:
            analysis = self._fit_internal(trainable, param_space)
        else:
            analysis = self._fit_resume(trainable, param_space)

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
        # Avoid overwriting the originally configured checkpoint config.
        checkpoint_config = copy.deepcopy(self._run_config.checkpoint_config)

        if checkpoint_config.checkpoint_frequency:
            # Function trainables (and thus most of our trainers) usually don't handle
            # this argument.
            handle_checkpoint_freq = getattr(
                trainable, "_handles_checkpoint_freq", None
            )
            if handle_checkpoint_freq is False:
                # If we specifically know this trainable doesn't support the
                # argument, raise an error
                raise ValueError(
                    "You passed `checkpoint_frequency="
                    f"{checkpoint_config.checkpoint_frequency}` to your "
                    "CheckpointConfig, but this trainer does not support "
                    "this argument. If you passed in a Trainer that takes in a "
                    "custom training loop, you will need to "
                    "report a checkpoint every `checkpoint_frequency` iterations "
                    "within your training loop using "
                    "`ray.train.report(metrics=..., checkpoint=...)` "
                    "to get this behavior."
                )
            elif handle_checkpoint_freq is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's bookkeeping.
                checkpoint_config.checkpoint_frequency = 0
            # Otherwise, the trainable is not a Trainer and we just keep the
            # user-supplied value.
            # Function trainables will raise a runtime error later if set > 0
        if checkpoint_config.checkpoint_at_end is not None:
            # Again, function trainables usually don't handle this argument.
            handle_cp_at_end = getattr(trainable, "_handles_checkpoint_at_end", None)
            if handle_cp_at_end is False:
                # If we specifically know we don't support it, raise an error.
                raise ValueError(
                    "You passed `checkpoint_at_end="
                    f"{checkpoint_config.checkpoint_at_end}` "
                    "to your CheckpointConfig, but this trainer does not support "
                    "this argument. If you passed in a Trainer that takes in a "
                    "custom training loop, you should include one last call to "
                    "`ray.train.report(metrics=..., checkpoint=...)` "
                    "at the end of your training loop to get this behavior."
                )
            elif handle_cp_at_end is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's internal bookkeeping.
                checkpoint_config.checkpoint_at_end = False
            # If this is a user-defined trainable, just keep the value
            # Function trainables will raise a runtime error later if set to True
        else:
            # Set default to False for function trainables and True for everything else
            if is_function_trainable(trainable):
                checkpoint_config.checkpoint_at_end = False
            else:
                checkpoint_config.checkpoint_at_end = True

        return dict(
            storage_path=self._run_config.storage_path,
            storage_filesystem=self._run_config.storage_filesystem,
            name=self._experiment_dir_name,
            mode=self._tune_config.mode,
            metric=self._tune_config.metric,
            callbacks=self._run_config.callbacks,
            sync_config=self._run_config.sync_config,
            stop=self._run_config.stop,
            max_failures=self._run_config.failure_config.max_failures,
            checkpoint_config=checkpoint_config,
            raise_on_failed_trial=False,
            fail_fast=(self._run_config.failure_config.fail_fast),
            progress_reporter=self._run_config.progress_reporter,
            verbose=self._run_config.verbose,
            reuse_actors=self._tune_config.reuse_actors,
            max_concurrent_trials=self._tune_config.max_concurrent_trials,
            time_budget_s=self._tune_config.time_budget_s,
            trial_name_creator=self._tune_config.trial_name_creator,
            trial_dirname_creator=self._tune_config.trial_dirname_creator,
            _entrypoint=self._entrypoint,
            # TODO(justinvyu): Finalize the local_dir vs. env var API in 2.8.
            # For now, keep accepting both options.
            local_dir=self._run_config.local_dir,
            # Deprecated
            chdir_to_trial_dir=self._tune_config.chdir_to_trial_dir,
            _experiment_checkpoint_dir=self._legacy_experiment_checkpoint_dir,
        )

    def _fit_internal(
        self, trainable: TrainableType, param_space: Optional[Dict[str, Any]]
    ) -> ExperimentAnalysis:
        """Fitting for a fresh Tuner."""
        args = {
            **self._get_tune_run_arguments(trainable),
            **dict(
                run_or_experiment=trainable,
                config=param_space,
                num_samples=self._tune_config.num_samples,
                search_alg=self._tune_config.search_alg,
                scheduler=self._tune_config.scheduler,
                log_to_file=self._run_config.log_to_file,
            ),
            **self._tuner_kwargs,
        }
        analysis = run(
            **args,
        )
        self.clear_remote_string_queue()
        return analysis

    def _fit_resume(
        self, trainable: TrainableType, param_space: Optional[Dict[str, Any]]
    ) -> ExperimentAnalysis:
        """Fitting for a restored Tuner."""
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
                config=param_space,
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
        trainable = state.pop(_CONVERTED_TRAINABLE_KEY, None)
        param_space = state.pop(_PARAM_SPACE_KEY, None)
        state.pop(_EXPERIMENT_ANALYSIS_KEY, None)

        state["__trainable_name"] = (
            Experiment.get_trainable_name(trainable) if trainable else None
        )
        state["__flattened_param_space_keys"] = (
            sorted(flatten_dict(param_space).keys())
            if param_space is not None
            else None
        )

        return state

    def __setstate__(self, state):
        # Make sure the magic metadata gets removed first.
        state.pop("__flattened_param_space_keys", None)
        state.pop("__trainable_name", None)

        self.__dict__.update(state)
