import copy
import os
from typing import Any, Callable, Dict, Optional, Type, Union

import ray.cloudpickle as pickle
from ray.air.config import RunConfig, ScalingConfig
from ray.train.trainer import BaseTrainer
from ray.tune import Experiment, TuneError, ExperimentAnalysis
from ray.tune.execution.trial_runner import _ResumeConfig
from ray.tune.registry import is_function_trainable
from ray.tune.result_grid import ResultGrid
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.tune.tune_config import TuneConfig


_TRAINABLE_PKL = "trainable.pkl"
_TUNER_PKL = "tuner.pkl"
_TRAINABLE_KEY = "_trainable"
_PARAM_SPACE_KEY = "_param_space"


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
        trainable: Optional[
            Union[
                str,
                Callable,
                Type[Trainable],
                BaseTrainer,
            ]
        ] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_config: Optional[TuneConfig] = None,
        run_config: Optional[RunConfig] = None,
        _tuner_kwargs: Optional[Dict] = None,
    ):
        # Restored from Tuner checkpoint.
        if restore_path:
            trainable_ckpt = os.path.join(restore_path, _TRAINABLE_PKL)
            with open(trainable_ckpt, "rb") as fp:
                trainable = pickle.load(fp)

            tuner_ckpt = os.path.join(restore_path, _TUNER_PKL)
            with open(tuner_ckpt, "rb") as fp:
                tuner = pickle.load(fp)
                self.__dict__.update(tuner.__dict__)

            self._is_restored = True
            self._trainable = trainable
            self._experiment_checkpoint_dir = restore_path
            self._resume_config = resume_config
            return

        # Start from fresh
        if not trainable:
            raise TuneError("You need to provide a trainable to tune.")

        self._resume_config = None

        # If no run config was passed to Tuner directly, use the one from the Trainer,
        # if available
        if not run_config and isinstance(trainable, BaseTrainer):
            run_config = trainable.run_config

        self._is_restored = False
        self._trainable = trainable
        self._tune_config = tune_config or TuneConfig()
        self._run_config = run_config or RunConfig()
        self._tuner_kwargs = copy.deepcopy(_tuner_kwargs) or {}
        self._experiment_checkpoint_dir = self._setup_create_experiment_checkpoint_dir(
            self._run_config
        )

        # Not used for restored Tuner.
        self._param_space = param_space or {}
        self._process_scaling_config()

        # This needs to happen before `tune.run()` is kicked in.
        # This is because currently tune does not exit gracefully if
        # run in ray client mode - if crash happens, it just exits immediately
        # without allowing for checkpointing tuner and trainable.
        # Thus this has to happen before tune.run() so that we can have something
        # to restore from.
        tuner_ckpt = os.path.join(self._experiment_checkpoint_dir, _TUNER_PKL)
        with open(tuner_ckpt, "wb") as fp:
            pickle.dump(self, fp)

        trainable_ckpt = os.path.join(self._experiment_checkpoint_dir, _TRAINABLE_PKL)
        with open(trainable_ckpt, "wb") as fp:
            pickle.dump(self._trainable, fp)

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
            self._convert_trainable(self._trainable),
            run_config.local_dir,
            run_config.name,
        )
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    # This has to be done through a function signature (@property won't do).
    def get_experiment_checkpoint_dir(self) -> str:
        return self._experiment_checkpoint_dir

    @staticmethod
    def _convert_trainable(trainable: Any) -> Type[Trainable]:
        if isinstance(trainable, BaseTrainer):
            trainable = trainable.as_trainable()
        else:
            trainable = trainable
        return trainable

    def fit(self) -> ResultGrid:
        trainable = self._convert_trainable(self._trainable)
        assert self._experiment_checkpoint_dir
        if not self._is_restored:
            param_space = copy.deepcopy(self._param_space)
            analysis = self._fit_internal(trainable, param_space)
        else:
            analysis = self._fit_resume(trainable)

        return ResultGrid(analysis)

    def _get_tune_run_arguments(self, trainable) -> Dict[str, Any]:
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
                    f"You passed `checkpoint_freq={checkpoint_freq}` to your "
                    f"CheckpointConfig, but this trainer does not support "
                    f"this argument. If the trainer takes in a training loop, "
                    f"you will need to trigger checkpointing yourself using "
                    f"`ray.air.session.report(metrics=..., checkpoint=...)`."
                )
            elif handle_checkpoint_freq is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's bookkeeping.
                checkpoint_freq = 0
            # Otherwise, this is a non-trainer trainable and we just keep the
            # user-supplied value.

        if checkpoint_at_end is not None:
            # Again, function trainables usually don't handle this argument.
            handle_cp_at_end = getattr(trainable, "_handles_checkpoint_at_end", None)
            if handle_cp_at_end is False:
                # If we specifically know we don't support it, raise an error.
                raise ValueError(
                    f"You passed `checkpoint_at_end={checkpoint_at_end}` to your "
                    f"CheckpointConfig, but this trainer does not support "
                    f"this argument. If the trainer takes in a training loop, "
                    f"you will need to trigger checkpointing yourself using "
                    f"`ray.air.session.report(metrics=..., checkpoint=...)`. "
                )
            elif handle_cp_at_end is True:
                # If we specifically support it, it's handled in the training loop,
                # so we disable tune's internal bookkeeping.
                checkpoint_at_end = False
            # If this is a user-defined trainable, just keep the value
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
            reuse_actors=self._run_config.reuse_actors,
            max_concurrent_trials=self._tune_config.max_concurrent_trials,
            time_budget_s=self._tune_config.time_budget_s,
        )

    def _fit_internal(self, trainable, param_space) -> ExperimentAnalysis:
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
        return analysis

    def _fit_resume(self, trainable) -> ExperimentAnalysis:
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
                resume=resume,
            ),
            **self._tuner_kwargs,
        }
        analysis = run(**args)
        return analysis

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop(_TRAINABLE_KEY, None)
        state.pop(_PARAM_SPACE_KEY, None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
