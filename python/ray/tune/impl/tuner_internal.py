import copy
import os
from typing import Any, Callable, Dict, Optional, Type, Union

import ray.cloudpickle as pickle
from ray.air.config import RunConfig
from ray.train.trainer import BaseTrainer
from ray.tune import Experiment, TuneError, ExperimentAnalysis
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
            return

        # Start from fresh
        if not trainable:
            raise TuneError("You need to provide a trainable to tune.")

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

    def _fit_internal(self, trainable, param_space) -> ExperimentAnalysis:
        """Fitting for a fresh Tuner."""
        analysis = run(
            trainable,
            config={**param_space},
            mode=self._tune_config.mode,
            metric=self._tune_config.metric,
            num_samples=self._tune_config.num_samples,
            search_alg=self._tune_config.search_alg,
            scheduler=self._tune_config.scheduler,
            name=self._run_config.name,
            callbacks=self._run_config.callbacks,
            sync_config=self._run_config.sync_config,
            stop=self._run_config.stop,
            max_failures=(
                self._run_config.failure_config.max_failures
                if self._run_config.failure_config
                else 0
            ),
            _experiment_checkpoint_dir=self._experiment_checkpoint_dir,
            raise_on_failed_trial=False,
            verbose=self._run_config.verbose,
            **self._tuner_kwargs,
        )
        return analysis

    def _fit_resume(self, trainable) -> ExperimentAnalysis:
        """Fitting for a restored Tuner."""
        analysis = run(
            trainable,
            resume=True,
            mode=self._tune_config.mode,
            metric=self._tune_config.metric,
            callbacks=self._run_config.callbacks,
            sync_config=self._run_config.sync_config,
            stop=self._run_config.stop,
            max_failures=(
                self._run_config.failure_config.max_failures
                if self._run_config.failure_config
                else 0
            ),
            _experiment_checkpoint_dir=self._experiment_checkpoint_dir,
            raise_on_failed_trial=False,
            **self._tuner_kwargs,
        )
        return analysis

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop(_TRAINABLE_KEY, None)
        state.pop(_PARAM_SPACE_KEY, None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
