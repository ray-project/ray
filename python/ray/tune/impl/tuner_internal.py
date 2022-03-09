import copy
import os
from typing import Any, Callable, Dict, List, Optional, Type, Union

import ray.cloudpickle as pickle
from ray.data import Dataset

from ray.tune import Experiment, TuneError
from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.callback import Callback
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.tune.tuner_config import TuneAlgoConfig


# The key that denotes the dataset config in param space.
_DATASETS = "datasets"


class TunerInternal:
    """The real implementation behind external facing ``Tuner``.

    The external facing ``Tuner`` multiplexes between local Tuner and remote Tuner
    depending on whether in Ray client mode.

    In Ray client mode, external Tuner wraps ``_TunerInternal`` into a remote actor,
    which is guaranteed to be placed on head node.
    """

    def __init__(
        self,
        restore_path: str = None,
        trainable: Optional[
            Union[
                str,
                Callable,
                Type[Trainable],
                Type[ConvertibleToTrainable],
                ConvertibleToTrainable,
            ]
        ] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_algo_config: Optional[TuneAlgoConfig] = None,
        name: Optional[str] = None,
        local_dir: Optional[str] = None,
        callbacks: Optional[List[Callback]] = None,
    ):
        """For initialization, there are two scenarios.
        1. fresh run. ``_TunerInternal`` is constructed from fresh.
            In this case, `trainable` needs to be provided, together
            with optional `param_space`, `tune_algo_config`, `name`,
            and `local_dir`.
        2. resume run. ``_TunerInternal`` is restored from Tuner checkpoint.
            In this case, `restore_path` needs to be provided.
        In either case, `callbacks` is considered a run time thing. It should be
        supplied across both fresh run and resume run.
        """
        # Restored from Tuner checkpoint.
        if restore_path:
            trainable_ckpt = os.path.join(restore_path, "trainable.pkl")
            with open(trainable_ckpt, "rb") as fp:
                trainable = pickle.load(fp)

            tuner_ckpt = os.path.join(restore_path, "tuner.pkl")
            with open(tuner_ckpt, "rb") as fp:
                tuner = pickle.load(fp)
                self.__dict__.update(tuner.__dict__)

            self.is_restored = True
            self.trainable = trainable
            self.experiment_checkpoint_dir = restore_path
            self.callbacks = callbacks
            return

        # Start from fresh
        assert trainable
        # TODO(xwjiang): Plumbing through `tune_algo_config`.
        self.is_restored = False
        self.trainable = trainable
        # Only used when constructing Tuner from scatch.
        # Not used for restored Tuner.
        self.param_space = param_space
        self._process_dataset_param()

        self.name = name
        self.callbacks = callbacks

        self.experiment_checkpoint_dir = self._get_or_create_experiment_checkpoint_dir(
            local_dir
        )

        # This needs to happen before `tune.run()` is kicked in.
        # This is because currently tune does not exit gracefully if
        # run in ray client mode - if crash happens, it just exits immediately
        # without allowing for checkpointing tuner and trainable.
        # Thus this has to happen before tune.run() so that we can have something
        # to restore from.
        tuner_ckpt = os.path.join(self.experiment_checkpoint_dir, "tuner.pkl")
        with open(tuner_ckpt, "wb") as fp:
            pickle.dump(self, fp)

        trainable_ckpt = os.path.join(self.experiment_checkpoint_dir, "trainable.pkl")
        with open(trainable_ckpt, "wb") as fp:
            pickle.dump(self.trainable, fp)

    def _process_dataset_param(self):
        """Dataset needs to be fully executed before sent over to trainables.

        A valid dataset configuration in param space looks like:
        "datasets": {
            "train_dataset": tune.grid_search([ds1, ds2]),
        },
        """
        print("===============================================================")

        def _helper(dataset_dict: dict):
            for k, v in dataset_dict.items():
                if isinstance(v, dict):
                    _helper(v)
                elif isinstance(v, Dataset):
                    dataset_dict[k] = v.fully_executed()
                # TODO(xwjiang): Consider CV config for beta.
                # elif isinstance(v, int):
                #     # CV settings
                #     pass
                elif isinstance(v, list):
                    if not all([isinstance(v_item, int) for v_item in v]) and not all(
                        [isinstance(v_item, Dataset) for v_item in v]
                    ):
                        raise TuneError(
                            "Wrongly formatted dataset param passed in Tune!"
                        )
                    if len(v) > 0 and isinstance(v[0], Dataset):
                        dataset_dict[k] = [v_item.fully_executed() for v_item in v]
                else:
                    raise TuneError("Unexpected dataset param passed in.")

        if _DATASETS in self.param_space:
            ds = self.param_space[_DATASETS]
            if isinstance(ds, Dataset):
                self.param_space[_DATASETS] = ds.fully_executed()
            elif isinstance(ds, dict):
                _helper(ds)
            else:
                # We shouldn't be expecting anything here.
                raise TuneError("Unexpected dataset param passed in.")

    def _get_or_create_experiment_checkpoint_dir(self, local_dir: Optional[str]):
        """Get experiment checkpoint dir before actually running experiment."""
        path = Experiment.get_experiment_checkpoint_dir(
            self._convert_trainable(self.trainable), local_dir, self.name
        )
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    # This has to be done through a function signature (@property won't do).
    def experiment_checkpoint_dir(self):
        return self.experiment_checkpoint_dir

    @staticmethod
    def _convert_trainable(trainable: Any):
        if isinstance(trainable, ConvertibleToTrainable):
            trainable = trainable.as_trainable()
        else:
            trainable = trainable
        return trainable

    def fit(self):
        trainable = self._convert_trainable(self.trainable)
        if not self.is_restored:
            param_space = copy.deepcopy(self.param_space)
            analysis = self._fit_internal(trainable, param_space)
        else:
            analysis = self._fit_resume(trainable)

        return analysis

    def _fit_internal(self, trainable, param_space):
        """Fitting for a fresh Tuner."""
        assert self.experiment_checkpoint_dir
        analysis = run(
            trainable,
            config={**param_space},
            name=self.name,
            callbacks=self.callbacks,
            _experiment_checkpoint_dir=self.experiment_checkpoint_dir,
        )
        return analysis

    def _fit_resume(self, trainable):
        """Fitting for a restored Tuner."""
        assert self.experiment_checkpoint_dir
        analysis = run(
            trainable,
            resume=True,
            callbacks=self.callbacks,
            _experiment_checkpoint_dir=self.experiment_checkpoint_dir,
        )
        return analysis

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("trainable", None)
        state.pop("param_space", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
