import copy
import os
from typing import Any, Callable, Dict, List, Optional, Type, Union

import ray
import ray.cloudpickle as pickle
from ray.data import Dataset

from ray.tune import TuneError
from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.callback import Callback
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.util.client.common import ClientActorHandle

TUNER_INTERNAL = "tuner_internal"
DATASETS = "datasets"


class TunerInternal:
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
        run_config: Optional[Dict[str, Any]] = None,
        param_space: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
        callbacks: Optional[List[Callback]] = None,
    ):
        # Start from restore
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
            self.experiment_path = restore_path
            return

        # Start from fresh
        assert trainable
        self.is_restored = False
        self.trainable = trainable
        self.run_config = run_config
        # Only used when constructing Tuner from scatch.
        # Not used for restored Tuner.
        self.param_space = param_space
        self._process_dataset_param()

        self.name = name
        self.callbacks = callbacks

        self.experiment_path = self._get_experiment_checkpoint_dir()

        # This needs to happen earlier before run() is kicked in.
        tuner_ckpt = os.path.join(self.experiment_path, "tuner.pkl")
        with open(tuner_ckpt, "wb") as fp:
            pickle.dump(self, fp)

        trainable_ckpt = os.path.join(self.experiment_path, "trainable.pkl")
        with open(trainable_ckpt, "wb") as fp:
            pickle.dump(self.trainable, fp)

    def _process_dataset_param(self):
        """Some processing on Dataset param."""

        def _helper(dataset_dict: dict):
            for k, v in dataset_dict.items():
                if isinstance(v, dict):
                    _helper(v)
                elif isinstance(v, Dataset):
                    dataset_dict[k].fully_executed()
                elif isinstance(v, int):
                    # CV settings
                    pass
                elif isinstance(v, list):
                    if not all([isinstance(v_item, int) for v_item in v]) and not all(
                        [isinstance(v_item, Dataset) for v_item in v]
                    ):
                        raise TuneError("Wrongly formed dataset param passed in Tune!")
                    if len(v) > 0 and isinstance(v[0], Dataset):
                        [v_item.fully_executed() for v_item in v]
                else:
                    # We shouldn't be expecting anything here.
                    raise TuneError("Unexpected dataset param passed in.")

        if DATASETS in self.param_space:
            ds = self.param_space[DATASETS]
            if isinstance(ds, Dataset):
                ds.fully_executed()
            elif isinstance(ds, dict):
                _helper(ds)
            elif isinstance(ds, int):
                pass
            else:
                # We shouldn't be expecting anything here.
                raise TuneError("Unexpected dataset param passed in.")

    @staticmethod
    def _convert_trainable(trainable: Any):
        if isinstance(trainable, ConvertibleToTrainable):
            trainable = trainable.as_trainable()
        else:
            trainable = trainable
        return trainable

    def _get_experiment_checkpoint_dir(self):
        # TODO: This needs to be generalized.
        path = "/Users/xwjiang/ray_results/tuner_resume"
        os.makedirs(path)
        return path

    def fit(self):
        trainable = self._convert_trainable(self.trainable)
        if not self.is_restored:
            param_space = copy.deepcopy(self.param_space)
            analysis = self._fit_internal(trainable, param_space)
        else:
            analysis = self._fit_resume(trainable)

        return analysis

    def _fit_internal(self, trainable, param_space):
        """Fitting for a fresh Tuner.

        `Tuner.experiment_path` is assigned from `analysis.experiment_dir`."""
        analysis = run(
            trainable,
            config={"run_config": self.run_config, **param_space},
            name=self.name,
            callbacks=self.callbacks,
        )
        return analysis

    def _fit_resume(self, trainable):
        """Fitting for a restored Tuner.

        `Tuner.experiment_path` is already set in this case."""
        assert self.experiment_path
        # E.g. /home/ray/ray_results/experiment_name
        experiment_dir = os.path.dirname(self.experiment_path + os.sep)
        # E.g. experiment_name
        experiment_name = os.path.basename(experiment_dir)
        # E.g. /home/ray/ray_results
        local_dir = os.path.dirname(experiment_dir)

        analysis = run(
            trainable, name=experiment_name, local_dir=local_dir, resume=True
        )

        return analysis

    # @classmethod
    # def restore(cls, path: str) -> "Tuner":
    #     trainable_ckpt = os.path.join(path, "trainable.pkl")
    #     with open(trainable_ckpt, "rb") as fp:
    #         trainable = pickle.load(fp)
    #
    #     tuner_ckpt = os.path.join(path, "tuner.pkl")
    #     with open(tuner_ckpt, "rb") as fp:
    #         tuner = pickle.load(fp)
    #
    #     tuner.trainable = trainable
    #     tuner.experiment_path = path
    #     return tuner

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("trainable", None)
        state.pop("param_space", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


class Tuner:
    """Multiplexes between local Tuner and remote Tuner depending on if in ray client.

    For ray client, wraps Tuner(TunerInternal) into a remote actor on head node.
    """

    _local_tuner: Optional[TunerInternal]  # Only used in none ray client mode.
    _remote_tuner: Optional[ClientActorHandle]  # Only used in ray client mode.

    def __init__(self, **kwargs):
        self._is_ray_client = ray.util.client.ray.is_connected()
        if TUNER_INTERNAL in kwargs:
            if not self._is_ray_client:
                self._local_tuner = kwargs[TUNER_INTERNAL]
            else:
                self._remote_tuner = kwargs[TUNER_INTERNAL]
        else:
            if not self._is_ray_client:
                self._local_tuner = TunerInternal(**kwargs)
            else:
                self._remote_tuner = ray.remote(num_cpus=0)(TunerInternal).remote(
                    **kwargs
                )

    @classmethod
    def restore(cls, path):
        if not ray.util.client.ray.is_connected():
            tuner_internal = TunerInternal(restore_path=path)
            return Tuner(tuner_internal=tuner_internal)
        else:
            tuner_internal = ray.remote(num_cpus=0)(TunerInternal).remote(
                restore_path=path
            )
            return Tuner(tuner_internal=tuner_internal)

    def fit(self):
        if not self._is_ray_client:
            return self._local_tuner.fit()
        else:
            return ray.get(self._remote_tuner.fit.remote())
