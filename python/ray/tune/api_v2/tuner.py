from collections import deque
import copy
import os
from typing import Any, Callable, Dict, List, Optional, Type, Union
from xgboost_ray.tune import TuneReportCheckpointCallback, _get_tune_resources

import ray
from ray import ObjectRef
import ray.cloudpickle as pickle
from ray.data import Dataset

from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.callback import Callback
from ray.tune.trainable import Trainable
from ray.tune.tune import run

TUNER_INTERNAL = "tuner_internal"
DATASETS = "datasets"
TRAIN_DATASET = "train_dataset"


class TunerInternal:
    def __init__(
        self,
        trainable: Union[
            str,
            Callable,
            Type[Trainable],
            Type[ConvertibleToTrainable],
            ConvertibleToTrainable,
        ],
        run_config: Dict[str, Any],
        param_space: Dict[str, Any],
        name: Optional[str] = None,
        callbacks: Optional[List[Callback]] = None,
    ):
        self.trainable = trainable
        self.run_config = run_config
        self.param_space = param_space
        self.experiment_path = None

        self.name = name
        self.callbacks = callbacks

    @staticmethod
    def _convert_trainable(trainable: Any):
        if isinstance(trainable, ConvertibleToTrainable):
            trainable = trainable.as_trainable()
        else:
            trainable = trainable
        return trainable

    # @staticmethod
    # def _materialize_datasets_if_needed(datasets):
    #     if callable(datasets):
    #         _true_datasets = datasets()
    #         _true_datasets.get_internal_block_refs()
    #     elif isinstance(datasets, dict):
    #         for key in datasets.keys():
    #             datasets[key] = TunerInternal._materialize_datasets_if_needed(datasets[key])
    #         return datasets
    #     else:
    #         assert isinstance(datasets, Dataset)
    #         return datasets

    def fit(self):
        param_space = copy.deepcopy(self.param_space)

        # Hacks until Dataset can be serialized across different clusters.
        # if DATASETS in param_space:
        #     param_space[DATASETS] = self._materialize_datasets_if_needed(param_space[DATASETS])

        trainable = self._convert_trainable(self.trainable)
        if not self.experiment_path:
            analysis = self._fit_internal(trainable, param_space)
        else:
            # This does not work yet. Need to figure out Dataset story.
            assert False, "resume flow is not working yet..."
            analysis = self._fit_resume(trainable)

        return self._post_fit(analysis)

    def _fit_internal(self, trainable, param_space):
        """Fitting for a fresh Tuner.

        `Tuner.experiment_path` is assigned from `analysis.experiment_dir`."""
        analysis = run(
            trainable,
            config={"run_config": self.run_config, **param_space},
            name=self.name,
            callbacks=self.callbacks,
        )
        self.experiment_path = copy.copy(analysis.experiment_dir)
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

    def _post_fit(self, analysis):
        tuner_ckpt = os.path.join(self.experiment_path, "tuner.pkl")
        with open(tuner_ckpt, "wb") as fp:
            pickle.dump(self, fp)

        trainable_ckpt = os.path.join(self.experiment_path, "trainable.pkl")
        with open(trainable_ckpt, "wb") as fp:
            pickle.dump(self.trainable, fp)

        return analysis, self.experiment_path

    @classmethod
    def restore(cls, path: str) -> "Tuner":
        trainable_ckpt = os.path.join(path, "trainable.pkl")
        with open(trainable_ckpt, "rb") as fp:
            trainable = pickle.load(fp)

        tuner_ckpt = os.path.join(path, "tuner.pkl")
        with open(tuner_ckpt, "rb") as fp:
            tuner = pickle.load(fp)

        tuner.trainable = trainable
        tuner.experiment_path = path
        return tuner

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("trainable", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


class Tuner:
    """Multiplexes between local Tuner and remote Tuner depending on if in ray client.

    For ray client, wraps Tuner(TunerInternal) into a remote actor on head node.
    """

    _local_tuner: Optional[TunerInternal]  # Only used in none ray client mode.
    _remote_tuner: Optional[ObjectRef]  # Only used in ray client mode.

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
            tuner_internal = TunerInternal.restore(path)
            return Tuner(tuner_internal=tuner_internal)
        else:
            tuner_internal = ray.remote(num_cpus=0)(TunerInternal.restore).remote(path)
            return Tuner(tuner_internal=tuner_internal)

    def fit(self):
        if not self._is_ray_client:
            return self._local_tuner.fit()
        else:
            return ray.get(self._remote_tuner.fit.remote())
