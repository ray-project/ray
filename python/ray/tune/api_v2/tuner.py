import copy
from dataclasses import dataclass
import os
import traceback
from typing import Any, Callable, Dict, List, Optional, Type, Union

import ray
import ray.cloudpickle as pickle
from ray.data import Dataset

from ray.tune import TuneError, Experiment
from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.callback import Callback
from ray.tune.schedulers import TrialScheduler
from ray.tune.suggest import Searcher
from ray.tune.trainable import Trainable
from ray.tune.tune import run
from ray.util import PublicAPI
from ray.util.client.common import ClientActorHandle
from ray.util.ml_utils.node import force_on_current_node


########################################################################
# The motivations behind the new Tuner API as part of unified ML effort is that
# 1. Users can seamlessly transition from their training process to tuning process.
# 2. Both training and tuning now use Tuner API as the execution engine.
#   This will help consolidate code on both sides.
# 3. Dataset and data preprocessing are now an integral part of training/tuning process.
#   Bonus point: you can even tune dataset and preprocessing.

# Some requirements on Tuner that may help you understand some implementation decisions:
# 1. Tuner needs to work in ray client mode.
# 2. Tuner needs to be restored in case of any failures.
########################################################################


@dataclass
@PublicAPI
class TuneAlgoConfig:
    """Tune specific config, related to how hyperparameter tuning algorithms
    are set up."""

    # Currently this is not at feature parity with `tune.run`, nor should it be.
    # The goal is to reach a fine balance between API flexibility and conciseness.
    # We should carefully introduce arguments here instead of just dumping everything.
    mode: Optional[str] = None
    metric: Optional[str] = None
    search_alg: Optional[Searcher] = None
    scheduler: Optional[TrialScheduler] = None
    num_samples: int = 1


# The magic key that is used when instantiating Tuner during resume.
_TUNER_INTERNAL = "tuner_internal"
# The key that denotes the dataset config in param space.
_DATASETS = "datasets"


class _TunerInternal:
    """The real implementation behind external facing ``Tuner``.

    The external facing Tuner multiplexes between local Tuner and remote Tuner
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
        1. resume run. ``_TunerInternal`` is restored from Tuner checkpoint.
            In this case, `restore_path` needs to be provided.
        2. fresh run. ``_TunerInternal`` is constructed from fresh.
            In this case, `trainable` needs to be provided, together
            with optional `param_space`, `tune_algo_config`, `name`,
            and `local_dir`.
        In either case, `callbacks` is considered a run time thing. It should be
        supplied across both fresh run and resume run.
        """
        # 1. Restored from Tuner checkpoint.
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
            self.callbacks = callbacks
            return

        # 2. Start from fresh
        assert trainable
        self.is_restored = False
        self.trainable = trainable
        # Only used when constructing Tuner from scatch.
        # Not used for restored Tuner.
        self.param_space = param_space
        self._process_dataset_param()

        self.name = name
        self.callbacks = callbacks

        self._experiment_checkpoint_dir = self._get_or_create_experiment_checkpoint_dir(
            local_dir
        )

        # This needs to happen before `tune.run()` is kicked in.
        # This is because currently tune does not exit gracefully if
        # run in ray client mode - if crash happens, it just exits immediately
        # without allowing for checkpointing tuner and trainable.
        # Thus this has to happen before tune.run() so that we can have something
        # to restore from.
        tuner_ckpt = os.path.join(self._experiment_checkpoint_dir, "tuner.pkl")
        with open(tuner_ckpt, "wb") as fp:
            pickle.dump(self, fp)

        trainable_ckpt = os.path.join(self._experiment_checkpoint_dir, "trainable.pkl")
        with open(trainable_ckpt, "wb") as fp:
            pickle.dump(self.trainable, fp)

    def _process_dataset_param(self):
        """Dataset needs to be fully executed before sent over to trainables.

        A valid dataset configuration in param space looks like:
        "datasets": {
            "train_dataset": tune.grid_search([ds1, ds2]),
        },
        """

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

    @staticmethod
    def _convert_trainable(trainable: Any):
        if isinstance(trainable, ConvertibleToTrainable):
            trainable = trainable.as_trainable()
        else:
            trainable = trainable
        return trainable

    @property
    def experiment_checkpoint_dir(self):
        return self._experiment_checkpoint_dir

    def _get_or_create_experiment_checkpoint_dir(self, local_dir: Optional[str]):
        """Get experiment checkpoint dir before actually running experiment."""
        path = Experiment.get_experiment_checkpoint_dir(
            self._convert_trainable(self.trainable), local_dir, self.name
        )
        if not os.path.exists(path):
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
        """Fitting for a fresh Tuner."""
        assert self.experiment_path
        analysis = run(
            trainable,
            config={**param_space},
            name=self.name,
            callbacks=self.callbacks,
            _experiment_checkpoint_dir=self.experiment_path,
        )
        return analysis

    def _fit_resume(self, trainable):
        """Fitting for a restored Tuner."""
        assert self.experiment_path
        analysis = run(
            trainable,
            resume=True,
            callbacks=self.callbacks,
            _experiment_checkpoint_dir=self.experiment_path,
        )
        return analysis

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("trainable", None)
        state.pop("param_space", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


class Tuner:
    """The external facing Tuner class as part of unified ML effort.

    Usage pattern:
    # Only do following, if you want to run in ray client mode.
    # This means the tune driver code will be running on head node of your cluster.
    ray.init("ray://my_ray_cluster_address")
    param_space = {
        "scaling_config": {
            "num_actors": tune.grid_search([2, 4]),
            "cpus_per_actor": 2,
            "gpus_per_actor": 0,
        },
        "preprocessor": tune.grid_search([prep_v1, prep_v2]),
        "datasets": {
            "train_dataset": tune.grid_search([ds1, ds2]),
        },
        "params": {
            "objective": "binary:logistic",
            "tree_method": "approx",
            "eval_metric": ["logloss", "error"],
            "eta": tune.loguniform(1e-4, 1e-1),
            "subsample": tune.uniform(0.5, 1.0),
            "max_depth": tune.randint(1, 9),
        },
    }
    tuner = Tuner(trainable=trainer, param_space=param_space, name="my_tune_run")
    analysis = tuner.fit()
    This returns an ``ExperimentAnalysis`` object, that you can interact with according
    to its API.

    If you ctrl+C to kill the run, later
    You can then do
    tuner = Tuner.restore(experiment_checkpoint_dir)
    tuner.fit() to resume your run.

    Note `experiment_checkpoint_dir` will be printed out for easy reference.
    """

    _local_tuner: Optional[_TunerInternal]  # Only used in none ray client mode.
    _remote_tuner: Optional[ClientActorHandle]  # Only used in ray client mode.

    def __init__(self, **kwargs):
        self._is_ray_client = ray.util.client.ray.is_connected()
        if _TUNER_INTERNAL in kwargs:
            if not self._is_ray_client:
                self._local_tuner = kwargs[_TUNER_INTERNAL]
            else:
                self._remote_tuner = kwargs[_TUNER_INTERNAL]
        else:
            if not self._is_ray_client:
                self._local_tuner = _TunerInternal(**kwargs)
            else:
                self._remote_tuner = force_on_current_node(
                    ray.remote(num_cpus=0)(_TunerInternal)
                ).remote(**kwargs)

    @classmethod
    def restore(cls, path, callbacks: Optional[List[Callback]] = None):
        """Restore Tuner.

        callbacks are passed again as they are considered run time thing."""
        if not ray.util.client.ray.is_connected():
            tuner_internal = _TunerInternal(restore_path=path, callbacks=callbacks)
            return Tuner(tuner_internal=tuner_internal)
        else:
            tuner_internal = force_on_current_node(
                ray.remote(num_cpus=0)(_TunerInternal)
            ).remote(restore_path=path, callbacks=callbacks)
            return Tuner(tuner_internal=tuner_internal)

    def fit(self):
        if not self._is_ray_client:
            try:
                self._local_tuner.fit()
            except Exception:
                raise TuneError(
                    f"Tune run fails with {traceback.format_exc()}. "
                    f"Please use tuner = Tuner.restore("
                    f"{self._local_tuner.experiment_checkpoint_dir}) to resume."
                )
        else:
            experiment_checkpoint_dir = ray.get(
                self._remote_tuner.experiment_checkpoint_dir.remote()
            )
            try:
                return ray.get(self._remote_tuner.fit.remote())
            except Exception:
                raise TuneError(
                    f"Tune run fails with {traceback.format_exc()}. "
                    f"Please use tuner = Tuner.restore("
                    f"{experiment_checkpoint_dir}) to resume."
                )
