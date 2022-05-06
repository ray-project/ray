from typing import Any, Callable, Dict, Optional, Type, Union

import ray

from ray.ml.config import RunConfig
from ray.ml.trainer import Trainer
from ray.tune import TuneError
from ray.tune.result_grid import ResultGrid
from ray.tune.trainable import Trainable
from ray.tune.impl.tuner_internal import TunerInternal
from ray.tune.tune_config import TuneConfig
from ray.util import PublicAPI
from ray.util.ml_utils.node import force_on_current_node

ClientActorHandle = Any

try:
    from ray.util.client.common import ClientActorHandle
except Exception:
    pass

# The magic key that is used when instantiating Tuner during resume.
_TUNER_INTERNAL = "_tuner_internal"
_SELF = "self"


@PublicAPI(stability="alpha")
class Tuner:
    """Tuner is the recommended way of launching hyperparameter tuning jobs with Ray Tune.

    Args:
        trainable: The trainable to be tuned.
        param_space: Search space of the tuning job.
            One thing to note is that both preprocessor and dataset can be tuned here.
        tune_config: Tuning algorithm specific configs.
            Refer to ray.tune.tune_config.TuneConfig for more info.
        run_config: Runtime configuration that is specific to individual trials.
            If passed, this will overwrite the run config passed to the Trainer,
            if applicable. Refer to ray.ml.config.RunConfig for more info.

    Usage pattern:

    .. code-block:: python

        from sklearn.datasets import load_breast_cancer

        from ray import tune
        from ray.data import from_pandas
        from ray.ml.config import RunConfig
        from ray.ml.train.integrations.xgboost import XGBoostTrainer
        from ray.tune.tuner import Tuner

        def get_dataset():
            data_raw = load_breast_cancer(as_frame=True)
            dataset_df = data_raw["data"]
            dataset_df["target"] = data_raw["target"]
            dataset = from_pandas(dataset_df)
            return dataset

        trainer = XGBoostTrainer(
            label_column="target",
            params={},
            datasets={"train": get_dataset()},
        )

        param_space = {
            "scaling_config": {
                "num_workers": tune.grid_search([2, 4]),
                "resources_per_worker": {
                    "CPU": tune.grid_search([1, 2]),
                },
            },
            # You can even grid search various datasets in Tune.
            # "datasets": {
            #     "train": tune.grid_search(
            #         [ds1, ds2]
            #     ),
            # },
            "params": {
                "objective": "binary:logistic",
                "tree_method": "approx",
                "eval_metric": ["logloss", "error"],
                "eta": tune.loguniform(1e-4, 1e-1),
                "subsample": tune.uniform(0.5, 1.0),
                "max_depth": tune.randint(1, 9),
            },
        }
        tuner = Tuner(trainable=trainer, param_space=param_space,
            run_config=RunConfig(name="my_tune_run"))
        analysis = tuner.fit()

    To retry a failed tune run, you can then do

    .. code-block:: python

        tuner = Tuner.restore(experiment_checkpoint_dir)
        tuner.fit()

    ``experiment_checkpoint_dir`` can be easily located near the end of the
    console output of your first failed run.
    """

    # One of the following is assigned.
    _local_tuner: Optional[TunerInternal]  # Only used in none ray client mode.
    _remote_tuner: Optional[ClientActorHandle]  # Only used in ray client mode.

    def __init__(
        self,
        trainable: Optional[
            Union[
                str,
                Callable,
                Type[Trainable],
                Trainer,
            ]
        ] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_config: Optional[TuneConfig] = None,
        run_config: Optional[RunConfig] = None,
        # This is internal only arg.
        # Only for dogfooding purposes. We can slowly promote these args
        # to RunConfig or TuneConfig as needed.
        # TODO(xwjiang): Remove this later.
        _tuner_kwargs: Optional[Dict] = None,
        _tuner_internal: Optional[TunerInternal] = None,
    ):
        """Configure and construct a tune run."""
        kwargs = locals().copy()
        self._is_ray_client = ray.util.client.ray.is_connected()
        if _tuner_internal:
            if not self._is_ray_client:
                self._local_tuner = kwargs[_TUNER_INTERNAL]
            else:
                self._remote_tuner = kwargs[_TUNER_INTERNAL]
        else:
            kwargs.pop(_TUNER_INTERNAL, None)
            kwargs.pop(_SELF, None)
            if not self._is_ray_client:
                self._local_tuner = TunerInternal(**kwargs)
            else:
                self._remote_tuner = force_on_current_node(
                    ray.remote(num_cpus=0)(TunerInternal)
                ).remote(**kwargs)

    @classmethod
    def restore(cls, path: str) -> "Tuner":
        """Restores Tuner after a previously failed run.

        Args:
           path: The path where the previous failed run is checkpointed.
               This information could be easily located near the end of the
               console output of previous run.
               Note: depending on whether ray client mode is used or not,
               this path may or may not exist on your local machine.
        """
        # TODO(xwjiang): Add some comments to clarify the config behavior across
        #  retored runs.
        #  For example, is callbacks supposed to be automatically applied
        #  when a Tuner is restored and fit again?
        if not ray.util.client.ray.is_connected():
            tuner_internal = TunerInternal(restore_path=path)
            return Tuner(_tuner_internal=tuner_internal)
        else:
            tuner_internal = force_on_current_node(
                ray.remote(num_cpus=0)(TunerInternal)
            ).remote(restore_path=path)
            return Tuner(_tuner_internal=tuner_internal)

    def fit(self) -> ResultGrid:
        """Executes hyperparameter tuning job as configured and returns result.

        Failure handling:
        For the kind of exception that happens during the execution of a trial,
        one may inspect it together with stacktrace through the returned result grid.
        See ``ResultGrid`` for reference. Each trial may fail up to a certain number.
        This is configured by ``RunConfig.FailureConfig.max_failures``.

        Exception that happens beyond trials will be thrown by this method as well.
        In such cases, there will be instruction like the following printed out
        at the end of console output to inform users on how to resume.

        Please use tuner = Tuner.restore("~/ray_results/tuner_resume")
        to resume.

        Raises:
            RayTaskError when the exception happens in trainable else TuneError.
        """

        if not self._is_ray_client:
            try:
                return self._local_tuner.fit()
            except Exception as e:
                raise TuneError(
                    f"Tune run failed. "
                    f'Please use tuner = Tuner.restore("'
                    f'{self._local_tuner.get_experiment_checkpoint_dir()}") to resume.'
                ) from e
        else:
            experiment_checkpoint_dir = ray.get(
                self._remote_tuner.get_experiment_checkpoint_dir.remote()
            )
            try:
                return ray.get(self._remote_tuner.fit.remote())
            except Exception as e:
                raise TuneError(
                    f"Tune run failed. "
                    f'Please use tuner = Tuner.restore("'
                    f'{experiment_checkpoint_dir}") to resume.'
                ) from e
