import traceback
from typing import Any, Callable, Dict, List, Optional, Type, Union

import ray

from ray.tune import TuneError, ExperimentAnalysis
from ray.tune.api_v2.convertible_to_trainable import ConvertibleToTrainable
from ray.tune.callback import Callback
from ray.tune.trainable import Trainable
from ray.tune.impl.tuner_internal import TunerInternal
from ray.tune.tune_config import TuneConfig
from ray.util import PublicAPI
from ray.util.client.common import ClientActorHandle
from ray.util.ml_utils.node import force_on_current_node


########################################################################
# The motivations behind the new Tuner API as part of Ray MLC effort are that
# 1. Users can seamlessly transition from their training process to tuning process.
# 2. Dataset and data preprocessing are now an integral part of training/tuning process.
#   Bonus point: you can even tune dataset and preprocessing.

# Some requirements on Tuner that may help you understand some implementation decisions:
# 1. Tuner needs to work in ray client mode.
# 2. Tuner needs to be restored in case of any failures.
########################################################################

# The magic key that is used when instantiating Tuner during resume.
_TUNER_INTERNAL = "tuner_internal"
_SELF = "self"


@PublicAPI(stability="beta")
class Tuner:
    """The external facing Tuner class as part of Ray MLC effort.

    Args:
        trainable (Union[
                str,
                Callable,
                Type[Trainable],
                Type[ConvertibleToTrainable],
                ConvertibleToTrainable,
            ]): The trainable to be tune.
        param_space (Optional[Dict[str, Any]]): Search space to run tuning on.
            One thing to note is that both preprocessor and dataset can be tuned here.
        tune_config (Optional[TuneConfig]): tune algo specific configs.
            Refer to ray.tune.tune_config.TuneConfig for more info.
        name (Optional[str]): Name of the run.
        local_dir (Optional[str]): Local dir to save training results to.
            Defaults to ``~/ray_results``.
        callbacks (Optional[List[Callback]]): Callbacks to invoke.
            Refer to ray.tune.callback.Callback for more info.

    Usage pattern:
    .. code-block:: python

    # Only do the following, if you want to run in ray client mode.
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

    If the run finishes in error, to retry
    you can then do
    .. code-block:: python

    tuner = Tuner.restore(experiment_checkpoint_dir)
    tuner.fit()

    to resume your run.
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
                Type[ConvertibleToTrainable],
                ConvertibleToTrainable,
            ]
        ] = None,
        param_space: Optional[Dict[str, Any]] = None,
        tune_algo_config: Optional[TuneConfig] = None,
        name: Optional[str] = None,
        local_dir: Optional[str] = None,
        callbacks: Optional[List[Callback]] = None,
        # This is internal only arg.
        tuner_internal: Optional[TunerInternal] = None,
    ):
        kwargs = locals().copy()
        self._is_ray_client = ray.util.client.ray.is_connected()
        if tuner_internal:
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
    def restore(cls, path: str, callbacks: Optional[List[Callback]] = None) -> "Tuner":
        """Restore Tuner.

        Note: depending on whether ray client mode is used or not, this path may
        or may not exist on your local machine.

        callbacks are passed again as they are considered run time thing."""
        if not ray.util.client.ray.is_connected():
            tuner_internal = TunerInternal(restore_path=path, callbacks=callbacks)
            return Tuner(tuner_internal=tuner_internal)
        else:
            tuner_internal = force_on_current_node(
                ray.remote(num_cpus=0)(TunerInternal)
            ).remote(restore_path=path, callbacks=callbacks)
            return Tuner(tuner_internal=tuner_internal)

    def fit(self) -> ExperimentAnalysis:
        """Runs the tune run."""
        if not self._is_ray_client:
            try:
                return self._local_tuner.fit()
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
