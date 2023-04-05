# DEPRECATED: Remove whole file in 2.5.

import logging
from typing import Callable, Dict, Optional

from ray.air.integrations.mlflow import MLflowLoggerCallback as _MLflowLoggerCallback
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

callback_deprecation_message = (
    "`ray.tune.integration.mlflow.MLflowLoggerCallback` is deprecated. "
    "Please use `ray.air.integrations.mlflow.MLflowLoggerCallback` instead."
)

mixin_deprecation_message = (
    "The `mlflow_mixin`/`MLflowTrainableMixin` is deprecated. "
    "Use `ray.air.integrations.mlflow.setup_mlflow` instead."
)


@Deprecated(message=callback_deprecation_message)
class MLflowLoggerCallback(_MLflowLoggerCallback):
    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        registry_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        save_artifact: bool = False,
    ):
        raise DeprecationWarning(callback_deprecation_message)


@Deprecated(message=mixin_deprecation_message)
def mlflow_mixin(func: Callable):
    """mlflow_mixin

    MLflow (https://mlflow.org) Tracking is an open source library for
    recording and querying experiments. This Ray Tune Trainable mixin helps
    initialize the MLflow API for use with the ``Trainable`` class or the
    ``@mlflow_mixin`` function API. This mixin automatically configures MLflow
    and creates a run in the same process as each Tune trial. You can then
    use the mlflow API inside the your training function and it will
    automatically get reported to the correct run.

    For basic usage, just prepend your training function with the
    ``@mlflow_mixin`` decorator:

    .. code-block:: python

        from ray.tune.integration.mlflow import mlflow_mixin

        @mlflow_mixin
        def train_fn(config):
            ...
            mlflow.log_metric(...)

    You can also use MlFlow's autologging feature if using a training
    framework like Pytorch Lightning, XGBoost, etc. More information can be
    found here
    (https://mlflow.org/docs/latest/tracking.html#automatic-logging).

    .. code-block:: python

        from ray.tune.integration.mlflow import mlflow_mixin

        @mlflow_mixin
        def train_fn(config):
            mlflow.autolog()
            xgboost_results = xgb.train(config, ...)

    The MlFlow configuration is done by passing a ``mlflow`` key to
    the ``config`` parameter of ``tune.Tuner()`` (see example below).

    The content of the ``mlflow`` config entry is used to
    configure MlFlow. Here are the keys you can pass in to this config entry:

    Args:
        tracking_uri: The tracking URI for MLflow tracking. If using
            Tune in a multi-node setting, make sure to use a remote server for
            tracking.
        experiment_id: The id of an already created MLflow experiment.
            All logs from all trials in ``tune.Tuner()`` will be reported to this
            experiment. If this is not provided or the experiment with this
            id does not exist, you must provide an``experiment_name``. This
            parameter takes precedence over ``experiment_name``.
        experiment_name: The name of an already existing MLflow
            experiment. All logs from all trials in ``tune.Tuner()`` will be
            reported to this experiment. If this is not provided, you must
            provide a valid ``experiment_id``.
        token: A token to use for HTTP authentication when
            logging to a remote tracking server. This is useful when you
            want to log to a Databricks server, for example. This value will
            be used to set the MLFLOW_TRACKING_TOKEN environment variable on
            all the remote training processes.

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.integration.mlflow import mlflow_mixin

        import mlflow

        # Create the MlFlow expriment.
        mlflow.create_experiment("my_experiment")

        @mlflow_mixin
        def train_fn(config):
            for i in range(10):
                loss = config["a"] + config["b"]
                mlflow.log_metric(key="loss", value=loss)
            tune.report(loss=loss, done=True)

        tuner = tune.Tuner(
            train_fn,
            param_space={
                # define search space here
                "a": tune.choice([1, 2, 3]),
                "b": tune.choice([4, 5, 6]),
                # mlflow configuration
                "mlflow": {
                    "experiment_name": "my_experiment",
                    "tracking_uri": mlflow.get_tracking_uri()
                }
            })

        tuner.fit()

    """
    raise DeprecationWarning(mixin_deprecation_message)


@Deprecated(message=mixin_deprecation_message)
class MLflowTrainableMixin:
    def __init__(self, config: Dict, *args, **kwargs):
        raise DeprecationWarning(mixin_deprecation_message)
