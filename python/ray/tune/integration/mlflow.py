import warnings

from ray.air.integrations.mlflow import MLflowLoggerCallback as _MLflowLoggerCallback

import logging
from typing import Callable, Dict, Optional

import ray
from ray.air._internal.mlflow import _MLflowLoggerUtil
from ray.tune.trainable import Trainable
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

callback_deprecation_message = (
    "`ray.tune.integration.mlflow.MLflowLoggerCallback` "
    "is deprecated and will be removed in "
    "the future. Please use `ray.air.integrations.mlflow.MLflowLoggerCallback` "
    "instead."
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
        logger.warning(callback_deprecation_message)
        super().__init__(
            tracking_uri, registry_uri, experiment_name, tags, save_artifact
        )


# Deprecate: Remove in 2.4
@Deprecated(
    message=(
        "The MLflowTrainableMixin is deprecated. "
        "Use `ray.air.integrations.mlflow.setup_mlflow` instead."
    )
)
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
    warnings.warn(
        "The mlflow_mixin/MLflowTrainableMixin is deprecated. "
        "Use `ray.air.integrations.mlflow.setup_mlflow` instead.",
        DeprecationWarning,
    )

    if ray.util.client.ray.is_connected():
        logger.warning(
            "When using mlflow_mixin with Ray Client, "
            "it is recommended to use a remote tracking "
            "server. If you are using a MLflow tracking server "
            "backed by the local filesystem, then it must be "
            "setup on the server side and not on the client "
            "side."
        )
    if hasattr(func, "__mixins__"):
        func.__mixins__ = func.__mixins__ + (MLflowTrainableMixin,)
    else:
        func.__mixins__ = (MLflowTrainableMixin,)
    return func


@Deprecated(
    message=(
        "The MLflowTrainableMixin is deprecated. "
        "Use `ray.air.integrations.mlflow.setup_mlflow` instead."
    )
)
class MLflowTrainableMixin:
    def __init__(self, config: Dict, *args, **kwargs):
        self.mlflow_util = _MLflowLoggerUtil()

        if not isinstance(self, Trainable):
            raise ValueError(
                "The `MLflowTrainableMixin` can only be used as a mixin "
                "for `tune.Trainable` classes. Please make sure your "
                "class inherits from both. For example: "
                "`class YourTrainable(MLflowTrainableMixin)`."
            )

        super().__init__(config, *args, **kwargs)
        _config = config.copy()
        try:
            mlflow_config = _config.pop("mlflow").copy()
        except KeyError as e:
            raise ValueError(
                "MLflow mixin specified but no configuration has been passed. "
                "Make sure to include a `mlflow` key in your `config` dict "
                "containing at least a `tracking_uri` and either "
                "`experiment_name` or `experiment_id` specification."
            ) from e

        tracking_uri = mlflow_config.pop("tracking_uri", None)
        if tracking_uri is None:
            raise ValueError(
                "MLflow mixin specified but no "
                "tracking_uri has been "
                "passed in. Make sure to include a `mlflow` "
                "key in your `config` dict containing at "
                "least a `tracking_uri`"
            )

        # Set the tracking token if one is passed in.
        tracking_token = mlflow_config.pop("token", None)

        experiment_id = mlflow_config.pop("experiment_id", None)

        experiment_name = mlflow_config.pop("experiment_name", None)

        # This initialization happens in each of the Trainables/workers.
        # So we have to set `create_experiment_if_not_exists` to False.
        # Otherwise there might be race conditions when each worker tries to
        # create the same experiment.
        # For the mixin, the experiment must be created beforehand.
        self.mlflow_util.setup_mlflow(
            tracking_uri=tracking_uri,
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            tracking_token=tracking_token,
            create_experiment_if_not_exists=False,
        )

        run_name = self.trial_name + "_" + self.trial_id
        run_name = run_name.replace("/", "_")
        self.mlflow_util.start_run(set_active=True, run_name=run_name)

    def stop(self):
        self.mlflow_util.end_run()
