import logging
import os
from packaging import version
from copy import deepcopy
from typing import TYPE_CHECKING, Dict, Optional

from ray._private.dict import flatten_dict

if TYPE_CHECKING:
    from mlflow.entities import Run
    from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


class _MLflowLoggerUtil:
    """Util class for setting up and logging to MLflow.

    Use this util for any library that needs MLflow logging/tracking logic
    such as Ray Tune or Ray Train.
    """

    def __init__(self):
        import mlflow

        self._mlflow = mlflow
        self.experiment_id = None

    def __deepcopy__(self, memo=None):
        # mlflow is a module, and thus cannot be copied
        _mlflow = self._mlflow
        self.__dict__.pop("_mlflow")
        dict_copy = deepcopy(self.__dict__, memo)
        copied_object = _MLflowLoggerUtil()
        copied_object.__dict__.update(dict_copy)
        self._mlflow = _mlflow
        copied_object._mlflow = _mlflow
        return copied_object

    def setup_mlflow(
        self,
        tracking_uri: Optional[str] = None,
        registry_uri: Optional[str] = None,
        experiment_id: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tracking_token: Optional[str] = None,
        artifact_location: Optional[str] = None,
        create_experiment_if_not_exists: bool = True,
    ):
        """
        Sets up MLflow.

        Sets the Mlflow tracking uri & token, and registry URI. Also sets
        the MLflow experiment that the logger should use, and possibly
        creates new experiment if it does not exist.

        Args:
            tracking_uri: The tracking URI for the MLflow tracking
                server.
            registry_uri: The registry URI for the MLflow model registry.
            experiment_id: The id of an already existing MLflow
                experiment to use for logging. If None is passed in
                here and the MFLOW_EXPERIMENT_ID is not set, or the
                experiment with this id does not exist,
                ``experiment_name`` will be used instead. This argument takes
                precedence over ``experiment_name`` if both are passed in.
            experiment_name: The experiment name to use for logging.
                If None is passed in here, the MLFLOW_EXPERIMENT_NAME environment
                variable is used to determine the experiment name.
                If the experiment with the name already exists with MLflow,
                it will be reused. If not, a new experiment will be created
                with the provided name if
                ``create_experiment_if_not_exists`` is set to True.
            artifact_location: The location to store run artifacts.
                If not provided, MLFlow picks an appropriate default.
                Ignored if experiment already exists.
            tracking_token: Tracking token used to authenticate with MLflow.
            create_experiment_if_not_exists: Whether to create an
                experiment with the provided name if it does not already
                exist. Defaults to True.

        Returns:
            Whether setup is successful.
        """
        if tracking_token:
            os.environ["MLFLOW_TRACKING_TOKEN"] = tracking_token

        self._mlflow.set_tracking_uri(tracking_uri)
        self._mlflow.set_registry_uri(registry_uri)

        # First check experiment_id.
        experiment_id = (
            experiment_id
            if experiment_id is not None
            else os.environ.get("MLFLOW_EXPERIMENT_ID")
        )
        if experiment_id is not None:
            from mlflow.exceptions import MlflowException

            try:
                self._mlflow.get_experiment(experiment_id=experiment_id)
                logger.debug(
                    f"Experiment with provided id {experiment_id} "
                    "exists. Setting that as the experiment."
                )
                self.experiment_id = experiment_id
                return
            except MlflowException:
                pass

        # Then check experiment_name.
        experiment_name = (
            experiment_name
            if experiment_name is not None
            else os.environ.get("MLFLOW_EXPERIMENT_NAME")
        )
        if experiment_name is not None and self._mlflow.get_experiment_by_name(
            name=experiment_name
        ):
            logger.debug(
                f"Experiment with provided name {experiment_name} "
                "exists. Setting that as the experiment."
            )
            self.experiment_id = self._mlflow.get_experiment_by_name(
                experiment_name
            ).experiment_id
            return

        # An experiment with the provided id or name does not exist.
        # Create a new experiment if applicable.
        if experiment_name and create_experiment_if_not_exists:
            logger.debug(
                "Existing experiment not found. Creating new "
                f"experiment with name: {experiment_name}"
            )
            self.experiment_id = self._mlflow.create_experiment(
                name=experiment_name, artifact_location=artifact_location
            )
            return

        if create_experiment_if_not_exists:
            raise ValueError(
                f"Experiment with the provided experiment_id: "
                f"{experiment_id} does not exist and no "
                f"experiment_name provided. At least one of "
                f"these has to be provided."
            )
        else:
            raise ValueError(
                f"Experiment with the provided experiment_id: "
                f"{experiment_id} or experiment_name: "
                f"{experiment_name} does not exist. Please "
                f"create an MLflow experiment and provide "
                f"either its id or name."
            )

    def _parse_dict(self, dict_to_log: Dict) -> Dict:
        """Parses provided dict to convert all values to float.

        MLflow can only log metrics that are floats. This does not apply to
        logging parameters or artifacts.

        Args:
            dict_to_log: The dictionary containing the metrics to log.

        Returns:
            A dictionary containing the metrics to log with all values being
                converted to floats, or skipped if not able to be converted.
        """
        new_dict = {}
        for key, value in dict_to_log.items():
            try:
                value = float(value)
                new_dict[key] = value
            except (ValueError, TypeError):
                logger.debug(
                    "Cannot log key {} with value {} since the "
                    "value cannot be converted to float.".format(key, value)
                )
                continue

        return new_dict

    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        set_active: bool = False,
    ) -> "Run":
        """Starts a new run and possibly sets it as the active run.

        Args:
            tags: Tags to set for the new run.
            set_active: Whether to set the new run as the active run.
                If an active run already exists, then that run is returned.

        Returns:
            The newly created MLflow run.
        """
        import mlflow
        from mlflow.utils.mlflow_tags import MLFLOW_RUN_NAME

        if tags is None:
            tags = {}

        if set_active:
            return self._start_active_run(run_name=run_name, tags=tags)

        client = self._get_client()
        # If `mlflow==1.30.0` and we don't use `run_name`, then MLflow might error. For
        # more information, see #29749.
        if version.parse(mlflow.__version__) >= version.parse("1.30.0"):
            run = client.create_run(
                run_name=run_name, experiment_id=self.experiment_id, tags=tags
            )
        else:
            tags[MLFLOW_RUN_NAME] = run_name
            run = client.create_run(experiment_id=self.experiment_id, tags=tags)

        return run

    def _start_active_run(
        self, run_name: Optional[str] = None, tags: Optional[Dict] = None
    ) -> "Run":
        """Starts a run and sets it as the active run if one does not exist.

        If an active run already exists, then returns it.
        """
        active_run = self._mlflow.active_run()
        if active_run:
            return active_run

        return self._mlflow.start_run(
            run_name=run_name, experiment_id=self.experiment_id, tags=tags
        )

    def _run_exists(self, run_id: str) -> bool:
        """Check if run with the provided id exists."""
        from mlflow.exceptions import MlflowException

        try:
            self._mlflow.get_run(run_id=run_id)
            return True
        except MlflowException:
            return False

    def _get_client(self) -> "MlflowClient":
        """Returns an ml.tracking.MlflowClient instance to use for logging."""
        tracking_uri = self._mlflow.get_tracking_uri()
        registry_uri = self._mlflow.get_registry_uri()

        from mlflow.tracking import MlflowClient

        return MlflowClient(tracking_uri=tracking_uri, registry_uri=registry_uri)

    def log_params(self, params_to_log: Dict, run_id: Optional[str] = None):
        """Logs the provided parameters to the run specified by run_id.

        If no ``run_id`` is passed in, then logs to the current active run.
        If there is not active run, then creates a new run and sets it as
        the active run.

        Args:
            params_to_log: Dictionary of parameters to log.
            run_id (Optional[str]): The ID of the run to log to.
        """
        params_to_log = flatten_dict(params_to_log)

        if run_id and self._run_exists(run_id):
            client = self._get_client()
            for key, value in params_to_log.items():
                client.log_param(run_id=run_id, key=key, value=value)

        else:
            for key, value in params_to_log.items():
                self._mlflow.log_param(key=key, value=value)

    def log_metrics(self, step, metrics_to_log: Dict, run_id: Optional[str] = None):
        """Logs the provided metrics to the run specified by run_id.


        If no ``run_id`` is passed in, then logs to the current active run.
        If there is not active run, then creates a new run and sets it as
        the active run.

        Args:
            metrics_to_log: Dictionary of metrics to log.
            run_id (Optional[str]): The ID of the run to log to.
        """
        metrics_to_log = flatten_dict(metrics_to_log)
        metrics_to_log = self._parse_dict(metrics_to_log)

        if run_id and self._run_exists(run_id):
            client = self._get_client()
            for key, value in metrics_to_log.items():
                client.log_metric(run_id=run_id, key=key, value=value, step=step)

        else:
            for key, value in metrics_to_log.items():
                self._mlflow.log_metric(key=key, value=value, step=step)

    def save_artifacts(self, dir: str, run_id: Optional[str] = None):
        """Saves directory as artifact to the run specified by run_id.

        If no ``run_id`` is passed in, then saves to the current active run.
        If there is not active run, then creates a new run and sets it as
        the active run.

        Args:
            dir: Path to directory containing the files to save.
            run_id (Optional[str]): The ID of the run to log to.
        """
        if run_id and self._run_exists(run_id):
            client = self._get_client()
            client.log_artifacts(run_id=run_id, local_dir=dir)
        else:
            self._mlflow.log_artifacts(local_dir=dir)

    def end_run(self, status: Optional[str] = None, run_id=None):
        """Terminates the run specified by run_id.

        If no ``run_id`` is passed in, then terminates the
        active run if one exists.

        Args:
            status (Optional[str]): The status to set when terminating the run.
            run_id (Optional[str]): The ID of the run to terminate.

        """
        if (
            run_id
            and self._run_exists(run_id)
            and not (
                self._mlflow.active_run()
                and self._mlflow.active_run().info.run_id == run_id
            )
        ):
            client = self._get_client()
            client.set_terminated(run_id=run_id, status=status)
        else:
            self._mlflow.end_run(status=status)
