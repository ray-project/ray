import os
from typing import Dict, List

from ray.tune.logger import LoggerCallback
from ray.tune.experiment import Trial
from ray.tune.utils import flatten_dict


def _import_comet():
    """Try importing comet_ml.

    Used to check if comet_ml is installed and, otherwise, pass an informative
    error message.
    """
    if "COMET_DISABLE_AUTO_LOGGING" not in os.environ:
        os.environ["COMET_DISABLE_AUTO_LOGGING"] = "1"

    try:
        import comet_ml  # noqa: F401
    except ImportError:
        raise RuntimeError("pip install 'comet-ml' to use CometLoggerCallback")

    return comet_ml


class CometLoggerCallback(LoggerCallback):
    """CometLoggerCallback for logging Tune results to Comet.

    Comet (https://comet.ml/site/) is a tool to manage and optimize the
    entire ML lifecycle, from experiment tracking, model optimization
    and dataset versioning to model production monitoring.

    This Ray Tune ``LoggerCallback`` sends metrics and parameters to
    Comet for tracking.

    In order to use the CometLoggerCallback you must first install Comet
    via ``pip install comet_ml``

    Then set the following environment variables
    ``export COMET_API_KEY=<Your API Key>``

    Alternatively, you can also pass in your API Key as an argument to the
    CometLoggerCallback constructor.

    ``CometLoggerCallback(api_key=<Your API Key>)``

    Args:
            online: Whether to make use of an Online or
                Offline Experiment. Defaults to True.
            tags: Tags to add to the logged Experiment.
                Defaults to None.
            save_checkpoints: If ``True``, model checkpoints will be saved to
                Comet ML as artifacts. Defaults to ``False``.
            **experiment_kwargs: Other keyword arguments will be passed to the
                constructor for comet_ml.Experiment (or OfflineExperiment if
                online=False).

    Please consult the Comet ML documentation for more information on the
    Experiment and OfflineExperiment classes: https://comet.ml/site/

    Example:

    .. code-block:: python

        from ray.air.integrations.comet import CometLoggerCallback
        tune.run(
            train,
            config=config
            callbacks=[CometLoggerCallback(
                True,
                ['tag1', 'tag2'],
                workspace='my_workspace',
                project_name='my_project_name'
                )]
        )

    """

    # Do not enable these auto log options unless overridden
    _exclude_autolog = [
        "auto_output_logging",
        "log_git_metadata",
        "log_git_patch",
        "log_env_cpu",
        "log_env_gpu",
    ]

    # Do not log these metrics.
    _exclude_results = ["done", "should_checkpoint"]

    # These values should be logged as system info instead of metrics.
    _system_results = ["node_ip", "hostname", "pid", "date"]

    # These values should be logged as "Other" instead of as metrics.
    _other_results = ["trial_id", "experiment_id", "experiment_tag"]

    _episode_results = ["hist_stats/episode_reward", "hist_stats/episode_lengths"]

    def __init__(
        self,
        online: bool = True,
        tags: List[str] = None,
        save_checkpoints: bool = False,
        **experiment_kwargs,
    ):
        _import_comet()
        self.online = online
        self.tags = tags
        self.save_checkpoints = save_checkpoints
        self.experiment_kwargs = experiment_kwargs

        # Disable the specific autologging features that cause throttling.
        self._configure_experiment_defaults()

        # Mapping from trial to experiment object.
        self._trial_experiments = {}

        self._to_exclude = self._exclude_results.copy()
        self._to_system = self._system_results.copy()
        self._to_other = self._other_results.copy()
        self._to_episodes = self._episode_results.copy()

    def _configure_experiment_defaults(self):
        """Disable the specific autologging features that cause throttling."""
        for option in self._exclude_autolog:
            if not self.experiment_kwargs.get(option):
                self.experiment_kwargs[option] = False

    def _check_key_name(self, key: str, item: str) -> bool:
        """
        Check if key argument is equal to item argument or starts with item and
        a forward slash. Used for parsing trial result dictionary into ignored
        keys, system metrics, episode logs, etc.
        """
        return key.startswith(item + "/") or key == item

    def log_trial_start(self, trial: "Trial"):
        """
        Initialize an Experiment (or OfflineExperiment if self.online=False)
        and start logging to Comet.

        Args:
            trial: Trial object.

        """
        _import_comet()  # is this necessary?
        from comet_ml import Experiment, OfflineExperiment
        from comet_ml.config import set_global_experiment

        if trial not in self._trial_experiments:
            experiment_cls = Experiment if self.online else OfflineExperiment
            experiment = experiment_cls(**self.experiment_kwargs)
            self._trial_experiments[trial] = experiment
            # Set global experiment to None to allow for multiple experiments.
            set_global_experiment(None)
        else:
            experiment = self._trial_experiments[trial]

        experiment.set_name(str(trial))
        experiment.add_tags(self.tags)
        experiment.log_other("Created from", "Ray")

        config = trial.config.copy()
        config.pop("callbacks", None)
        experiment.log_parameters(config)

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        """
        Log the current result of a Trial upon each iteration.
        """
        if trial not in self._trial_experiments:
            self.log_trial_start(trial)
        experiment = self._trial_experiments[trial]
        step = result["training_iteration"]

        config_update = result.pop("config", {}).copy()
        config_update.pop("callbacks", None)  # Remove callbacks
        for k, v in config_update.items():
            if isinstance(v, dict):
                experiment.log_parameters(flatten_dict({k: v}, "/"), step=step)

            else:
                experiment.log_parameter(k, v, step=step)

        other_logs = {}
        metric_logs = {}
        system_logs = {}
        episode_logs = {}

        flat_result = flatten_dict(result, delimiter="/")
        for k, v in flat_result.items():
            if any(self._check_key_name(k, item) for item in self._to_exclude):
                continue

            if any(self._check_key_name(k, item) for item in self._to_other):
                other_logs[k] = v

            elif any(self._check_key_name(k, item) for item in self._to_system):
                system_logs[k] = v

            elif any(self._check_key_name(k, item) for item in self._to_episodes):
                episode_logs[k] = v

            else:
                metric_logs[k] = v

        experiment.log_others(other_logs)
        experiment.log_metrics(metric_logs, step=step)

        for k, v in system_logs.items():
            experiment.log_system_info(k, v)

        for k, v in episode_logs.items():
            experiment.log_curve(k, x=range(len(v)), y=v, step=step)

    def log_trial_save(self, trial: "Trial"):
        comet_ml = _import_comet()

        if self.save_checkpoints and trial.checkpoint:
            experiment = self._trial_experiments[trial]

            artifact = comet_ml.Artifact(
                name=f"checkpoint_{(str(trial))}", artifact_type="model"
            )

            # Walk through checkpoint directory and add all files to artifact
            checkpoint_root = trial.checkpoint.dir_or_data
            for root, dirs, files in os.walk(checkpoint_root):
                rel_root = os.path.relpath(root, checkpoint_root)
                for file in files:
                    local_file = os.path.join(checkpoint_root, rel_root, file)
                    logical_path = os.path.join(rel_root, file)

                    # Strip leading `./`
                    if logical_path.startswith("./"):
                        logical_path = logical_path[2:]

                    artifact.add(local_file, logical_path=logical_path)

            experiment.log_artifact(artifact)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        self._trial_experiments[trial].end()
        del self._trial_experiments[trial]

    def __del__(self):
        for trial, experiment in self._trial_experiments.items():
            experiment.end()
        self._trial_experiments = {}
