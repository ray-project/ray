from typing import Any, Callable, Dict, Optional, Type, Union, TYPE_CHECKING

import ray

from ray.air.config import RunConfig
from ray.tune import TuneError
from ray.tune.execution.trial_runner import _ResumeConfig
from ray.tune.result_grid import ResultGrid
from ray.tune.trainable import Trainable
from ray.tune.impl.tuner_internal import TunerInternal
from ray.tune.tune_config import TuneConfig
from ray.tune.progress_reporter import (
    _prepare_progress_reporter_for_ray_client,
    _stream_client_output,
)
from ray.tune.utils.node import _force_on_current_node
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.train.base_trainer import BaseTrainer

ClientActorHandle = Any

# try:
#     # Breaks lint right now.
#     from ray.util.client.common import ClientActorHandle
# except Exception:
#     pass

# The magic key that is used when instantiating Tuner during resume.
_TUNER_INTERNAL = "_tuner_internal"
_SELF = "self"


_TUNER_FAILED_MSG = (
    "The Ray Tune run failed. Please inspect the previous error messages for a "
    "cause. After fixing the issue, you can restart the run from scratch or "
    "continue this run. To continue this run, you can use "
    '`tuner = Tuner.restore("{path}")`.'
)


@PublicAPI(stability="beta")
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
            if applicable. Refer to ray.air.config.RunConfig for more info.

    Usage pattern:

    .. code-block:: python

        from sklearn.datasets import load_breast_cancer

        from ray import tune
        from ray.data import from_pandas
        from ray.air.config import RunConfig, ScalingConfig
        from ray.train.xgboost import XGBoostTrainer
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
            "scaling_config": ScalingConfig(
                num_workers=tune.grid_search([2, 4]),
                resources_per_worker={
                    "CPU": tune.grid_search([1, 2]),
                },
            ),
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
            Union[str, Callable, Type[Trainable], "BaseTrainer"]
        ] = None,
        *,
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
                self._remote_tuner = _force_on_current_node(
                    ray.remote(num_cpus=0)(TunerInternal)
                ).remote(**kwargs)

    @classmethod
    def restore(
        cls,
        path: str,
        resume_unfinished: bool = True,
        resume_errored: bool = False,
        restart_errored: bool = False,
        overwrite_trainable: Optional[
            Union[str, Callable, Type[Trainable], "BaseTrainer"]
        ] = None,
    ) -> "Tuner":
        """Restores Tuner after a previously failed run.

        All trials from the existing run will be added to the result table. The
        argument flags control how existing but unfinished or errored trials are
        resumed.

        Finished trials are always added to the overview table. They will not be
        resumed.

        Unfinished trials can be controlled with the ``resume_unfinished`` flag.
        If ``True`` (default), they will be continued. If ``False``, they will
        be added as terminated trials (even if they were only created and never
        trained).

        Errored trials can be controlled with the ``resume_errored`` and
        ``restart_errored`` flags. The former will resume errored trials from
        their latest checkpoints. The latter will restart errored trials from
        scratch and prevent loading their last checkpoints.

        Args:
            path: The path where the previous failed run is checkpointed.
                This information could be easily located near the end of the
                console output of previous run.
                Note: depending on whether ray client mode is used or not,
                this path may or may not exist on your local machine.
            resume_unfinished: If True, will continue to run unfinished trials.
            resume_errored: If True, will re-schedule errored trials and try to
                restore from their latest checkpoints.
            restart_errored: If True, will re-schedule errored trials but force
                restarting them from scratch (no checkpoint will be loaded).
            overwrite_trainable: A newly specified trainable that will overwrite
                the trainable that was originally saved by Tune. This should
                only be used to resume an experiment where the original trainable
                is not fully serializable (e.g. when the trainable has object
                references attached to it via ``tune.with_parameters``, the objects
                they point to may not exist if restoring from a new Ray cluster).
                NOTE: This API is experimental and should be used with caution.
        """
        # TODO(xwjiang): Add some comments to clarify the config behavior across
        #  retored runs.
        #  For example, is callbacks supposed to be automatically applied
        #  when a Tuner is restored and fit again?

        resume_config = _ResumeConfig(
            resume_unfinished=resume_unfinished,
            resume_errored=resume_errored,
            restart_errored=restart_errored,
        )

        if not ray.util.client.ray.is_connected():
            tuner_internal = TunerInternal(
                restore_path=path,
                resume_config=resume_config,
                trainable=overwrite_trainable,
            )
            return Tuner(_tuner_internal=tuner_internal)
        else:
            tuner_internal = _force_on_current_node(
                ray.remote(num_cpus=0)(TunerInternal)
            ).remote(
                restore_path=path,
                resume_config=resume_config,
                trainable=overwrite_trainable,
            )
            return Tuner(_tuner_internal=tuner_internal)

    def _prepare_remote_tuner_for_jupyter_progress_reporting(self):
        run_config: RunConfig = ray.get(self._remote_tuner.get_run_config.remote())
        progress_reporter, string_queue = _prepare_progress_reporter_for_ray_client(
            run_config.progress_reporter, run_config.verbose
        )
        run_config.progress_reporter = progress_reporter
        ray.get(
            self._remote_tuner.set_run_config_and_remote_string_queue.remote(
                run_config, string_queue
            )
        )

        return progress_reporter, string_queue

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
            RayTaskError: If user-provided trainable raises an exception
            TuneError: General Ray Tune error.
        """

        if not self._is_ray_client:
            try:
                return self._local_tuner.fit()
            except TuneError as e:
                raise TuneError(
                    _TUNER_FAILED_MSG.format(
                        path=self._local_tuner.get_experiment_checkpoint_dir()
                    )
                ) from e
        else:
            experiment_checkpoint_dir = ray.get(
                self._remote_tuner.get_experiment_checkpoint_dir.remote()
            )
            (
                progress_reporter,
                string_queue,
            ) = self._prepare_remote_tuner_for_jupyter_progress_reporting()
            try:
                fit_future = self._remote_tuner.fit.remote()
                _stream_client_output(
                    fit_future,
                    progress_reporter,
                    string_queue,
                )
                return ray.get(fit_future)
            except TuneError as e:
                raise TuneError(
                    _TUNER_FAILED_MSG.format(path=experiment_checkpoint_dir)
                ) from e

    def get_results(self) -> ResultGrid:
        """Get results of a hyperparameter tuning run.

        This method returns the same results as :meth:`fit() <ray.tune.tuner.Tuner.fit>`
        and can be used to retrieve the results after restoring a tuner without
        calling ``fit()`` again.

        If the tuner has not been fit before, an error will be raised.

        .. code-block:: python

            from ray.tune import Tuner

            tuner = Tuner.restore("/path/to/experiment')
            results = tuner.get_results()

        Returns:
            Result grid of a previously fitted tuning run.

        """
        if not self._is_ray_client:
            return self._local_tuner.get_results()
        else:
            (
                progress_reporter,
                string_queue,
            ) = self._prepare_remote_tuner_for_jupyter_progress_reporting()
            fit_future = self._remote_tuner.fit.remote()
            _stream_client_output(
                fit_future,
                progress_reporter,
                string_queue,
            )
            return ray.get(fit_future)
