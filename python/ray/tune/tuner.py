from ray.tune.result_grid import ResultGrid
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
class Tuner:
    """Tuner is the recommended way of launching hyperparameter tuning jobs with Ray Tune.

    Attributes:
        trainable: The trainable to be tuned.
        param_space: Search space of the tuning job.
            One thing to note is that both preprocessor and dataset can be tuned here.
        tune_config: Tuning algorithm specific configs.
            Refer to ray.tune.tune_config.TuneConfig for more info.
        run_config: Runtime configuration that is specific to individual trials.
            Refer to ray.ml.config.RunConfig for more info.

    Returns:
        ``ResultGrid`` object.

    Usage pattern:
    .. code-block:: python

        # TODO(xwjiang): Make this runnable. Add imports.

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
        tuner = Tuner(trainable=trainer, param_space=param_space,
            run_config(name="my_tune_run"))
        analysis = tuner.fit()

    To retry a failed tune run, you can then do
    .. code-block:: python

        tuner = Tuner.restore(experiment_checkpoint_dir)
        tuner.fit()

    `experiment_checkpoint_dir` can be easily located near the end of the
    console output of your first failed run.
    """

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
        raise NotImplementedError

    def fit(self) -> ResultGrid:
        """Executes hyperparameter tuning job as configured and returns result.

        Failure handling:
        For the kind of exception that happens during the execution of a trial,
        one may inspect it together with stacktrace through the returned result grid.
        See ``ResultGrid`` for reference. Each trial may fail up to a certain number.
        This is configured by `RunConfig.FailureConfig.max_failures`.

        Exception that happens beyond trials will be thrown by this method as well.
        In such cases, there will be instruction like the following printed out
        at the end of console output to inform users on how to resume.

        Please use tuner = Tuner.restore("/Users/xwjiang/ray_results/tuner_resume")
        to resume.

        Exception that happens in non-essential integration blocks like during invoking
        callbacks will not crash the whole run.

        Raises:
            TuneError: If errors occur executing the experiment that originate from
                Tune.
        """
        raise NotImplementedError
