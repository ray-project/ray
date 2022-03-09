from ray.tune.result_grid import ResultGrid
from ray.util import PublicAPI


# The magic key that is used when instantiating Tuner during resume.
_TUNER_INTERNAL = "_tuner_internal"
_SELF = "self"


# TODO(xwjiang): Change to alpha
@PublicAPI(stability="beta")
class Tuner:
    """The external facing Tuner class as part of Ray MLC effort.

    Args:
        trainable: The trainable to be tuned.
        param_space: Search space to run tuning on.
            One thing to note is that both preprocessor and dataset can be tuned here.
        tune_config: Tune algo specific configs.
            Refer to ray.tune.tune_config.TuneConfig for more info.
        run_config: Run time configuration that is universal between
            Tune and Train.

    Returns:
        ``ResultGrid`` object.

    Usage pattern:
    .. code-block:: python

    # TODO(xwjiang): Make this runnable. Add imports.
    # Only do the following, if you want to run in ray client mode.
    # This means the tune driver code will be running on head node of your cluster.
    ray.init("ray://127.0.0.1:10001")

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
        """Restore Tuner after a previously failed run.

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
        """Runs the tune run based on a configured Tune object."""
        raise NotImplementedError
