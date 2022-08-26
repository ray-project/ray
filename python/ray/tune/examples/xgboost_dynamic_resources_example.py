from typing import Union, Dict, Any, Optional
import sklearn.datasets
import sklearn.metrics
import os
from sklearn.model_selection import train_test_split
import xgboost as xgb
from xgboost.core import Booster
import pickle

import ray
from ray import air, tune
from ray.tune.schedulers import ResourceChangingScheduler, ASHAScheduler
from ray.tune import Trainable
from ray.tune.resources import Resources
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment import Trial
from ray.tune.execution import trial_runner
from ray.tune.integration.xgboost import TuneReportCheckpointCallback

CHECKPOINT_FILENAME = "model.xgb"


def get_best_model_checkpoint(best_result: "ray.air.Result"):
    best_bst = xgb.Booster()

    with best_result.checkpoint.as_directory() as checkpoint_dir:
        to_load = os.path.join(checkpoint_dir, CHECKPOINT_FILENAME)

        if not os.path.exists(to_load):
            # Class trainable
            with open(os.path.join(checkpoint_dir, "checkpoint"), "rb") as f:
                _, _, raw_model = pickle.load(f)
            to_load = bytearray(raw_model)

        best_bst.load_model(to_load)

    accuracy = 1.0 - best_result.metrics["eval-logloss"]
    print(f"Best model parameters: {best_result.config}")
    print(f"Best model total accuracy: {accuracy:.4f}")
    return best_bst


# FUNCTION API EXAMPLE


# our train function needs to be able to checkpoint
# to work with ResourceChangingScheduler
def train_breast_cancer(config: dict, checkpoint_dir=None):
    # This is a simple training function to be passed into Tune
    # Load dataset
    data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(data, labels, test_size=0.25)
    # Build input matrices for XGBoost
    train_set = xgb.DMatrix(train_x, label=train_y)
    test_set = xgb.DMatrix(test_x, label=test_y)

    # Checkpointing needs to be set up in order for dynamic
    # resource allocation to work as intended
    xgb_model = None
    if checkpoint_dir:
        xgb_model = xgb.Booster()
        xgb_model.load_model(os.path.join(checkpoint_dir, CHECKPOINT_FILENAME))

    # we can obtain current trial resources through
    # `tune.get_trial_resources()`
    config["nthread"] = int(tune.get_trial_resources().head_cpus)
    print(f"nthreads: {config['nthread']} xgb_model: {xgb_model}")
    # Train the classifier, using the Tune callback
    xgb.train(
        config,
        train_set,
        evals=[(test_set, "eval")],
        verbose_eval=False,
        xgb_model=xgb_model,
        callbacks=[
            TuneReportCheckpointCallback(
                filename=CHECKPOINT_FILENAME,
                # checkpointing should happen every iteration
                # with dynamic resource allocation
                frequency=1,
            )
        ],
    )


# TRAINABLE (CLASS) API EXAMPLE
class BreastCancerTrainable(Trainable):
    def setup(self, config):
        self.config = config
        self.nthread = config.pop("nthread", 1)
        self.model: xgb.Booster = None
        # Load dataset
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25
        )
        # Build input matrices for XGBoost
        self.train_set = xgb.DMatrix(train_x, label=train_y)
        self.test_set = xgb.DMatrix(test_x, label=test_y)

    def step(self):
        # you can also obtain current trial resources:
        current_resources = self.trial_resources
        if isinstance(current_resources, PlacementGroupFactory):
            self.nthread = current_resources.head_cpus
        else:
            self.nthread = current_resources.cpu

        results = {}
        config = self.config.copy()
        config["nthread"] = int(self.nthread)
        self.model = xgb.train(
            config,
            self.train_set,
            evals=[(self.test_set, "eval")],
            verbose_eval=False,
            xgb_model=self.model,
            evals_result=results,
            num_boost_round=1,
        )
        print(config, results)
        return {"eval-logloss": results["eval"]["logloss"][-1], "nthread": self.nthread}

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "wb") as outputFile:
            pickle.dump((self.config, self.nthread, self.model.save_raw()), outputFile)
        return path

    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path, "rb") as inputFile:
            self.config, self.nthread, raw_model = pickle.load(inputFile)
        self.model = Booster()
        self.model.load_model(bytearray(raw_model))
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25
        )
        # Build input matrices for XGBoost
        self.train_set = xgb.DMatrix(train_x, label=train_y)
        self.test_set = xgb.DMatrix(test_x, label=test_y)


def tune_xgboost(use_class_trainable=True):
    search_space = {
        # You can mix constants with search space objects.
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 9,
        "learning_rate": 1,
        "min_child_weight": tune.grid_search([2, 3]),
        "subsample": tune.grid_search([0.8, 0.9]),
        "colsample_bynode": tune.grid_search([0.8, 0.9]),
        "random_state": 1,
        "num_parallel_tree": 2000,
    }
    # This will enable aggressive early stopping of bad trials.
    base_scheduler = ASHAScheduler(
        max_t=16, grace_period=1, reduction_factor=2  # 16 training iterations
    )

    def example_resources_allocation_function(
        trial_runner: "trial_runner.TrialRunner",
        trial: Trial,
        result: Dict[str, Any],
        scheduler: "ResourceChangingScheduler",
    ) -> Optional[Union[PlacementGroupFactory, Resources]]:
        """This is a basic example of a resource allocating function.

        The function naively balances available CPUs over live trials.

        This function returns a new ``PlacementGroupFactory`` with updated
        resource requirements, or None. If the returned
        ``PlacementGroupFactory`` is equal by value to the one the
        trial has currently, the scheduler will skip the update process
        internally (same with None).

        See :class:`DistributeResources` for a more complex,
        robust approach.

        Args:
            trial_runner: Trial runner for this Tune run.
                Can be used to obtain information about other trials.
            trial: The trial to allocate new resources to.
            result: The latest results of trial.
            scheduler: The scheduler calling the function.
        """

        # Get base trial resources as defined in
        # ``tune.with_resources``
        base_trial_resource = scheduler._base_trial_resources

        # Don't bother if this is just the first iteration
        if result["training_iteration"] < 1:
            return None

        # default values if resources_per_trial is unspecified
        if base_trial_resource is None:
            base_trial_resource = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])

        # Assume that the number of CPUs cannot go below what was
        # specified in ``Tuner.fit()``.
        min_cpu = base_trial_resource.required_resources.get("CPU", 0)

        # Get the number of CPUs available in total (not just free)
        total_available_cpus = (
            trial_runner.trial_executor._resource_updater.get_num_cpus()
        )

        # Divide the free CPUs among all live trials
        cpu_to_use = max(
            min_cpu, total_available_cpus // len(trial_runner.get_live_trials())
        )

        # Assign new CPUs to the trial in a PlacementGroupFactory
        return PlacementGroupFactory([{"CPU": cpu_to_use, "GPU": 0}])

    # You can either define your own resources_allocation_function, or
    # use the default one - DistributeResources

    # from ray.tune.schedulers.resource_changing_scheduler import \
    #    DistributeResources

    scheduler = ResourceChangingScheduler(
        base_scheduler=base_scheduler,
        resources_allocation_function=example_resources_allocation_function
        # resources_allocation_function=DistributeResources()  # default
    )

    if use_class_trainable:
        fn = BreastCancerTrainable
    else:
        fn = train_breast_cancer

    tuner = tune.Tuner(
        tune.with_resources(
            fn, resources=PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        ),
        tune_config=tune.TuneConfig(
            metric="eval-logloss",
            mode="min",
            num_samples=1,
            scheduler=scheduler,
        ),
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(
                checkpoint_at_end=use_class_trainable,
            )
        ),
        param_space=search_space,
    )
    results = tuner.fit()

    if use_class_trainable:
        assert results.get_dataframe()["nthread"].max() > 1

    return results.get_best_result()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--class-trainable",
        action="store_true",
        default=False,
        help="set to use the Trainable (class) API instead of functional one",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        default=False,
        help="set to run both functional and Trainable APIs",
    )
    args, _ = parser.parse_known_args()

    ray.init(num_cpus=8)

    if args.test:
        best_result = tune_xgboost(use_class_trainable=True)
        best_bst = get_best_model_checkpoint(best_result)

    best_result = tune_xgboost(use_class_trainable=args.class_trainable)

    best_bst = get_best_model_checkpoint(best_result)

    # You could now do further predictions with
    # best_bst.predict(...)
