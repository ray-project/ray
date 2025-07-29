from typing import TYPE_CHECKING, Any, Dict, Optional

import sklearn.datasets
import sklearn.metrics
import xgboost as xgb
from sklearn.model_selection import train_test_split

import ray
from ray import tune
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.experiment import Trial
from ray.tune.integration.xgboost import TuneReportCheckpointCallback
from ray.tune.schedulers import ASHAScheduler, ResourceChangingScheduler

if TYPE_CHECKING:
    from ray.tune.execution.tune_controller import TuneController

CHECKPOINT_FILENAME = "booster-checkpoint.json"


def get_best_model_checkpoint(best_result: "ray.train.Result"):
    best_bst = TuneReportCheckpointCallback.get_model(
        best_result.checkpoint, filename=CHECKPOINT_FILENAME
    )

    accuracy = 1.0 - best_result.metrics["eval-logloss"]
    print(f"Best model parameters: {best_result.config}")
    print(f"Best model total accuracy: {accuracy:.4f}")
    return best_bst


# our train function needs to be able to checkpoint
# to work with ResourceChangingScheduler
def train_breast_cancer(config: dict):
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
    checkpoint = tune.get_checkpoint()
    if checkpoint:
        xgb_model = TuneReportCheckpointCallback.get_model(
            checkpoint, filename=CHECKPOINT_FILENAME
        )

    # Set `nthread` to the number of CPUs available to the trial,
    # which is assigned by the scheduler.
    config["nthread"] = int(tune.get_context().get_trial_resources().head_cpus)
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
                # checkpointing should happen every iteration
                # with dynamic resource allocation
                frequency=1,
                filename=CHECKPOINT_FILENAME,
            )
        ],
    )


def tune_xgboost():
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
        tune_controller: "TuneController",
        trial: Trial,
        result: Dict[str, Any],
        scheduler: "ResourceChangingScheduler",
    ) -> Optional[PlacementGroupFactory]:
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
            tune_controller: Trial runner for this Tune run.
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
        total_available_cpus = tune_controller._resource_updater.get_num_cpus()

        # Divide the free CPUs among all live trials
        cpu_to_use = max(
            min_cpu, total_available_cpus // len(tune_controller.get_live_trials())
        )

        # Assign new CPUs to the trial in a PlacementGroupFactory
        return PlacementGroupFactory([{"CPU": cpu_to_use, "GPU": 0}])

    # You can either define your own resources_allocation_function, or
    # use the default one - DistributeResources

    # from ray.tune.schedulers.resource_changing_scheduler import \
    #    DistributeResources

    scheduler = ResourceChangingScheduler(
        base_scheduler=base_scheduler,
        resources_allocation_function=example_resources_allocation_function,
        # resources_allocation_function=DistributeResources()  # default
    )

    tuner = tune.Tuner(
        tune.with_resources(
            train_breast_cancer, resources=PlacementGroupFactory([{"CPU": 1, "GPU": 0}])
        ),
        tune_config=tune.TuneConfig(
            metric="eval-logloss",
            mode="min",
            num_samples=1,
            scheduler=scheduler,
        ),
        param_space=search_space,
    )
    results = tuner.fit()

    return results.get_best_result()


if __name__ == "__main__":
    ray.init(num_cpus=8)

    best_result = tune_xgboost()
    best_bst = get_best_model_checkpoint(best_result)

    # You could now do further predictions with
    # best_bst.predict(...)
