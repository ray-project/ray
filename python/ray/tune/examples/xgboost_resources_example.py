from typing import Union
import sklearn.datasets
import sklearn.metrics
import os
import math
from sklearn.model_selection import train_test_split
import xgboost as xgb
from xgboost.core import Booster
import pickle

import ray
from ray import tune
from ray.tune.integration.xgboost import TuneReportCheckpointCallback
from ray.tune.schedulers import ResourceChangingScheduler, ASHAScheduler
from ray.tune import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.tune.suggest.basic_variant import BasicVariantGenerator
from ray.tune.trial import Trial
from ray.tune import trial_runner


def train_breast_cancer(config: dict):
    # This is a simple training function to be passed into Tune
    # Load dataset
    data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(
        data, labels, test_size=0.25)
    # Build input matrices for XGBoost
    train_set = xgb.DMatrix(train_x, label=train_y)
    test_set = xgb.DMatrix(test_x, label=test_y)
    # Train the classifier, using the Tune callback
    xgb.train(
        config,
        train_set,
        evals=[(test_set, "eval")],
        verbose_eval=False,
        callbacks=[TuneReportCheckpointCallback(filename="model.xgb")])


class BreastCancerTrainable(Trainable):
    def setup(self, config):
        self.config = config
        self.nthread = config.pop("nthread", 1)
        self.model: xgb.Booster = None
        # This is a simple training function to be passed into Tune
        # Load dataset
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25)
        # Build input matrices for XGBoost
        self.train_set = xgb.DMatrix(train_x, label=train_y)
        self.test_set = xgb.DMatrix(test_x, label=test_y)

    def step(self):
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
            num_boost_round=1)
        print(config, results)
        return {
            "eval-logloss": results["eval"]["logloss"][-1],
            # "eval-error": results["eval"]["error"][0],
            "nthread": self.nthread
        }

    def save_checkpoint(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "wb") as outputFile:
            pickle.dump((self.config, self.nthread, self.model.save_raw()),
                        outputFile)
        return path

    def load_checkpoint(self, checkpoint_path):
        with open(checkpoint_path, "rb") as inputFile:
            self.config, self.nthread, raw_model = pickle.load(inputFile)
        self.model = Booster()
        self.model.load_model(bytearray(raw_model))
        data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
        # Split into train and test set
        train_x, test_x, train_y, test_y = train_test_split(
            data, labels, test_size=0.25)
        # Build input matrices for XGBoost
        self.train_set = xgb.DMatrix(train_x, label=train_y)
        self.test_set = xgb.DMatrix(test_x, label=test_y)

    def update_resources(self, new_resource):
        self.nthread = new_resource.cpu


def get_best_model_checkpoint(analysis):
    best_bst = xgb.Booster()
    with open(analysis.best_checkpoint, "rb") as inputFile:
        _, _, raw_model = pickle.load(inputFile)
    best_bst.load_model(bytearray(raw_model))
    accuracy = 1. - analysis.best_result["eval-logloss"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")
    return best_bst


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
        max_t=16,  # 10 training iterations
        grace_period=1,
        reduction_factor=2)

    def resource_allocation_function(
            trial_runner: "trial_runner.TrialRunner", trial: Trial,
            result: dict,
            base_trial_resource) -> Union[None, dict, PlacementGroupFactory]:
        if result["training_iteration"] < 1:
            return None

        min_cpu = base_trial_resource.required_resources["CPU"]

        # only start dynamic resource allocation after a sufficient
        # number of trials has been completed

        # todo make this use placement group factories
        total_available_resources = (
            trial_runner.trial_executor._avail_resources.cpu)
        upper_resource_limit = math.ceil(total_available_resources / len(
            trial_runner.get_live_trials()) / min_cpu)
        if upper_resource_limit <= min_cpu:
            return None

        used_resources = sum([
            trial.resources.cpu for t in trial_runner.get_live_trials()
            if t is not trial
        ])
        free_resources = total_available_resources - used_resources
        print(f"Trial {trial} ({trial.resources.cpu}) free resources",
              f"{free_resources} upper limit {upper_resource_limit}")
        new_cpu = min(upper_resource_limit,
                      max(min_cpu + free_resources, min_cpu))
        if new_cpu == trial.resources.cpu:
            return None
        return {"cpu": new_cpu, "gpu": 0}

    scheduler = ResourceChangingScheduler(base_scheduler,
                                          resource_allocation_function)

    search = BasicVariantGenerator(
        # max_concurrent=4
    )

    analysis = tune.run(
        BreastCancerTrainable,
        metric="eval-logloss",
        mode="min",
        # You can add "gpu": 0.1 to allocate GPUs
        resources_per_trial={"cpu": 1},
        config=search_space,
        search_alg=search,
        num_samples=1,
        checkpoint_at_end=True,
        scheduler=scheduler)

    assert analysis.results_df["training_iteration"].max() == 16

    return analysis


if __name__ == "__main__":
    # os.environ["TUNE_PLACEMENT_GROUP_AUTO_DISABLED"] = "1"
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using "
        "Ray Client.")
    args, _ = parser.parse_known_args()

    if args.server_address:
        ray.util.connect(args.server_address)
    else:
        ray.init(num_cpus=8)

    analysis = tune_xgboost()

    # Load the best model checkpoint.
    if args.server_address:
        # If connecting to a remote server with Ray Client, checkpoint loading
        # should be wrapped in a task so it will execute on the server.
        # We have to make sure it gets executed on the same node that
        # ``tune.run`` is called on.
        from ray.tune.utils import force_on_current_node
        remote_fn = force_on_current_node(
            ray.remote(get_best_model_checkpoint))
        best_bst = ray.get(remote_fn.remote(analysis))
    else:
        best_bst = get_best_model_checkpoint(analysis)

    # You could now do further predictions with
    # best_bst.predict(...)
