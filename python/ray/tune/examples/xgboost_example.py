from typing import Dict, List
import sklearn.datasets
import sklearn.metrics
import os
import numpy as np
from ray.tune.schedulers import ASHAScheduler
from sklearn.model_selection import train_test_split
import xgboost as xgb

from ray import tune
from ray.tune.integration.xgboost import (
    TuneReportCheckpointCallback,
    TuneReportCallback,
)


def train_breast_cancer(config: dict):
    # This is a simple training function to be passed into Tune

    # Load dataset
    data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)

    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(data, labels, test_size=0.25)
    # Build input matrices for XGBoost
    train_set = xgb.DMatrix(train_x, label=train_y)
    test_set = xgb.DMatrix(test_x, label=test_y)

    # Train the classifier, using the Tune callback
    xgb.train(
        config,
        train_set,
        evals=[(test_set, "test")],
        verbose_eval=False,
        callbacks=[TuneReportCheckpointCallback(filename="model.xgb")],
    )


def train_breast_cancer_cv(config: dict):
    # This is a simple training function to be passed into Tune
    # using xgboost's cross validation functionality

    # Load dataset
    data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)

    # For CV, we need to average over a list of results form folds
    def average_cv_folds(results_dict: Dict[str, List[float]]) -> Dict[str, float]:
        return {k: np.mean(v) for k, v in results_dict.items()}

    train_set = xgb.DMatrix(data, label=labels)

    # Run CV, using the Tune callback
    xgb.cv(
        config,
        train_set,
        verbose_eval=False,
        stratified=True,
        # Checkpointing is not supported for CV
        callbacks=[TuneReportCallback(results_postprocessing_fn=average_cv_folds)],
    )


def get_best_model_checkpoint(analysis):
    best_bst = xgb.Booster()
    best_bst.load_model(os.path.join(analysis.best_checkpoint, "model.xgb"))
    accuracy = 1.0 - analysis.best_result["test-error"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")
    return best_bst


def tune_xgboost(use_cv: bool = False):
    search_space = {
        # You can mix constants with search space objects.
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": tune.randint(1, 9),
        "min_child_weight": tune.choice([1, 2, 3]),
        "subsample": tune.uniform(0.5, 1.0),
        "eta": tune.loguniform(1e-4, 1e-1),
    }
    # This will enable aggressive early stopping of bad trials.
    scheduler = ASHAScheduler(
        max_t=10, grace_period=1, reduction_factor=2  # 10 training iterations
    )

    analysis = tune.run(
        train_breast_cancer if not use_cv else train_breast_cancer_cv,
        metric="test-logloss",
        mode="min",
        # You can add "gpu": 0.1 to allocate GPUs
        resources_per_trial={"cpu": 1},
        config=search_space,
        num_samples=10,
        scheduler=scheduler,
    )

    return analysis


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    parser.add_argument(
        "--use-cv", action="store_true", help="Use `xgb.cv` instead of `xgb.train`."
    )
    args, _ = parser.parse_known_args()

    if args.server_address:
        import ray

        ray.init(f"ray://{args.server_address}")

    analysis = tune_xgboost(args.use_cv)

    # Load the best model checkpoint.
    # Checkpointing is not supported when using `xgb.cv`
    if not args.use_cv:
        if args.server_address:
            # If connecting to a remote server with Ray Client, checkpoint loading
            # should be wrapped in a task so it will execute on the server.
            # We have to make sure it gets executed on the same node that
            # ``tune.run`` is called on.
            from ray.util.ml_utils.node import force_on_current_node

            remote_fn = force_on_current_node(ray.remote(get_best_model_checkpoint))
            best_bst = ray.get(remote_fn.remote(analysis))
        else:
            best_bst = get_best_model_checkpoint(analysis)

        # You could now do further predictions with
        # best_bst.predict(...)
