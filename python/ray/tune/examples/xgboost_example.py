from typing import Dict, List
import sklearn.datasets
import sklearn.metrics
import os
import numpy as np
from sklearn.model_selection import train_test_split
import xgboost as xgb

import ray
from ray import tune
from ray.tune.schedulers import ASHAScheduler
from ray.tune.integration.xgboost import TuneReportCheckpointCallback


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
        callbacks=[TuneReportCheckpointCallback(filename="model.xgb", frequency=1)],
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
        callbacks=[
            TuneReportCheckpointCallback(
                results_postprocessing_fn=average_cv_folds, frequency=0
            )
        ],
    )


def get_best_model_checkpoint(best_result: "ray.train.Result"):
    best_bst = xgb.Booster()
    with best_result.checkpoint.as_directory() as checkpoint_dir:
        best_bst.load_model(os.path.join(checkpoint_dir, "model.xgb"))
    accuracy = 1.0 - best_result.metrics["test-error"]
    print(f"Best model parameters: {best_result.config}")
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

    tuner = tune.Tuner(
        tune.with_resources(
            train_breast_cancer if not use_cv else train_breast_cancer_cv,
            # You can add "gpu": 0.1 to allocate GPUs
            resources={"cpu": 1},
        ),
        tune_config=tune.TuneConfig(
            metric="test-logloss",
            mode="min",
            num_samples=10,
            scheduler=scheduler,
        ),
        param_space=search_space,
    )
    results = tuner.fit()

    return results.get_best_result()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--use-cv", action="store_true", help="Use `xgb.cv` instead of `xgb.train`."
    )
    args, _ = parser.parse_known_args()

    best_result = tune_xgboost(args.use_cv)

    # Load the best model checkpoint.
    # Checkpointing is not supported when using `xgb.cv`
    if not args.use_cv:
        best_bst = get_best_model_checkpoint(best_result)

        # You could now do further predictions with
        # best_bst.predict(...)
