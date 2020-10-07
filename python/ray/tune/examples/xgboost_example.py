import sklearn.datasets
import sklearn.metrics
from ray.tune.schedulers import ASHAScheduler
from sklearn.model_selection import train_test_split
import xgboost as xgb

from ray import tune
from ray.tune.integration.xgboost import TuneReportCheckpointCallback


def train_breast_cancer(config):
    # Load dataset
    data, labels = sklearn.datasets.load_breast_cancer(return_X_y=True)
    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(
        data, labels, test_size=0.25)
    # Build input matrices for XGBoost
    train_set = xgb.DMatrix(train_x, label=train_y)
    test_set = xgb.DMatrix(test_x, label=test_y)
    # Train the classifier
    xgb.train(
        config,
        train_set,
        evals=[(test_set, "eval")],
        verbose_eval=False,
        callbacks=[TuneReportCheckpointCallback(filename="model.xgb")])


if __name__ == "__main__":
    config = {
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": tune.randint(1, 9),
        "min_child_weight": tune.choice([1, 2, 3]),
        "subsample": tune.uniform(0.5, 1.0),
        "eta": tune.loguniform(1e-4, 1e-1)
    }
    scheduler = ASHAScheduler(
        max_t=10,  # 10 training iterations
        grace_period=1,
        reduction_factor=2)

    analysis = tune.run(
        train_breast_cancer,
        metric="eval-logloss",
        mode="min",
        resources_per_trial={"cpu": 1},  # You can add "gpu": 0.1 here
        config=config,
        num_samples=10,
        scheduler=scheduler)

    # Load the best model checkpoint
    import os
    best_bst = xgb.Booster()
    best_bst.load_model(os.path.join(analysis.best_checkpoint, "model.xgb"))
    accuracy = 1. - analysis.best_result["eval-error"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")

    # You could now do further predictions with
    # best_bst.predict(...)
