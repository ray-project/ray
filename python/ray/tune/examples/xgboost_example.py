import numpy as np
import sklearn.datasets
import sklearn.metrics
from ray.tune.schedulers import ASHAScheduler
from sklearn.model_selection import train_test_split
import xgboost as xgb

from ray import tune


def XGBCallback(env):
    # After every training iteration, report loss to Tune
    tune.report(**dict(env.evaluation_result_list))


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
    bst = xgb.train(
        config,
        train_set,
        evals=[(test_set, "eval")],
        verbose_eval=False,
        callbacks=[XGBCallback])
    # Predict labels for the test set
    preds = bst.predict(test_set)
    pred_labels = np.rint(preds)
    # Return prediction accuracy
    accuracy = sklearn.metrics.accuracy_score(test_y, pred_labels)
    tune.report(mean_accuracy=accuracy, done=True)


if __name__ == "__main__":
    config = {
        "objective": "binary:logistic",
        "max_depth": tune.randint(1, 9),
        "min_child_weight": tune.choice([1, 2, 3]),
        "subsample": tune.uniform(0.5, 1.0),
        "eta": tune.loguniform(1e-4, 1e-1),
        "eval_metric": ["auc", "ams@0", "logloss"]
    }
    # The ASHAScheduler stops bad performing configurations early
    scheduler = ASHAScheduler(
        metric="eval-logloss",  # The `eval` prefix is defined in xgb.train
        mode="min",  # Retain configurations with a low logloss
        max_t=11,  # 10 training iterations + 1 final evaluation
        grace_period=1,  # Number of minimum iterations for each trial
        reduction_factor=2)  # How aggressively to stop trials
    tune.run(
        train_breast_cancer,  # your training function
        resources_per_trial={"cpu": 1},  # You can add "gpu": 0.1 here
        config=config,
        num_samples=10,  # number of parameter configurations to try
        scheduler=scheduler)
