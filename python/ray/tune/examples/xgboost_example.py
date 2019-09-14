from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import xgboost as xgb
import numpy as np
import sklearn.datasets
import sklearn.metrics
from sklearn.model_selection import train_test_split

from ray import tune


def XGBCallback(env):
    tune.track.log(**dict(env.evaluation_result_list))


def train_breast_cancer(config):
    data, target = sklearn.datasets.load_breast_cancer(return_X_y=True)
    train_x, test_x, train_y, test_y = train_test_split(
        data, target, test_size=0.25)
    train_set = xgb.DMatrix(train_x, label=train_y)
    test_set = xgb.DMatrix(test_x, label=test_y)
    bst = xgb.train(
        config, train_set, evals=[(test_set, "eval")], callbacks=[XGBCallback])
    preds = bst.predict(test_set)
    pred_labels = np.rint(preds)
    tune.track.log(
        mean_accuracy=sklearn.metrics.accuracy_score(test_y, pred_labels),
        done=True)


if __name__ == "__main__":
    num_threads = 2
    config = {
        "verbosity": 0,
        "num_threads": num_threads,
        "objective": "binary:logistic",
        "booster": "gbtree",
        "eval_metric": ["auc", "ams@0", "logloss"],
        "max_depth": tune.randint(1, 9),
        "eta": tune.loguniform(1e-4, 1e-1),
        "gamma": tune.loguniform(1e-8, 1.0),
        "grow_policy": tune.choice(["depthwise", "lossguide"])
    }

    from ray.tune.schedulers import ASHAScheduler
    tune.run(
        train_breast_cancer,
        resources_per_trial={"cpu": num_threads},
        config=config,
        num_samples=2,
        scheduler=ASHAScheduler(metric="eval-logloss", mode="min"))
