from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import lightgbm as lgb
import numpy as np
import sklearn.datasets
import sklearn.metrics
from sklearn.model_selection import train_test_split

from ray import tune


def LightGBMCallback(env):
    """Assumes that `valid_0` is the target validation score."""
    _, metric, score, _ = env.evaluation_result_list[0]
    tune.track.log(**{metric: score})


def train_breast_cancer(config):
    data, target = sklearn.datasets.load_breast_cancer(return_X_y=True)
    train_x, test_x, train_y, test_y = train_test_split(
        data, target, test_size=0.25)
    train_set = lgb.Dataset(train_x, label=train_y)
    test_set = lgb.Dataset(test_x, label=test_y)
    gbm = lgb.train(
        config,
        train_set,
        valid_sets=[test_set],
        verbose_eval=False,
        callbacks=[LightGBMCallback])
    preds = gbm.predict(test_x)
    pred_labels = np.rint(preds)
    tune.track.log(
        mean_accuracy=sklearn.metrics.accuracy_score(test_y, pred_labels),
        done=True)


if __name__ == "__main__":
    config = {
        "objective": "binary",
        "metric": "binary_error",
        "verbose": -1,
        "boosting_type": tune.grid_search(["gbdt", "dart"]),
        "num_leaves": tune.randint(10, 1000),
        "learning_rate": tune.loguniform(1e-8, 1e-1)
    }
    from ray.tune.schedulers import ASHAScheduler
    tune.run(
        train_breast_cancer,
        config=config,
        num_samples=2,
        scheduler=ASHAScheduler(metric="binary_error", mode="min"))
