import lightgbm as lgb
import sklearn.datasets
import sklearn.metrics
from sklearn.model_selection import train_test_split

from ray import tune
from ray.tune.integration.lightgbm import TuneReportCheckpointCallback
from ray.tune.schedulers import ASHAScheduler


def train_breast_cancer(config: dict):
    # This is a simple training function to be passed into Tune

    # Load dataset
    data, target = sklearn.datasets.load_breast_cancer(return_X_y=True)

    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(data, target, test_size=0.25)

    # Build input Datasets for LightGBM
    train_set = lgb.Dataset(train_x, label=train_y)
    test_set = lgb.Dataset(test_x, label=test_y)

    # Train the classifier, using the Tune callback
    lgb.train(
        config,
        train_set,
        valid_sets=[test_set],
        valid_names=["eval"],
        verbose_eval=False,
        callbacks=[
            TuneReportCheckpointCallback(
                {
                    "binary_error": "eval-binary_error",
                    "binary_logloss": "eval-binary_logloss",
                }
            )
        ],
    )


def train_breast_cancer_cv(config: dict):
    # This is a simple training function to be passed into Tune, using
    # lightgbm's cross validation functionality

    # Load dataset
    data, target = sklearn.datasets.load_breast_cancer(return_X_y=True)

    train_set = lgb.Dataset(data, label=target)

    # Run CV, using the Tune callback
    lgb.cv(
        config,
        train_set,
        verbose_eval=False,
        stratified=True,
        # Checkpointing is not supported for CV
        # LightGBM aggregates metrics over folds automatically
        # with the cv_agg key. Both mean and standard deviation
        # are provided.
        callbacks=[
            TuneReportCheckpointCallback(
                {
                    "binary_error": "cv_agg-binary_error-mean",
                    "binary_logloss": "cv_agg-binary_logloss-mean",
                    "binary_error_stdv": "cv_agg-binary_error-stdv",
                    "binary_logloss_stdv": "cv_agg-binary_logloss-stdv",
                },
                frequency=0,
            )
        ],
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--use-cv", action="store_true", help="Use `lgb.cv` instead of `lgb.train`."
    )
    args, _ = parser.parse_known_args()

    config = {
        "objective": "binary",
        "metric": ["binary_error", "binary_logloss"],
        "verbose": -1,
        "boosting_type": tune.grid_search(["gbdt", "dart"]),
        "num_leaves": tune.randint(10, 1000),
        "learning_rate": tune.loguniform(1e-8, 1e-1),
    }

    tuner = tune.Tuner(
        train_breast_cancer if not args.use_cv else train_breast_cancer_cv,
        tune_config=tune.TuneConfig(
            metric="binary_error",
            mode="min",
            num_samples=2,
            scheduler=ASHAScheduler(),
        ),
        param_space=config,
    )
    results = tuner.fit()

    print("Best hyperparameters found were: ", results.get_best_result().config)
