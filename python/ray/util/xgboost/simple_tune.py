from sklearn import datasets
from sklearn.model_selection import train_test_split

from ray.util.xgboost import RayDMatrix, RayParams, train

# __train_begin__
num_cpus_per_actor = 1
num_actors = 1


def train_model(config):
    # Load dataset
    data, labels = datasets.load_breast_cancer(return_X_y=True)
    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(
        data, labels, test_size=0.25)

    train_set = RayDMatrix(train_x, train_y)
    test_set = RayDMatrix(test_x, test_y)

    evals_result = {}
    bst = train(
        params=config,
        dtrain=train_set,
        evals=[(test_set, "eval")],
        evals_result=evals_result,
        verbose_eval=False,
        ray_params=RayParams(
            num_actors=num_actors, cpus_per_actor=num_cpus_per_actor))
    bst.save_model("model.xgb")


# __train_end__


def main():
    # __tune_begin__
    from ray import tune

    # Set config
    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": tune.randint(1, 9)
    }
    # __tune_end__

    # __tune_run_begin__
    analysis = tune.run(
        train_model,
        config=config,
        metric="eval-error",
        mode="min",
        num_samples=4,
        resources_per_trial={
            "cpu": 1,
            "extra_cpu": num_actors * num_cpus_per_actor
        })

    # Load the best model checkpoint
    import xgboost as xgb
    import os

    # Load in the best performing model.
    best_bst = xgb.Booster()
    best_bst.load_model(os.path.join(analysis.best_logdir, "model.xgb"))

    accuracy = 1. - analysis.best_result["eval-error"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")
    # __tune_run_end__


if __name__ == "__main__":
    main()
