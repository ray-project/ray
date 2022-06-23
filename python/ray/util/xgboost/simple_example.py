from sklearn import datasets
from sklearn.model_selection import train_test_split

from ray.util.xgboost import RayDMatrix, RayParams, train


# __xgboost_begin__
def main():
    # Load dataset
    data, labels = datasets.load_breast_cancer(return_X_y=True)
    # Split into train and test set
    train_x, test_x, train_y, test_y = train_test_split(data, labels, test_size=0.25)

    train_set = RayDMatrix(train_x, train_y)
    test_set = RayDMatrix(test_x, test_y)

    # Set config
    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "max_depth": 3,
    }

    evals_result = {}

    # Train the classifier
    bst = train(
        config,
        train_set,
        evals=[(test_set, "eval")],
        evals_result=evals_result,
        ray_params=RayParams(max_actor_restarts=1, num_actors=1),
        verbose_eval=False,
    )

    bst.save_model("simple.xgb")
    print("Final validation error: {:.4f}".format(evals_result["eval"]["error"][-1]))


# __xgboost_end__

if __name__ == "__main__":
    main()
