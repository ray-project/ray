# For reading from cloud storage paths.
from smart_open import smart_open
import pandas as pd

import ray
from ray import air, tune
from ray.air import session

ray.init()


def trainable_func(config: dict):
    data = pd.read_csv(smart_open(config["file_path"], "r"))

    # Train your model here.
    from sklearn.linear_model import LinearRegression

    X, y = data[["id4", "id5"]], data["v3"]
    reg = LinearRegression().fit(X, y)
    score = reg.score(X, y)

    # Report a single dict of the model and stats, etc.
    session.report(
        metrics={
            "coef": reg.coef_,
            "intercept": reg.intercept_,
            "customer_id": data["customer_id"][0],
            "score": score,
        }
    )


# Tune is designed for up to thousands of trials.
num_nodes = len(ray.nodes())
num_trials = 250 * num_nodes

param_space = {
    "file_path": tune.grid_search(
        [
            f"s3://air-example-data/h2oai_1m_files/file_{i:07}.csv"
            for i in range(num_trials)
        ]
    )
}

tuner = tune.Tuner(
    trainable_func,
    param_space=param_space,
    run_config=air.RunConfig(
        name="many_model_training_template",
        local_dir="/mnt/cluster_storage/",
        sync_config=tune.SyncConfig(syncer=None),
    ),
)
result_grid = tuner.fit()

print(result_grid.get_dataframe())
