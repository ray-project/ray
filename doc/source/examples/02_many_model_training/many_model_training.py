from ray import tune

# For reading from cloud storage paths.
from smart_open import smart_open
import pandas as pd


def trainable_func(config: dict):
    data = pd.read_csv(smart_open(config["file_path"], "r"))

    ## Train your model here.
    from sklearn.linear_model import LinearRegression

    lr = LinearRegression()
    lr.fit(data[["id4", "id5"]], data["v3"])

    # Return a single dict of the model and stats, etc.
    return {
        "coef": lr.coef_,
        "intercept": lr.intercept_,
        "customer_id": data["customer_id"][0],
    }


# Tune is designed for up to thousands of trials.
param_space = {
    "file_path": tune.grid_search(
        [f"s3://air-example-data/h2oai_1m_files/file_{i:07}.csv" for i in range(1000)]
    )
}

tuner = tune.Tuner(trainable_func, param_space=param_space)
print(tuner.fit())
