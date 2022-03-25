# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.6
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # XGBoost-Ray with Modin
#
# This notebook includes an example workflow using
# [XGBoost-Ray](https://docs.ray.io/en/latest/xgboost-ray.html) and
# [Modin](https://modin.readthedocs.io/en/latest/) for distributed model
# training and prediction.
#
# ## Cluster Setup
#
# First, we'll set up our Ray Cluster. The provided ``modin_xgboost.yaml``
# cluster config can be used to set up an AWS cluster with 64 CPUs.
#
# The following steps assume you are in a directory with both
# ``modin_xgboost.yaml`` and this file saved as ``modin_xgboost.ipynb``.
#
# **Step 1:** Bring up the Ray cluster.
#
# ```bash
# pip install ray boto3
# ray up modin_xgboost.yaml
# ```
#
# **Step 2:** Move ``modin_xgboost.ipynb`` to the cluster and start Jupyter.
#
# ```bash
# ray rsync_up modin_xgboost.yaml "./modin_xgboost.ipynb" \
#   "~/modin_xgboost.ipynb"
# ray exec modin_xgboost.yaml --port-forward=9999 "jupyter notebook \
#   --port=9999"
# ```
#
# You can then access this notebook at the URL that is output:
# ``http://localhost:9999/?token=<token>``
#
# ## Python Setup
#
# First, we'll import all the libraries we'll be using. This step also helps us
# verify that the environment is configured correctly. If any of the imports
# are missing, an exception will be raised.

# +
import argparse
import time

import modin.pandas as pd
from modin.experimental.sklearn.model_selection import train_test_split
from xgboost_ray import RayDMatrix, RayParams, train, predict

import ray

# -

# Next, let's parse some arguments. This will be used for executing the ``.py``
# file, but not for the ``.ipynb``. If you are using the interactive notebook,
# you can directly override the arguments manually.

parser = argparse.ArgumentParser()
parser.add_argument(
    "--address", type=str, default="auto", help="The address to use for Ray."
)
parser.add_argument(
    "--smoke-test",
    action="store_true",
    help="Read a smaller dataset for quick testing purposes.",
)
parser.add_argument(
    "--num-actors", type=int, default=4, help="Sets number of actors for training."
)
parser.add_argument(
    "--cpus-per-actor",
    type=int,
    default=8,
    help="The number of CPUs per actor for training.",
)
parser.add_argument(
    "--num-actors-inference",
    type=int,
    default=16,
    help="Sets number of actors for inference.",
)
parser.add_argument(
    "--cpus-per-actor-inference",
    type=int,
    default=2,
    help="The number of CPUs per actor for inference.",
)
# Ignore -f from ipykernel_launcher
args, _ = parser.parse_known_args()

#  Override these arguments as needed:

address = args.address
smoke_test = args.smoke_test
num_actors = args.num_actors
cpus_per_actor = args.cpus_per_actor
num_actors_inference = args.num_actors_inference
cpus_per_actor_inference = args.cpus_per_actor_inference

# ## Connecting to the Ray cluster
#
# Now, let's connect our Python script to this newly deployed Ray cluster!

if not ray.is_initialized():
    ray.init(address=address)

# ## Data Preparation
#
# We will use the [HIGGS dataset from the UCI Machine Learning dataset
# repository](https://archive.ics.uci.edu/ml/datasets/HIGGS). The HIGGS
# dataset consists of 11,000,000 samples and 28 attributes, which is large
# enough size to show the benefits of distributed computation.

# +
LABEL_COLUMN = "label"
if smoke_test:
    # Test dataset with only 10,000 records.
    FILE_URL = "https://ray-ci-higgs.s3.us-west-2.amazonaws.com/simpleHIGGS" ".csv"
else:
    # Full dataset. This may take a couple of minutes to load.
    FILE_URL = (
        "https://archive.ics.uci.edu/ml/machine-learning-databases"
        "/00280/HIGGS.csv.gz"
    )

colnames = [LABEL_COLUMN] + ["feature-%02d" % i for i in range(1, 29)]

# +
load_data_start_time = time.time()

df = pd.read_csv(FILE_URL, names=colnames)

load_data_end_time = time.time()
load_data_duration = load_data_end_time - load_data_start_time
print(f"Dataset loaded in {load_data_duration} seconds.")
# -

# Split data into training and validation.

df_train, df_validation = train_test_split(df)
print(df_train, df_validation)


# ## Distributed Training
#
# The ``train_xgboost`` function contains all the logic necessary for
# training using XGBoost-Ray.
#
# Distributed training can not only speed up the process, but also allow you
# to use datasets that are too large to fit in memory of a single node. With
# distributed training, the dataset is sharded across different actors
# running on separate nodes. Those actors communicate with each other to
# create the final model.
#
# First, the dataframes are wrapped in ``RayDMatrix`` objects, which handle
# data sharding across the cluster. Then, the ``train`` function is called.
# The evaluation scores will be saved to ``evals_result`` dictionary. The
# function returns a tuple of the trained model (booster) and the evaluation
# scores.
#
# The ``ray_params`` variable expects a ``RayParams`` object that contains
# Ray-specific settings, such as the number of workers.


def train_xgboost(config, train_df, test_df, target_column, ray_params):
    train_set = RayDMatrix(train_df, target_column)
    test_set = RayDMatrix(test_df, target_column)

    evals_result = {}

    train_start_time = time.time()

    # Train the classifier
    bst = train(
        params=config,
        dtrain=train_set,
        evals=[(test_set, "eval")],
        evals_result=evals_result,
        verbose_eval=False,
        num_boost_round=100,
        ray_params=ray_params,
    )

    train_end_time = time.time()
    train_duration = train_end_time - train_start_time
    print(f"Total time taken: {train_duration} seconds.")

    model_path = "model.xgb"
    bst.save_model(model_path)
    print("Final validation error: {:.4f}".format(evals_result["eval"]["error"][-1]))

    return bst, evals_result


# We can now pass our Modin dataframes and run the function. We will use
# ``RayParams`` to specify that the number of actors and CPUs to train with.

# +
# standard XGBoost config for classification
config = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}

bst, evals_result = train_xgboost(
    config,
    df_train,
    df_validation,
    LABEL_COLUMN,
    RayParams(cpus_per_actor=cpus_per_actor, num_actors=num_actors),
)
print(f"Results: {evals_result}")
# -

# ## Prediction
#
# With the model trained, we can now predict on unseen data. For the
# purposes of this example, we will use the same dataset for prediction as
# for training.
#
# Since prediction is naively parallelizable, distributing it over multiple
# actors can measurably reduce the amount of time needed.

# +
inference_df = RayDMatrix(df, ignore=[LABEL_COLUMN, "partition"])
results = predict(
    bst,
    inference_df,
    ray_params=RayParams(
        cpus_per_actor=cpus_per_actor_inference, num_actors=num_actors_inference
    ),
)

print(results)
