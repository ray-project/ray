# flake8: noqa: E501
"""
XGBoost-Ray with Dask
======================

This notebook includes an example workflow using
`XGBoost-Ray <https://docs.ray.io/en/latest/xgboost-ray.html>`_ and
`Dask <https://docs.dask.org/en/latest/>`_ for distributed model training,
hyperparameter optimization, and prediction.
"""

###############################################################################
# Cluster Setup
# -------------
#
# First, we'll set up our Ray Cluster. The provided ``dask_xgboost.yaml``
# cluster config can be used to set up an AWS cluster with 64 CPUs.
#
# The following steps assume you are in a directory with both
# ``dask_xgboost.yaml`` and this file saved as ``dask_xgboost.ipynb``.
#
# **Step 1:** Bring up the Ray cluster.
#
# .. code-block:: bash
#
#     $ pip install ray boto3
#     $ ray up dask_xgboost.yaml
#
# **Step 2:** Move ``dask_xgboost.ipynb`` to the cluster and start Jupyter.
#
# .. code-block:: bash
#
#     $ ray rsync_up dask_xgboost.yaml "./dask_xgboost.ipynb" \
#       "~/dask_xgboost.ipynb"
#     $ ray exec dask_xgboost.yaml --port-forward=9999 "jupyter notebook \
#       --port=9999"
#
# You can then access this notebook at the URL that is output:
# ``http://localhost:9999/?token=<token>``

###############################################################################
# Python Setup
# ------------
#
# First, we'll import all the libraries we'll be using. This step also helps us
# verify that the environment is configured correctly. If any of the imports
# are missing, an exception will be raised.

import argparse
import time

import dask
import dask.dataframe as dd
from xgboost_ray import RayDMatrix, RayParams, train, predict

import ray
from ray import tune
from ray.util.dask import ray_dask_get

###############################################################################
#
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
    default=6,
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

###############################################################################
#  Override these arguments as needed:

address = args.address
smoke_test = args.smoke_test
num_actors = args.num_actors
cpus_per_actor = args.cpus_per_actor
num_actors_inference = args.num_actors_inference
cpus_per_actor_inference = args.cpus_per_actor_inference

###############################################################################
# Connecting to the Ray cluster
# -----------------------------
# Now, let's connect our Python script to this newly deployed Ray cluster!

if not ray.is_initialized():
    ray.init(address=address)

###############################################################################
# Data Preparation
# -----------------
# We will use the `HIGGS dataset from the UCI Machine Learning dataset
# repository <https://archive.ics.uci.edu/ml/datasets/HIGGS>`_. The HIGGS
# dataset consists of 11,000,000 samples and 28 attributes, which is large
# enough size to show the benefits of distributed computation.
#
# We set the Dask scheduler to ``ray_dask_get`` to use `Dask on Ray
# <https://docs.ray.io/en/latest/data/dask-on-ray.html>`_ backend.

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
dask.config.set(scheduler=ray_dask_get)

###############################################################################

load_data_start_time = time.time()

data = dd.read_csv(FILE_URL, names=colnames)
data = data[sorted(colnames)]
data = data.persist()

load_data_end_time = time.time()
load_data_duration = load_data_end_time - load_data_start_time
print(f"Dataset loaded in {load_data_duration} seconds.")

###############################################################################
# With the connection established, we can now create the Dask dataframe.
#
# We will split the data into a training set and a evaluation set using a 80-20
# proportion.

train_df, eval_df = data.random_split([0.8, 0.2])
train_df, eval_df = train_df.persist(), eval_df.persist()
print(train_df, eval_df)

###############################################################################
# Distributed Training
# --------------------
# The ``train_xgboost`` function contains all of the logic necessary for
# training using XGBoost-Ray.
#
# Distributed training can not only speed up the process, but also allow you
# to use datasets that are to large to fit in memory of a single node. With
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
        ray_params=ray_params,
    )

    train_end_time = time.time()
    train_duration = train_end_time - train_start_time
    print(f"Total time taken: {train_duration} seconds.")

    model_path = "model.xgb"
    bst.save_model(model_path)
    print("Final validation error: {:.4f}".format(evals_result["eval"]["error"][-1]))

    return bst, evals_result


###############################################################################
# We can now pass our Dask dataframes and run the function. We will use
# ``RayParams`` to specify that the number of actors and CPUs to train with.
#
# The dataset has to be downloaded onto the cluster, which may take a few
# minutes.

# standard XGBoost config for classification
config = {
    "tree_method": "approx",
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error"],
}

bst, evals_result = train_xgboost(
    config,
    train_df,
    eval_df,
    LABEL_COLUMN,
    RayParams(cpus_per_actor=cpus_per_actor, num_actors=num_actors),
)
print(f"Results: {evals_result}")

###############################################################################
# Hyperparameter optimization
# ---------------------------
# If we are not content with the results obtained with default XGBoost
# parameters, we can use `Ray Tune
# <https://docs.ray.io/en/latest/tune/index.html>`_ for cutting-edge
# distributed hyperparameter tuning. XGBoost-Ray automatically integrates
# with Ray Tune, meaning we can use the same training function as before.
#
# In this workflow, we will tune three hyperparameters - ``eta``, ``subsample``
# and ``max_depth``. We are using `Tune's samplers to define the search
# space <https://docs.ray.io/en/latest/tune/user-guide.html#search-space-grid-random>`_.
#
# The experiment configuration is done through ``tune.run``. We set the amount
# of resources each trial (hyperparameter combination) requires by using the
# ``get_tune_resources`` method of ``RayParams``. The ``num_samples`` argument
# controls how many trials will be ran in total. In the end, the best
# combination of hyperparameters evaluated during the experiment will be
# returned.
#
# By default, Tune will use simple random search. However, Tune also
# provides various `search algorithms
# <https://docs.ray.io/en/latest/tune/api_docs/suggestion.html>`_ and
# `schedulers <https://docs.ray.io/en/latest/tune/api_docs/schedulers.html>`_
# to further improve the optimization process.


def tune_xgboost(train_df, test_df, target_column):
    # Set XGBoost config.
    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": tune.randint(1, 9),
    }

    ray_params = RayParams(
        max_actor_restarts=1, cpus_per_actor=cpus_per_actor, num_actors=num_actors
    )

    tune_start_time = time.time()

    analysis = tune.run(
        tune.with_parameters(
            train_xgboost,
            train_df=train_df,
            test_df=test_df,
            target_column=target_column,
            ray_params=ray_params,
        ),
        # Use the `get_tune_resources` helper function to set the resources.
        resources_per_trial=ray_params.get_tune_resources(),
        config=config,
        num_samples=10,
        metric="eval-error",
        mode="min",
    )

    tune_end_time = time.time()
    tune_duration = tune_end_time - tune_start_time
    print(f"Total time taken: {tune_duration} seconds.")

    accuracy = 1.0 - analysis.best_result["eval-error"]
    print(f"Best model parameters: {analysis.best_config}")
    print(f"Best model total accuracy: {accuracy:.4f}")

    return analysis.best_config


###############################################################################
# Hyperparameter optimization may take some time to complete.

tune_xgboost(train_df, eval_df, LABEL_COLUMN)

###############################################################################
# Prediction
# ----------
# With the model trained, we can now predict on unseen data. For the
# purposes of this example, we will use the same dataset for prediction as
# for training.
#
# Since prediction is naively parallelizable, distributing it over multiple
# actors can measurably reduce the amount of time needed.

inference_df = RayDMatrix(data, ignore=[LABEL_COLUMN, "partition"])
results = predict(
    bst,
    inference_df,
    ray_params=RayParams(
        cpus_per_actor=cpus_per_actor_inference, num_actors=num_actors_inference
    ),
)

print(results)
