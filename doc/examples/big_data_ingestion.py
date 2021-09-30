# flake8: noqa: E501
"""
Large-scale shuffle ingestion pipeline
===============================================

In this example, you will learn how to build, deploy and scale up a machine
learning shuffle ingestion pipeline using
`Ray Dataset <https://docs.ray.io/en/latest/data/dataset.html>`_ and
`Dataset Pipelines <https://docs.ray.io/en/latest/data/dataset-pipeline.html>`_.

In particular, we will show you:

* How to build a shuffle ingestion pipeline that loads, shuffles and feeds data
  into distributed trainers in few lines of code;
* How to scale the pipeline from ingesting 100MB data to
  500GB data.

.. image:: ../data/dataset-repeat-2.svg
    :align: center

"""

###############################################################################
# Python Setup
# ------------
#
# First, we'll import all the libraries we'll be using. This step also helps us
# verify that the environment is configured correctly. If any of the imports
# are missing, an exception will be raised.

import ray

from typing import List
from ray.data.dataset_pipeline import DatasetPipeline

#######################################################################
# Build shuffle ingestion pipeline
# ----------------------------------
#
# A typical machine learning ingestion pipeline consists of the following 4
# steps:
#
# 1. Load the training data from external storage;
# 2. Iterate the data for multiple epochs;
# 3. In each epoch, applying global shuffle to normalize the data;
# 4. In each epoch, split the shuffled data into shards, and feed shards to
#    distributed trainers;
#
# Let’s see how we implement such pipeline using Ray Dataset:


def create_shuffle_pipeline(training_data_dir: str, num_epochs: int,
                            num_shards: int) -> List[DatasetPipeline]:

    return ray.data.read_parquet(training_data_dir)\
        .repeat(num_epochs)\
        .random_shuffle()\
        .split(num_shards, equal=True)


############################################################################
# We’ve now defined a ``create_shuffle_pipeline`` function that creates an
# ingestion pipeline.
# It reads ``training_data_dir``, iterates for ``num_epochs`` times,
# where in each epoch it
# shuffles and splits the result into ``num_shards``.

###############################################################################
# Feed the pipeline into trainers
# -----------------------------------
# Let’s also implement a ``TrainingWorker`` which consumes the shuffled data
# from each shard.
#
# For simplicity, we will define a
# `Ray Actor <https://docs.ray.io/en/latest/actors.html>`_ that emulates
# training workers. Specifically,
#
# 1. It takes one shard of the shuffle pipeline for training;
# 2. It iterates the shard to get per epoch training dataset;
# 3. It then consumes the dataset by batches;


@ray.remote
class TrainingWorker:
    def __init__(self, rank: int, shard: DatasetPipeline):
        self.rank = rank
        self.shard = shard

    def train(self):
        for epoch, training_dataset in enumerate(self.shard):
            # Following code emulates epoch based SGD training.
            # TODO: replace the code for real training.
            for i, batch in enumerate(training_dataset.iter_batches()):
                print(f"Training... epoch: {epoch}, batch: {i}")


###########################################################################
# Let's run it
# -----------------------------
# Now let’s run the data pipeline end-to-end:
# We read parquet files from the example s3 bucket,
# create the shuffle pipeline,
# and use training workers to consume the pipeline.

NUM_WORKERS = 4
NUM_EPOCHS = 5

TRAINING_DATA_100MB = "s3://ingest-100MB-example/"
TRAINING_DATA_500GB = "s3://ingest-500GB-example/"

ray.init()

# TODO: use TRAINING_DATA_500GB when running on cluster.
splits = create_shuffle_pipeline(TRAINING_DATA_100MB, NUM_WORKERS, NUM_EPOCHS)
workers = [TrainingWorker(rank, shard) for rank, shard in enumerate(splits)]

# Let's run the e2e pipeline
ray.get([worker.train.remote() for worker in workers])

#################################################################################
# Scale the shuffle ingestion pipeline
# --------------------------------------------------------
#
# Scale the shuffle ingestion pipeline is simple. With Ray, we can linearly
# scale the pipeline from ingesting 100MB of data to 500GB of data simply by adding
# more machines. To achieve that, we will run the same data pipeline we
# just defined on a 74 nodes Ray cluster.
#
# First, we'll set up our Ray Cluster. The provided ``big_data_ingestion.yaml``
# cluster config can be used to set up an AWS cluster with 70 CPU nodes and
# 4 GPU nodes.
#
# **Step 1:** Bring up the Ray cluster.
#
# .. code-block:: bash
#
#     $ pip install ray boto3
#     $ ray up big_data_ingestion.yaml
#
# **Step 2:**  Run ``big_data_ingestion.py`` on the cluster.
#
# .. code-block:: bash
#
#     $ ray rsync_up big_data_ingestion.yaml "./big_data_ingestion.py" \
#       "~/big_data_ingestion.py"
#     $ ray exec big_data_ingestion.yaml "~/big_data_ingestion.py"
