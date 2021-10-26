# flake8: noqa: E501
"""
Example: Large-scale ML Ingest
=================================================

In this example, you will learn how to build, deploy and scale up a machine
learning shuffle ingestion pipeline using
`Ray Dataset <https://docs.ray.io/en/latest/data/dataset.html>`_ and
`Dataset Pipelines <https://docs.ray.io/en/latest/data/dataset-pipeline.html>`_.

In particular, we will show you:

* How to build a shuffle ingestion pipeline that loads, shuffles and feeds data
  into distributed trainers in a few lines of code;
* How to scale the pipeline from ingesting 100MiB data to
  500GiB data.

.. image:: ../../data/dataset-repeat-2.svg
    :align: center

"""

###############################################################################
# Python Setup
# ------------
#
# First, we'll import all of the libraries we'll be using. This step also helps us
# verify that the environment is configured correctly. If any of the imports
# are missing, an exception will be raised.

import argparse
from typing import Dict

import torch
import torch.nn as nn
from torch.nn.parallel import DistributedDataParallel

import ray
import ray.train as train
from ray.data import Dataset
from ray.data.dataset_pipeline import DatasetPipeline
from ray.data.tests.util import LinearCombinationDatasource
from ray.train import Trainer
from ray.train.callbacks import JsonLoggerCallback, TBXLoggerCallback


#######################################################################
# Build shuffle ingestion pipeline
# ----------------------------------
#
# A typical machine learning ingestion pipeline consists of the following 4
# steps:
#
# 1. Load the training data from external storage;
# 2. Iterate over the data for multiple epochs;
# 3. In each epoch, applying global shuffle to decorrelate the data;
# 4. In each epoch, split the shuffled data into shards, and feed shards to
#    distributed trainers;
#
# Let’s see how we implement such pipeline using Ray Dataset:

def create_dataset(a=10: float, b=3: float, size_bytes: int) -> Dataset:
    return ray.data.read_datasource(
            LinearCombinationDatasource(), n=size_bytes // 16,
            a=a, b=b)


def create_shuffle_pipeline(dataset: Dataset, split=0.8) -> Dict[str, DatasetPipeline]:
    split_index = int(dataset.count() * split)
    train_dataset, validation_dataset = \
        dataset.random_shuffle().split_at_indices([split_index])

    train_dataset_pipeline = \
        train_dataset.repeat().random_shuffle_each_window()
    validation_dataset_pipeline = validation_dataset.repeat()

    datasets = {
        "train": train_dataset_pipeline,
        "validation": validation_dataset_pipeline
    }
    return datasets

############################################################################
# We’ve now defined a ``create_shuffle_pipeline`` function that creates an
# ingestion pipeline.
# It reads ``training_data_dir``, iterates for ``num_epochs`` times,
# where in each epoch it
# shuffles and splits the training data into ``num_shards``.

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
# 2. It iterates over the shard to get a training dataset per epoch;
# 3. It then consumes the dataset by batches;

def train_epoch(iterable_dataset, model, loss_fn, optimizer, device):
    model.train()
    for X, y in iterable_dataset:
        X = X.to(device)
        y = y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(iterable_dataset, model, loss_fn, device):
    num_batches = 0
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in iterable_dataset:
            X = X.to(device)
            y = y.to(device)
            num_batches += 1
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"loss": loss}
    return result


def train_func(config):
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset_pipeline_shard = train.get_dataset_shard("train")
    validation_dataset_pipeline_shard = train.get_dataset_shard("validation")

    device = torch.device(f"cuda:{train.local_rank()}"
                          if torch.cuda.is_available() else "cpu")
    if torch.cuda.is_available():
        torch.cuda.set_device(device)

    model = nn.Linear(1, hidden_size)
    model = model.to(device)
    model = DistributedDataParallel(
        model,
        device_ids=[train.local_rank()] if torch.cuda.is_available() else None)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []

    train_dataset_iterator = train_dataset_pipeline_shard.iter_datasets()
    validation_dataset_iterator = \
        validation_dataset_pipeline_shard.iter_datasets()

    for _ in range(epochs):
        train_dataset = next(train_dataset_iterator)
        validation_dataset = next(validation_dataset_iterator)

        train_torch_dataset = train_dataset.to_torch(
            label_column="y",
            feature_columns=["x"],
            label_column_dtype=torch.float,
            feature_column_dtypes=[torch.float],
            batch_size=batch_size,
        )
        validation_torch_dataset = validation_dataset.to_torch(
            label_column="y",
            feature_columns=["x"],
            label_column_dtype=torch.float,
            feature_column_dtypes=[torch.float],
            batch_size=batch_size)

        train_epoch(train_torch_dataset, model, loss_fn, optimizer, device)
        result = validate_epoch(validation_torch_dataset, model, loss_fn,
                                device)
        train.report(**result)
        results.append(result)

    return results

###########################################################################
# Let's run it
# -----------------------------
#
# Now let’s run the data pipeline end-to-end:
#
# First, let's parse some arguments.

parser = argparse.ArgumentParser()
parser.add_argument(
    "--large-scale-test",
    action="store_true",
    help="Run large scale test (500GiB of data).")

args, _ = parser.parse_known_args()

###############################################################################
#
# After that, let's generate 100MiB of Parquet files,
# create the shuffle pipeline by reading those generated Parquet files,
# and use training workers to consume the pipeline.

if not args.large_scale_test:
    NUM_TRAINING_WORKERS = 4
    SIZE_100MiB = 100 * 1024 * 1024

    start = time.time()
    datasets = create_dataset()

    trainer = Trainer("torch", num_workers=NUM_TRAINING_WORKERS, use_gpu=False)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}
    trainer.start()
    results = trainer.run(
        train_func,
        config,
        dataset=datasets,
        callbacks=[JsonLoggerCallback(),
                   TBXLoggerCallback()])
    trainer.shutdown()
    print(f"total ingestion time: {int(time.time() - start)}s")
    print(results)

    # -> Write Progress: 100%|████████████████████| 201/201 [00:00<00:00, 228.67it/s]
    # -> Stage 0:   0%|          | 0/5 [00:00<?, ?it/s]
    # -> Stage 0:  40%|████      | 2/5 [00:11<00:17,  5.75s/it]
    # -> Stage 0:  60%|██████    | 3/5 [00:23<00:16,  8.15s/it]
    # -> ...
    # -> (TrainingWorker pid=1651600) Training... worker: 2, epoch: 0
    # -> Stage 0:  80%|████████  | 4/5 [00:35<00:09,  9.59s/it]
    # -> ...
    # -> (TrainingWorker pid=1651599) Training... worker: 0, epoch: 1
    # -> Stage 0: 100%|██████████| 5/5 [00:46<00:00, 10.34s/it]
    # -> ...
    # -> (TrainingWorker pid=1651387) Training... worker: 3, epoch: 4
    # -> total ingestion time: 61s


#################################################################################
# Scale the shuffle ingestion pipeline
# --------------------------------------------------------
#
# Scaling the shuffle ingestion pipeline is simple. With Ray, we can linearly
# scale the pipeline from ingesting 100MiB of data to 500GiB of data by adding
# more machines.
#
# To ingest 500GiB of data, we'll set up a Ray Cluster.
# The provided :download:`big_data_ingestion.yaml <../big_data_ingestion.yaml>`
# cluster config can be used to set up an AWS cluster with 70 CPU nodes and
# 16 GPU nodes. Using following command to bring up the Ray cluster.
#
# .. code-block:: bash
#
#     $ pip install ray boto3
#     $ ray up big_data_ingestion.yaml
#
# After the cluster is started, let's implement our large scale ingestion test:
#
# First, since we are runing on a cluster, let's create the pipeline from
# RandomIntRowDatasource directly. In this way we don't need to set up S3 for storing
# generated data.

if args.large_scale_test:
    NUM_TRAINING_WORKERS = 16
    NUM_EPOCHS = 5
    NUM_COLUMNS = 10
    GiB = 1024 * 1024 * 1024
    SIZE_500GiB = 500 * GiB
    TOTAL_NUM_NODES = 70 + 16 + 1

    # use the AWS cluster we just set up.
    ray.init(address="auto")

    # waiting for cluster nodes to come up.
    while len(ray.nodes()) < TOTAL_NUM_NODES:
        print(
            f"waiting for nodes to start up: {len(ray.nodes())}/{TOTAL_NUM_NODES}"
        )
    time.sleep(5)
    start = time.time()
    datasets = create_dataset(SIZE_500GiB)

    trainer = Trainer("torch", num_workers=NUM_TRAINING_WORKERS, use_gpu=True)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 32, "epochs": 3}
    trainer.start()
    results = trainer.run(
        train_func,
        config,
        dataset=datasets,
        callbacks=[JsonLoggerCallback(),
                   TBXLoggerCallback()])
    trainer.shutdown()
    print(results)
    print(f"total ingestion time: {int(time.time() - start)}s")
    throughput = SIZE_500GiB * NUM_EPOCHS / (time.time() - start) / GiB
    print("throughput: {0:0.2f}GiB/s".format(throughput))

#################################################################################
#
# Finally, let's run our pipeline on the cluster we just started:
#
# .. code-block:: bash
#
#     $ ray submit ./big_data_ingestion.yaml ./big_data_ingestion.py --large-scale-test
#     # -> Connecting to existing Ray cluster at address: 172.31.47.38:6379
#     # -> waiting for nodes to start up: 1/87
#     # -> ...
#     # -> waiting for nodes to start up: 87/87
#     # -> Stage 0:   0%|          | 0/5 [00:00<?, ?it/s]
#     # -> Stage 0:  20%|██        | 1/5 [00:00<00:02,  1.77it/s]
#     # -> Stage 0:  40%|████      | 2/5 [00:38<00:35, 11.67s/it]
#     # -> Stage 0:  60%|██████    | 3/5 [01:13<00:37, 18.83s/it]
#     # -> ...
#     # -> (TrainingWorker pid=5084, ip=172.31.35.245) Training... worker: 12, epoch: 0
#     # -> Stage 0:  80%|████████  | 4/5 [03:15<00:49, 49.63s/it]
#     # -> ...
#     # -> (TrainingWorker pid=5076, ip=172.31.40.190) Training... worker: 9, epoch: 1
#     # -> Stage 0: 100%|██████████| 5/5 [05:02<00:00, 67.01s/it]
#     # -> ...
#     # -> (TrainingWorker pid=5074, ip=172.31.40.190) Training... worker: 0, epoch: 4
#     # -> total ingestion time: 291s
#     # -> throughput: 8.56GiB/s