"""
Big Data Training
=================

This notebook includes an example workflow for training a PyTorch deep learning model on tabular data using Ray Train, and handling the data ingest with Ray Datasets.
"""

###############################################################################
# Imports
# -------
#
# Let's import a few libraries we'll need.

import argparse
import collections
import os
import sys
import time
from typing import Tuple

import boto3
import mlflow
import pandas as pd
import ray
import torch
import torch.nn as nn
import torch.optim as optim
from ray import train
from ray.data.aggregate import Mean, Std
from ray.train import Trainer
from ray.train.callbacks.logging import MLflowLoggerCallback
from ray.train.callbacks import TBXLoggerCallback
from torch.nn.parallel import DistributedDataParallel


###############################################################################
# MLflow Setup
# ------------
#
# First, let's set up some boilerplate code that will allow us to track and save our
# ML model to MLflow.

def register_mlflow_model(model):
    mlflow.pytorch.log_model(
        model, artifact_path="models", registered_model_name="torch_model")

    # Get the latest model from mlflow model registry.
    client = mlflow.tracking.MlflowClient()
    registered_model_name = "torch_model"
    # Get the info for the latest model.
    # By default, registered models are in stage "None".
    latest_model_info = client.get_latest_versions(
        registered_model_name, stages=["None"])[0]
    latest_version = latest_model_info.version

    def load_model_func():
        model_uri = f"models:/torch_model/{latest_version}"
        return mlflow.pytorch.load_model(model_uri)

    return load_model_func

###############################################################################
# Generating a synthetic dataset
# ------------------------------
#
# With this example, we wanted to make it easy to generate a dataset of arbitrary size
# to allow you to increase it to your desired scale, depending on what you're trying to
# prove out as you prototype with Ray.
#
# In particular, you can specify:
#
# * ``num_examples```: number of rows of tabular parquet data to generate
# * ``num_features```: number of columns of the data
# * ``parquet_file_chunk_size_rows``: how many rows per file
#
# The data is generated with ``sklearn.datasets.make_classification``.
#
# **NOTE**: To make things more interesting and more like "real" tabular data, we're going to introduce column names, categorical columns, and turn a few randomly selected cells into NULL values. Not all data is clean and purely numeric!

def make_and_upload_dataset(
        dir_path, num_examples=2_000_000, num_features=20,
        parquet_file_chunk_size_rows=50_000, upload_to_s3=False):

    import random
    import os

    import pandas as pd
    import sklearn.datasets

    num_files = num_examples // parquet_file_chunk_size_rows

    def create_data_chunk(n, d, seed, include_label=False):
        X, y = sklearn.datasets.make_classification(
            n_samples=n,
            n_features=d,
            n_informative=10,
            n_redundant=2,
            n_repeated=0,
            n_classes=2,
            n_clusters_per_class=3,
            weights=None,
            flip_y=0.03,
            class_sep=0.8,
            hypercube=True,
            shift=0.0,
            scale=1.0,
            shuffle=False,
            random_state=seed)

        # turn into dataframe with column names
        col_names = ["feature_%0d" % i for i in range(1, d + 1, 1)]
        df = pd.DataFrame(X)
        df.columns = col_names

        # add some bogus categorical data columns
        options = ["apple", "banana", "orange"]
        df["fruit"] = df.feature_1.map(
            lambda x: random.choice(options)
        )  # bogus, but nice to test categoricals

        # add some nullable columns
        options = [None, 1, 2]
        df["nullable_feature"] = df.feature_1.map(
            lambda x: random.choice(options)
        )  # bogus, but nice to test categoricals

        # add label column
        if include_label:
            df["label"] = y
        return df

    # create data files
    print("Creating synthetic dataset...")
    data_path = os.path.join(dir_path, "data")
    os.makedirs(data_path, exist_ok=True)
    for i in range(num_files):
        path = os.path.join(data_path, f"data_{i:05d}.parquet.snappy")
        if not os.path.exists(path):
            tmp_df = create_data_chunk(
                n=parquet_file_chunk_size_rows,
                d=num_features,
                seed=i,
                include_label=True)
            tmp_df.to_parquet(path, compression="snappy", index=False)
        print(f"Wrote {path} to disk...")
        # todo: at large enough scale we might want to upload the rest after
        #  first N files rather than write to disk
        # to simulate a user with local copy of subset of data

    print("Creating synthetic inference dataset...")
    inference_path = os.path.join(dir_path, "inference")
    os.makedirs(inference_path, exist_ok=True)
    for i in range(num_files):
        path = os.path.join(inference_path, f"data_{i:05d}.parquet.snappy")
        if not os.path.exists(path):
            tmp_df = create_data_chunk(
                n=parquet_file_chunk_size_rows,
                d=num_features,
                seed=i,
                include_label=False)
            tmp_df.to_parquet(path, compression="snappy", index=False)
        print(f"Wrote {path} to disk...")
        # todo: at large enough scale we might want to upload the rest after
        #  first N files rather than write to disk
        # to simulate a user with local copy of subset of data

    if upload_to_s3:
        os.system("aws s3 sync ./data s3://cuj-big-data/data")
        os.system("aws s3 sync ./inference s3://cuj-big-data/inference")

###############################################################################
# Reading parquet data from anywhere with Ray Datasets
# -----------------------------------------------------
#
# Note that the code for reading a parquet file from S3 or a local path is identical!

def read_dataset(path: str) -> ray.data.Dataset:
    print(f"reading data from {path}")
    return ray.data.read_parquet(path, _spread_resource_prefix="node:") \
        .random_shuffle(_spread_resource_prefix="node:")

###############################################################################
# Data preprocessing with Ray and Ray Datasets
# -----------------------------------------------------
#
# While we don't yet recommend Ray Datasets as a full-blown ETL solution, often data scientists want to do "last mile preprocessing" performantly with their model training pipeline. This is Ray Datasets' sweet spot as it integrates well with Ray Train for models in PyTorch, TensorFlow, Horovod, or similar.
#
# You'll note the workhorse of batch computations for a Ray Dataset object (ds) involves:
#
# .. code-block:: python
#
#     ds = ds.map_batches(batch_transform_func, batch_format="pandas")
#
# This is a powerful way to map inference, transforms, or any function over a Ray Dataset object.
#
# ``DataPreprocessor``` wrapper class
# -----------------------------------
#
# This isn't Ray-specific, but it simplifies how we call our preprocessing logic, depending on whether we are training or inferencing.
#
# It includes the following methods:
#
# * ``_preprocess``: shared preprocessing code across training and inference
# * ``preprocess_train_data``: includes splitting into train and test sets as well as saving mean/stddev vector
# * ``preprocess_inference_data``: utilizes saved column-specific mean/stddev vector to prepare for inference
#
# Note that the ``DataPreprocessor`` has state: the mean and stddev vectors for "standard" (z-score) scaling of the columns. This scaling helps the network learn faster and better.
#

class DataPreprocessor:
    """A Datasets-based preprocessor that fits scalers/encoders to the training
    dataset and transforms the training, testing, and inference datasets using
    those fitted scalers/encoders.
    """

    def __init__(self):
        # List of present fruits, used for one-hot encoding of fruit column.
        self.fruits = None
        # Mean and stddev stats used for standard scaling of the feature
        # columns.
        self.standard_stats = None

    def preprocess_train_data(self, ds: ray.data.Dataset
                              ) -> Tuple[ray.data.Dataset, ray.data.Dataset]:
        print("\n\nPreprocessing training dataset.\n")
        return self._preprocess(ds, False)

    def preprocess_inference_data(self,
                                  df: ray.data.Dataset) -> ray.data.Dataset:
        print("\n\nPreprocessing inference dataset.\n")
        return self._preprocess(df, True)[0]

    def _preprocess(self, ds: ray.data.Dataset, inferencing: bool
                    ) -> Tuple[ray.data.Dataset, ray.data.Dataset]:
        print(
            "\nStep 1: Dropping nulls, creating new_col, updating feature_1\n")

        def batch_transformer(df: pd.DataFrame):
            # Disable chained assignment warning.
            pd.options.mode.chained_assignment = None

            # Drop nulls.
            df = df.dropna(subset=["nullable_feature"])

            # Add new column.
            df["new_col"] = (
                df["feature_1"] - 2 * df["feature_2"] + df["feature_3"]) / 3.

            # Transform column.
            df["feature_1"] = 2. * df["feature_1"] + 0.1

            return df

        ds = ds.map_batches(batch_transformer, batch_format="pandas")

        print("\nStep 2: Precalculating fruit-grouped mean for new column and "
              "for one-hot encoding (latter only uses fruit groups)\n")
        agg_ds = ds.groupby("fruit").mean("feature_1")
        fruit_means = {
            r["fruit"]: r["mean(feature_1)"]
            for r in agg_ds.take_all()
        }

        print("\nStep 3: create mean_by_fruit as mean of feature_1 groupby "
              "fruit; one-hot encode fruit column\n")

        if inferencing:
            assert self.fruits is not None
        else:
            assert self.fruits is None
            self.fruits = list(fruit_means.keys())

        fruit_one_hots = {
            fruit: collections.defaultdict(int, **{fruit: 1})
            for fruit in self.fruits
        }

        def batch_transformer(df: pd.DataFrame):
            # Add column containing the feature_1-mean of the fruit groups.
            df["mean_by_fruit"] = df["fruit"].map(fruit_means)

            # One-hot encode the fruit column.
            for fruit, one_hot in fruit_one_hots.items():
                df[f"fruit_{fruit}"] = df["fruit"].map(one_hot)

            # Drop the fruit column, which is no longer needed.
            df.drop(columns="fruit", inplace=True)

            return df

        ds = ds.map_batches(batch_transformer, batch_format="pandas")

        if inferencing:
            print("\nStep 4: Standardize inference dataset\n")
            assert self.standard_stats is not None
        else:
            assert self.standard_stats is None

            print("\nStep 4a: Split training dataset into train-test split\n")

            # Split into train/test datasets.
            split_index = int(0.9 * ds.count())
            # Split into 90% training set, 10% test set.
            train_ds, test_ds = ds.split_at_indices([split_index])

            print("\nStep 4b: Precalculate training dataset stats for "
                  "standard scaling\n")
            # Calculate stats needed for standard scaling feature columns.
            feature_columns = [
                col for col in train_ds.schema().names if col != "label"
            ]
            standard_aggs = [
                agg(on=col) for col in feature_columns for agg in (Mean, Std)
            ]
            self.standard_stats = train_ds.aggregate(*standard_aggs)
            print("\nStep 4c: Standardize training dataset\n")

        # Standard scaling of feature columns.
        standard_stats = self.standard_stats

        def batch_standard_scaler(df: pd.DataFrame):
            def column_standard_scaler(s: pd.Series):
                if s.name == "label":
                    # Don't scale the label column.
                    return s
                s_mean = standard_stats[f"mean({s.name})"]
                s_std = standard_stats[f"std({s.name})"]
                return (s - s_mean) / s_std

            return df.transform(column_standard_scaler)

        if inferencing:
            # Apply standard scaling to inference dataset.
            inference_ds = ds.map_batches(
                batch_standard_scaler, batch_format="pandas")
            return inference_ds, None
        else:
            # Apply standard scaling to both training dataset and test dataset.
            train_ds = train_ds.map_batches(
                batch_standard_scaler, batch_format="pandas")
            test_ds = test_ds.map_batches(
                batch_standard_scaler, batch_format="pandas")
            return train_ds, test_ds

###############################################################################
# Performing inference!
# -----------------------------------------------------
#
# Here we can see our model inference in action. Note the useful chaining of Ray Datasets methods:
#
# .. code-block:: python
#
#     ds.map_batches(...).write_parquet(...)
#
# We can also specify very granular resource allocations like CPU, GPU, and more.

def inference(dataset, model_cls: type, batch_size: int, result_path: str,
              use_gpu: bool):
    print("inferencing...")
    num_gpus = 1 if use_gpu else 0
    dataset \
        .map_batches(
            model_cls,
            compute="actors",
            batch_size=batch_size,
            num_gpus=num_gpus,
            num_cpus=0) \
        .write_parquet(result_path)

# Later we'll use this convenience class to perform inference to minimize the number of times we load the model into memory, but this is largely an optimization for giant models or large numbers of batches.

class BatchInferModel:
    def __init__(self, load_model_func):
        self.device = torch.device("cuda:0"
                                    if torch.cuda.is_available() else "cpu")
        self.model = load_model_func().to(self.device)

    def __call__(self, batch) -> "pd.DataFrame":
        tensor = torch.FloatTensor(batch.to_pandas().values).to(
            self.device)
        return pd.DataFrame(self.model(tensor).cpu().detach().numpy())

###############################################################################
# Defining a neural network in PyTorch
# -----------------------------------------------------
#
# Let's define our net!
#
# Once again, we wanted to allow you to benchmark your model size with a few inputs like:
#
# * ``n_layers``: layers in your NN
# * ``n_features``: input features -- should match dimensionality of your input data
# * ``num_hidden``: hidden layers
# * ``dropout_every``: every few layers, insert a dropout layer to prevent overfitting
# * ``drop_prob``: probability in each dropout layer that a neuron will zero out input activations
#

class Net(nn.Module):
    def __init__(self, n_layers, n_features, num_hidden, dropout_every,
                 drop_prob):
        super().__init__()
        self.n_layers = n_layers
        self.dropout_every = dropout_every
        self.drop_prob = drop_prob

        self.fc_input = nn.Linear(n_features, num_hidden)
        self.relu_input = nn.ReLU()

        for i in range(self.n_layers):
            layer = nn.Linear(num_hidden, num_hidden)
            relu = nn.ReLU()
            dropout = nn.Dropout(p=self.drop_prob)

            setattr(self, f"fc_{i}", layer)
            setattr(self, f"relu_{i}", relu)
            if i % self.dropout_every == 0:
                # only apply every few layers
                setattr(self, f"drop_{i}", dropout)
                self.add_module(f"drop_{i}", dropout)

            self.add_module(f"fc_{i}", layer)

        self.fc_output = nn.Linear(num_hidden, 1)

    def forward(self, x):
        x = self.fc_input(x)
        x = self.relu_input(x)

        for i in range(self.n_layers):
            x = getattr(self, f"fc_{i}")(x)
            x = getattr(self, f"relu_{i}")(x)
            if i % self.dropout_every == 0:
                x = getattr(self, f"drop_{i}")(x)

        x = self.fc_output(x)
        return x

###############################################################################
# Training functions in PyTorch
# -----------------------------------------------------
#
# Nothing special to see here -- Ray lets you bring your PyTorch code with almost no modifications.
#
# Here, note that the input arg, ``dataset``, is in fact a PyTorch dataset, which Ray Datasets nicely provides an interface to creating from raw data (in our case, parquet files).

def train_epoch(dataset, model, device, criterion, optimizer):
    num_correct = 0
    num_total = 0
    running_loss = 0.0

    for i, (inputs, labels) in enumerate(dataset):
        inputs = inputs.to(device)
        labels = labels.to(device)

        # Zero the parameter gradients
        optimizer.zero_grad()

        # Forward + backward + optimize
        outputs = model(inputs.float())
        loss = criterion(outputs, labels.float())
        loss.backward()
        optimizer.step()

        # how are we doing?
        predictions = (torch.sigmoid(outputs) > 0.5).int()
        num_correct += (predictions == labels).sum().item()
        num_total += len(outputs)

        # Save loss to plot
        running_loss += loss.item()
        if i % 100 == 0:
            print(f"training batch [{i}] loss: {loss.item()}")

    return (running_loss, num_correct, num_total)


def test_epoch(dataset, model, device, criterion):
    num_correct = 0
    num_total = 0
    running_loss = 0.0

    with torch.no_grad():
        for i, (inputs, labels) in enumerate(dataset):
            inputs = inputs.to(device)
            labels = labels.to(device)

            # Forward + backward + optimize
            outputs = model(inputs.float())
            loss = criterion(outputs, labels.float())

            # how are we doing?
            predictions = (torch.sigmoid(outputs) > 0.5).int()
            num_correct += (predictions == labels).sum().item()
            num_total += len(outputs)

            # Save loss to plot
            running_loss += loss.item()
            if i % 100 == 0:
                print(f"testing batch [{i}] loss: {loss.item()}")

    return (running_loss, num_correct, num_total)

###############################################################################
# Our final training function with Ray Datasets input
# ------------------------------------------------------
#
# Here again, you shouldn't see many surprises. Notably, we're transforming Ray Datasets into PyTorch datasets with one simple line:
#
# .. code-block:: python
#
#     train_dataset.to_torch(label_column="your_label_col_name", batch_size=1024)
#
# This makes it really easy to leverage Ray's niceties around application development and data at scale without needing to code up tons of PyTorch connector boilerplate.
#
# The **Ray Train wrapper** is also quite simple:
#
# .. code-block:: python
#
#     net = train.torch.prepare_model(net)

def train_func(config):
    use_gpu = config["use_gpu"]
    num_epochs = config["num_epochs"]
    batch_size = config["batch_size"]
    num_layers = config["num_layers"]
    num_hidden = config["num_hidden"]
    dropout_every = config["dropout_every"]
    dropout_prob = config["dropout_prob"]
    num_features = config["num_features"]
    print("Defining model, loss, and optimizer...")

    # Setup device.
    device = torch.device(f"cuda:{train.local_rank()}"
                          if use_gpu and torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    # Setup data.
    train_dataset_pipeline = train.get_dataset_shard("train_dataset")
    train_dataset_epoch_iterator = train_dataset_pipeline.iter_epochs()
    test_dataset = train.get_dataset_shard("test_dataset")
    test_torch_dataset = test_dataset.to_torch(
        label_column="label", batch_size=batch_size)

    net = Net(
        n_layers=num_layers,
        n_features=num_features,
        num_hidden=num_hidden,
        dropout_every=dropout_every,
        drop_prob=dropout_prob,
    ).to(device)
    print(net.parameters)

    net = train.torch.prepare_model(net)

    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(net.parameters(), weight_decay=0.0001)

    print("Starting training...")
    for epoch in range(num_epochs):
        train_dataset = next(train_dataset_epoch_iterator)

        train_torch_dataset = train_dataset.to_torch(
            label_column="label", batch_size=batch_size)

        train_running_loss, train_num_correct, train_num_total = train_epoch(
            train_torch_dataset, net, device, criterion, optimizer)
        train_acc = train_num_correct / train_num_total
        print(f"epoch [{epoch + 1}]: training accuracy: "
              f"{train_num_correct} / {train_num_total} = {train_acc:.4f}")

        test_running_loss, test_num_correct, test_num_total = test_epoch(
            test_torch_dataset, net, device, criterion)
        test_acc = test_num_correct / test_num_total
        print(f"epoch [{epoch + 1}]: testing accuracy: "
              f"{test_num_correct} / {test_num_total} = {test_acc:.4f}")

        # Record and log stats.
        train.report(
            train_acc=train_acc,
            train_loss=train_running_loss,
            test_acc=test_acc,
            test_loss=test_running_loss)

        # Checkpoint model.
        module = (net.module
                  if isinstance(net, DistributedDataParallel) else net)
        train.save_checkpoint(model_state_dict=module.state_dict())

    if train.world_rank() == 0:
        return module.cpu()

###############################################################################
# Input parsing
# -------------
#
# * ``--dir-path <path>``: where we should store our generated parquet data for reading/writing
# * ``--use-s3``: whether we should also sync written data to s3, read from s3, and write inference results to s3.
# * ``--smoke-test``: run on a small amount of data / epochs to test
# * ``--address <address>``: address of Ray cluster. can be ``<ip>:<port>``, or external provider like ``anyscale://<cluster-name>``
# * ``--num-workers``: training worker count
# * ``--use-gpu``: use GPU for training?
# * ``--mlflow-register-model``: save our trained model to MLflow. Requires that you start an MLflow server. See docs on how to do this.

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dir-path",
        default=".",
        type=str,
        help="Path to read and write data from")
    parser.add_argument(
        "--use-s3",
        action="store_true",
        default=False,
        help="Sync data to S3 and then read from S3.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="The address to use for Ray. "
        "`auto` if running through `ray submit.")
    parser.add_argument(
        "--num-workers",
        default=1,
        type=int,
        help="The number of Ray workers to use for distributed training")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Use GPU for training.")
    parser.add_argument(
        "--mlflow-register-model",
        action="store_true",
        help="Whether to use mlflow model registry. If set, a local MLflow "
        "tracking server is expected to have already been started.")

    args = parser.parse_args()
    smoke_test = args.smoke_test
    address = args.address
    num_workers = args.num_workers
    use_gpu = args.use_gpu
    use_s3 = args.use_s3
    dir_path = args.dir_path

    start_time = time.time()

    ray.init(address=address)

    make_and_upload_dataset(dir_path, upload_to_s3=use_s3)

    # Setup MLflow.

    # By default, all metrics & artifacts for each run will be saved to disk
    # in ./mlruns directory. Uncomment the below lines if you want to change
    # the URI for the tracking uri.
    # TODO: Use S3 backed tracking server for golden notebook.
    if args.mlflow_register_model:
        # MLflow model registry does not work with a local file system backend.
        # Have to start a mlflow tracking server on localhost
        mlflow.set_tracking_uri("http://127.0.0.1:5000")

    # Set the experiment. This will create the experiment if not already
    # exists.
    mlflow.set_experiment("cuj-big-data-training")

    if use_s3:
        # Check if s3 data is populated.
        BUCKET_NAME = "cuj-big-data"
        FOLDER_NAME = "data/"
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(BUCKET_NAME)
        count = bucket.objects.filter(Prefix=FOLDER_NAME)
        if len(list(count)) == 0:
            print("please run `make_and_upload_dataset(upload_to_s3=True)` first")
            sys.exit(1)
        data_path = "s3://cuj-big-data/data/"
        inference_path = "s3://cuj-big-data/inference/"
        inference_output_path = "s3://cuj-big-data/output/"
    else:
        data_path = os.path.join(dir_path, "data")
        inference_path = os.path.join(dir_path, "inference")
        inference_output_path = "/tmp"

        if len(os.listdir(data_path)) <= 1 or len(
                os.listdir(inference_path)) <= 1:
            print("please run `make_and_upload_dataset(upload_to_s3=True)` first")
            sys.exit(1)

    if smoke_test:
        # Only read a single file.
        data_path = os.path.join(data_path, "data_00000.parquet.snappy")
        inference_path = os.path.join(inference_path,
                                      "data_00000.parquet.snappy")

    preprocessor = DataPreprocessor()
    train_dataset, test_dataset = preprocessor.preprocess_train_data(
        read_dataset(data_path))

    num_columns = len(train_dataset.schema().names)
    # remove label column and internal Arrow column.
    num_features = num_columns - 2

    # Random global shuffle
    train_dataset_pipeline = train_dataset.repeat() \
        .random_shuffle_each_window(_spread_resource_prefix="node:")
    del train_dataset

    datasets = {
        "train_dataset": train_dataset_pipeline,
        "test_dataset": test_dataset
    }

    config = {
        "use_gpu": use_gpu,
        "num_epochs": 2,
        "batch_size": 512,
        "num_hidden": 50,
        "num_layers": 3,
        "dropout_every": 5,
        "dropout_prob": 0.2,
        "num_features": num_features
    }

    # Create 2 callbacks: one for Tensorboard Logging and one for MLflow
    # logging. Pass these into Trainer, and all results that are
    # reported by ``train.report()`` will be logged to these 2 places.
    tbx_logdir = "./runs"
    os.makedirs(tbx_logdir, exist_ok=True)
    callbacks = [
        TBXLoggerCallback(logdir=tbx_logdir),
        MLflowLoggerCallback(
            experiment_name="big-data-training", save_artifact=True)
    ]

    # Remove CPU resource so Datasets can be scheduled.
    resources_per_worker = {"CPU": 0, "GPU": 1} if use_gpu else None

    trainer = Trainer(
        backend="torch",
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker=resources_per_worker)
    trainer.start()
    results = trainer.run(
        train_func=train_func,
        config=config,
        callbacks=callbacks,
        dataset=datasets)
    model = results[0]
    trainer.shutdown()

    if args.mlflow_register_model:
        load_model_func = register_mlflow_model(model)
    else:
        state_dict = model.state_dict()

        def load_model_func():
            num_layers = config["num_layers"]
            num_hidden = config["num_hidden"]
            dropout_every = config["dropout_every"]
            dropout_prob = config["dropout_prob"]
            num_features = config["num_features"]

            model = Net(
                n_layers=num_layers,
                n_features=num_features,
                num_hidden=num_hidden,
                dropout_every=dropout_every,
                drop_prob=dropout_prob)
            model.load_state_dict(state_dict)
            return model

    inference_dataset = preprocessor.preprocess_inference_data(
        read_dataset(inference_path))
    inference(inference_dataset, BatchInferModel(load_model_func), 100,
              inference_output_path, use_gpu)

    end_time = time.time()

    total_time = end_time - start_time
    print(f"Job finished in {total_time} seconds.")
