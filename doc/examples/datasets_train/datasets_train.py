# TODO(matt): Reformat script.
# flake8: noqa
"""
Big Data Training
=================
"""

###############################################################################
# train
###############################################################################

import argparse
import os
import sys
import time
from typing import Tuple

import boto3
import dask
import dask.dataframe as dd
import mlflow
import pandas as pd
import ray
import torch
import torch.nn as nn
import torch.optim as optim
from dask_ml.compose import ColumnTransformer
from dask_ml.model_selection import train_test_split
from dask_ml.preprocessing import OneHotEncoder
from dask_ml.preprocessing import StandardScaler
from ray import train
from ray.train import Trainer
from ray.train import TrainingCallback
from ray.train.callbacks import TBXLoggerCallback
from ray.util.dask import ray_dask_get
from torch.nn.parallel import DistributedDataParallel
from torch.utils.tensorboard import SummaryWriter


# TODO(amogkam): Upstream this into Ray Train.
class MLflowCallback(TrainingCallback):
    def __init__(self, config):
        self.config = config

    def handle_result(self, results, **info):
        # For each result that's being reported by ``train.report()``,
        # we get the result from the rank 0 worker (i.e. first worker) and
        # report it to MLflow.
        rank_zero_results = results[0]
        mlflow.log_metrics(rank_zero_results)

    # TODO: fix type hint for logdir
    def start_training(self, logdir, **info):
        mlflow.start_run(run_name=str(logdir.name))
        mlflow.log_params(config)

        # TODO: Update TrainCallback to provide logdir in finish_training.
        self.logdir = logdir

    def finish_training(self, error: bool = False, **info):
        # Save the Trainer checkpoints as artifacts to mlflow.
        mlflow.log_artifacts(self.logdir)


def read_dask_dataframe(path: str) -> "dask.DataFrame":
    print(f"reading data from {path}")
    return ray.data.read_parquet(path).random_shuffle().to_dask().reset_index()


class DataPreprocessor:
    def __init__(self):
        self.column_transformer = None
        self.scaler = None

    def preprocess_train_data(self, df: "dask.DataFrame"
                              ) -> Tuple["dask.DataFrame", "dask.DataFrame"]:
        return self._preprocess(df, False)

    def preprocess_inference_data(
            self,
            df: "dask.DataFrame") -> Tuple["dask.DataFrame", "dask.DataFrame"]:
        return self._preprocess(df, True)[0]

    def _preprocess(self, df: "dask.DataFrame", inferencing: bool
                    ) -> Tuple["dask.DataFrame", "dask.DataFrame"]:
        df = df.loc[:, df.columns != "index"]
        # remove nulls and/or NaNs scalably with dask
        print(f"step1: drop nulls from rows")
        df = df.dropna(subset=["nullable_feature"])

        print(f"step2: creating new_col and updatingfeature_1")
        df["new_col"] = (
            df["feature_1"] - 2 * df["feature_2"] + df["feature_3"]) / 3.
        df["feature_1"] = 2. * df["feature_1"] + 0.1
        # TODO: this doesn't work with more than 1 parquet file
        # df['mean_by_fruit'] = df.groupby('fruit')['feature_1'].transform('mean')

        print(f"step3: one-hot encoding fruit")
        df = df.astype({"fruit": "category"})
        df = df.categorize()
        df.persist()

        if inferencing:
            assert self.column_transformer is not None
            df_fruits = self.column_transformer.transform(df)
        else:
            assert self.column_transformer is None
            self.column_transformer = ColumnTransformer(
                [("one-hot", OneHotEncoder(sparse=False), ["fruit"])])
            df_fruits = self.column_transformer.fit_transform(df)

        df_data = df.loc[:, (df.columns != "label") & (df.columns != "fruit")]
        df_data = dd.concat([df_data, df_fruits], axis=1)

        assert df_data.isnull().sum().sum().compute(
        ) == 0, "There are nulls or Nans in the data!"

        if inferencing:
            print(f"step4: standardrize inference dataset")
            assert self.scaler is not None
            df_data_inference = self.scaler.transform(df_data)
            return df_data_inference, None
        else:
            print(f"step4: standardrize train dataset")
            df_labels = df.loc[:, df.columns == "label"]
            df_data_train, df_data_test, df_label_train, df_label_test = train_test_split(
                df_data, df_labels)
            df_data_train.persist()
            assert self.scaler is None
            self.scaler = StandardScaler(
            )  # this just turns col values to z-scores
            df_data_train = self.scaler.fit_transform(df_data_train)
            df_data_test = self.scaler.transform(df_data_test)
            df_train = dd.concat([df_data_train, df_label_train], axis=1)
            df_test = dd.concat([df_data_test, df_label_test], axis=1)
            return df_train, df_test


def inference(dataset, model_cls: type, batch_size: int, result_path: str,
              use_gpu: bool):
    print("inferencing...")
    num_gpus = 1 if use_gpu else 0
    dataset.map_batches(model_cls, compute="actors", batch_size=batch_size, num_gpus=num_gpus). \
        write_parquet(result_path)


"""
TODO: Define neural network code in pytorch
P0:
1) can take arguments to change size of net arbitrarily so we can stress test against distributed training on cluster
2) has a network (nn.module?), optimizer, and loss function for binary classification
3) has some semblence of regularization (ie: via dropout) so that this artificially gigantic net doesn't just overfit horrendously
4) works well with pytorch dataset we'll create from Ray data .to_torch_dataset()
P1:
1) also tracks AUC for training, testing sets and records to tensorboard to
"""


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
            self.add_module(f"relu_{i}", relu)
            # self.register_parameter(name=f"fc_{i}", param=getattr(self, f"fc_{i}"))
            # self.register_parameter(name=f"relu_{i}", param=getattr(self, f"relu_{i}"))

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


"""
TODO: training loop for NN
P0 Requirements:
1) should iterate through epochs, inner loop per batch
2) should keep running total of accuracy, loss (training & test) and record those to tensorboard
3) should perform windowing / shuffling per epoch
P1:
1) use Ray Tune for tuning / checkpointing
"""


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


def train_func(config):
    is_distributed = config.get("is_distributed", False)
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
    if is_distributed:
        device = torch.device(f"cuda:{train.local_rank()}" if use_gpu
                              and torch.cuda.is_available() else "cpu")
    else:
        device = torch.device("cuda:0" if use_gpu
                              and torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    # Setup data.
    if is_distributed:
        train_dataset_pipeline = train.get_dataset_shard("train_dataset")
        train_dataset_epoch_iterator = train_dataset_pipeline.iter_epochs()
        test_dataset = train.get_dataset_shard("test_dataset")
    else:
        train_dataset_epoch_iterator = config["train_dataset"].iter_epochs()
        test_dataset = config["test_dataset"]
    test_torch_dataset = test_dataset.to_torch(
        label_column="label", batch_size=batch_size)

    # Setup Tensorboard and MLflow.
    if is_distributed:
        # Setup is done through Callback.
        pass
    else:
        writer = SummaryWriter()
        mlflow.start_run()
        mlflow_config = config.copy()
        mlflow_config.pop("test_dataset")
        mlflow_config.pop("train_dataset")
        mlflow.log_params(mlflow_config)

    net = Net(
        n_layers=num_layers,
        n_features=num_features,
        num_hidden=num_hidden,
        dropout_every=dropout_every,
        drop_prob=dropout_prob,
    ).to(device)
    print(net.parameters)

    if is_distributed:
        net = DistributedDataParallel(net)

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
        print(
            f"epoch [{epoch + 1}]: training accuracy: {train_num_correct} / {train_num_total} = {train_acc:.4f}"
        )

        test_running_loss, test_num_correct, test_num_total = test_epoch(
            test_torch_dataset, net, device, criterion)
        test_acc = test_num_correct / test_num_total
        print(
            f"epoch [{epoch + 1}]: testing accuracy: {test_num_correct} / {test_num_total} = {test_acc:.4f}"
        )

        # Record and log stats.
        if is_distributed:
            train.report(
                train_acc=train_acc,
                train_loss=train_running_loss,
                test_acc=test_acc,
                test_loss=test_running_loss)
        else:
            writer.add_scalar("Accuracy/train", train_acc, epoch)
            writer.add_scalar("Loss/train", train_running_loss, epoch)
            writer.add_scalar("Accuracy/test", test_acc, epoch)
            writer.add_scalar("Loss/test", test_running_loss, epoch)
            writer.flush()

            mlflow.log_metrics({
                "train_acc": train_acc,
                "train_loss": train_running_loss,
                "test_acc": test_acc,
                "test_loss": test_running_loss
            })

        # Checkpoint model.
        if is_distributed:
            import copy
            model_copy = copy.deepcopy(net.module)
            train.save_checkpoint(
                model_state_dict=model_copy.cpu().state_dict())
        else:
            torch.save(net.state_dict(), f"models/model-epoch-{epoch}.torch")

    # Shutdown Tensorboard and MLflow.
    if is_distributed:
        pass
    else:
        writer.close()
        # mlflow.end_run()

    if is_distributed:
        if train.world_rank() == 0:
            return net.module.cpu()
        else:
            return None
    else:
        return net


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
        help="Use data from s3 for testing.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="The address to use for Ray. `auto` if running through `ray submit"
    )
    parser.add_argument(
        "--num-workers",
        default=1,
        type=int,
        help="If > 1, number of Ray workers to use for distributed training")
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
    dask.config.set(scheduler=ray_dask_get)

    ###############################################################################
    # make_and_upload_dataset
    ###############################################################################

    import random
    import os
    import shutil

    import pandas as pd
    import sklearn.datasets

    NUM_EXAMPLES = 2_000_000
    NUM_FEATURES = 20
    PARQUET_FILE_CHUNK_SIZE = 50_000
    NUM_FILES = NUM_EXAMPLES // PARQUET_FILE_CHUNK_SIZE

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
        col_names = ['feature_%0d' % i for i in range(1, d + 1, 1)]
        df = pd.DataFrame(X)
        df.columns = col_names

        # add some bogus categorical data columns
        options = ['apple', 'banana', 'orange']
        df['fruit'] = df.feature_1.map(
            lambda x: random.choice(options)
        )  # bogus, but nice to test categoricals

        # add some nullable columns
        options = [None, 1, 2]
        df['nullable_feature'] = df.feature_1.map(
            lambda x: random.choice(options)
        )  # bogus, but nice to test categoricals

        # add label column
        if include_label:
            df['label'] = y
        return df

    # create data files
    print("Creating synthetic dataset...")
    data_path = os.path.join(dir_path, "data")
    os.makedirs(data_path, exist_ok=True)
    for i in range(NUM_FILES):
        path = os.path.join(data_path, f"data_{i:05d}.parquet.snappy")
        if not os.path.exists(path):
            tmp_df = create_data_chunk(
                n=PARQUET_FILE_CHUNK_SIZE,
                d=NUM_FEATURES,
                seed=i,
                include_label=True)
            tmp_df.to_parquet(path, compression="snappy", index=False)
        print(f"Wrote {path} to disk...")
        # todo: at large enough scale we might want to upload the rest after first N files rather than write to disk
        # to simulate a user with local copy of subset of data

    print("Creating synthetic inference dataset...")
    inference_path = os.path.join(dir_path, "inference")
    os.makedirs(inference_path, exist_ok=True)
    for i in range(NUM_FILES):
        path = os.path.join(inference_path, f"data_{i:05d}.parquet.snappy")
        if not os.path.exists(path):
            tmp_df = create_data_chunk(
                n=PARQUET_FILE_CHUNK_SIZE,
                d=NUM_FEATURES,
                seed=i,
                include_label=False)
            tmp_df.to_parquet(path, compression="snappy", index=False)
        print(f"Wrote {path} to disk...")
        # todo: at large enough scale we might want to upload the rest after first N files rather than write to disk
        # to simulate a user with local copy of subset of data

    # os.system("aws s3 sync ./data s3://cuj-big-data/data")
    # os.system("aws s3 sync ./inference s3://cuj-big-data/inference")

    ###############################################################################
    # train
    ###############################################################################

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
        BUCKET_NAME = 'cuj-big-data'
        FOLDER_NAME = 'data/'
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(BUCKET_NAME)
        count = bucket.objects.filter(Prefix=FOLDER_NAME)
        if len(list(count)) == 0:
            print("please run `python make_and_upload_dataset.py` first")
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
            print("please run `python make_and_upload_dataset.py` first")
            sys.exit(1)

    if smoke_test:
        # Only read a single file.
        data_path = os.path.join(data_path, "data_00000.parquet.snappy")
        inference_path = os.path.join(inference_path,
                                      "data_00000.parquet.snappy")

    preprocessor = DataPreprocessor()
    train_df, test_df = preprocessor.preprocess_train_data(
        read_dask_dataframe(data_path))

    num_columns = len(train_df.columns)
    num_features = num_columns - 1  # remove label column

    train_dataset = ray.data.from_dask(train_df)
    test_dataset = ray.data.from_dask(test_df)

    NUM_EPOCHS = 5
    BATCH_SIZE = 64
    NUM_HIDDEN = 50  # 200
    NUM_LAYERS = 3  # 15
    DROPOUT_EVERY = 5
    DROPOUT_PROB = 0.2

    # Random global shuffle
    train_dataset_pipeline = train_dataset.repeat().random_shuffle_each_window(
    )

    datasets = {
        "train_dataset": train_dataset_pipeline,
        "test_dataset": test_dataset
    }

    if num_workers <= 1:
        config = {
            "use_gpu": use_gpu,
            "num_epochs": NUM_EPOCHS,
            "batch_size": BATCH_SIZE,
            "num_hidden": NUM_HIDDEN,
            "num_layers": NUM_LAYERS,
            "dropout_every": DROPOUT_EVERY,
            "dropout_prob": DROPOUT_PROB,
            "num_features": num_features,
        }

        config.update(datasets)

        model = train_func(config=config)

    else:
        config = {
            "is_distributed": True,
            "use_gpu": use_gpu,
            "num_epochs": NUM_EPOCHS,
            "batch_size": BATCH_SIZE,
            "num_hidden": NUM_HIDDEN,
            "num_layers": NUM_LAYERS,
            "dropout_every": DROPOUT_EVERY,
            "dropout_prob": DROPOUT_PROB,
            "num_features": num_features
        }

        # Create 2 callbacks: one for Tensorboard Logging and one for MLflow
        # logging. Pass these into Trainer, and all results that are
        # reported by ``train.report()`` will be logged to these 2 places.
        # TODO: TBXLoggerCallback should create nonexistent logdir
        #       and should also create 1 directory per file.
        tbx_logdir = "./runs"
        os.makedirs("./runs", exist_ok=True)
        callbacks = [
            TBXLoggerCallback(logdir=tbx_logdir),
            MLflowCallback(config)
        ]

        trainer = Trainer(
            backend="torch", num_workers=num_workers, use_gpu=use_gpu)
        trainer.start()
        results = trainer.run(
            train_func=train_func,
            config=config,
            callbacks=callbacks,
            dataset=datasets)
        model = results[0]
        trainer.shutdown()

    if args.mlflow_register_model:
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

    class BatchInferModel:
        def __init__(self, load_model_func):
            self.device = torch.device("cuda:0"
                                       if torch.cuda.is_available() else "cpu")
            self.model = load_model_func().to(self.device)

        def __call__(self, batch) -> "pd.DataFrame":
            tensor = torch.FloatTensor(batch.to_pandas().values).to(
                self.device)
            return pd.DataFrame(self.model(tensor).cpu().detach().numpy())

    inference_df = preprocessor.preprocess_inference_data(
        read_dask_dataframe(inference_path))
    inference_dataset = ray.data.from_dask(inference_df)
    inference(inference_dataset, BatchInferModel(load_model_func), 100,
              inference_output_path, use_gpu)

    end_time = time.time()

    total_time = end_time - start_time
    print(f"Job finished in {total_time} seconds.")
