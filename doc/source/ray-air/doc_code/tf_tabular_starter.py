# isort: skip_file

# __air_tf_preprocess_start__
import numpy as np
import ray
from ray.data.preprocessors import StandardScaler, BatchMapper, Chain
from ray.air import train_test_split
import pandas as pd

# Load data.
dataset = ray.data.read_csv("s3://air-example-data/breast_cancer.csv")
# Split data into train and validation.
train_dataset, valid_dataset = train_test_split(dataset, test_size=0.3)

# Create a test dataset by dropping the target column.
test_dataset = valid_dataset.map_batches(
    lambda df: df.drop("target", axis=1), batch_format="pandas"
)

# Get the training data schema
schema_order = [k for k in train_dataset.schema().names if k != "target"]


def concat_for_tensor(dataframe):
    # Concatenate the dataframe into a single tensor.
    from ray.data.extensions import TensorArray

    result = {}
    result["input"] = TensorArray(dataframe[schema_order].to_numpy(dtype=np.float32))
    if "target" in dataframe:
        result["target"] = TensorArray(dataframe["target"].to_numpy(dtype=np.float32))
    return pd.DataFrame(result)


# Create a preprocessor to scale some columns
columns_to_scale = ["mean radius", "mean texture"]

preprocessor = Chain(
    StandardScaler(columns=columns_to_scale), BatchMapper(concat_for_tensor)
)
# __air_tf_preprocess_end__


# __air_tf_train_start__
import tensorflow as tf
from tensorflow.keras.callbacks import Callback
from tensorflow import keras
from tensorflow.keras import layers

from ray import train
from ray.air import session
from ray.train.tensorflow import (
    TensorflowTrainer,
    to_air_checkpoint,
    prepare_dataset_shard,
)


def create_keras_model(input_features):
    return keras.Sequential([
        keras.Input(shape=(input_features,)),
        layers.Dense(16, activation="relu"),
        layers.Dense(16, activation="relu"),
        layers.Dense(1),
    ])


class TrainCheckpointReportCallback(Callback):
    def on_epoch_end(self, epoch, logs=None):
        session.report(logs, checkpoint=to_air_checkpoint(self.model))


def to_tf_dataset(dataset, batch_size):
    def to_tensor_iterator():
        data_iterator = dataset.iter_batches(batch_format="numpy", batch_size=batch_size)
        for d in data_iterator:
            yield  (
                tf.convert_to_tensor(d["input"], dtype=tf.float32),
                tf.convert_to_tensor(d["target"], dtype=tf.float32),
            )

    output_signature = (
        tf.TensorSpec(shape=(None, num_features), dtype=tf.float32),
        tf.TensorSpec(shape=(None), dtype=tf.float32),
    )
    tf_dataset = tf.data.Dataset.from_generator(
        to_tensor_iterator, output_signature=output_signature
    )
    return prepare_dataset_shard(tf_dataset)


def train_loop_per_worker(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["num_epochs"]
    num_features = config["num_features"]

    # Get the Ray Dataset shard for this data parallel worker,
    # and convert it to a Tensorflow Dataset.
    train_data = train.get_dataset_shard("train")
    val_data = train.get_dataset_shard("validate")

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = create_keras_model(num_features)
        multi_worker_model.compile(
            optimizer=tf.keras.optimizers.SGD(learning_rate=lr),
            loss="binary_crossentropy",
            metrics=[tf.keras.metrics.BinaryCrossentropy(name="loss")],
        )

    results = []
    for _ in range(epochs):
        tf_dataset = to_tf_dataset(dataset=train_data, batch_size=batch_size)
        history = multi_worker_model.fit(
            tf_dataset, callbacks=[TrainCheckpointReportCallback()]
        ) # TODO: does this refitting actually do anything?
        results.append(history.history)
    return results  # TODO: How do I fetch these results?


num_features = len(schema_order)

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        # Training batch size
        "batch_size": 128,
        # Number of epochs to train each task for.
        "num_epochs": 10,
        # Number of columns of datset
        "num_features": num_features,
        # Optimizer args.
        "lr": 0.001,
        "momentum": 0.9,
    },
    scaling_config={
        # Number of workers to use for data parallelism.
        "num_workers": 2,
        # Whether to use GPU acceleration.
        "use_gpu": False,
        # trainer_resources=0 so that the example works on Colab.
        "trainer_resources": {"CPU": 0},
    },
    datasets={"train": train_dataset, "validate": valid_dataset},
    preprocessor=preprocessor,
)

result = trainer.fit()
print(f"Last result: {result.metrics}")
# Last result: {'loss': 8.997025489807129, ...}
# __air_tf_train_end__

# __air_tf_tuner_start__
from ray import tune
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainer,
    param_space={"train_loop_config": {"lr": tune.uniform(0.001, 0.01)}},
    tune_config=TuneConfig(num_samples=1, metric="loss", mode="min"),
)
result_grid = tuner.fit()
best_result = result_grid.get_best_result()
print("Best Result:", best_result)
# Best Result: Result(metrics={'loss': 8.997025489807129, ...)
# __air_tf_tuner_end__

# __air_tf_batchpred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowPredictor

batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, 
    TensorflowPredictor,
    model_definition=lambda: create_keras_model(num_features)
)

predicted_probabilities = batch_predictor.predict(test_dataset)
print("PREDICTED PROBABILITIES")
predicted_probabilities.show()
# {'predictions': -226.1644744873047}
# {'predictions': -256.8736267089844}
# ...
# __air_tf_batchpred_end__

# __air_tf_online_predict_start__
from ray import serve
from ray.serve.model_wrappers import ModelWrapperDeployment

from fastapi import Request
import requests


async def adapter(request: Request):
    content = await request.json()
    print(content)
    return pd.DataFrame.from_dict(content)


serve.start(detached=True)
deployment = ModelWrapperDeployment.options(name="my-deployment")
# deployment.deploy(
#     TorchPredictor,
#     checkpoint,
#     batching_params=False,
#     http_adapter=adapter,
#     model=create_model(num_features))

# sample_input = test_dataset.take(1)
# sample_input = dict(sample_input[0])

# output = requests.post(deployment.url, json=[sample_input]).json()
# print(output)

# __air_tf_online_predict_end__
