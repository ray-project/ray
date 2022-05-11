"""
Adapted from https://www.tensorflow.org/tfx/tutorials/tfx/components_keras.
The goal is to predict whether a trip may generate a big tip.

In this example, we showcase how to convert a tfx pipeline to AIR, covering
very step from data ingestion to pushing a model to serving.

1. Read a CSV file into ray dataset.
2. Process the dataset by chaining a variety of ready-to-use preprocessors.
3. Train the model using distributed tensorflow with few lines of code.
4. Serve the model that will apply the same preprocessing to the incoming requests.

Note, ``ray.ml.checkpoint.Checkpoint`` serves as the bridge between step 3 and step 4.
By capturing both model and preprocessing steps in a way compatible with Ray Serve, this
abstraction makes sure ml workload can transition seamlessly between training and
serving.
"""

import pandas as pd
from fastapi import Request
import requests
from sklearn.model_selection import train_test_split
import tensorflow as tf

import ray
from ray import serve
from ray import train
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.train.integrations.tensorflow import TensorflowTrainer
from ray.ml.preprocessors import (
    BatchMapper,
    Chain,
    OneHotEncoder,
    SimpleImputer,
)
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray.train.tensorflow import prepare_dataset_shard


taxi_data = pd.read_csv(
    "https://raw.githubusercontent.com/tensorflow/tfx/master/"
    "tfx/examples/chicago_taxi_pipeline/data/simple/data.csv"
)
taxi_data["is_big_tip"] = taxi_data["tips"] / taxi_data["fare"] > 0.2
# Remove the following features as we don't have bucketizer yet.
taxi_data = taxi_data.drop(
    [
        "tips",
        "fare",
        "dropoff_latitude",
        "dropoff_longitude",
        "pickup_latitude",
        "pickup_longitude",
        "pickup_census_tract",
    ],
    axis=1,
)

# `from_csv` doesn't work right now.
train_data, test_data = train_test_split(
    taxi_data, stratify=taxi_data[["is_big_tip"]], random_state=1113
)
train_dataset = ray.data.from_pandas(train_data)
test_data_label = test_data["is_big_tip"].values
test_data = test_data.drop(["is_big_tip"], axis=1)


imputer1 = SimpleImputer(
    ["dropoff_census_tract"], strategy="constant", fill_value=17031839100
)
imputer2 = SimpleImputer(
    ["pickup_community_area", "dropoff_community_area"],
    strategy="constant",
    fill_value=8,
)
imputer3 = SimpleImputer(["payment_type"], strategy="constant", fill_value="Cash")
imputer4 = SimpleImputer(
    ["company"], strategy="constant", fill_value="Taxi Affiliation Services"
)
imputer5 = SimpleImputer(
    ["trip_start_timestamp", "trip_miles", "trip_seconds"], strategy="mean"
)

ohe = OneHotEncoder(
    columns=[
        "trip_start_hour",
        "trip_start_day",
        "trip_start_month",
        "dropoff_census_tract",
        "pickup_community_area",
        "dropoff_community_area",
        "payment_type",
        "company",
    ],
    limit={
        "dropoff_census_tract": 25,
        "pickup_community_area": 20,
        "dropoff_community_area": 20,
        "payment_type": 2,
        "company": 7,
    },
)


def fn(df):
    df["trip_start_year"] = pd.to_datetime(df["trip_start_timestamp"], unit="s").dt.year
    return df


chained_pp = Chain(
    imputer1,
    imputer2,
    imputer3,
    imputer4,
    imputer5,
    ohe,
    BatchMapper(fn),
    BatchMapper(lambda x: x.drop(["trip_start_timestamp"], axis=1)),
)

# This is came up by manually inspecting the dataset after preprocessing.
# Hopefully we can get rid of it in the future.
INPUT_SIZE = 120
# Not sure what to do with partial batch.
BATCH_SIZE = 1
EPOCH = 10


def build_model():
    model = tf.keras.models.Sequential()
    model.add(tf.keras.Input(shape=(INPUT_SIZE,)))
    model.add(tf.keras.layers.Dense(50, activation="relu"))
    model.add(tf.keras.layers.Dense(1, activation="sigmoid"))
    return model


def train_loop_per_worker(config):
    dataset_shard = train.get_dataset_shard("train")

    strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
    with strategy.scope():
        model = build_model()
        model.compile(
            loss="binary_crossentropy",
            optimizer="adam",
            metrics=["accuracy"],
        )

    for epoch in range(EPOCH):
        print(f"Training epoch = {epoch}...")
        tf_dataset = prepare_dataset_shard(
            dataset_shard.to_tf(
                label_column="is_big_tip",
                output_signature=(
                    tf.TensorSpec(shape=(BATCH_SIZE, INPUT_SIZE), dtype=tf.float32),
                    tf.TensorSpec(shape=(BATCH_SIZE,), dtype=tf.int64),
                ),
                batch_size=BATCH_SIZE,
            )
        )

        model.fit(tf_dataset)
        train.save_checkpoint(epoch=epoch, model=model.get_weights())


trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config={"num_workers": 1},
    datasets={"train": train_dataset},
    preprocessor=chained_pp,
)
print("Start training...")
result = trainer.fit()

print("Start serving...")


async def adapter(request: Request):
    content = await request.json()
    return pd.DataFrame.from_dict(content)


def serve_model(checkpoint: Checkpoint, model_definition, name="Model") -> str:
    serve.start(detached=True)
    deployment = ModelWrapperDeployment.options(name=name)
    deployment.deploy(
        TensorflowPredictor,
        checkpoint,
        batching_params=False,
        model_definition=model_definition,
        http_adapter=adapter,
    )
    return deployment.url


endpoint_uri = serve_model(result.checkpoint, build_model)

for i in range(100):
    one_row = test_data.iloc[[i]].to_dict()
    serve_result = requests.post(endpoint_uri, json=one_row).json()
    print(
        f"request[{i}] prediction: {serve_result['predictions']['0']} "
        f"- label: {str(test_data_label[i])}"
    )
