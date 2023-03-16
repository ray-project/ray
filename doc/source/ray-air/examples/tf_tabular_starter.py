# flake8: noqa
# isort: skip_file

# __air_generic_preprocess_start__
import ray

# Load data.
dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

# Split data into train and validation.
train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

# Create a test dataset by dropping the target column.
test_dataset = valid_dataset.drop_columns(cols=["target"])
# __air_generic_preprocess_end__

# __air_tf_preprocess_start__
import numpy as np

from ray.data.preprocessors import Concatenator, Chain, StandardScaler

# Create a preprocessor to scale some columns and concatenate the result.
preprocessor = Chain(
    StandardScaler(columns=["mean radius", "mean texture"]),
    Concatenator(exclude=["target"], dtype=np.float32),
)
# __air_tf_preprocess_end__


# __air_tf_train_start__
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

from ray.air import session
from ray.air.config import ScalingConfig
from ray.air.integrations.keras import Callback as KerasCallback
from ray.train.tensorflow import TensorflowTrainer


def create_keras_model(input_features):
    return keras.Sequential(
        [
            keras.Input(shape=(input_features,)),
            layers.Dense(16, activation="relu"),
            layers.Dense(16, activation="relu"),
            layers.Dense(1),
        ]
    )


def train_loop_per_worker(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["num_epochs"]
    num_features = config["num_features"]

    # Get the Ray Dataset shard for this data parallel worker,
    # and convert it to a Tensorflow Dataset.
    train_data = session.get_dataset_shard("train")

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Model building/compiling need to be within `strategy.scope()`.
        multi_worker_model = create_keras_model(num_features)
        multi_worker_model.compile(
            optimizer=tf.keras.optimizers.SGD(learning_rate=lr),
            loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
            metrics=[
                tf.keras.metrics.BinaryCrossentropy(
                    name="loss",
                )
            ],
        )

    for _ in range(epochs):
        tf_dataset = train_data.to_tf(
            feature_columns="concat_out", label_columns="target", batch_size=batch_size
        )
        multi_worker_model.fit(
            tf_dataset,
            callbacks=[KerasCallback()],
            verbose=0,
        )


num_features = len(train_dataset.schema().names) - 1

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "batch_size": 128,
        "num_epochs": 50,
        "num_features": num_features,
        "lr": 0.0001,
    },
    scaling_config=ScalingConfig(
        num_workers=2,  # Number of data parallel training workers
        use_gpu=False,
        trainer_resources={"CPU": 0},  # so that the example works on Colab.
    ),
    datasets={"train": train_dataset},
    preprocessor=preprocessor,
)

result = trainer.fit()
print(f"Last result: {result.metrics}")
# Last result: {'loss': 8.997025489807129, ...}
# __air_tf_train_end__

# __air_tf_tuner_start__
from ray import tune

param_space = {"train_loop_config": {"lr": tune.loguniform(0.0001, 0.01)}}
metric = "loss"
# __air_tf_tuner_end__

# __air_tune_generic_start__
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainer,
    param_space=param_space,
    tune_config=TuneConfig(num_samples=3, metric=metric, mode="min"),
)
# Execute tuning.
result_grid = tuner.fit()

# Fetch the best result.
best_result = result_grid.get_best_result()
print("Best Result:", best_result)
# Best Result: Result(metrics={'loss': 4.997025489807129, ...)
# __air_tune_generic_end__

# __air_tf_batchpred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowPredictor

# You can also create a checkpoint from a trained model using `TensorflowCheckpoint`.
checkpoint = best_result.checkpoint

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint,
    TensorflowPredictor,
    model_definition=lambda: create_keras_model(num_features),
)

predicted_probabilities = batch_predictor.predict(test_dataset)
predicted_probabilities.show()
# {'predictions': 0.033036969602108}
# {'predictions': 0.05944341793656349}
# {'predictions': 0.1657751202583313}
# ...
# __air_tf_batchpred_end__
