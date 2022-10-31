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

# __air_pytorch_preprocess_start__
import numpy as np

from ray.data.preprocessors import Concatenator, Chain, StandardScaler

# Create a preprocessor to scale some columns and concatenate the result.
preprocessor = Chain(
    StandardScaler(columns=["mean radius", "mean texture"]),
    Concatenator(exclude=["target"], dtype=np.float32),
)
# __air_pytorch_preprocess_end__


# __air_pytorch_train_start__
import torch
import torch.nn as nn

from ray import train
from ray.air import session
from ray.air.config import ScalingConfig
from ray.train.torch import TorchCheckpoint, TorchTrainer


def create_model(input_features):
    return nn.Sequential(
        nn.Linear(in_features=input_features, out_features=16),
        nn.ReLU(),
        nn.Linear(16, 16),
        nn.ReLU(),
        nn.Linear(16, 1),
        nn.Sigmoid(),
    )


def train_loop_per_worker(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["num_epochs"]
    num_features = config["num_features"]

    # Get the Ray Dataset shard for this data parallel worker,
    # and convert it to a PyTorch Dataset.
    train_data = session.get_dataset_shard("train")
    # Create model.
    model = create_model(num_features)
    model = train.torch.prepare_model(model)

    loss_fn = nn.BCELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for cur_epoch in range(epochs):
        for batch in train_data.iter_torch_batches(
            batch_size=batch_size, dtypes=torch.float32
        ):
            # "concat_out" is the output column of the Concatenator.
            inputs, labels = batch["concat_out"], batch["target"]
            optimizer.zero_grad()
            predictions = model(inputs)
            train_loss = loss_fn(predictions, labels.unsqueeze(1))
            train_loss.backward()
            optimizer.step()
        loss = train_loss.item()
        session.report({"loss": loss}, checkpoint=TorchCheckpoint.from_model(model))


num_features = len(train_dataset.schema().names) - 1

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "batch_size": 128,
        "num_epochs": 20,
        "num_features": num_features,
        "lr": 0.001,
    },
    scaling_config=ScalingConfig(
        num_workers=3,  # Number of workers to use for data parallelism.
        use_gpu=False,
        trainer_resources={"CPU": 0},  # so that the example works on Colab.
    ),
    datasets={"train": train_dataset},
    preprocessor=preprocessor,
)
# Execute training.
result = trainer.fit()
print(f"Last result: {result.metrics}")
# Last result: {'loss': 0.6559339960416158, ...}
# __air_pytorch_train_end__

# __air_pytorch_tuner_start__
from ray import tune

param_space = {"train_loop_config": {"lr": tune.loguniform(0.0001, 0.01)}}
metric = "loss"
# __air_pytorch_tuner_end__

# __air_tune_generic_start__
from ray.tune.tuner import Tuner, TuneConfig
from ray.air.config import RunConfig

tuner = Tuner(
    trainer,
    param_space=param_space,
    tune_config=TuneConfig(num_samples=5, metric=metric, mode="min"),
)
# Execute tuning.
result_grid = tuner.fit()

# Fetch the best result.
best_result = result_grid.get_best_result()
print("Best Result:", best_result)
# Best Result: Result(metrics={'loss': 0.278409322102863, ...})
# __air_tune_generic_end__

# __air_pytorch_batchpred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor

# You can also create a checkpoint from a trained model using
# `TorchCheckpoint.from_model`.
checkpoint = best_result.checkpoint

batch_predictor = BatchPredictor.from_checkpoint(
    checkpoint, TorchPredictor, model=create_model(num_features)
)

predicted_probabilities = batch_predictor.predict(test_dataset)
predicted_probabilities.show()
# {'predictions': array([1.], dtype=float32)}
# {'predictions': array([0.], dtype=float32)}
# ...
# __air_pytorch_batchpred_end__
