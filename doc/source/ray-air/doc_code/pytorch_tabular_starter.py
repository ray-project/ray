# isort: skip_file

# __air_pytorch_preprocess_start__
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
# __air_pytorch_preprocess_end__


# __air_pytorch_train_start__
import torch
import torch.nn as nn
from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present

from ray import train
from ray.train.torch import TorchTrainer


def create_model(input_features):
    return nn.Sequential(
        nn.Linear(in_features=input_features, out_features=16),
        nn.ReLU(),
        nn.Linear(16, 16),
        nn.ReLU(),
        nn.Linear(16, 1),
        nn.Sigmoid(),
    )


def validate_epoch(dataloader, model, loss_fn):
    model.eval()
    test_loss, correct = 0, 0
    evaluated = False
    with torch.no_grad():
        for idx, (inputs, labels) in enumerate(dataloader):
            evaluated = True
            pred = model(inputs)
            test_loss += loss_fn(pred, labels.unsqueeze(1)).item()
            correct += (pred.argmax(1) == labels).type(torch.float).sum().item()
    if not evaluated:
        return
    test_loss /= idx + 1
    return test_loss


def train_loop_per_worker(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["num_epochs"]
    num_features = config["num_features"]

    # Get the Ray Dataset shard for this data parallel worker,
    # and convert it to a PyTorch Dataset.
    train_iterator = train.get_dataset_shard("train").iter_batches(
        batch_format="numpy", batch_size=batch_size
    )
    val_dataloader = train.get_dataset_shard("validate").iter_batches(
        batch_format="numpy", batch_size=batch_size
    )

    def to_tensor_iterator(dict_iterator):
        for d in dict_iterator:
            yield torch.Tensor(d["input"]).float(), torch.Tensor(d["target"]).float()

    # Create model.
    model = create_model(num_features)
    model = train.torch.prepare_model(model)

    loss_fn = nn.BCELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for _ in range(epochs):
        for inputs, labels in to_tensor_iterator(train_iterator):
            optimizer.zero_grad()
            predictions = model(inputs)
            train_loss = loss_fn(predictions, labels.unsqueeze(1))
            train_loss.backward()
            optimizer.step()

        loss = validate_epoch(to_tensor_iterator(val_dataloader), model, loss_fn)
        train.report(loss=loss)

        # Checkpoint model after every epoch.
        state_dict = model.state_dict()
        consume_prefix_in_state_dict_if_present(state_dict, "module.")
        train.save_checkpoint(model=state_dict)

num_features = len(schema_order)

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        # Training batch size
        "batch_size": 32,
        # Number of epochs to train each task for.
        "num_epochs": 4,
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
# __air_pytorch_train_end__

# __air_pytorch_tuner_start__
from ray import tune
from ray.tune.tuner import Tuner, TuneConfig

tuner = Tuner(
    trainer,
    param_space={"params": {"lr": tune.uniform(0.001, 0.01)}},
    tune_config=TuneConfig(num_samples=5, metric="loss", mode="min"),
)
result_grid = tuner.fit()
best_result = result_grid.get_best_result()
print(best_result)
# __air_pytorch_tuner_end__

# __air_pytorch_batchpred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor

batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint, TorchPredictor, model=create_model(num_features)
)


predicted_probabilities = batch_predictor.predict(test_dataset)
print("PREDICTED PROBABILITIES")
predicted_probabilities.show()

# __air_pytorch_batchpred_end__
