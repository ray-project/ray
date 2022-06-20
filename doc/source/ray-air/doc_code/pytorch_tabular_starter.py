# flake8: noqa
# isort: skip_file

# __air_pytorch_preprocess_start__
from venv import create
import ray
from ray.data.preprocessors import StandardScaler

import pandas as pd

from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split

data_raw = load_breast_cancer()
dataset_df = pd.DataFrame(data_raw["data"], columns=data_raw["feature_names"])
dataset_df["target"] = data_raw["target"]
train_df, test_df = train_test_split(dataset_df, test_size=0.3)
train_dataset = ray.data.from_pandas(train_df)
valid_dataset = ray.data.from_pandas(test_df)
test_dataset = ray.data.from_pandas(test_df.drop("target", axis=1))


# Create a preprocessor to scale some columns
columns_to_scale = ["mean radius", "mean texture"]
preprocessor = StandardScaler(columns=columns_to_scale)
# __air_pytorch_preprocess_end__


# __air_pytorch_train_start__
import torch.nn as nn
from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present

def create_model(input_features):
    return nn.Sequential(
        nn.Linear(in_features=input_features, out_features=16),
        nn.ReLU(),
        nn.Linear(16, 16),
        nn.ReLU(),
        nn.Linear(16, 1),
        nn.Sigmoid())


def validate_epoch(dataloader, model, loss_fn):
    size = len(dataloader.dataset) // train.world_size()
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    return test_loss


def train_loop_per_worker(config):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]

    worker_batch_size = batch_size // train.world_size()

    # Create data loaders.
    # Get the Ray Dataset shard for this data parallel worker, and convert it to a PyTorch Dataset.
    train_dataloader = train.get_dataset_shard("train").to_torch(
        label_column="label",
        batch_size=batch_size,
        unsqueeze_feature_tensors=False,
        unsqueeze_label_tensor=False,
    )
    val_dataloader = train.get_dataset_shard("validate").to_torch(
        label_column="label",
        batch_size=batch_size,
        unsqueeze_feature_tensors=False,
        unsqueeze_label_tensor=False,
    )

    # Create model.
    model = create_model()
    model = train.torch.prepare_model(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    for _ in range(epochs):
        for inputs, labels in train_dataloader:
            optimizer.zero_grad()

            predictions = model(inputs) 
            loss = loss_fn(predictions, labels)
            loss.backward()
            optimizer.step()

        loss = validate_epoch(val_dataloader, model, loss_fn)
        train.report(loss=loss)

        # Checkpoint model after every epoch.
        state_dict = model.state_dict()
        consume_prefix_in_state_dict_if_present(state_dict, "module.")
        train.save_checkpoint(model=state_dict)

num_workers = 2
use_gpu = False  # use GPUs if detected.

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "num_epochs": num_epochs,
        "learning_rate": learning_rate,
        "momentum": momentum,
        "batch_size": batch_size,
    },
    # Have to specify trainer_resources as 0 so that the example works on Colab. 
    scaling_config={"num_workers": num_workers, "use_gpu": use_gpu, "trainer_resources": {"CPU": 0}},
    datasets={"train": train_dataset, "validate": valid_dataset},
    preprocessor=preprocessor
)

result = trainer.fit()
print(f"Last result: {result.metrics}")
# __air_pytorch_train_end__

# __air_pytorch_batchpred_start__
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TorchPredictor)

predicted_labels = (
    batch_predictor.predict(test_dataset)
    .map_batches(lambda df: (df > 0.5).astype(int), batch_format="pandas")
    .to_pandas(limit=float("inf"))
)
print("PREDICTED LABELS")
print(f"{predicted_labels}")
# __air_pytorch_batchpred_end__
