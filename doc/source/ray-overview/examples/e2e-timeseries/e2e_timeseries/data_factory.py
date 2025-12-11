import numpy as np
import ray

from e2e_timeseries.data_loader import Dataset_ETT_hour

# Make Ray Data less verbose.
ray.data.DataContext.get_current().enable_progress_bars = False
ray.data.DataContext.get_current().print_on_execution_start = False


def data_provider(config: dict, flag: str) -> ray.data.Dataset:
    data_set = Dataset_ETT_hour(
        flag=flag,
        size=[config["seq_len"], config["label_len"], config["pred_len"]],
        features=config["features"],
        target=config["target"],
        train_only=config["train_only"],
        smoke_test=config.get("smoke_test", False),
    )
    print(f"{flag} subset size: {len(data_set)}")

    # Convert PyTorch Dataset to Ray Dataset.
    # Note: This command prints `ArrowConversionError: Error converting data to Arrow` due to
    # the data having an extra feature dimension. However, Ray falls back to using
    # pickle to store the data and continue without issue.
    ds = ray.data.from_torch(data_set)

    def preprocess_items(item: dict) -> dict:
        # ray.data.from_torch wraps items in a dictionary {'item': (tensor_x, tensor_y)}
        # Convert these to numpy arrays and assign to 'x' and 'y' keys.
        # The tensors from PyTorch Dataset are already on CPU.
        return {"x": np.array(item["item"][0]), "y": np.array(item["item"][1])}

    ds = ds.map(preprocess_items)

    if flag == "train":
        ds = ds.random_shuffle()

    return ds
