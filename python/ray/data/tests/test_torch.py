import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_iter_torch_batches(ray_start_10_cpus_shared):
    import torch

    df1 = pd.DataFrame(
        {"one": [1, 2, 3], "two": [1.0, 2.0, 3.0], "label": [1.0, 2.0, 3.0]}
    )
    df2 = pd.DataFrame(
        {"one": [4, 5, 6], "two": [4.0, 5.0, 6.0], "label": [4.0, 5.0, 6.0]}
    )
    df3 = pd.DataFrame({"one": [7, 8], "two": [7.0, 8.0], "label": [7.0, 8.0]})
    df = pd.concat([df1, df2, df3])
    ds = ray.data.from_pandas([df1, df2, df3])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=3):
            iterations.append(
                torch.stack(
                    (batch["one"], batch["two"], batch["label"]),
                    dim=1,
                ).numpy()
            )
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


def test_iter_torch_batches_tensor_ds(ray_start_10_cpus_shared):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])

    num_epochs = 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=2):
            iterations.append(batch["data"].numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


# This test catches an error in stream_split_iterator dealing with empty blocks,
# which is difficult to reproduce outside of TorchTrainer.
def test_torch_trainer_crash(ray_start_10_cpus_shared):
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    ray.data.DataContext.get_current().execution_options.verbose_progress = True

    train_ds = ray.data.range_tensor(100)
    train_ds = train_ds.materialize()

    def train_loop_per_worker():
        it = train.get_dataset_shard("train")
        for i in range(2):
            count = 0
            for batch in it.iter_batches():
                count += len(batch["data"])
            assert count == 50

    my_trainer = TorchTrainer(
        train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
        datasets={"train": train_ds},
    )
    my_trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
