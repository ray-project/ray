import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.extensions.tensor_extension import TensorArray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def maybe_pipeline(ds, enabled):
    if enabled:
        return ds.window(blocks_per_window=1)
    else:
        return ds


@pytest.mark.parametrize("pipelined", [False, True])
def test_to_torch(ray_start_regular_shared, pipelined):
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
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(label_column="label", batch_size=3)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in iter(torchd):
            iterations.append(torch.cat((batch[0], batch[1]), dim=1).numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(np.sort(df.values), np.sort(combined_iterations))


@pytest.mark.parametrize("input", ["single", "list", "dict"])
@pytest.mark.parametrize("force_dtype", [False, True])
@pytest.mark.parametrize("label_type", [None, "squeezed", "unsqueezed"])
def test_to_torch_feature_columns(
    ray_start_regular_shared, input, force_dtype, label_type
):
    import torch

    df1 = pd.DataFrame(
        {
            "one": [1, 2, 3],
            "two": [1.0, 2.0, 3.0],
            "three": [4.0, 5.0, 6.0],
            "label": [1.0, 2.0, 3.0],
        }
    )
    df2 = pd.DataFrame(
        {
            "one": [4, 5, 6],
            "two": [4.0, 5.0, 6.0],
            "three": [7.0, 8.0, 9.0],
            "label": [4.0, 5.0, 6.0],
        }
    )
    df3 = pd.DataFrame(
        {"one": [7, 8], "two": [7.0, 8.0], "three": [10.0, 11.0], "label": [7.0, 8.0]}
    )
    df = pd.concat([df1, df2, df3]).drop("three", axis=1)
    ds = ray.data.from_pandas([df1, df2, df3])

    feature_column_dtypes = None
    label_column_dtype = None
    if force_dtype:
        label_column_dtype = torch.long
    if input == "single":
        feature_columns = ["one", "two"]
        if force_dtype:
            feature_column_dtypes = torch.long
    elif input == "list":
        feature_columns = [["one"], ["two"]]
        if force_dtype:
            feature_column_dtypes = [torch.long, torch.long]
    elif input == "dict":
        feature_columns = {"X1": ["one"], "X2": ["two"]}
        if force_dtype:
            feature_column_dtypes = {"X1": torch.long, "X2": torch.long}

    label_column = None if label_type is None else "label"
    unsqueeze_label_tensor = label_type == "unsqueezed"

    torchd = ds.to_torch(
        label_column=label_column,
        feature_columns=feature_columns,
        feature_column_dtypes=feature_column_dtypes,
        label_column_dtype=label_column_dtype,
        unsqueeze_label_tensor=unsqueeze_label_tensor,
        batch_size=3,
    )
    iterations = []

    for batch in iter(torchd):
        features, label = batch

        if input == "single":
            assert isinstance(features, torch.Tensor)
            if force_dtype:
                assert features.dtype == torch.long
            data = features
        elif input == "list":
            assert isinstance(features, list)
            assert all(isinstance(item, torch.Tensor) for item in features)
            if force_dtype:
                assert all(item.dtype == torch.long for item in features)
            data = torch.cat(tuple(features), dim=1)
        elif input == "dict":
            assert isinstance(features, dict)
            assert all(isinstance(item, torch.Tensor) for item in features.values())
            if force_dtype:
                assert all(item.dtype == torch.long for item in features.values())
            data = torch.cat(tuple(features.values()), dim=1)

        if not label_type:
            assert label is None
        else:
            assert isinstance(label, torch.Tensor)
            if force_dtype:
                assert label.dtype == torch.long
            if unsqueeze_label_tensor:
                assert label.dim() == 2
            else:
                assert label.dim() == 1
                label = label.view(-1, 1)
            data = torch.cat((data, label), dim=1)
        iterations.append(data.numpy())

    combined_iterations = np.concatenate(iterations)
    if not label_type:
        df.drop("label", axis=1, inplace=True)
    np.testing.assert_array_equal(df.values, combined_iterations)


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch(ray_start_regular_shared, pipelined):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {"one": TensorArray(arr), "two": TensorArray(arr + 1), "label": [1.0, 2.0, 3.0]}
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": TensorArray(arr2 + 1),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label", batch_size=2, unsqueeze_label_tensor=False
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        features, labels = [], []
        for batch in iter(torchd):
            features.append(batch[0].numpy())
            labels.append(batch[1].numpy())
        features, labels = np.concatenate(features), np.concatenate(labels)
        values = np.stack([df["one"].to_numpy(), df["two"].to_numpy()], axis=1)
        np.testing.assert_array_equal(values, features)
        np.testing.assert_array_equal(df["label"].to_numpy(), labels)


@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch_mix(ray_start_regular_shared, pipelined):
    outer_dim = 3
    inner_shape = (2, 2, 2)
    shape = (outer_dim,) + inner_shape
    num_items = np.prod(np.array(shape))
    arr = np.arange(num_items).reshape(shape)
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arr),
            "two": [1, 2, 3],
            "label": [1.0, 2.0, 3.0],
        }
    )
    arr2 = np.arange(num_items, 2 * num_items).reshape(shape)
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arr2),
            "two": [4, 5, 6],
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label",
        feature_columns=[["one"], ["two"]],
        batch_size=2,
        unsqueeze_label_tensor=False,
        unsqueeze_feature_tensors=False,
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        col1, col2, labels = [], [], []
        for batch in iter(torchd):
            col1.append(batch[0][0].numpy())
            col2.append(batch[0][1].numpy())
            labels.append(batch[1].numpy())
        col1, col2 = np.concatenate(col1), np.concatenate(col2)
        labels = np.concatenate(labels)
        np.testing.assert_array_equal(col1, np.sort(df["one"].to_numpy()))
        np.testing.assert_array_equal(col2, np.sort(df["two"].to_numpy()))
        np.testing.assert_array_equal(labels, np.sort(df["label"].to_numpy()))


@pytest.mark.skip(
    reason=(
        "Waiting for Torch to support unsqueezing and concatenating nested tensors."
    )
)
@pytest.mark.parametrize("pipelined", [False, True])
def test_tensors_in_tables_to_torch_variable_shaped(
    ray_start_regular_shared, pipelined
):
    shapes = [(2, 2), (3, 3), (4, 4)]
    cumsum_sizes = np.cumsum([0] + [np.prod(shape) for shape in shapes[:-1]])
    arrs1 = [
        np.arange(offset, offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    df1 = pd.DataFrame(
        {
            "one": TensorArray(arrs1),
            "two": TensorArray([a + 1 for a in arrs1]),
            "label": [1.0, 2.0, 3.0],
        }
    )
    base = cumsum_sizes[-1]
    arrs2 = [
        np.arange(base + offset, base + offset + np.prod(shape)).reshape(shape)
        for offset, shape in zip(cumsum_sizes, shapes)
    ]
    df2 = pd.DataFrame(
        {
            "one": TensorArray(arrs2),
            "two": TensorArray([a + 1 for a in arrs2]),
            "label": [4.0, 5.0, 6.0],
        }
    )
    df = pd.concat([df1, df2])
    ds = ray.data.from_pandas([df1, df2])
    ds = maybe_pipeline(ds, pipelined)
    torchd = ds.to_torch(
        label_column="label", batch_size=2, unsqueeze_label_tensor=False
    )

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        features, labels = [], []
        for batch in iter(torchd):
            features.append(batch[0].numpy())
            labels.append(batch[1].numpy())
        features, labels = np.concatenate(features), np.concatenate(labels)
        values = np.stack([df["one"].to_numpy(), df["two"].to_numpy()], axis=1)
        np.testing.assert_array_equal(values, features)
        np.testing.assert_array_equal(df["label"].to_numpy(), labels)


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_torch_batches(ray_start_regular_shared, pipelined):
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
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
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


@pytest.mark.parametrize("pipelined", [False, True])
def test_iter_torch_batches_tensor_ds(ray_start_regular_shared, pipelined):
    arr1 = np.arange(12).reshape((3, 2, 2))
    arr2 = np.arange(12, 24).reshape((3, 2, 2))
    arr = np.concatenate((arr1, arr2))
    ds = ray.data.from_numpy([arr1, arr2])
    ds = maybe_pipeline(ds, pipelined)

    num_epochs = 1 if pipelined else 2
    for _ in range(num_epochs):
        iterations = []
        for batch in ds.iter_torch_batches(batch_size=2):
            iterations.append(batch["data"].numpy())
        combined_iterations = np.concatenate(iterations)
        np.testing.assert_array_equal(arr, combined_iterations)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
