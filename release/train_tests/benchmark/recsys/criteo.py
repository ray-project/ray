from typing import Dict, List

import numpy as np

import ray.data
from ray.data import DataContext
from ray.data.context import ShuffleStrategy


INT_FEATURE_COUNT = 13
CAT_FEATURE_COUNT = 26
DAYS = 24
DEFAULT_LABEL_NAME = "label"
DEFAULT_INT_NAMES: List[str] = [f"int_{idx}" for idx in range(INT_FEATURE_COUNT)]
DEFAULT_CAT_NAMES: List[str] = [f"cat_{idx}" for idx in range(CAT_FEATURE_COUNT)]
DEFAULT_COLUMN_NAMES: List[str] = [
    DEFAULT_LABEL_NAME,
    *DEFAULT_INT_NAMES,
    *DEFAULT_CAT_NAMES,
]


def fill_missing(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """Fill in missing feature values with defaults.
    Default to 0 for dense features, empty string "" for categorical features.
    """
    for feature_name in DEFAULT_INT_NAMES:
        batch[feature_name] = np.nan_to_num(batch[feature_name], nan=0)
        # batch[feature_name] = batch[feature_name].astype(np.int32)
    for feature_name in DEFAULT_CAT_NAMES:
        features = batch[feature_name]
        features[features == None] = ""
    return batch


def concat_and_normalize_dense_features(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """Concatenate dense and sparse features together.
    Apply log transformation to dense features."""

    out = {}

    out["dense"] = np.column_stack([batch[feature_name] for feature_name in DEFAULT_INT_NAMES])
    out["sparse"] = np.column_stack([batch[feature_name] for feature_name in DEFAULT_CAT_NAMES])

    out["dense"] += 3  # Prevent log(0)
    out["dense"] = np.log(out["dense"], dtype=np.float32)
    out["label"] = batch["label"]

    return out


def convert_to_torchrec_batch_format(batch: Dict[str, np.ndarray]):
    """Convert to a Batch, packaging sparse features as a KJT."""
    import torch

    from torchrec.datasets.utils import Batch
    from torchrec.sparse.jagged_tensor import KeyedJaggedTensor

    dense = batch["dense"]
    sparse = batch["sparse"]
    labels = batch["label"]

    batch_size = len(dense)
    lengths = torch.ones((batch_size * CAT_FEATURE_COUNT,), dtype=torch.int32)
    offsets = torch.arange(0, batch_size * CAT_FEATURE_COUNT + 1, dtype=torch.int32)
    length_per_key: List[int] = [batch_size] * CAT_FEATURE_COUNT
    offset_per_key = [batch_size * i for i in range(CAT_FEATURE_COUNT + 1)]
    index_per_key = {key: i for i, key in enumerate(DEFAULT_CAT_NAMES)}

    # if batch_size == self.batch_size:
    #     length_per_key = self.length_per_key
    #     offset_per_key = self.offset_per_key
    # else:
    #     # handle last batch in dataset when it's an incomplete batch.
    #     length_per_key = CAT_FEATURE_COUNT * [batch_size]
    #     offset_per_key = [batch_size * i for i in range(CAT_FEATURE_COUNT + 1)]

    return Batch(
        dense_features=torch.from_numpy(dense),
        sparse_features=KeyedJaggedTensor(
            keys=DEFAULT_CAT_NAMES,
            # transpose().reshape(-1) introduces a copy
            values=torch.from_numpy(sparse.transpose(1, 0).reshape(-1)),
            lengths=lengths,
            offsets=offsets,
            stride=batch_size,
            length_per_key=length_per_key,
            offset_per_key=offset_per_key,
            index_per_key=index_per_key,
        ),
        labels=torch.from_numpy(labels.reshape(-1)),
    )

def get_ray_dataset(stage: str = "train"):
    ctx = DataContext.get_current()

    # Enable hash-shuffling by default for aggregations and repartitions
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    stage_to_path = {
        "train": "s3://ray-benchmark-data-internal/criteo/tsv.gz/train/day_0_aa.tsv.gz",
        # "train": "s3://ray-benchmark-data-internal/criteo/tsv.gz/train/",
        "valid": "s3://ray-benchmark-data-internal/criteo/tsv.gz/valid/",
        "test": "s3://ray-benchmark-data-internal/criteo/tsv.gz/test/",
    }
    ds_path = stage_to_path[stage]

    import pyarrow.csv

    ds = ray.data.read_csv(
        ds_path,
        read_options=pyarrow.csv.ReadOptions(column_names=DEFAULT_COLUMN_NAMES),
        parse_options=pyarrow.csv.ParseOptions(delimiter="\t"),
        override_num_blocks=32,
        shuffle="files",  # coarse file-level shuffle
    )

    # Clean data.
    ds = ds.map_batches(fill_missing)
    
    # Convert categorical features to integers.

    # Option 1: Use the built-in preprocessor
    # from ray.data.preprocessors import OrdinalEncoder
    # categorical_to_indices_preprocessor = OrdinalEncoder(columns=DEFAULT_CAT_NAMES)
    # ds = categorical_to_indices_preprocessor.fit_transform(ds)

    # Option 2: groupby() w/ Hash-based shuffle
    # FREQUENCY_THRESHOLD = 3
    # LOW_FREQUENCY_INDEX = 1  # map low frequency values -> 1
    # CATEGORICAL_VALUE_TO_INDEX = {}
    # for cat_feature in DEFAULT_CAT_NAMES:
    #     print("Calculating value counts for:", cat_feature)
    #     value_counts = [
    #         (group["count()"], group[cat_feature])
    #         for group in ds.groupby(key=cat_feature).count().take_all()
    #         if group["count()"] >= FREQUENCY_THRESHOLD
    #     ]
    #     value_counts.sort(reverse=True)  # probably unnecessary
    #     print(value_counts[:20])
    #     CATEGORICAL_VALUE_TO_INDEX[cat_feature] = {val: i for i, (_, val) in enumerate(value_counts, start=2)}

    # def categorical_values_to_indices(batch: Dict[str, np.ndarray], mapping: Dict[str, int]):
    #     for cat_feature in DEFAULT_CAT_NAMES:
    #         batch[cat_feature] = np.vectorize(
    #             lambda k: mapping.get(k, LOW_FREQUENCY_INDEX)
    #         )(batch[cat_feature])
    #     return batch
    # ds = ds.map_batches(categorical_values_to_indices, fn_args=(CATEGORICAL_VALUE_TO_INDEX,))

    # HACK: Dummy encoding for quicker testing.
    def dummy_categorical_encoder(batch):
        for feature_name in DEFAULT_CAT_NAMES:
            batch[feature_name] = np.random.randint(0, 3, size=(len(batch[feature_name]),))
        return batch
    ds = ds.map_batches(dummy_categorical_encoder)

    ds = ds.map_batches(concat_and_normalize_dense_features)

    # TODO: Need to shuffle the data.

    return ds



if __name__ == "__main__":
    ds = get_ray_dataset()

    data_iterator = ds.iter_torch_batches(
        batch_size=32,
        drop_last=True,
        collate_fn=convert_to_torchrec_batch_format,
    )
    # TODO: Augment data with synthetic multi-hot data.

    batch = next(iter(data_iterator))
