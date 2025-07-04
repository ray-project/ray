import logging
import os
from typing import TYPE_CHECKING, Dict, List, Tuple

import boto3
import json
import numpy as np
import pyarrow.csv

import ray.data

from constants import DatasetKey

if TYPE_CHECKING:
    from torchrec.datasets.utils import Batch

logger = logging.getLogger(__name__)


S3_BUCKET = "ray-benchmark-data-internal-us-west-2"
CRITEO_S3_URI = f"s3://{S3_BUCKET}/criteo/tsv.gz"
CAT_FEATURE_VALUE_COUNT_JSON_PATH_PATTERN = (
    "criteo/tsv.gz/categorical_feature_value_counts/{}-value_counts.json"
)


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
CRITEO_NUM_EMBEDDINGS_PER_FEATURE: List[int] = [
    45833188,
    36746,
    17245,
    7413,
    20243,
    3,
    7114,
    1441,
    62,
    29275261,
    1572176,
    345138,
    10,
    2209,
    11267,
    128,
    4,
    974,
    14,
    48937457,
    11316796,
    40094537,
    452104,
    12606,
    104,
    35,
]

DATASET_PATHS = {
    DatasetKey.TRAIN: f"{CRITEO_S3_URI}/train",
    DatasetKey.VALID: f"{CRITEO_S3_URI}/valid",
    DatasetKey.TEST: f"{CRITEO_S3_URI}/test",
}


def fill_missing(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """Fill in missing feature values with defaults.
    Default to 0 for dense features, empty string "" for categorical features.
    """
    for feature_name in DEFAULT_INT_NAMES:
        batch[feature_name] = np.nan_to_num(batch[feature_name], nan=0)
    for feature_name in DEFAULT_CAT_NAMES:
        features = batch[feature_name]
        features[np.equal(features, None)] = ""
    return batch


def concat_and_normalize_dense_features(
    batch: Dict[str, np.ndarray],
) -> Dict[str, np.ndarray]:
    """Concatenate dense and sparse features together.
    Apply log transformation to dense features."""

    out = {}

    out["dense"] = np.column_stack(
        [batch[feature_name] for feature_name in DEFAULT_INT_NAMES]
    )
    out["sparse"] = np.column_stack(
        [batch[feature_name] for feature_name in DEFAULT_CAT_NAMES]
    )

    out["dense"] += 3  # Prevent log(0)
    out["dense"] = np.log(out["dense"], dtype=np.float32)
    out["label"] = batch["label"]

    return out


def mock_dataloader(num_batches: int, batch_size: int):
    """Creates a dummy batch of size `batch_size` and yields it `num_batches` times."""
    dense = np.random.randn(batch_size, INT_FEATURE_COUNT).astype(np.float32)
    sparse = np.random.randint(
        1,
        np.array(CRITEO_NUM_EMBEDDINGS_PER_FEATURE),
        (batch_size, CAT_FEATURE_COUNT),
    ).astype(np.int32)
    labels = np.random.randint(0, 1, (batch_size,)).astype(np.int32)
    batch = convert_to_torchrec_batch_format(
        {"dense": dense, "sparse": sparse, "label": labels}
    )
    batch = batch.pin_memory()

    for _ in range(num_batches):
        yield batch


def convert_to_torchrec_batch_format(batch: Dict[str, np.ndarray]) -> "Batch":
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

    # Handle partial batches (last batch).
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


def read_json_from_s3(bucket_name, key):
    s3 = boto3.client("s3")

    # Download object content
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response["Body"].read().decode("utf-8")

    # Parse JSON
    data = json.loads(content)
    return data


def _get_base_dataset(stage: DatasetKey = DatasetKey.TRAIN):
    ds_path = DATASET_PATHS[stage]

    ds = ray.data.read_csv(
        ds_path,
        read_options=pyarrow.csv.ReadOptions(column_names=DEFAULT_COLUMN_NAMES),
        parse_options=pyarrow.csv.ParseOptions(delimiter="\t"),
        shuffle=(
            "files" if stage == DatasetKey.TRAIN else None
        ),  # coarse file-level shuffle
    )
    return ds


def get_ray_dataset(stage: DatasetKey = DatasetKey.TRAIN):
    ds = _get_base_dataset(stage)

    # Convert categorical features to integers.

    # Fetch cached value counts instead of "fitting" the preprocessor from scratch.
    COMPUTE_VALUE_COUNTS_FROM_SCRATCH: bool = False

    FREQUENCY_THRESHOLD = 3
    LOW_FREQUENCY_INDEX = 1  # map low frequency values -> 1
    categorical_value_to_index = {}
    for cat_feature in DEFAULT_CAT_NAMES:
        if COMPUTE_VALUE_COUNTS_FROM_SCRATCH:
            value_counts = _compute_value_counts(ds, cat_feature)
        else:
            json_filepath = CAT_FEATURE_VALUE_COUNT_JSON_PATH_PATTERN.format(
                cat_feature
            )
            logger.info(f"Downloading value counts file: {json_filepath}")
            value_counts = read_json_from_s3(bucket_name=S3_BUCKET, key=json_filepath)

        value_counts = filter(lambda x: x[1] >= FREQUENCY_THRESHOLD, value_counts)
        categorical_value_to_index[cat_feature] = {
            val: i for i, (val, _) in enumerate(value_counts, start=2)
        }

    # TODO: This will not scale well for the full dataset, since this dict might be 10s of GBs,
    # which is expensive to copy to each map task.
    # This mapping is large, so put a shared copy in the object store for all the map tasks to use.
    categorical_value_to_index_ref = ray.put(categorical_value_to_index)

    # Clean data.
    ds = ds.map_batches(fill_missing)

    def categorical_values_to_indices(
        batch: Dict[str, np.ndarray], mapping_ref: ray.ObjectRef
    ):
        mapping: Dict[str, int] = ray.get(mapping_ref)
        for cat_feature in DEFAULT_CAT_NAMES:
            batch[cat_feature] = np.vectorize(
                lambda k: mapping.get(cat_feature, {}).get(k, LOW_FREQUENCY_INDEX)
            )(batch[cat_feature])
        return batch

    ds = ds.map_batches(
        categorical_values_to_indices, fn_args=(categorical_value_to_index_ref,)
    )

    # HACK: Dummy encoding for quicker testing.
    # def dummy_categorical_encoder(batch):
    #     for feature_name in DEFAULT_CAT_NAMES:
    #         batch[feature_name] = np.random.randint(0, 3, size=(len(batch[feature_name]),))
    #     return batch
    # ds = ds.map_batches(dummy_categorical_encoder)

    ds = ds.map_batches(concat_and_normalize_dense_features)

    # TODO: Need to shuffle the data.

    return ds


def _compute_value_counts(ds, feature_name) -> List[Tuple]:
    logger.info(f"Computing value counts for: {feature_name}")

    # TODO: This needs to be optimized in order to run on the full dataset.
    # Need to fill missing values with empty string.
    value_counts = [
        (
            group[feature_name] if group[feature_name] is not None else "",
            group["count()"],
        )
        for group in (
            ds.select_columns(feature_name).groupby(key=feature_name).count().take_all()
        )
    ]

    return value_counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    ds = _get_base_dataset(stage="train")

    # Create a directory for the value counts files.
    save_dir = "/mnt/cluster_storage/criteo"
    os.makedirs(save_dir, exist_ok=True)

    for cat_feature in DEFAULT_CAT_NAMES:
        value_counts = _compute_value_counts(ds, cat_feature)

        json_filepath = os.path.join(save_dir, f"{cat_feature}-value_counts.json")
        logger.info(f"Writing value counts to: {json_filepath}")

        with open(json_filepath, "w") as f:
            json.dump(value_counts, f)
