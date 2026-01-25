#!/usr/bin/env python

# Copyright 2024 The HuggingFace Inc. team. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import contextlib
import importlib.resources
import json
import logging
from collections import deque
from collections.abc import Iterable, Iterator
from pathlib import Path
from pprint import pformat
from typing import Any, Generic, TypeVar

import datasets
import numpy as np
import packaging.version
import pandas
import pandas as pd
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import torch
from datasets import Dataset
from datasets.table import embed_table_storage
from datasets.utils.logging import disable_progress_bar, enable_progress_bar
from huggingface_hub import DatasetCard, DatasetCardData, HfApi
from huggingface_hub.errors import RevisionNotFoundError
from PIL import Image as PILImage
from torchvision import transforms

DEFAULT_CHUNK_SIZE = 1000  # Max number of files per chunk
DEFAULT_DATA_FILE_SIZE_IN_MB = 100  # Max size per file
DEFAULT_VIDEO_FILE_SIZE_IN_MB = 200  # Max size per file

INFO_PATH = "meta/info.json"
STATS_PATH = "meta/stats.json"

EPISODES_DIR = "meta/episodes"
DATA_DIR = "data"
VIDEO_DIR = "videos"

CHUNK_FILE_PATTERN = "chunk-{chunk_index:03d}/file-{file_index:03d}"
DEFAULT_TASKS_PATH = "meta/tasks.parquet"
DEFAULT_EPISODES_PATH = EPISODES_DIR + "/" + CHUNK_FILE_PATTERN + ".parquet"
DEFAULT_DATA_PATH = DATA_DIR + "/" + CHUNK_FILE_PATTERN + ".parquet"
DEFAULT_VIDEO_PATH = VIDEO_DIR + "/{video_key}/" + CHUNK_FILE_PATTERN + ".mp4"
DEFAULT_IMAGE_PATH = (
    "images/{image_key}/episode-{episode_index:06d}/frame-{frame_index:06d}.png"
)

# LEGACY_EPISODES_PATH = "meta/episodes.jsonl"
# LEGACY_EPISODES_STATS_PATH = "meta/episodes_stats.jsonl"
# LEGACY_TASKS_PATH = "meta/tasks.jsonl"

DEFAULT_FEATURES = {
    "timestamp": {"dtype": "float32", "shape": (1,), "names": None},
    "frame_index": {"dtype": "int64", "shape": (1,), "names": None},
    "episode_index": {"dtype": "int64", "shape": (1,), "names": None},
    "index": {"dtype": "int64", "shape": (1,), "names": None},
    "task_index": {"dtype": "int64", "shape": (1,), "names": None},
}

T = TypeVar("T")


class SuppressProgressBars:
    """
    Context manager to suppress progress bars.

    Example
    --------
    ```python
    with SuppressProgressBars():
        # Code that would normally show progress bars
    ```
    """

    def __enter__(self):
        disable_progress_bar()

    def __exit__(self, exc_type, exc_val, exc_tb):
        enable_progress_bar()


def get_parquet_file_size_in_mb(parquet_path: str | Path) -> float:
    metadata = pq.read_metadata(parquet_path)
    total_uncompressed_size = 0
    for row_group in range(metadata.num_row_groups):
        rg_metadata = metadata.row_group(row_group)
        for column in range(rg_metadata.num_columns):
            col_metadata = rg_metadata.column(column)
            total_uncompressed_size += col_metadata.total_uncompressed_size
    return total_uncompressed_size / (1024**2)


def get_hf_dataset_size_in_mb(hf_ds: Dataset) -> int:
    return hf_ds.data.nbytes // (1024**2)


def update_chunk_file_indices(
    chunk_idx: int, file_idx: int, chunks_size: int
) -> tuple[int, int]:
    if file_idx == chunks_size - 1:
        file_idx = 0
        chunk_idx += 1
    else:
        file_idx += 1
    return chunk_idx, file_idx


def load_nested_dataset(
    pq_dir: Path,
    features: datasets.Features | None = None,
    episodes: list[int] | None = None,
) -> Dataset:
    """Find parquet files in provided directory {pq_dir}/chunk-xxx/file-xxx.parquet
    Convert parquet files to pyarrow memory mapped in a cache folder for efficient RAM usage
    Concatenate all pyarrow references to return HF Dataset format

    Args:
        pq_dir: Path to directory containing parquet files
        features: Optional features schema to ensure consistent loading of complex types like images
        episodes: Optional list of episode indices to filter. Uses PyArrow predicate pushdown for efficiency.
    Returns:
        Dataset: The loaded dataset.
    """
    paths = sorted(pq_dir.glob("*/*.parquet"))
    if len(paths) == 0:
        raise FileNotFoundError(
            f"Provided directory does not contain any parquet file: {pq_dir}"
        )

    with SuppressProgressBars():
        # When no filtering needed, load using PyArrow first to avoid old metadata issues
        # PyArrow loads the dataset, then we create HF Dataset without the embedded metadata
        if episodes is None:
            # Load with PyArrow to avoid parsing old 'List' feature types in embedded metadata
            arrow_dataset = pa_ds.dataset(paths, format="parquet")
            table = arrow_dataset.to_table()

            if features is not None:
                table = table.cast(features.arrow_schema)

            # Remove embedded HuggingFace metadata that may contain old 'List' feature types
            # This prevents Dataset.__init__ from trying to parse outdated feature definitions
            table = table.replace_schema_metadata(None)

            return Dataset(table)

        arrow_dataset = pa_ds.dataset(paths, format="parquet")
        filter_expr = pa_ds.field("episode_index").isin(episodes)
        table = arrow_dataset.to_table(filter=filter_expr)

        if features is not None:
            table = table.cast(features.arrow_schema)

        # Remove embedded HuggingFace metadata that may contain old 'List' feature types
        table = table.replace_schema_metadata(None)

        return Dataset(table)


def get_parquet_num_frames(parquet_path: str | Path) -> int:
    metadata = pq.read_metadata(parquet_path)
    return metadata.num_rows


def get_file_size_in_mb(file_path: Path) -> float:
    """Get file size on disk in megabytes.

    Args:
        file_path: The path to the file.

    Returns:
        float: The size of the file in megabytes.
    """
    file_size_bytes = file_path.stat().st_size
    return file_size_bytes / (1024**2)


def flatten_dict(d: dict, parent_key: str = "", sep: str = "/") -> dict:
    """Flatten a nested dictionary by joining keys with a separator.

    Example:
        >>> dct = {"a": {"b": 1, "c": {"d": 2}}, "e": 3}
        >>> print(flatten_dict(dct))
        {'a/b': 1, 'a/c/d': 2, 'e': 3}

    Args:
        d: The dictionary to flatten.
        parent_key: The base key to prepend to the keys in this level.
        sep: The separator to use between keys.

    Returns:
        dict: A flattened dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def unflatten_dict(d: dict, sep: str = "/") -> dict:
    """Unflatten a dictionary with delimited keys into a nested dictionary.

    Example:
        >>> flat_dct = {"a/b": 1, "a/c/d": 2, "e": 3}
        >>> print(unflatten_dict(flat_dct))
        {'a': {'b': 1, 'c': {'d': 2}}, 'e': 3}

    Args:
        d: A dictionary with flattened keys.
        sep: The separator used in the keys.

    Returns:
        dict: A nested dictionary.
    """
    outdict = {}
    for key, value in d.items():
        parts = key.split(sep)
        d = outdict
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value
    return outdict


def serialize_dict(stats: dict[str, torch.Tensor | np.ndarray | dict]) -> dict:
    """Serialize a dictionary containing tensors or numpy arrays to be JSON-compatible.

    Converts torch.Tensor, np.ndarray, and np.generic types to lists or native Python types.

    Args:
        stats: A dictionary that may contain non-serializable numeric types.

    Returns:
        dict: A dictionary with all values converted to JSON-serializable types.

    Raises:
        NotImplementedError: If a value has an unsupported type.
    """
    serialized_dict = {}
    for key, value in flatten_dict(stats).items():
        if isinstance(value, (torch.Tensor | np.ndarray)):
            serialized_dict[key] = value.tolist()
        elif isinstance(value, list) and isinstance(value[0], (int | float | list)):
            serialized_dict[key] = value
        elif isinstance(value, np.generic):
            serialized_dict[key] = value.item()
        elif isinstance(value, (int | float)):
            serialized_dict[key] = value
        else:
            raise NotImplementedError(
                f"The value '{value}' of type '{type(value)}' is not supported."
            )
    return unflatten_dict(serialized_dict)


def embed_images(dataset: datasets.Dataset) -> datasets.Dataset:
    """Embed image bytes into the dataset table before saving to Parquet.

    This function prepares a Hugging Face dataset for serialization by converting
    image objects into an embedded format that can be stored in Arrow/Parquet.

    Args:
        dataset: The input dataset, possibly containing image features.

    Returns:
        datasets.Dataset: The dataset with images embedded in the table storage.
    """
    # Embed image bytes into the table before saving to parquet
    format = dataset.format
    dataset = dataset.with_format("arrow")
    dataset = dataset.map(embed_table_storage, batched=False)
    dataset = dataset.with_format(**format)
    return dataset


def load_json(fpath: Path) -> Any:
    """Load data from a JSON file.

    Args:
        fpath: Path to the JSON file.

    Returns:
        Any: The data loaded from the JSON file.
    """
    with open(fpath) as f:
        return json.load(f)


def write_json(data: dict, fpath: Path) -> None:
    """Write data to a JSON file.

    Creates parent directories if they don't exist.

    Args:
        data: The dictionary to write.
        fpath: The path to the output JSON file.
    """
    fpath.parent.mkdir(exist_ok=True, parents=True)
    with open(fpath, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def write_info(info: dict, local_dir: Path) -> None:
    write_json(info, local_dir / INFO_PATH)


def load_info(local_dir: Path) -> dict:
    """Load dataset info metadata from its standard file path.

    Also converts shape lists to tuples for consistency.

    Args:
        local_dir: The root directory of the dataset.

    Returns:
        dict: The dataset information dictionary.
    """
    info = load_json(local_dir / INFO_PATH)
    for ft in info["features"].values():
        ft["shape"] = tuple(ft["shape"])
    return info


def write_stats(stats: dict, local_dir: Path) -> None:
    """Serialize and write dataset statistics to their standard file path.

    Args:
        stats: The statistics dictionary (can contain tensors/numpy arrays).
        local_dir: The root directory of the dataset.
    """
    serialized_stats = serialize_dict(stats)
    write_json(serialized_stats, local_dir / STATS_PATH)


def cast_stats_to_numpy(stats: dict) -> dict[str, dict[str, np.ndarray]]:
    """Recursively cast numerical values in a stats dictionary to numpy arrays.

    Args:
        stats: The statistics dictionary.

    Returns:
        dict: The statistics dictionary with values cast to numpy arrays.
    """
    stats = {key: np.array(value) for key, value in flatten_dict(stats).items()}
    return unflatten_dict(stats)


def load_stats(local_dir: Path) -> dict[str, dict[str, np.ndarray]] | None:
    """Load dataset statistics and cast numerical values to numpy arrays.

    Returns None if the stats file doesn't exist.

    Args:
        local_dir: The root directory of the dataset.

    Returns:
        A dictionary of statistics or None if the file is not found.
    """
    if not (local_dir / STATS_PATH).exists():
        return None
    stats = load_json(local_dir / STATS_PATH)
    return cast_stats_to_numpy(stats)


def write_tasks(tasks: pandas.DataFrame, local_dir: Path) -> None:
    path = local_dir / DEFAULT_TASKS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tasks.to_parquet(path)


def load_tasks(local_dir: Path) -> pandas.DataFrame:
    tasks = pd.read_parquet(local_dir / DEFAULT_TASKS_PATH)
    return tasks


def write_episodes(episodes: Dataset, local_dir: Path) -> None:
    """Write episode metadata to a parquet file in the LeRobot v3.0 format.
    This function writes episode-level metadata to a single parquet file.
    Used primarily during dataset conversion (v2.1 → v3.0) and in test fixtures.

    Args:
        episodes: HuggingFace Dataset containing episode metadata
        local_dir: Root directory where the dataset will be stored
    """
    episode_size_mb = get_hf_dataset_size_in_mb(episodes)
    if episode_size_mb > DEFAULT_DATA_FILE_SIZE_IN_MB:
        raise NotImplementedError(
            f"Episodes dataset is too large ({episode_size_mb} MB) to write to a single file. "
            f"The current limit is {DEFAULT_DATA_FILE_SIZE_IN_MB} MB. "
            "This function only supports single-file episode metadata. "
        )

    fpath = local_dir / DEFAULT_EPISODES_PATH.format(chunk_index=0, file_index=0)
    fpath.parent.mkdir(parents=True, exist_ok=True)
    episodes.to_parquet(fpath)


def load_episodes(local_dir: Path) -> datasets.Dataset:
    episodes = load_nested_dataset(local_dir / EPISODES_DIR)
    # Select episode features/columns containing references to episode data and videos
    # (e.g. tasks, dataset_from_index, dataset_to_index, data/chunk_index, data/file_index, etc.)
    # This is to speedup access to these data, instead of having to load episode stats.
    episodes = episodes.select_columns(
        [key for key in episodes.features if not key.startswith("stats/")]
    )
    return episodes


def load_image_as_numpy(
    fpath: str | Path, dtype: np.dtype = np.float32, channel_first: bool = True
) -> np.ndarray:
    """Load an image from a file into a numpy array.

    Args:
        fpath: Path to the image file.
        dtype: The desired data type of the output array. If floating,
            pixels are scaled to [0, 1].
        channel_first: If True, converts the image to (C, H, W) format.
            Otherwise, it remains in (H, W, C) format.

    Returns:
        np.ndarray: The image as a numpy array.
    """
    img = PILImage.open(fpath).convert("RGB")
    img_array = np.array(img, dtype=dtype)
    if channel_first:  # (H, W, C) -> (C, H, W)
        img_array = np.transpose(img_array, (2, 0, 1))
    if np.issubdtype(dtype, np.floating):
        img_array /= 255.0
    return img_array


def hf_transform_to_torch(
    items_dict: dict[str, list[Any]],
) -> dict[str, list[torch.Tensor | str]]:
    """Convert a batch from a Hugging Face dataset to torch tensors.

    This transform function converts items from Hugging Face dataset format (pyarrow)
    to torch tensors. Importantly, images are converted from PIL objects (H, W, C, uint8)
    to a torch image representation (C, H, W, float32) in the range [0, 1]. Other
    types are converted to torch.tensor.

    Args:
        items_dict: A dictionary representing a batch of data from a
            Hugging Face dataset.

    Returns:
        dict: The batch with items converted to torch tensors.
    """
    for key in items_dict:
        first_item = items_dict[key][0]
        if isinstance(first_item, PILImage.Image):
            to_tensor = transforms.ToTensor()
            items_dict[key] = [to_tensor(img) for img in items_dict[key]]
        elif first_item is None:
            pass
        else:
            items_dict[key] = [
                x if isinstance(x, str) else torch.tensor(x) for x in items_dict[key]
            ]
    return items_dict


def is_valid_version(version: str) -> bool:
    """Check if a string is a valid PEP 440 version.

    Args:
        version: The version string to check.

    Returns:
        bool: True if the version string is valid, False otherwise.
    """
    try:
        packaging.version.parse(version)
        return True
    except packaging.version.InvalidVersion:
        return False


def check_version_compatibility(
    repo_id: str,
    version_to_check: str | packaging.version.Version,
    current_version: str | packaging.version.Version,
    enforce_breaking_major: bool = True,
) -> None:
    """Check for version compatibility between a dataset and the current codebase.

    Args:
        repo_id: The repository ID for logging purposes.
        version_to_check: The version of the dataset.
        current_version: The current version of the codebase.
        enforce_breaking_major: If True, raise an error on major version mismatch.

    Raises:
        BackwardCompatibilityError: If the dataset version is from a newer, incompatible
            major version of the codebase.
    """
    v_check = (
        packaging.version.parse(version_to_check)
        if not isinstance(version_to_check, packaging.version.Version)
        else version_to_check
    )
    v_current = (
        packaging.version.parse(current_version)
        if not isinstance(current_version, packaging.version.Version)
        else current_version
    )
    if v_check.major < v_current.major and enforce_breaking_major:
        raise NotImplementedError(
            "Not implemented"
        )  # BackwardCompatibilityError(repo_id, v_check)
    elif v_check.minor < v_current.minor:
        logging.warning(
            NotImplementedError("Not implemented")
        )  # FUTURE_MESSAGE.format(repo_id=repo_id, version=v_check))


def get_repo_versions(repo_id: str) -> list[packaging.version.Version]:
    """Return available valid versions (branches and tags) on a given Hub repo.

    Args:
        repo_id: The repository ID on the Hugging Face Hub.

    Returns:
        list[packaging.version.Version]: A list of valid versions found.
    """
    api = HfApi()
    repo_refs = api.list_repo_refs(repo_id, repo_type="dataset")
    repo_refs = [b.name for b in repo_refs.branches + repo_refs.tags]
    repo_versions = []
    for ref in repo_refs:
        with contextlib.suppress(packaging.version.InvalidVersion):
            repo_versions.append(packaging.version.parse(ref))

    return repo_versions


def get_safe_version(repo_id: str, version: str | packaging.version.Version) -> str:
    """Return the specified version if available on repo, or the latest compatible one.

    If the exact version is not found, it looks for the latest version with the
    same major version number that is less than or equal to the target minor version.

    Args:
        repo_id: The repository ID on the Hugging Face Hub.
        version: The target version.

    Returns:
        str: The safe version string (e.g., "v1.2.3") to use as a revision.

    Raises:
        RevisionNotFoundError: If the repo has no version tags.
        BackwardCompatibilityError: If only older major versions are available.
        ForwardCompatibilityError: If only newer major versions are available.
    """
    target_version = (
        packaging.version.parse(version)
        if not isinstance(version, packaging.version.Version)
        else version
    )
    hub_versions = get_repo_versions(repo_id)

    if not hub_versions:
        raise RevisionNotFoundError(
            f"""Your dataset must be tagged with a codebase version.
            Assuming _version_ is the codebase_version value in the info.json, you can run this:
            ```python
            from huggingface_hub import HfApi

            hub_api = HfApi()
            hub_api.create_tag("{repo_id}", tag="_version_", repo_type="dataset")
            ```
            """
        )

    if target_version in hub_versions:
        return f"v{target_version}"

    compatibles = [
        v
        for v in hub_versions
        if v.major == target_version.major and v.minor <= target_version.minor
    ]
    if compatibles:
        return_version = max(compatibles)
        if return_version < target_version:
            logging.warning(
                f"Revision {version} for {repo_id} not found, using version v{return_version}"
            )
        return f"v{return_version}"

    lower_major = [v for v in hub_versions if v.major < target_version.major]
    if lower_major:
        raise NotImplementedError(
            "Not implemented"
        )  # BackwardCompatibilityError(repo_id, max(lower_major))

    upper_versions = [v for v in hub_versions if v > target_version]
    assert len(upper_versions) > 0
    raise NotImplementedError(
        "Not implemented"
    )  # ForwardCompatibilityError(repo_id, min(upper_versions))


def get_hf_features_from_features(features: dict) -> datasets.Features:
    """Convert a LeRobot features dictionary to a `datasets.Features` object.

    Args:
        features: A LeRobot-style feature dictionary.

    Returns:
        datasets.Features: The corresponding Hugging Face `datasets.Features` object.

    Raises:
        ValueError: If a feature has an unsupported shape.
    """
    hf_features = {}
    for key, ft in features.items():
        if ft["dtype"] == "video":
            continue
        elif ft["dtype"] == "image":
            hf_features[key] = datasets.Image()
        elif ft["shape"] == (1,):
            hf_features[key] = datasets.Value(dtype=ft["dtype"])
        elif len(ft["shape"]) == 1:
            hf_features[key] = datasets.Sequence(
                length=ft["shape"][0], feature=datasets.Value(dtype=ft["dtype"])
            )
        elif len(ft["shape"]) == 2:
            hf_features[key] = datasets.Array2D(shape=ft["shape"], dtype=ft["dtype"])
        elif len(ft["shape"]) == 3:
            hf_features[key] = datasets.Array3D(shape=ft["shape"], dtype=ft["dtype"])
        elif len(ft["shape"]) == 4:
            hf_features[key] = datasets.Array4D(shape=ft["shape"], dtype=ft["dtype"])
        elif len(ft["shape"]) == 5:
            hf_features[key] = datasets.Array5D(shape=ft["shape"], dtype=ft["dtype"])
        else:
            raise ValueError(f"Corresponding feature is not valid: {ft}")

    return datasets.Features(hf_features)


def _validate_feature_names(features: dict[str, dict]) -> None:
    """Validate that feature names do not contain invalid characters.

    Args:
        features: The LeRobot features dictionary.

    Raises:
        ValueError: If any feature name contains '/'.
    """
    invalid_features = {name: ft for name, ft in features.items() if "/" in name}
    if invalid_features:
        raise ValueError(
            f"Feature names should not contain '/'. Found '/' in '{invalid_features}'."
        )


def hw_to_dataset_features(
    hw_features: dict[str, type | tuple], prefix: str, use_video: bool = True
) -> dict[str, dict]:
    """Convert hardware-specific features to a LeRobot dataset feature dictionary.

    This function takes a dictionary describing hardware outputs (like joint states
    or camera image shapes) and formats it into the standard LeRobot feature
    specification.

    Args:
        hw_features: Dictionary mapping feature names to their type (float for
            joints) or shape (tuple for images).
        prefix: The prefix to add to the feature keys (e.g., "observation"
            or "action").
        use_video: If True, image features are marked as "video", otherwise "image".

    Returns:
        dict: A LeRobot features dictionary.
    """
    features = {}
    joint_fts = {
        key: ftype
        for key, ftype in hw_features.items()
        if ftype
        is float  # or (isinstance(ftype, PolicyFeature) and ftype.type != FeatureType.VISUAL)
    }
    cam_fts = {
        key: shape for key, shape in hw_features.items() if isinstance(shape, tuple)
    }

    if joint_fts and prefix == "action":
        features[prefix] = {
            "dtype": "float32",
            "shape": (len(joint_fts),),
            "names": list(joint_fts),
        }

    if joint_fts and prefix == "observation":
        features[f"{prefix}.state"] = {
            "dtype": "float32",
            "shape": (len(joint_fts),),
            "names": list(joint_fts),
        }

    for key, shape in cam_fts.items():
        features[f"{prefix}.images.{key}"] = {
            "dtype": "video" if use_video else "image",
            "shape": shape,
            "names": ["height", "width", "channels"],
        }

    _validate_feature_names(features)
    return features


def build_dataset_frame(
    ds_features: dict[str, dict], values: dict[str, Any], prefix: str
) -> dict[str, np.ndarray]:
    """Construct a single data frame from raw values based on dataset features.

    A "frame" is a dictionary containing all the data for a single timestep,
    formatted as numpy arrays according to the feature specification.

    Args:
        ds_features: The LeRobot dataset features dictionary.
        values: A dictionary of raw values from the hardware/environment.
        prefix: The prefix to filter features by (e.g., "observation"
            or "action").

    Returns:
        dict: A dictionary representing a single frame of data.
    """
    frame = {}
    for key, ft in ds_features.items():
        if key in DEFAULT_FEATURES or not key.startswith(prefix):
            continue
        elif ft["dtype"] == "float32" and len(ft["shape"]) == 1:
            frame[key] = np.array(
                [values[name] for name in ft["names"]], dtype=np.float32
            )
        elif ft["dtype"] in ["image", "video"]:
            frame[key] = values[key.removeprefix(f"{prefix}.images.")]

    return frame


def dataset_to_policy_features(features: dict[str, dict]) -> dict[str, dict]:
    """Convert dataset features to policy features.

    This function transforms the dataset's feature specification into a format
    that a policy can use, classifying features by type (e.g., visual, state,
    action) and ensuring correct shapes (e.g., channel-first for images).

    Args:
        features: The LeRobot dataset features dictionary.

    Returns:
        dict: A dictionary mapping feature keys to `PolicyFeature` objects.

    Raises:
        ValueError: If an image feature does not have a 3D shape.
    """
    # TODO(aliberts): Implement "type" in dataset features and simplify this
    policy_features = {}
    for key, ft in features.items():
        shape = ft["shape"]
        if ft["dtype"] in ["image", "video"]:
            type = "visual"
            if len(shape) != 3:
                raise ValueError(f"Number of dimensions of {key} != 3 (shape={shape})")

            names = ft["names"]
            # Backward compatibility for "channel" which is an error introduced in LeRobotDataset v2.0 for ported datasets.
            if names[2] in ["channel", "channels"]:  # (h, w, c) -> (c, h, w)
                shape = (shape[2], shape[0], shape[1])
        elif key == "observation.state":
            type = "env"
        elif key.startswith("observation"):
            type = "state"
        elif key.startswith("action"):
            type = "action"
        else:
            continue

        policy_features[key] = dict(
            type=type,
            shape=shape,
        )

    return policy_features


def combine_feature_dicts(*dicts: dict) -> dict:
    """Merge LeRobot grouped feature dicts.

    - For 1D numeric specs (dtype not image/video/string) with "names": we merge the names and recompute the shape.
    - For others (e.g. `observation.images.*`), the last one wins (if they are identical).

    Args:
        *dicts: A variable number of LeRobot feature dictionaries to merge.

    Returns:
        dict: A single merged feature dictionary.

    Raises:
        ValueError: If there's a dtype mismatch for a feature being merged.
    """
    out: dict = {}
    for d in dicts:
        for key, value in d.items():
            if not isinstance(value, dict):
                out[key] = value
                continue

            dtype = value.get("dtype")
            shape = value.get("shape")
            is_vector = (
                dtype not in ("image", "video", "string")
                and isinstance(shape, tuple)
                and len(shape) == 1
                and "names" in value
            )

            if is_vector:
                # Initialize or retrieve the accumulating dict for this feature key
                target = out.setdefault(
                    key, {"dtype": dtype, "names": [], "shape": (0,)}
                )
                # Ensure consistent data types across merged entries
                if "dtype" in target and dtype != target["dtype"]:
                    raise ValueError(
                        f"dtype mismatch for '{key}': {target['dtype']} vs {dtype}"
                    )

                # Merge feature names: append only new ones to preserve order without duplicates
                seen = set(target["names"])
                for n in value["names"]:
                    if n not in seen:
                        target["names"].append(n)
                        seen.add(n)
                # Recompute the shape to reflect the updated number of features
                target["shape"] = (len(target["names"]),)
            else:
                # For images/videos and non-1D entries: override with the latest definition
                out[key] = value
    return out


def create_empty_dataset_info(
    codebase_version: str,
    fps: int,
    features: dict,
    use_videos: bool,
    robot_type: str | None = None,
    chunks_size: int | None = None,
    data_files_size_in_mb: int | None = None,
    video_files_size_in_mb: int | None = None,
) -> dict:
    """Create a template dictionary for a new dataset's `info.json`.

    Args:
        codebase_version: The version of the LeRobot codebase.
        fps: The frames per second of the data.
        features: The LeRobot features dictionary for the dataset.
        use_videos: Whether the dataset will store videos.
        robot_type: The type of robot used, if any.
        chunks_size: The number of chunks to use for the dataset.
        data_files_size_in_mb: The size of the data files in MB.
        video_files_size_in_mb: The size of the video files in MB.

    Returns:
        dict: A dictionary with the initial dataset metadata.
    """
    return {
        "codebase_version": codebase_version,
        "robot_type": robot_type,
        "total_episodes": 0,
        "total_frames": 0,
        "total_tasks": 0,
        "chunks_size": chunks_size or DEFAULT_CHUNK_SIZE,
        "data_files_size_in_mb": data_files_size_in_mb or DEFAULT_DATA_FILE_SIZE_IN_MB,
        "video_files_size_in_mb": video_files_size_in_mb
        or DEFAULT_VIDEO_FILE_SIZE_IN_MB,
        "fps": fps,
        "splits": {},
        "data_path": DEFAULT_DATA_PATH,
        "video_path": DEFAULT_VIDEO_PATH if use_videos else None,
        "features": features,
    }


def check_delta_timestamps(
    delta_timestamps: dict[str, list[float]],
    fps: int,
    tolerance_s: float,
    raise_value_error: bool = True,
) -> bool:
    """Check if delta timestamps are multiples of 1/fps +/- tolerance.

    This ensures that adding these delta timestamps to any existing timestamp in
    the dataset will result in a value that aligns with the dataset's frame rate.

    Args:
        delta_timestamps: A dictionary where values are lists of time
            deltas in seconds.
        fps: The frames per second of the dataset.
        tolerance_s: The allowed tolerance in seconds.
        raise_value_error: If True, raises an error on failure.

    Returns:
        bool: True if all deltas are valid, False otherwise.

    Raises:
        ValueError: If any delta is outside the tolerance and `raise_value_error` is True.
    """
    outside_tolerance = {}
    for key, delta_ts in delta_timestamps.items():
        within_tolerance = [
            abs(ts * fps - round(ts * fps)) / fps <= tolerance_s for ts in delta_ts
        ]
        if not all(within_tolerance):
            outside_tolerance[key] = [
                ts
                for ts, is_within in zip(delta_ts, within_tolerance, strict=True)
                if not is_within
            ]

    if len(outside_tolerance) > 0:
        if raise_value_error:
            raise ValueError(
                f"""
                The following delta_timestamps are found outside of tolerance range.
                Please make sure they are multiples of 1/{fps} +/- tolerance and adjust
                their values accordingly.
                \n{pformat(outside_tolerance)}
                """
            )
        return False

    return True


def get_delta_indices(
    delta_timestamps: dict[str, list[float]], fps: int
) -> dict[str, list[int]]:
    """Convert delta timestamps in seconds to delta indices in frames.

    Args:
        delta_timestamps: A dictionary of time deltas in seconds.
        fps: The frames per second of the dataset.

    Returns:
        dict: A dictionary of frame delta indices.
    """
    delta_indices = {}
    for key, delta_ts in delta_timestamps.items():
        delta_indices[key] = [round(d * fps) for d in delta_ts]

    return delta_indices


def cycle(iterable: Any) -> Iterator[Any]:
    """Create a dataloader-safe cyclical iterator.

    This is an equivalent of `itertools.cycle` but is safe for use with
    PyTorch DataLoaders with multiple workers.
    See https://github.com/pytorch/pytorch/issues/23900 for details.

    Args:
        iterable: The iterable to cycle over.

    Yields:
        Any: The next item in the iterable.
    """
    iterator = iter(iterable)
    while True:
        try:
            yield next(iterator)
        except StopIteration:
            iterator = iter(iterable)


def create_branch(repo_id: str, *, branch: str, repo_type: str | None = None) -> None:
    """Create a branch on an existing Hugging Face repo.

    Deletes the branch if it already exists before creating it.

    Args:
        repo_id: The ID of the repository.
        branch: The name of the branch to create.
        repo_type: The type of the repository (e.g., "dataset").
    """
    api = HfApi()

    branches = api.list_repo_refs(repo_id, repo_type=repo_type).branches
    refs = [branch.ref for branch in branches]
    ref = f"refs/heads/{branch}"
    if ref in refs:
        api.delete_branch(repo_id, repo_type=repo_type, branch=branch)

    api.create_branch(repo_id, repo_type=repo_type, branch=branch)


def create_lerobot_dataset_card(
    tags: list | None = None,
    dataset_info: dict | None = None,
    **kwargs,
) -> DatasetCard:
    """Create a `DatasetCard` for a LeRobot dataset.

    Keyword arguments are used to replace values in the card template.
    Note: If specified, `license` must be a valid license identifier from
    https://huggingface.co/docs/hub/repositories-licenses.

    Args:
        tags: A list of tags to add to the dataset card.
        dataset_info: The dataset's info dictionary, which will
            be displayed on the card.
        **kwargs: Additional keyword arguments to populate the card template.

    Returns:
        DatasetCard: The generated dataset card object.
    """
    card_tags = ["LeRobot"]

    if tags:
        card_tags += tags
    if dataset_info:
        dataset_structure = "[meta/info.json](meta/info.json):\n"
        dataset_structure += f"```json\n{json.dumps(dataset_info, indent=4)}\n```\n"
        kwargs = {**kwargs, "dataset_structure": dataset_structure}
    card_data = DatasetCardData(
        license=kwargs.get("license"),
        tags=card_tags,
        task_categories=["robotics"],
        configs=[
            {
                "config_name": "default",
                "data_files": "data/*/*.parquet",
            }
        ],
    )

    card_template = (
        importlib.resources.files("lerobot.datasets") / "card_template.md"
    ).read_text()

    return DatasetCard.from_template(
        card_data=card_data,
        template_str=card_template,
        **kwargs,
    )


def validate_frame(frame: dict, features: dict) -> None:
    expected_features = set(features) - set(DEFAULT_FEATURES)
    actual_features = set(frame)

    # task is a special required field that's not part of regular features
    if "task" not in actual_features:
        raise ValueError(
            "Feature mismatch in `frame` dictionary:\nMissing features: {'task'}\n"
        )

    # Remove task from actual_features for regular feature validation
    actual_features_for_validation = actual_features - {"task"}

    error_message = validate_features_presence(
        actual_features_for_validation, expected_features
    )

    common_features = actual_features_for_validation & expected_features
    for name in common_features:
        error_message += validate_feature_dtype_and_shape(
            name, features[name], frame[name]
        )

    if error_message:
        raise ValueError(error_message)


def validate_features_presence(
    actual_features: set[str], expected_features: set[str]
) -> str:
    """Check for missing or extra features in a frame.

    Args:
        actual_features: The set of feature names present in the frame.
        expected_features: The set of feature names expected in the frame.

    Returns:
        str: An error message string if there's a mismatch, otherwise an empty string.
    """
    error_message = ""
    missing_features = expected_features - actual_features
    extra_features = actual_features - expected_features

    if missing_features or extra_features:
        error_message += "Feature mismatch in `frame` dictionary:\n"
        if missing_features:
            error_message += f"Missing features: {missing_features}\n"
        if extra_features:
            error_message += f"Extra features: {extra_features}\n"

    return error_message


def validate_feature_dtype_and_shape(
    name: str, feature: dict, value: np.ndarray | PILImage.Image | str
) -> str:
    """Validate the dtype and shape of a single feature's value.

    Args:
        name: The name of the feature.
        feature: The feature specification from the LeRobot features dictionary.
        value: The value of the feature to validate.

    Returns:
        str: An error message if validation fails, otherwise an empty string.

    Raises:
        NotImplementedError: If the feature dtype is not supported for validation.
    """
    expected_dtype = feature["dtype"]
    expected_shape = feature["shape"]
    if expected_dtype in ["float32", "int64", "depth"]:
        return validate_feature_numpy_array(name, expected_dtype, expected_shape, value)
    elif expected_dtype in ["image", "video"]:
        return validate_feature_image_or_video(name, expected_shape, value)
    elif expected_dtype == "string":
        return validate_feature_string(name, value)
    else:
        raise NotImplementedError(
            f"The feature dtype '{expected_dtype}' is not implemented yet."
        )


def validate_feature_numpy_array(
    name: str, expected_dtype: str, expected_shape: list[int], value: np.ndarray
) -> str:
    """Validate a feature that is expected to be a numpy array.

    Args:
        name: The name of the feature.
        expected_dtype: The expected numpy dtype as a string.
        expected_shape: The expected shape.
        value: The numpy array to validate.

    Returns:
        str: An error message if validation fails, otherwise an empty string.
    """
    error_message = ""
    if isinstance(value, np.ndarray):
        actual_dtype = value.dtype
        actual_shape = value.shape

        if actual_dtype != np.dtype(expected_dtype):
            error_message += f"The feature '{name}' of dtype '{actual_dtype}' is not of the expected dtype '{expected_dtype}'.\n"

        if actual_shape != expected_shape:
            error_message += f"The feature '{name}' of shape '{actual_shape}' does not have the expected shape '{expected_shape}'.\n"
    else:
        error_message += f"The feature '{name}' is not a 'np.ndarray'. Expected type is '{expected_dtype}', but type '{type(value)}' provided instead.\n"

    return error_message


def validate_feature_image_or_video(
    name: str, expected_shape: list[str], value: np.ndarray | PILImage.Image
) -> str:
    """Validate a feature that is expected to be an image or video frame.

    Accepts `np.ndarray` (channel-first or channel-last) or `PIL.Image.Image`.

    Args:
        name: The name of the feature.
        expected_shape: The expected shape (C, H, W).
        value: The image data to validate.

    Returns:
        str: An error message if validation fails, otherwise an empty string.
    """
    # Note: The check of pixels range ([0,1] for float and [0,255] for uint8) is done by the image writer threads.
    error_message = ""
    if isinstance(value, np.ndarray):
        actual_shape = value.shape
        c, h, w = expected_shape
        if len(actual_shape) != 3 or (
            actual_shape != (c, h, w) and actual_shape != (h, w, c)
        ):
            error_message += f"The feature '{name}' of shape '{actual_shape}' does not have the expected shape '{(c, h, w)}' or '{(h, w, c)}'.\n"
    elif isinstance(value, PILImage.Image):
        pass
    else:
        error_message += f"The feature '{name}' is expected to be of type 'PIL.Image' or 'np.ndarray' channel first or channel last, but type '{type(value)}' provided instead.\n"

    return error_message


def validate_feature_string(name: str, value: str) -> str:
    """Validate a feature that is expected to be a string.

    Args:
        name: The name of the feature.
        value: The value to validate.

    Returns:
        str: An error message if validation fails, otherwise an empty string.
    """
    if not isinstance(value, str):
        return f"The feature '{name}' is expected to be of type 'str', but type '{type(value)}' provided instead.\n"
    return ""


def validate_episode_buffer(
    episode_buffer: dict, total_episodes: int, features: dict
) -> None:
    """Validate the episode buffer before it's written to disk.

    Ensures the buffer has the required keys, contains at least one frame, and
    has features consistent with the dataset's specification.

    Args:
        episode_buffer: The buffer containing data for a single episode.
        total_episodes: The current total number of episodes in the dataset.
        features: The LeRobot features dictionary for the dataset.

    Raises:
        ValueError: If the buffer is invalid.
        NotImplementedError: If the episode index is manually set and doesn't match.
    """
    if "size" not in episode_buffer:
        raise ValueError("size key not found in episode_buffer")

    if "task" not in episode_buffer:
        raise ValueError("task key not found in episode_buffer")

    if episode_buffer["episode_index"] != total_episodes:
        # TODO(aliberts): Add option to use existing episode_index
        raise NotImplementedError(
            "You might have manually provided the episode_buffer with an episode_index that doesn't "
            "match the total number of episodes already in the dataset. This is not supported for now."
        )

    if episode_buffer["size"] == 0:
        raise ValueError(
            "You must add one or several frames with `add_frame` before calling `add_episode`."
        )

    buffer_keys = set(episode_buffer.keys()) - {"task", "size"}
    if not buffer_keys == set(features):
        raise ValueError(
            f"Features from `episode_buffer` don't match the ones in `features`."
            f"In episode_buffer not in features: {buffer_keys - set(features)}"
            f"In features not in episode_buffer: {set(features) - buffer_keys}"
        )


def to_parquet_with_hf_images(df: pandas.DataFrame, path: Path) -> None:
    """This function correctly writes to parquet a panda DataFrame that contains images encoded by HF dataset.
    This way, it can be loaded by HF dataset and correctly formatted images are returned.
    """
    # TODO(qlhoest): replace this weird synthax by `df.to_parquet(path)` only
    datasets.Dataset.from_dict(df.to_dict(orient="list")).to_parquet(path)


def item_to_torch(item: dict) -> dict:
    """Convert all items in a dictionary to PyTorch tensors where appropriate.

    This function is used to convert an item from a streaming dataset to PyTorch tensors.

    Args:
        item: Dictionary of items from a dataset.

    Returns:
        dict: Dictionary with all tensor-like items converted to torch.Tensor.
    """
    for key, val in item.items():
        if isinstance(val, (np.ndarray | list)) and key not in ["task"]:
            # Convert numpy arrays and lists to torch tensors
            item[key] = torch.tensor(val)
    return item


def is_float_in_list(target, float_list, threshold=1e-6):
    return any(abs(target - x) <= threshold for x in float_list)


def find_float_index(target, float_list, threshold=1e-6):
    for i, x in enumerate(float_list):
        if abs(target - x) <= threshold:
            return i
    return -1


class LookBackError(Exception):
    """
    Exception raised when trying to look back in the history of a Backtrackable object.
    """

    pass


class LookAheadError(Exception):
    """
    Exception raised when trying to look ahead in the future of a Backtrackable object.
    """

    pass


class Backtrackable(Generic[T]):
    """
    Wrap any iterator/iterable so you can step back up to `history` items
    and look ahead up to `lookahead` items.

    This is useful for streaming datasets where you need to access previous and future items
    but can't load the entire dataset into memory.

    Example:
    -------
    ```python
    ds = load_dataset("c4", "en", streaming=True, split="train")
    rev = Backtrackable(ds, history=3, lookahead=2)

    x0 = next(rev)  # forward
    x1 = next(rev)
    x2 = next(rev)

    # Look ahead
    x3_peek = rev.peek_ahead(1)  # next item without moving cursor
    x4_peek = rev.peek_ahead(2)  # two items ahead

    # Look back
    x1_again = rev.peek_back(1)  # previous item without moving cursor
    x0_again = rev.peek_back(2)  # two items back

    # Move backward
    x1_back = rev.prev()  # back one step
    next(rev)  # returns x2, continues forward from where we were
    ```
    """

    __slots__ = (
        "_source",
        "_back_buf",
        "_ahead_buf",
        "_cursor",
        "_history",
        "_lookahead",
    )

    def __init__(self, iterable: Iterable[T], *, history: int = 1, lookahead: int = 0):
        """Initialize a Backtrackable iterator.

        Args:
            iterable: The iterable to wrap.
            history: Number of items to keep in history buffer (must be >= 1).
            lookahead: Number of items to allow peeking ahead (must be > 0).

        Raises:
            ValueError: If history < 1 or lookahead <= 0.
        """
        if history < 1:
            raise ValueError("history must be >= 1")
        if lookahead <= 0:
            raise ValueError("lookahead must be > 0")

        self._source: Iterator[T] = iter(iterable)
        self._back_buf: deque[T] = deque(maxlen=history)
        self._ahead_buf: deque[T] = (
            deque(maxlen=lookahead) if lookahead > 0 else deque()
        )
        self._cursor: int = 0
        self._history = history
        self._lookahead = lookahead

    def __iter__(self) -> "Backtrackable[T]":
        return self

    def __next__(self) -> T:
        # If we've stepped back, consume from back buffer first
        if self._cursor < 0:  # -1 means "last item", etc.
            self._cursor += 1
            return self._back_buf[self._cursor]

        # If we have items in the ahead buffer, use them first
        item = self._ahead_buf.popleft() if self._ahead_buf else next(self._source)

        # Add current item to back buffer and reset cursor
        self._back_buf.append(item)
        self._cursor = 0
        return item

    def prev(self) -> T:
        """
        Step one item back in history and return it.
        Raises IndexError if already at the oldest buffered item.
        """
        if len(self._back_buf) + self._cursor <= 1:
            raise LookBackError("At start of history")

        self._cursor -= 1
        return self._back_buf[self._cursor]

    def peek_back(self, n: int = 1) -> T:
        """
        Look `n` items back (n=1 == previous item) without moving the cursor.
        """
        if n < 0 or n + 1 > len(self._back_buf) + self._cursor:
            raise LookBackError("peek_back distance out of range")

        return self._back_buf[self._cursor - (n + 1)]

    def peek_ahead(self, n: int = 1) -> T:
        """
        Look `n` items ahead (n=1 == next item) without moving the cursor.
        Fills the ahead buffer if necessary.
        """
        if n < 1:
            raise LookAheadError("peek_ahead distance must be 1 or more")
        elif n > self._lookahead:
            raise LookAheadError("peek_ahead distance exceeds lookahead limit")

        # Fill ahead buffer if we don't have enough items
        while len(self._ahead_buf) < n:
            try:
                item = next(self._source)
                self._ahead_buf.append(item)

            except StopIteration as err:
                raise LookAheadError("peek_ahead: not enough items in source") from err

        return self._ahead_buf[n - 1]

    def history(self) -> list[T]:
        """
        Return a copy of the buffered history (most recent last).
        The list length ≤ `history` argument passed at construction.
        """
        if self._cursor == 0:
            return list(self._back_buf)

        # When cursor<0, slice so the order remains chronological
        return list(self._back_buf)[: self._cursor or None]

    def can_peek_back(self, steps: int = 1) -> bool:
        """
        Check if we can go back `steps` items without raising an IndexError.
        """
        return steps <= len(self._back_buf) + self._cursor

    def can_peek_ahead(self, steps: int = 1) -> bool:
        """
        Check if we can peek ahead `steps` items.
        This may involve trying to fill the ahead buffer.
        """
        if self._lookahead > 0 and steps > self._lookahead:
            return False

        # Try to fill ahead buffer to check if we can peek that far
        try:
            while len(self._ahead_buf) < steps:
                if self._lookahead > 0 and len(self._ahead_buf) >= self._lookahead:
                    return False
                item = next(self._source)
                self._ahead_buf.append(item)
            return True
        except StopIteration:
            return False


def safe_shard(
    dataset: datasets.IterableDataset, index: int, num_shards: int
) -> datasets.Dataset:
    """
    Safe shards the dataset.
    """
    shard_idx = min(dataset.num_shards, index + 1) - 1

    return dataset.shard(num_shards, index=shard_idx)
