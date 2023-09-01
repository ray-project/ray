import concurrent.futures
import itertools
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

import numpy as np

from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.datasource import DefaultFileMetadataProvider
from ray.data.datasource.file_based_datasource import (
    _unwrap_s3_serialization_workaround,
    _wrap_s3_serialization_workaround,
)
from ray.data.datasource.file_meta_provider import _get_file_infos
from ray.data.datasource.partitioning import Partitioning

if TYPE_CHECKING:
    import pyarrow

NUM_CPUS_PER_TASK = 0.5  # Quantity of CPUs to reserve for each fetch task
NUM_PATHS_PER_TASK = 32  # Number of files to delegate to each fetch task
NUM_PATHS_PER_BATCH = 5  # Number of files to delegate to each fetch thread

SAMPLING_RATIO = 0.1  # Proportion of files to sample
MIN_SAMPLE_SIZE = 100  # Min number of files to sample
MAX_SAMPLE_SIZE = 1000  # Max number of files to sample


class AnyscaleFileMetadataProvider(DefaultFileMetadataProvider):
    """A metadata provider that implements proprietary optimizations.

    Args:
        file_extensions: If not ``None`` and all input paths end with one the specified
            file extensions, then fetch metadata from a random sample of paths.
    """

    def __init__(self, file_extensions: Optional[List[str]] = None):
        if file_extensions is None:
            file_extensions = []
        self.file_extensions = file_extensions

    def expand_paths(
        self,
        paths: List[str],
        filesystem: Optional["pyarrow.fs.FileSystem"],
        partitioning: Optional[Partitioning] = None,
        ignore_missing_paths: bool = False,
    ) -> Iterator[Tuple[str, int]]:
        def is_file(path):
            return any(
                path.endswith("." + file_extension)
                for file_extension in self.file_extensions
            )

        use_sampling = all(is_file(path) for path in paths) and not ignore_missing_paths
        if len(paths) == 1:
            yield from _fetch_metadata(paths, filesystem, ignore_missing_paths)
        elif use_sampling:
            yield from _fetch_metadata_with_sampling_and_tasks_and_threads(
                paths, filesystem, ignore_missing_paths
            )
        else:
            yield from _fetch_metadata_with_tasks_and_threads(
                paths, filesystem, ignore_missing_paths
            )


def _fetch_metadata(
    paths: List[str], filesystem: "pyarrow.fs.FileSystem", ignore_missing_paths: bool
) -> List[Tuple[str, int]]:
    metadata = []
    for path in paths:
        metadata.extend(_get_file_infos(path, filesystem, ignore_missing_paths))
    return metadata


def _fetch_metadata_with_sampling_and_tasks_and_threads(
    paths: List[str],
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_paths: bool,
):
    sampled_paths = _sample_paths(paths)
    sampled_file_sizes: Dict[str, int] = dict(
        _fetch_metadata_with_tasks_and_threads(
            sampled_paths, filesystem, ignore_missing_paths
        )
    )
    median_file_size = np.median(list(sampled_file_sizes.values()))

    # NOTE: We should only perform sampling if all of the input paths point to files.
    # If there are more file sizes than input paths, then one or more paths point to
    # folders.
    assert len(sampled_file_sizes) == len(sampled_paths)

    for path in paths:
        if path in sampled_file_sizes:
            yield path, sampled_file_sizes[path]
        else:
            yield path, median_file_size


def _sample_paths(paths: List[str]) -> List[str]:
    sample_size = int(SAMPLING_RATIO * len(paths))
    sample_size = max(MIN_SAMPLE_SIZE, min(sample_size, MAX_SAMPLE_SIZE))
    paths = np.array(paths)
    np.random.shuffle(paths)
    return paths[:sample_size]


def _fetch_metadata_with_tasks_and_threads(
    paths: List[str],
    filesystem: "pyarrow.fs.FileSystem",
    ignore_missing_paths: bool,
) -> List[Tuple[str, int]]:
    filesystem = _wrap_s3_serialization_workaround(filesystem)

    # NOTE: For some reason, it's way faster if we define `_fetch_metadata_with_threads`
    # as a nested function.
    def _fetch_metadata_with_threads(paths) -> List[Tuple[str, int]]:
        nonlocal filesystem
        filesystem = _unwrap_s3_serialization_workaround(filesystem)

        num_batches = max(len(paths) // NUM_PATHS_PER_BATCH, 1)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for split in np.array_split(paths, num_batches):
                future = executor.submit(
                    _fetch_metadata, split, filesystem, ignore_missing_paths
                )
                futures.append(future)

        results = [future.result() for future in futures]
        return list(itertools.chain.from_iterable(results))

    remote_fetch_func = cached_remote_fn(
        _fetch_metadata_with_threads, num_cpus=NUM_CPUS_PER_TASK
    )
    num_fetch_tasks = max(len(paths) // NUM_PATHS_PER_TASK, 1)
    fetch_tasks = []
    for split in np.array_split(paths, num_fetch_tasks):
        fetch_tasks.append(remote_fetch_func.remote(split))

    progress_bar = ProgressBar("Metadata Fetch Progress", total=num_fetch_tasks)
    results = progress_bar.fetch_until_complete(fetch_tasks)
    return list(itertools.chain.from_iterable(results))
