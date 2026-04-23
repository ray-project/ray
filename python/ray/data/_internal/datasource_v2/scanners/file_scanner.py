from dataclasses import dataclass, field
from typing import List, Literal, Optional, Union

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.scanners.scanner import Scanner
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import (
    FileShuffleConfig,
    _validate_shuffle_arg,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class FileScanner(Scanner[FileManifest]):
    """Base scanner for file-based datasources.

    Provides a default plan() implementation that shuffles (if configured)
    and splits a FileManifest by files. Subclasses implement format-specific
    read_schema() and create_reader().

    PyArrow Dataset-based scanners should subclass ``ArrowFileScanner``; use
    ``FileScanner`` directly for non-Arrow file formats that only share
    splitting and shuffling.
    """

    # kw_only so subclass dataclasses can declare their own required fields
    # (like ``ArrowFileScanner.schema``) without running into the "non-default
    # argument follows default argument" dataclass inheritance rule.
    shuffle: Union[Literal["files"], FileShuffleConfig, None] = field(
        default=None, kw_only=True
    )

    def __post_init__(self) -> None:
        _validate_shuffle_arg(self.shuffle)

    def plan(
        self,
        manifest: FileManifest,
        parallelism: int,
        data_context: Optional["DataContext"] = None,
    ) -> List[FileManifest]:
        """Shuffle (if configured) and split a manifest into parallel work units.

        Kept for tests and non-V2 callers. The ``ListFiles → ReadFiles``
        pipeline bypasses this method — shuffling and bucketing move to
        the transform chain in ``plan_list_files_op``.

        Args:
            manifest: FileManifest to partition.
            parallelism: Target number of parallel tasks.
            data_context: Optional data context.

        Returns:
            List of FileManifest objects for parallel execution.
        """
        if self.shuffle is not None:
            execution_idx = (
                data_context._execution_idx if data_context is not None else 0
            )
            if self.shuffle == "files":
                seed = None
            else:
                assert isinstance(self.shuffle, FileShuffleConfig)
                seed = self.shuffle.get_seed(execution_idx)
            manifest = manifest.shuffle(seed)

        num_files = len(manifest)
        if parallelism <= 0 or num_files == 0:
            return [manifest] if num_files > 0 else []

        # Ensure we don't create more splits than files
        actual_splits = min(parallelism, num_files)
        partitions = []

        # Distribute files as evenly as possible
        base_size = num_files // actual_splits
        remainder = num_files % actual_splits

        start = 0
        for i in range(actual_splits):
            # Add one extra file to the first 'remainder' splits
            size = base_size + (1 if i < remainder else 0)
            if size > 0:
                # Slice the underlying block
                block = manifest.as_block()
                sliced_block = block.slice(start, size)  # pyrefly: ignore[not-callable]
                partitions.append(FileManifest(sliced_block))
                start += size

        return partitions
