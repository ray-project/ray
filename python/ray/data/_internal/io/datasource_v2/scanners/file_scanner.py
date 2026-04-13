from typing import List, Optional

from ray.data._internal.io.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.io.datasource_v2.scanners.scanner import Scanner
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class FileScanner(Scanner[FileManifest]):
    """Base scanner for file-based datasources.

    Provides a default plan() implementation that splits FileManifest by files.
    Subclasses implement format-specific read_schema() and create_reader().

    This is the recommended base class for file-based datasources like
    Parquet, CSV, JSON, etc.
    """

    def plan(
        self,
        manifest: FileManifest,
        parallelism: int,
        data_context: Optional["DataContext"] = None,
    ) -> List[FileManifest]:
        """Split manifest into parallel work units by files.

        Distributes files as evenly as possible across the target parallelism.
        If there are fewer files than parallelism, creates one partition per file.

        Args:
            manifest: FileManifest to partition.
            parallelism: Target number of parallel tasks.
            data_context: Optional data context.

        Returns:
            List of FileManifest objects for parallel execution.
        """
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
