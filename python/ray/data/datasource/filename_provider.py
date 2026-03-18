from typing import Any, Dict, Optional, Tuple

from ray.data.block import Block
from ray.util.annotations import Deprecated, PublicAPI


def _split_base_and_ext(filename: str) -> Tuple[str, str]:
    """Split a filename into (base, extension) where extension includes the dot.

    Returns (base, ext) where ext includes the leading dot (e.g., ".parquet"),
    or is empty string if the filename has no extension.

    This is the single source of truth for separating a task filename's base
    from its extension. Used by both row-filename derivation and checkpoint
    base-filename extraction — these MUST agree for prefix-trie recovery.
    """
    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        return base, f".{ext}"
    return filename, ""


@PublicAPI(stability="alpha")
class FilenameProvider:
    """Generates filenames when you write a :class:`~ray.data.Dataset`.

    Use this class to customize the filenames used when writing a Dataset.

    Override :meth:`~FilenameProvider.get_filename_for_task` to customize filenames.
    For row-based writes (e.g., :meth:`~ray.data.Dataset.write_images`), row filenames
    are automatically derived by appending ``_{block_index:06}_{row_index:06}`` to the
    task filename.

    Example:

        This snippet shows you how to customize filenames with a prefix. For example,
        a file might be named ``images_abc123_000000.png``.

        .. testcode::

            import ray
            from ray.data.datasource import FilenameProvider

            class ImageFilenameProvider(FilenameProvider):

                def __init__(self, prefix: str, file_format: str):
                    super().__init__(file_format=file_format)
                    self.prefix = prefix

                def get_filename_for_task(self, write_uuid, task_index):
                    return f"{self.prefix}_{write_uuid}_{task_index:06}.{self.file_format}"

            ds = ray.data.read_parquet("s3://anonymous@ray-example-data/images.parquet")
            ds.write_images(
                "/tmp/results",
                column="image",
                filename_provider=ImageFilenameProvider("images", "png")
            )
    """  # noqa: E501

    def __init__(
        self,
        dataset_uuid: Optional[str] = None,
        file_format: Optional[str] = None,
    ) -> None:
        self.dataset_uuid = dataset_uuid
        self.file_format = file_format

    def get_filename_for_task(self, write_uuid: str, task_index: int) -> str:
        """Generate a filename for a write task.

        Override this method to customize filenames when writing a Dataset.

        .. note::
            Filenames must be unique and deterministic for a given write UUID and
            task index.

        Args:
            write_uuid: The UUID of the write operation.
            task_index: The index of the write task.

        Returns:
            The generated filename string.
        """
        file_id = f"{write_uuid}_{task_index:06}"
        filename = ""
        if self.dataset_uuid is not None:
            filename += f"{self.dataset_uuid}_"
        filename += file_id
        if self.file_format is not None:
            filename += f".{self.file_format}"
        return filename

    @Deprecated(
        message="Use get_filename_for_task() instead. The block and block_index "
        "parameters are unused in practice because datasinks merge all blocks into "
        "one before writing. These parameters will be removed in a future release. "
        "Do not depend on block content or block_index in your FilenameProvider "
        "implementation - filenames must be deterministic from (write_uuid, task_index) "
        "alone to ensure checkpointing correctness."
    )
    def get_filename_for_block(
        self, block: Optional[Block], write_uuid: str, task_index: int, block_index: int
    ) -> str:
        """Generate a filename for a block of data.

        .. note::
            Filenames must be unique and deterministic for a given write UUID and
            task index. Do NOT depend on block content or block_index.

            Checkpointing requires predicting the output filename BEFORE writing
            data. This enables 2-phase commit: if a write fails after creating the
            file but before committing the checkpoint, recovery can use the
            predicted filename to delete orphaned files and retry cleanly. If
            filenames depend on block content, this prediction is impossible and
            checkpointing cannot guarantee exactly-once semantics.

        Args:
            block: Deprecated, unused. Do not depend on block content.
            write_uuid: The UUID of the write operation.
            task_index: The index of the write task.
            block_index: Deprecated, always 0. Do not depend on this value.
        """
        raise NotImplementedError

    @Deprecated(
        message="Implement get_filename_for_task() instead. Row filenames are "
        "automatically derived by appending _{block_index:06}_{row_index:06} to the "
        "task filename. All files from the same task must share the task filename as "
        "a prefix so that uncommitted data files can be identified and cleaned up "
        "during checkpoint recovery."
    )
    def get_filename_for_row(
        self,
        row: Dict[str, Any],
        write_uuid: str,
        task_index: int,
        block_index: int,
        row_index: int,
    ) -> str:
        """Generate a filename for a row.

        .. deprecated::
            Implement :meth:`get_filename_for_task` instead. Row filenames are
            automatically derived by appending ``_{block_index:06}_{row_index:06}``
            to the task filename.

        Args:
            row: The row that will be written to a file.
            write_uuid: The UUID of the write operation.
            task_index: The index of the write task.
            block_index: The index of the block *within* the write task.
            row_index: The index of the row *within* the block.
        """
        raise NotImplementedError
