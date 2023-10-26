from typing import Any, Dict, Optional

from ray.data.block import Block
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FilenameProvider:
    """Generates filenames when you write a :class:`~ray.data.Dataset`.

    Use this class to customize the filenames used when writing a Dataset.

    Some methods write each row to a separate file, while others write each block to a
    separate file. For example, :meth:`ray.data.Dataset.write_images` writes individual
    rows, and :func:`ray.data.Dataset.write_parquet` writes blocks of data. For more
    information about blocks, see :ref:`Data internals <_datasets_scheduling>`.

    If you're writing each row to a separate file, implement
    :meth:`~FilenameProvider.get_filename_for_row`. Otherwise, implement
    :meth:`~FilenameProvider.get_filename_for_block`.

    Example:

        This snippets show you how to encode labels in written files. For example, if
        `"cat"` is a label, you might write a file named `cat_000000_000000_000000.png`.

        .. testcode::

            from ray.data.datasource import FilenameProvider

            class ImageFilenameProvider(FilenameProvider):

                def __init__(self, file_format: str):
                    self.file_format = file_format

                def get_filename_for_row(self, row, task_index, block_index, row_index):
                    return f"{row['label']}_{task_index:06}_{block_index:06}_{row_index:06}.png"
    """  # noqa: E501

    def get_filename_for_block(
        self, block: Block, task_index: int, block_index: int
    ) -> str:
        """Generate a filename for a block of data.

        .. note::
            Filenames must be unique and deterministic for a given task and block index.

        Args:
            block: The block that will be written to a file.
            task_index: The index of the the write task.
            block_index: The index of the block *within* the write task.
        """
        raise NotImplementedError

    def get_filename_for_row(
        self, row: Dict[str, Any], task_index: int, block_index: int, row_index: int
    ) -> str:
        """Generate a filename for a row.

        .. note::
            Filenames must be unique and deterministic for a given task, block, and row
            index.

        Args:
            row: The row that will be written to a file.
            task_index: The index of the the write task.
            block_index: The index of the block *within* the write task.
            row_index: The index of the row *within* the block.
        """
        raise NotImplementedError


class _DefaultFilenameProvider(FilenameProvider):
    def __init__(self, dataset_uuid: str, file_format: Optional[str] = None):
        self._dataset_uuid = dataset_uuid
        self._file_format = file_format

    def get_filename_for_block(
        self, block: Block, task_index: int, block_index: int
    ) -> str:

        filename = f"{self._dataset_uuid}_{task_index:06}_{block_index:06}"
        if self._file_format is not None:
            filename += f".{self._file_format}"
        return filename

    def get_filename_for_row(
        self, row: Dict[str, Any], task_index: int, block_index: int, row_index: int
    ) -> str:
        filename = (
            f"{self._dataset_uuid}_{task_index:06}_{block_index:06}_{row_index:06}"
        )
        if self._file_format is not None:
            filename += f".{self._file_format}"
        return filename
