from typing import Any, Dict, Optional

from ray.data.block import Block
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FilenameProvider:
    """Generates filenames when you write a :class:`~ray.data.Dataset`.

    Use this class to customize what your written files look like.

    Some methods write each row to a separate file, while others write each block to a
    separate file. For example, :meth:`ray.data.Dataset.write_images` writes individual
    rows, and :func:`ray.data.Dataset.write_parquet` writes blocks of data.

    If you're writing each row to a separate file, implement
    :meth:`~FilenameProvider.get_filename_for_row`. Otherwise, implement
    :meth:`~FilenameProvider.get_filename_for_block`.

    Example:

        .. testcode::

            from ray.data.datasource import FilenameProvider

            class LabeledFilenameProvider(FilenameProvider):

                def __init__(self, file_format: str):
                    self.file_format = file_format

                def get_filename_for_row(self, row, file_index):
                    return f"{row['label']}-{file_index}.{file_format}"
    """

    def get_filename_for_block(self, block: Block, file_index: int) -> str:
        """Generate a filename for a block of data.

        .. note::
            Filenames must be unique and idempotent for a given file index. This means
            your filenames can't be random, and if you call this method twice with the
            same arguments, you should get the same filename both times.

        Args:
            block: The block that will be written to a file.
            file_index: The index of the file that will be written.
        """
        raise NotImplementedError

    def get_filename_for_row(self, row: Dict[str, Any], file_index: int) -> str:
        """Generate a filename for a row.

        .. note::
            Filenames must be unique and idempotent for a given file index. This means
            your filenames can't be random, and if you call this method twice with the
            same arguments, you should get the same filename both times.

        Args:
            row: The row that will be written to a file.
            file_index: The index of the file that will be written.
        """
        raise NotImplementedError


class _DefaultFilenameProvider(FilenameProvider):
    def __init__(self, dataset_uuid: str, file_format: Optional[str] = None):
        self._dataset_uuid = dataset_uuid
        self._file_format = file_format

    def get_filename_for_block(self, block: Block, file_index: int) -> str:
        return self._get_filename(file_index)

    def get_filename_for_row(self, row: Dict[str, Any], file_index: int) -> str:
        return self._get_filename(file_index)

    def _get_filename(self, file_index: int) -> str:
        filename = f"{self._dataset_uuid}_{file_index:06}"
        if self._file_format is not None:
            filename += f".{self._file_format}"
        return filename
