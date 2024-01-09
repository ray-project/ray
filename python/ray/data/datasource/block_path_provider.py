import posixpath
from typing import TYPE_CHECKING, Optional

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow


@DeveloperAPI
class BlockWritePathProvider:
    """Abstract callable that provides concrete output paths when writing
    dataset blocks.

    Current subclasses:
        DefaultBlockWritePathProvider
    """

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        task_index: Optional[int] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        """
        Resolves and returns the write path for the given dataset block. When
        implementing this method, care should be taken to ensure that a unique
        path is provided for every dataset block.

        Args:
            base_path: The base path to write the dataset block out to. This is
                expected to be the same for all blocks in the dataset, and may
                point to either a directory or file prefix.
            filesystem: The filesystem implementation that will be used to
                write a file out to the write path returned.
            dataset_uuid: Unique identifier for the dataset that this block
                belongs to.
            block: The block to write.
            task_index: Ordered index of the write task within its parent
                dataset.
            block_index: Ordered index of the block to write within its parent
                write task.
            file_format: File format string for the block that can be used as
                the file extension in the write path returned.

        Returns:
            The dataset block write path.
        """
        raise NotImplementedError

    def __call__(
        self,
        base_path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        task_index: Optional[int] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return self._get_write_path_for_block(
            base_path,
            filesystem=filesystem,
            dataset_uuid=dataset_uuid,
            task_index=task_index,
            block_index=block_index,
            file_format=file_format,
        )


@DeveloperAPI
class DefaultBlockWritePathProvider(BlockWritePathProvider):
    """Default block write path provider implementation that writes each
    dataset block out to a file of the form:
    {base_path}/{dataset_uuid}_{task_index}_{block_index}.{file_format}
    """

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        task_index: Optional[int] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        assert task_index is not None
        # Add the task index to the filename to make sure that each task writes
        # to a different and deterministically generated filename.
        if block_index is not None:
            suffix = f"{dataset_uuid}_{task_index:06}_{block_index:06}.{file_format}"
        else:
            suffix = f"{dataset_uuid}_{task_index:06}.{file_format}"
        # Uses POSIX path for cross-filesystem compatibility, since PyArrow
        # FileSystem paths are always forward slash separated, see:
        # https://arrow.apache.org/docs/python/filesystems.html
        return posixpath.join(base_path, suffix)
