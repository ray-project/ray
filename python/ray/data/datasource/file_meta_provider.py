from typing import (
    List,
    Optional,
    Union,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    import pyarrow

from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class FileMetadataProvider:
    """Abstract callable that provides metadata for files in a list of paths.

    Current subclasses:
        BaseFileMetadataProvider
        ParquetMetadataProvider
    """

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        **kwargs,
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files in the given paths.

        Args:
            paths: The paths to aggregate block metadata across.
            schema: The user-provided or inferred schema for the given paths,
                if any.

        Returns:
            BlockMetadata aggregated across the given paths.
        """
        raise NotImplementedError

    def __call__(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        **kwargs,
    ) -> BlockMetadata:
        return self._get_block_metadata(paths, schema, **kwargs)
