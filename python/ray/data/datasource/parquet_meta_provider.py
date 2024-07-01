from typing import TYPE_CHECKING, List, Optional, Union

import ray.cloudpickle as cloudpickle
from ray.data._internal.util import call_with_retry
from ray.data.block import BlockMetadata
from ray.data.datasource.file_meta_provider import (
    FileMetadataProvider,
    _fetch_metadata_parallel,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow

    from ray.data._internal.datasource.parquet_datasource import _SerializedFragment


FRAGMENTS_PER_META_FETCH = 6
PARALLELIZE_META_FETCH_THRESHOLD = 24

# The application-level exceptions to retry for metadata prefetching task.
# Default to retry on access denied and read timeout errors because AWS S3 would throw
# these transient errors when load is too high.
RETRY_EXCEPTIONS_FOR_META_FETCH_TASK = ["AWS Error ACCESS_DENIED", "Timeout"]
# Maximum number of retries for metadata prefetching task due to transient errors.
RETRY_MAX_ATTEMPTS_FOR_META_FETCH_TASK = 32
# Maximum retry back-off interval in seconds for failed metadata prefetching task.
RETRY_MAX_BACKOFF_S_FOR_META_FETCH_TASK = 64


class _ParquetFileFragmentMetaData:
    """Class to store metadata of a Parquet file fragment. This includes
    all attributes from `pyarrow.parquet.FileMetaData` except for `schema`,
    which is stored in `self.schema_pickled` as a pickled object from
    `cloudpickle.loads()`, used in deduplicating schemas across multiple fragments."""

    def __init__(self, fragment_metadata: "pyarrow.parquet.FileMetaData"):
        self.created_by = fragment_metadata.created_by
        self.format_version = fragment_metadata.format_version
        self.num_columns = fragment_metadata.num_columns
        self.num_row_groups = fragment_metadata.num_row_groups
        self.num_rows = fragment_metadata.num_rows
        self.serialized_size = fragment_metadata.serialized_size
        # This is a pickled schema object, to be set later with
        # `self.set_schema_pickled()`. To get the underlying schema, use
        # `cloudpickle.loads(self.schema_pickled)`.
        self.schema_pickled = None

        # Calculate the total byte size of the file fragment using the original
        # object, as it is not possible to access row groups from this class.
        self.total_byte_size = 0
        for row_group_idx in range(fragment_metadata.num_row_groups):
            row_group_metadata = fragment_metadata.row_group(row_group_idx)
            self.total_byte_size += row_group_metadata.total_byte_size

    def set_schema_pickled(self, schema_pickled: bytes):
        """Note: to get the underlying schema, use
        `cloudpickle.loads(self.schema_pickled)`."""
        self.schema_pickled = schema_pickled


@DeveloperAPI
class ParquetMetadataProvider(FileMetadataProvider):
    """Provides block metadata for Arrow Parquet file fragments."""

    def _get_block_metadata(
        self,
        paths: List[str],
        schema: Optional[Union[type, "pyarrow.lib.Schema"]],
        *,
        num_fragments: int,
        prefetched_metadata: Optional[List["_ParquetFileFragmentMetaData"]],
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files of a single dataset block.

        Args:
            paths: The file paths for a single dataset block.
            schema: The user-provided or inferred schema for the given file
                paths, if any.
            num_fragments: The number of Parquet file fragments derived from the input
                file paths.
            prefetched_metadata: Metadata previously returned from
                `prefetch_file_metadata()` for each file fragment, where
                `prefetched_metadata[i]` contains the metadata for `fragments[i]`.

        Returns:
            BlockMetadata aggregated across the given file paths.
        """
        if (
            prefetched_metadata is not None
            and len(prefetched_metadata) == num_fragments
            and all(m is not None for m in prefetched_metadata)
        ):
            # Fragment metadata was available, construct a normal
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=sum(m.num_rows for m in prefetched_metadata),
                size_bytes=sum(m.total_byte_size for m in prefetched_metadata),
                schema=schema,
                input_files=paths,
                exec_stats=None,
            )  # Exec stats filled in later.
        else:
            # Fragment metadata was not available, construct an empty
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=schema,
                input_files=paths,
                exec_stats=None,
            )
        return block_metadata

    def prefetch_file_metadata(
        self,
        fragments: List["pyarrow.dataset.ParquetFileFragment"],
        **ray_remote_args,
    ) -> Optional[List[_ParquetFileFragmentMetaData]]:
        """Pre-fetches file metadata for all Parquet file fragments in a single batch.

        Subsets of the metadata returned will be provided as input to subsequent calls
        to ``_get_block_metadata`` together with their corresponding Parquet file
        fragments.

        Args:
            fragments: The Parquet file fragments to fetch metadata for.

        Returns:
            Metadata resolved for each input file fragment, or `None`. Metadata
            must be returned in the same order as all input file fragments, such
            that `metadata[i]` always contains the metadata for `fragments[i]`.
        """
        from ray.data._internal.datasource.parquet_datasource import _SerializedFragment

        if len(fragments) > PARALLELIZE_META_FETCH_THRESHOLD:
            # Wrap Parquet fragments in serialization workaround.
            fragments = [_SerializedFragment(fragment) for fragment in fragments]
            # Fetch Parquet metadata in parallel using Ray tasks.

            def fetch_func(fragments):
                return _fetch_metadata_serialization_wrapper(
                    fragments,
                    # Ensure that retry settings are propagated to remote tasks.
                    retry_match=RETRY_EXCEPTIONS_FOR_META_FETCH_TASK,
                    retry_max_attempts=RETRY_MAX_ATTEMPTS_FOR_META_FETCH_TASK,
                    retry_max_interval=RETRY_MAX_BACKOFF_S_FOR_META_FETCH_TASK,
                )

            raw_metadata = list(
                _fetch_metadata_parallel(
                    fragments,
                    fetch_func,
                    FRAGMENTS_PER_META_FETCH,
                    **ray_remote_args,
                )
            )
        else:
            raw_metadata = _fetch_metadata(fragments)

        return _dedupe_metadata(raw_metadata)


def _fetch_metadata_serialization_wrapper(
    fragments: List["_SerializedFragment"],
    retry_match: Optional[List[str]],
    retry_max_attempts: int,
    retry_max_interval: int,
) -> List["pyarrow.parquet.FileMetaData"]:
    from ray.data._internal.datasource.parquet_datasource import (
        _deserialize_fragments_with_retry,
    )

    deserialized_fragments = _deserialize_fragments_with_retry(fragments)
    try:
        metadata = call_with_retry(
            lambda: _fetch_metadata(deserialized_fragments),
            description="fetch metdata",
            match=retry_match,
            max_attempts=retry_max_attempts,
            max_backoff_s=retry_max_interval,
        )
    except OSError as e:
        raise RuntimeError(
            f"Exceeded maximum number of attempts ({retry_max_attempts}) to retry "
            "metadata fetching task. Metadata fetching tasks can fail due to transient "
            "errors like rate limiting.\n"
            "\n"
            "To increase the maximum number of attempts, configure "
            "`RETRY_MAX_ATTEMPTS_FOR_META_FETCH_TASK`. For example:\n"
            "```\n"
            "ray.data._internal.datasource.parquet_datasource.RETRY_MAX_ATTEMPTS_FOR_META_FETCH_TASK = 64\n"  # noqa: E501
            "```\n"
            "To increase the maximum retry backoff interval, configure "
            "`RETRY_MAX_BACKOFF_S_FOR_META_FETCH_TASK`. For example:\n"
            "```\n"
            "ray.data._internal.datasource.parquet_datasource.RETRY_MAX_BACKOFF_S_FOR_META_FETCH_TASK = 128\n"  # noqa: E501
            "```\n"
            "If the error continues to occur, you can also try decresasing the "
            "concurency of metadata fetching tasks by setting "
            "`NUM_CPUS_FOR_META_FETCH_TASK` to a larger value. For example:\n"
            "```\n"
            "ray.data._internal.datasource.parquet_datasource.NUM_CPUS_FOR_META_FETCH_TASK = 4.\n"  # noqa: E501
            "```\n"
            "To change which exceptions to retry on, set "
            "`RETRY_EXCEPTIONS_FOR_META_FETCH_TASK` to a list of error messages. For "
            "example:\n"
            "```\n"
            'ray.data._internal.datasource.parquet_datasource.RETRY_EXCEPTIONS_FOR_META_FETCH_TASK = ["AWS Error ACCESS_DENIED", "Timeout"]\n'  # noqa: E501
            "```"
        ) from e
    return metadata


def _fetch_metadata(
    fragments: List["pyarrow.dataset.ParquetFileFragment"],
) -> List["pyarrow.parquet.FileMetaData"]:
    fragment_metadata = []
    for f in fragments:
        try:
            fragment_metadata.append(f.metadata)
        except AttributeError:
            break
    return fragment_metadata


def _dedupe_metadata(
    raw_metadatas: List["pyarrow.parquet.FileMetaData"],
) -> List[_ParquetFileFragmentMetaData]:
    """For datasets with a large number of columns, the FileMetaData
    (in particular the schema) can be very large. We can reduce the
    memory usage by only keeping unique schema objects across all
    file fragments. This method deduplicates the schemas and returns
    a list of `_ParquetFileFragmentMetaData` objects."""
    schema_to_id = {}  # schema_id -> serialized_schema
    id_to_schema = {}  # serialized_schema -> schema_id
    stripped_metadatas = []
    for fragment_metadata in raw_metadatas:
        stripped_md = _ParquetFileFragmentMetaData(fragment_metadata)

        schema_ser = cloudpickle.dumps(fragment_metadata.schema.to_arrow_schema())
        if schema_ser not in schema_to_id:
            schema_id = len(schema_to_id)
            schema_to_id[schema_ser] = schema_id
            id_to_schema[schema_id] = schema_ser
            stripped_md.set_schema_pickled(schema_ser)
        else:
            schema_id = schema_to_id.get(schema_ser)
            existing_schema_ser = id_to_schema[schema_id]
            stripped_md.set_schema_pickled(existing_schema_ser)
        stripped_metadatas.append(stripped_md)
    return stripped_metadatas
