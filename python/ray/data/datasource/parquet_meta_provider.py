import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Tuple

from ray.data._internal.util import call_with_retry
from ray.data.block import BlockMetadata
from ray.data.datasource.file_meta_provider import (
    FileMetadataProvider,
    _fetch_metadata_parallel,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from pyarrow.dataset import ParquetFileFragment


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


logger = logging.getLogger(__name__)


@dataclass
class ParquetFileMetadata:
    num_bytes: int
    num_rows: Optional[int] = field(default=None)

    @classmethod
    def from_(cls, pqm: "pyarrow.parquet.FileMetaData"):
        return ParquetFileMetadata(
            num_rows=pqm.num_rows,
            num_bytes=_get_total_bytes(pqm),
        )


@DeveloperAPI
class ParquetMetadataProvider(FileMetadataProvider):
    """Provides block metadata for Arrow Parquet file fragments."""

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        num_fragments: int,
        prefetched_metadata: Optional[List["ParquetFileMetadata"]],
    ) -> BlockMetadata:
        """Resolves and returns block metadata for files of a single dataset block.

        Args:
            paths: The file paths for a single dataset block.
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
            total_bytes, total_rows = self._derive_totals(prefetched_metadata)

            # Fragment metadata was available, construct a normal
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=total_rows,
                size_bytes=total_bytes,
                input_files=paths,
                exec_stats=None,
            )  # Exec stats filled in later.
        else:
            # Fragment metadata was not available, construct an empty
            # BlockMetadata.
            block_metadata = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                input_files=paths,
                exec_stats=None,
            )
        return block_metadata

    @staticmethod
    def _derive_totals(
        prefetched_metadata: List["ParquetFileMetadata"],
    ) -> Tuple[int, int]:
        total_bytes = 0
        total_rows = 0

        for m in prefetched_metadata:
            total_bytes += m.num_bytes

            if total_rows is not None:
                if m.num_rows is not None:
                    total_rows += m.num_rows
                else:
                    total_rows = None

        return total_bytes, total_rows

    def prefetch_file_metadata(
        self,
        fragments: List["pyarrow.dataset.ParquetFileFragment"],
        **ray_remote_args,
    ) -> Optional[List[ParquetFileMetadata]]:
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
        from ray.data._internal.datasource.parquet_datasource import (
            _NoIOSerializableFragmentWrapper,
        )

        if len(fragments) > PARALLELIZE_META_FETCH_THRESHOLD:
            # Wrap Parquet fragments in serialization workaround.
            fragments = [
                _NoIOSerializableFragmentWrapper(fragment) for fragment in fragments
            ]
            # Fetch Parquet metadata in parallel using Ray tasks.
            def _remote_fetch(fragments: List["ParquetFileFragment"]):
                return _fetch_metadata_with_retry(
                    fragments,
                    # Ensure that retry settings are propagated to remote tasks.
                    retry_match=RETRY_EXCEPTIONS_FOR_META_FETCH_TASK,
                    retry_max_attempts=RETRY_MAX_ATTEMPTS_FOR_META_FETCH_TASK,
                    retry_max_interval=RETRY_MAX_BACKOFF_S_FOR_META_FETCH_TASK,
                )

            raw_metadata = list(
                _fetch_metadata_parallel(
                    fragments,
                    _remote_fetch,
                    FRAGMENTS_PER_META_FETCH,
                    **ray_remote_args,
                )
            )

            return raw_metadata

        else:
            # We don't deduplicate schemas in this branch because they're already
            # deduplicated in `_fetch_metadata`. See
            # https://github.com/ray-project/ray/pull/54821/files#r2265140929 for
            # related discussion.
            raw_metadata = _fetch_metadata(fragments)
            return raw_metadata


def _fetch_metadata_with_retry(
    fragments: List["ParquetFileFragment"],
    retry_match: Optional[List[str]],
    retry_max_attempts: int,
    retry_max_interval: int,
) -> List["ParquetFileMetadata"]:
    try:
        metadata = call_with_retry(
            lambda: _fetch_metadata(fragments),
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
) -> List["ParquetFileMetadata"]:
    fragment_metadatas = []
    for f in fragments:
        try:
            # Convert directly to _ParquetFileFragmentMetaData
            fragment_metadatas.append(ParquetFileMetadata.from_(f.metadata))
        except AttributeError as ae:
            logger.warning(f"Failed to extract metadata from parquet file: {ae}")
            break
    # Deduplicate schemas to reduce memory usage
    return fragment_metadatas


def _get_total_bytes(pqm: "pyarrow.parquet.FileMetaData") -> int:
    return sum(pqm.row_group(i).total_byte_size for i in range(pqm.num_row_groups))
