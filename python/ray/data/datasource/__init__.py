# Lazy imports for symbols from _internal to avoid circular import:
# ray.data._internal.datasource is loaded by ray.data.__init__, which then
# loads submodules (e.g. audio_datasource) that import from this package.
# If we import _internal here at top level, _internal is not fully loaded yet.
from ray.data.datasource.datasink import (
    Datasink,
    DummyOutputDatasink,
    WriteResult,
    WriteReturnType,
)
from ray.data.datasource.datasource import (
    Datasource,
    RandomIntRowDatasource,
    Reader,
    ReadTask,
)
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    FileShuffleConfig,
    _S3FileSystemWrapper,
)
from ray.data.datasource.file_datasink import (
    BlockBasedFileDatasink,
    RowBasedFileDatasink,
)
from ray.data.datasource.file_meta_provider import (
    BaseFileMetadataProvider,
    DefaultFileMetadataProvider,
    FileMetadataProvider,
)
from ray.data.datasource.filename_provider import FilenameProvider
from ray.data.datasource.partitioning import (
    Partitioning,
    PartitionStyle,
    PathPartitionFilter,
    PathPartitionParser,
)

# Note: HuggingFaceDatasource should NOT be imported here, because
# we want to only import the Hugging Face datasets library when we use
# ray.data.from_huggingface() or HuggingFaceDatasource() directly.

# Lazy-loaded names from _internal (see __getattr__ below)
_INTERNAL_NAMES = frozenset(
    {
        "Connection",
        "DeltaSharingDatasource",
        "MCAPDatasource",
        "TimeRange",
        "SaveMode",
        "TurbopufferDatasink",
    }
)


def __getattr__(name: str):
    if name in _INTERNAL_NAMES:
        if name == "TurbopufferDatasink":
            from ray.data._internal.datasource.turbopuffer_datasink import (
                TurbopufferDatasink,
            )

            return TurbopufferDatasink
        if name == "SaveMode":
            from ray.data._internal.savemode import SaveMode

            return SaveMode
        return {
            "Connection": _get_connection,
            "DeltaSharingDatasource": _get_delta_sharing_datasource,
            "MCAPDatasource": _get_mcap_datasource,
            "TimeRange": _get_time_range,
        }[name]()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _get_connection():
    from ray.data._internal.datasource.sql_datasource import Connection

    return Connection


def _get_delta_sharing_datasource():
    from ray.data._internal.datasource.delta_sharing_datasource import (
        DeltaSharingDatasource,
    )

    return DeltaSharingDatasource


def _get_mcap_datasource():
    from ray.data._internal.datasource.mcap_datasource import MCAPDatasource

    return MCAPDatasource


def _get_time_range():
    from ray.data._internal.datasource.mcap_datasource import TimeRange

    return TimeRange


__all__ = [
    "BaseFileMetadataProvider",
    "Connection",
    "Datasink",
    "Datasource",
    "DefaultFileMetadataProvider",
    "DeltaSharingDatasource",
    "DummyOutputDatasink",
    "FileBasedDatasource",
    "FileShuffleConfig",
    "FileMetadataProvider",
    "FilenameProvider",
    "MCAPDatasource",
    "PartitionStyle",
    "PathPartitionFilter",
    "PathPartitionParser",
    "Partitioning",
    "RandomIntRowDatasource",
    "ReadTask",
    "Reader",
    "RowBasedFileDatasink",
    "TurbopufferDatasink",
    "BlockBasedFileDatasink",
    "_S3FileSystemWrapper",
    "TimeRange",
    "WriteResult",
    "WriteReturnType",
    "SaveMode",
]
