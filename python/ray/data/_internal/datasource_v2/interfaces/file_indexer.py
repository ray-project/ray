from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Iterable,
    List,
    Optional,
)

from pyarrow.fs import FileSystem

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data.block import BlockColumn

if TYPE_CHECKING:
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr


class FileIndexer(ABC):
    """Turns root paths into ``FileManifest`` blocks inside ``ListFiles`` tasks.

    Chosen by ``DataSourceV2._get_file_indexer``. How a file is split into
    manifest rows is the implementation's business, not this interface's.
    """

    def as_whole_file_indexer(self) -> Optional["FileIndexer"]:
        """An equivalent indexer that emits each file exactly once, or ``None``.

        Metadata-only consumers -- currently the ``PushdownCountFiles`` rule --
        need a listing where one file means one manifest row and listing itself
        does no per-file IO. An indexer that splits files, bin-packs them, or
        reads metadata while listing cannot provide that, and would over-count.

        Default ``None`` means "cannot provide it", so such consumers decline
        and fall back to a real read. Fail-closed on purpose: a wrong ``count()``
        is silent, a declined optimization is merely slower.
        """
        return None

    @abstractmethod
    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
        excluded_read_unit_ids: Optional[AbstractSet[str]] = None,
    ) -> Iterable[FileManifest]:
        """List files and their on-disk sizes for the given path.

        Args:
            paths: A column of paths pointing to files or directories.
            filesystem: PyArrow filesystem to list through, or ``None`` for an
                indexer that does its own IO (the framework forwards
                ``DataSourceV2.filesystem`` unchanged).
            pruners: A list of file pruners to apply.
            preserve_order: Whether to preserve order in file listing.
            predicate: Pushed-down row filter. Indexers that read file
                metadata (e.g. the footer-based Parquet indexer) use it to skip
                row groups; others ignore it.
            limit: Pushed-down row limit, for indexers that can stop listing
                early. Others ignore it.
            projected_columns: Pushed-down column projection, for metadata-aware
                sizing. Others ignore it.
            shuffle_config: When set, listed files are shuffled after path
                discovery and before any metadata fetch (footer reads).
                :meth:`list_file_infos` is never shuffled.
            execution_idx: Execution index used with ``shuffle_config`` to
                derive a per-execution seed.
            excluded_read_unit_ids: Ids of read units (see ``ReadUnit.id``) a
                checkpoint already finished; they must not be listed. This is
                all or nothing: only pass completely checkpointed units. A
                partially finished unit is skipped whole if passed. A file
                path names the whole file and every indexer honors it. An
                indexer that lists a file in parts also honors the ids its
                reader reports for those parts (the footer-based Parquet
                indexer: ``"<path>#rg<N>"`` per row group). Ids that name
                nothing in the listing are ignored.

        Returns:
            An iterator of `FileManifest` objects, each of which contains a file path
            and the on-disk size of the file in bytes.
        """
        ...

    @abstractmethod
    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable["FileInfo"]:
        """List files as raw ``FileInfo``\\ s (path + on-disk size).

        Unlike :meth:`list_files`, this yields the pre-chunk file stream and is
        never shuffled. The footer-based Parquet path consumes it (after an
        optional file shuffle) -- it reads each file's footer and bin-packs
        row groups itself, so it needs paths + sizes rather than pre-chunked
        manifest rows.
        """
        ...


@dataclass(frozen=True)
class FileInfo:
    """File information for file listing."""

    path: str
    size: Optional[int]
