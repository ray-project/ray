from dataclasses import dataclass, field
from typing import Literal, Union

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.scanners.scanner import Scanner
from ray.data.datasource.file_based_datasource import (
    FileShuffleConfig,
    _validate_shuffle_arg,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass(frozen=True)
class FileScanner(Scanner[FileManifest]):
    """Base scanner for file-based datasources.

    Subclasses implement format-specific ``read_schema()`` and
    ``create_reader()``. Shuffling and parallel bucketing are handled
    upstream in the ``ListFiles`` transform chain (``shuffle_files`` +
    ``RoundRobinPartitioner`` via ``plan_list_files_op``), not here.

    PyArrow Dataset-based scanners should subclass ``ArrowFileScanner``;
    use ``FileScanner`` directly for non-Arrow file formats.
    """

    # kw_only so subclass dataclasses can declare their own required fields
    # (like ``ArrowFileScanner.schema``) without running into the "non-default
    # argument follows default argument" dataclass inheritance rule.
    shuffle: Union[Literal["files"], FileShuffleConfig, None] = field(
        default=None, kw_only=True
    )

    def __post_init__(self) -> None:
        _validate_shuffle_arg(self.shuffle)

    def prune_manifest(self, manifest: FileManifest) -> FileManifest:
        """Return a filtered view of ``manifest``.

        Default: identity. Subclasses that support file-level predicate
        pruning (e.g. :class:`ArrowFileScanner`'s ``partition_predicate``)
        override this to drop rows whose partition values fail the
        predicate. Invoked per-block from
        :func:`plan_read_files_op.do_read`.
        """
        return manifest
