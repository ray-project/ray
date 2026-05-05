from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Literal, Optional, Union

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.scanners.scanner import Scanner
from ray.data._internal.util import _is_local_scheme
from ray.data.datasource.file_based_datasource import (
    FileShuffleConfig,
    _validate_shuffle_arg,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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

    @staticmethod
    def compute_local_scheduling(
        paths: Union[str, List[str]],
    ) -> Optional["NodeAffinitySchedulingStrategy"]:
        """Return a node-affinity scheduling strategy for local-scheme reads.

        When paths point to a local filesystem, pin read tasks to the driver
        node with hard affinity so they can access those files. Returns
        ``None`` for remote paths, which any node can read. Raises under Ray
        Client because cluster nodes can't see the driver's local files.
        """
        import ray
        import ray.util.client

        if not _is_local_scheme(paths):
            return None

        if ray.util.client.ray.is_connected():
            raise ValueError(
                "Because you're using Ray Client, read tasks scheduled on the "
                "Ray cluster can't access your local files. To fix this issue, "
                "store files in cloud storage or a distributed filesystem like "
                "NFS."
            )

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        return NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
