"""Deterministic, bounded ORC layout discovery and online grouping."""

import math
import secrets
from collections import deque
from dataclasses import replace
from itertools import islice

import ray
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
    _shuffle_file_infos,
)
from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    orc_read_unit_id,
    read_orc_metadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
    PartitionHints,
)
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class _OrcMetadataReader:
    def __init__(self, filesystem, context):
        self._filesystem = filesystem
        DataContext._set_current(context)

    def read(self, path):
        return read_orc_metadata(path, self._filesystem)


OrcMetadataActor = ray.remote(num_cpus=0)(_OrcMetadataReader)


class OrcStripeIndexer(NonSamplingFileIndexer):
    """Keep discovery and delivery stable even when footer requests finish out of order.

    Seeds are immutable and per read. All attempt-local cursors, pending results
    and dedup state live in the generator, so task retry starts from scratch.
    """

    def __init__(
        self,
        *,
        seeds=(),
        local_only=False,
        local_node_id=None,
        shuffle_seed=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._seeds = tuple(seeds)
        self._local_only = local_only
        self._local_node_id = local_node_id
        if local_only and local_node_id is None and ray.is_initialized():
            self._local_node_id = ray.get_runtime_context().get_node_id()
        self._shuffle_seed = (
            secrets.randbits(64) if shuffle_seed is None else shuffle_seed
        )

    def _replayable_shuffle(self, config):
        if config is not None and config.seed is None:
            # A random seed is selected once when preparing the Dataset, never
            # inside an attempt. execution_idx still reseeds a fresh execution.
            return replace(config, seed=self._shuffle_seed, reseed_after_execution=True)
        return config

    def list_files(
        self,
        paths,
        *,
        filesystem,
        pruners=None,
        preserve_order=False,
        predicate=None,
        limit=None,
        projected_columns=None,
        shuffle_config=None,
        execution_idx=0,
        excluded_read_unit_ids=None
    ):
        files = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=True,
            shuffle_config=self._replayable_shuffle(shuffle_config),
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        yield from self._process_files(
            files, filesystem, predicate, limit, excluded_read_unit_ids
        )

    def list_files_from_file_infos(
        self,
        file_infos,
        *,
        filesystem,
        preserve_order=False,
        predicate=None,
        limit=None,
        projected_columns=None,
        shuffle_config=None,
        execution_idx=0,
        excluded_read_unit_ids=None
    ):
        shuffle_config = self._replayable_shuffle(shuffle_config)
        if shuffle_config is not None:
            file_infos = _shuffle_file_infos(
                list(file_infos), shuffle_config.get_seed(execution_idx)
            )
        yield from self._process_files(
            file_infos, filesystem, predicate, limit, excluded_read_unit_ids
        )

    def _iter_layouts(self, files, filesystem):
        seeds = {(seed.path, seed.size): seed.layout for seed in self._seeds}
        files = iter(files)
        pending = deque()
        actors = []
        refs = set()
        next_actor = 0
        exhausted = False
        try:
            while pending or not exhausted:
                batch = list(islice(files, 8 - len(pending)))
                exhausted = len(batch) < 8 - len(pending)
                misses = sum((file.path, file.size) not in seeds for file in batch)
                needed = min(4, misses + len(refs))
                while len(actors) < needed:
                    strategy = (
                        NodeAffinitySchedulingStrategy(
                            self._local_node_id
                            or ray.get_runtime_context().get_node_id(),
                            soft=False,
                        )
                        if self._local_only
                        else "SPREAD"
                    )
                    actors.append(
                        OrcMetadataActor.options(scheduling_strategy=strategy).remote(
                            filesystem, DataContext.get_current()
                        )
                    )
                for file in batch:
                    layout = seeds.get((file.path, file.size))
                    if layout is not None:
                        pending.append((file, layout, None))
                    else:
                        ref = actors[next_actor % len(actors)].read.remote(file.path)
                        next_actor += 1
                        refs.add(ref)
                        pending.append((file, None, ref))
                if not pending:
                    break
                file, layout, ref = pending[0]
                if ref is not None:
                    layout = ray.get(ref)
                # Hold the window credit while the caller delivers *all* runs.
                yield file, layout
                pending.popleft()
                if ref is not None:
                    refs.remove(ref)
        finally:
            for ref in refs:
                ray.cancel(ref, force=False)
            for actor in actors:
                ray.kill(actor, no_restart=True)
            close = getattr(files, "close", None)
            if close is not None:
                close()

    def _process_files(self, files, filesystem, predicate, limit, excluded):
        excluded = excluded or frozenset()
        seen = set()

        def unique_files():
            for file in files:
                if (
                    file.size is not None
                    and file.path not in seen
                    and file.path not in excluded
                ):
                    seen.add(file.path)
                    yield file

        matched_rows = 0
        layouts = self._iter_layouts(unique_files(), filesystem)
        try:
            if limit is not None and limit <= 0:
                return
            for file, layout in layouts:
                if len(layout.stripes) <= 1:
                    yield FileManifest.construct_manifest(
                        paths=[file.path], sizes=[file.size], chunk_metadatas=[None]
                    )
                    if predicate is None:
                        matched_rows += layout.num_rows
                else:
                    selected = [
                        stripe
                        for stripe in layout.stripes
                        if stripe.num_rows
                        and orc_read_unit_id(file.path, stripe.index) not in excluded
                    ]
                    for start in range(0, len(selected), 128):
                        stripes = selected[start : start + 128]
                        if predicate is None and limit is not None:
                            keep = []
                            for stripe in stripes:
                                keep.append(stripe)
                                matched_rows += stripe.num_rows
                                if matched_rows >= limit:
                                    break
                            stripes = keep
                        chunk = FileChunk(
                            unit_ids=tuple(s.index for s in stripes),
                            num_rows=sum(s.num_rows for s in stripes),
                            size_bytes=sum(s.size_bytes for s in stripes),
                            fully_matched=predicate is None,
                            unit_sizes=tuple(s.size_bytes for s in stripes),
                            unit_rows=tuple(s.num_rows for s in stripes),
                            unit_start_rows=tuple(s.row_offset for s in stripes),
                            file_num_units=len(layout.stripes),
                        )
                        yield FileManifest.construct_manifest(
                            paths=[file.path],
                            sizes=[file.size],
                            chunk_metadatas=[chunk.to_metadata()],
                        )
                        if (
                            predicate is None
                            and limit is not None
                            and matched_rows >= limit
                        ):
                            return
                if predicate is None and limit is not None and matched_rows >= limit:
                    return
        finally:
            layouts.close()


class OrcStripePartitioner(FilePartitioner):
    """Freeze an encoded-byte budget before execution; deliver sealed groups online."""

    def __init__(self, hints: PartitionHints, *, bootstrap_bytes=None):
        import os

        target = int(
            os.environ.get("RAY_DATA_ORC_BIN_PACKING_BYTES", 128 * 1024 * 1024)
        )
        if target <= 0:
            raise ValueError("RAY_DATA_ORC_BIN_PACKING_BYTES must be positive")
        if bootstrap_bytes is not None:
            target = min(target, math.ceil(bootstrap_bytes / max(1, hints.num_buckets)))
        self._budget = max(
            1, min(hints.max_bucket_size, max(hints.min_bucket_size, target))
        )
        self._preserve_order = (
            DataContext.get_current().execution_options.preserve_order
        )
        self._packer = OnlineBinPacker(self._budget, split_coalesced=True)
        self._output = deque()
        self._bucket = []
        self._bucket_bytes = 0

    @property
    def requires_global_input(self):
        return True

    def add_input(self, input_manifest: FileManifest) -> None:
        manifest = input_manifest
        if not self._preserve_order:
            self._packer.add_input(manifest)
            return
        for path, size, metadata in zip(
            manifest.paths, manifest.file_sizes, manifest.file_chunk_metadatas
        ):
            run = FileChunk.from_metadata(metadata) if metadata is not None else None
            n = len(run.unit_ids) if run else 1
            for i in range(n):
                chunk = (
                    None
                    if run is None
                    else FileChunk(
                        unit_ids=(run.unit_ids[i],),
                        num_rows=run.unit_rows[i],
                        size_bytes=run.unit_sizes[i],
                        fully_matched=run.fully_matched,
                        unit_rows=(run.unit_rows[i],),
                        unit_sizes=(run.unit_sizes[i],),
                        unit_start_rows=(run.unit_start_rows[i],),
                        file_num_units=run.file_num_units,
                    )
                )
                weight = chunk.size_bytes if chunk else int(size)
                if self._bucket and self._bucket_bytes + weight > self._budget:
                    self._seal()
                self._bucket.append(
                    FileManifest.construct_manifest(
                        paths=[path],
                        sizes=[int(size)],
                        chunk_metadatas=[chunk.to_metadata() if chunk else None],
                    )
                )
                self._bucket_bytes += weight
                if self._bucket_bytes >= self._budget:
                    self._seal()

    def _seal(self):
        if self._bucket:
            self._output.append(FileManifest.concat(self._bucket))
            self._bucket = []
            self._bucket_bytes = 0

    def has_partition(self):
        return (
            bool(self._output) if self._preserve_order else self._packer.has_partition()
        )

    def next_partition(self):
        return (
            self._output.popleft()
            if self._preserve_order
            else self._packer.next_partition()
        )

    def finalize(self):
        if self._preserve_order:
            self._seal()
        else:
            self._packer.finalize()
