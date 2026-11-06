"""Deterministic, bounded ORC layout discovery and online grouping."""

import math
import secrets
from collections import deque
from contextlib import closing
from dataclasses import replace
from itertools import accumulate, islice
from typing import List, cast

import ray
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
    _shuffle_file_infos,
)
from ray.data._internal.datasource_v2.common.online_bin_packer import (
    BinItem,
    OnlineBinPacker,
    _bin_items,
    _slice_bin_item,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    orc_read_unit_id,
    read_orc_metadata,
    require_stripe_reads,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
    PartitionHints,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class _OrcMetadataReader:
    def __init__(self, filesystem, context):
        self._filesystem = filesystem
        DataContext._set_current(context)

    def read(self, path):
        require_stripe_reads(path)
        return read_orc_metadata(path, self._filesystem)


OrcMetadataActor = ray.remote(num_cpus=0)(_OrcMetadataReader)


class OrcStripeIndexer(NonSamplingFileIndexer):
    """Keep discovery and delivery stable even when footer requests finish out of order.

    Seeds are immutable and per read. All attempt-local cursors, pending results
    and dedup state live in the generator, so task retry starts from scratch.
    """

    def __init__(self, *, seeds=(), local_only=False, **kwargs):
        super().__init__(**kwargs)
        self._seeds = tuple(seeds)
        self._local_only = local_only
        self._local_node_id = None
        if local_only and ray.is_initialized():
            self._local_node_id = ray.get_runtime_context().get_node_id()
        self._shuffle_seed = secrets.randbits(64)

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
        seeds = {(seed.path, seed.size): seed for seed in self._seeds}
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
                            filesystem,
                            DataContext.get_current(),
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
        excluded, seen = excluded or frozenset(), set()

        def unique():
            for file in files:
                if (
                    file.size is not None
                    and file.path not in seen
                    and file.path not in excluded
                ):
                    seen.add(file.path)
                    yield file

        matched = 0
        with closing(self._iter_layouts(unique(), filesystem)) as layouts:
            if limit is not None and limit <= 0:
                return
            for file, layout in layouts:
                runs = [None]
                if len(layout.stripes or ()) > 1:
                    selected = [
                        s
                        for s in layout.stripes
                        if s.num_rows
                        and orc_read_unit_id(file.path, s.index) not in excluded
                    ]
                    if predicate is None and limit is not None:
                        selected = [
                            s
                            for s, total in zip(
                                selected, accumulate(s.num_rows for s in selected)
                            )
                            if total - s.num_rows < limit - matched
                        ]
                    runs = (
                        FileChunk(
                            tuple(s.index for s in group),
                            sum(s.num_rows for s in group),
                            sum(s.size_bytes for s in group),
                            fully_matched=predicate is None,
                            unit_rows=tuple(s.num_rows for s in group),
                            unit_sizes=tuple(s.size_bytes for s in group),
                        )
                        for group in (
                            selected[i : i + 128] for i in range(0, len(selected), 128)
                        )
                    )
                for run in runs:
                    yield FileManifest.construct_manifest(
                        paths=[file.path],
                        sizes=[file.size],
                        chunk_metadatas=[run.to_metadata() if run else None],
                    )
                    if predicate is None:
                        matched += run.num_rows if run else layout.num_rows
                        if limit is not None and matched >= limit:
                            return


class OrcStripePartitioner(FilePartitioner):
    """Freeze one encoded-byte budget and drain sealed groups online."""

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
        self._ordered = DataContext.get_current().execution_options.preserve_order
        self._packer = (
            WeightedRoundRobinPartitioner(
                min_bucket_size=self._budget,
                max_bucket_size=self._budget,
                num_buckets=1,
                emit_before_overflow=True,
            )
            if self._ordered
            else OnlineBinPacker(self._budget, split_coalesced=True)
        )

    @property
    def requires_global_input(self):
        return True

    def add_input(self, input_manifest):
        if not self._ordered:
            cast(OnlineBinPacker, self._packer).add_input(input_manifest)
            return
        for item in _bin_items(input_manifest):
            run = item.run
            for i in range(len(run.unit_ids) if run else 1):
                unit = (
                    _slice_bin_item(item, i, i + 1)
                    if run and len(run.unit_ids) > 1
                    else item
                )
                cast(WeightedRoundRobinPartitioner, self._packer).add_item(
                    unit, unit.size_bytes
                )

    def has_partition(self):
        return self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        value = self._packer.next_partition()
        if not self._ordered:
            return cast(FileManifest, value)
        value = cast(List[BinItem], value)
        return FileManifest.construct_manifest(
            paths=[item.file.path for item in value],
            sizes=[cast(int, item.file.size) for item in value],
            chunk_metadatas=[
                item.run.to_metadata() if item.run else None for item in value
            ],
        )

    def finalize(self):
        self._packer.finalize()
