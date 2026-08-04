from __future__ import annotations

import bisect
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Tuple, cast

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    ParquetRowGroupChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    Bin,
    BinItem,
    FileChunks,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest


@dataclass
class _OpenBin:
    items: List[BinItem] = field(default_factory=list)
    used_bytes: int = 0

    def add(self, item: BinItem) -> None:
        self.items.append(item)
        self.used_bytes += item.uncompressed_size

    def seal(self) -> Bin:
        return Bin(tuple(self.items), self.used_bytes)


def _prefix_sums(unit_sizes: List[int]) -> List[int]:
    # prefix[i] == sum of the first i unit sizes (prefix[0] == 0).
    prefix = [0]
    for size in unit_sizes:
        prefix.append(prefix[-1] + size)
    return prefix


def _largest_prefix_fit(prefix: List[int], start: int, cap_left: int) -> int:
    # Largest end (exclusive), end >= start, such that the row groups [start, end)
    # sum to <= cap_left. prefix[i] is the cumulative size of the first i row
    # groups, so sum(sizes[start:end]) == prefix[end] - prefix[start]. Binary
    # search for the largest end with prefix[end] <= cap_left + prefix[start].
    # Returns ``start`` when not even one row group fits (caller treats that as
    # "nothing fits here").
    end = bisect.bisect_right(prefix, cap_left + prefix[start]) - 1
    return max(end, start)


def _slice_bin_item(item: BinItem, a: int, b: int) -> BinItem:
    # Row groups [a, b) of a coalesced item as a new contiguous BinItem. Uses the
    # exact per-RG sizes/rows so num_rows stays an exact survivor count for the
    # limit push-down, and rg_idx shifts by ``a`` because the run is contiguous.
    sizes = item.rg_sizes[a:b]
    rows = item.rg_rows[a:b]
    count = b - a
    return BinItem(
        path=item.path,
        rg_idx=item.rg_idx + a,
        uncompressed_size=sum(sizes),
        num_rows=sum(rows),
        fully_matched=item.fully_matched,
        rg_count=count,
        rg_sizes=sizes if count > 1 else (),
        rg_rows=rows if count > 1 else (),
    )


def _subitem(item: BinItem, num_units: int, start: int, end: int) -> BinItem:
    # The item covering units [start, end). When that is the whole item, return it
    # unchanged (so a non-split item keeps its original rg_count/rg_sizes);
    # otherwise carve out the row-group range -- only reached for splittable runs.
    if start == 0 and end == num_units:
        return item
    return _slice_bin_item(item, start, end)


def _best_open_bin(
    bins: List[_OpenBin], prefix: List[int], start: int, cap: int
) -> Tuple[Optional[_OpenBin], int]:
    # Among open bins, the one that swallows the largest prefix of units[start:]
    # with the least leftover space (best fit). Returns (bin, end); (None, start)
    # if no open bin can take even one unit.
    best: Optional[_OpenBin] = None
    best_end, best_gap = start, 0
    for b in bins:
        room = cap - b.used_bytes
        end = _largest_prefix_fit(prefix, start, room)
        if end > start:
            gap = room - (prefix[end] - prefix[start])
            if best is None or gap < best_gap:
                best, best_end, best_gap = b, end, gap
    return best, best_end


class OnlineBinPacker:
    """Streaming coloured bin packer over row-group chunks.

    Feed ``FileChunks`` via :meth:`add_file_chunks`; drain sealed bins (as
    :class:`FileManifest` blocks) via :meth:`has_partition` / :meth:`next_partition`
    as they become available; call :meth:`finalize` once all chunks are added to
    flush the still-open bins.
    """

    def __init__(
        self,
        max_bin_bytes: int,
        *,
        max_shared_open_bins: int = 16,
        split_coalesced: bool = False,
    ):
        # ``max_bin_bytes`` doubles as the "colour turns heavy" isolate threshold.
        self._cap = max_bin_bytes
        self._max_shared_open_bins = max_shared_open_bins
        # When True, a coalesced item (rg_count > 1) that does not fit whole is
        # split at physical-row-group boundaries to fill residual bin space
        # instead of opening a fresh bin. Single row groups stay atomic, so with
        # coalescing off (every rg_count == 1) this is a no-op and the packer
        # behaves exactly as when the flag is False.
        self._split_coalesced = split_coalesced

        self._seen_bytes_by_path: dict = {}  # running w(c) per colour
        self._shared_bins: List[_OpenBin] = []  # non-isolated bins (mixed colours)
        self._heavy_path: Optional[str] = None  # current heavy colour
        self._heavy_bin: Optional[_OpenBin] = None  # its open monochromatic bin
        self._output: Deque[Bin] = deque()  # sealed bins awaiting drain

    # === Feeding ===

    def add_file_chunks(self, file_chunks: FileChunks) -> None:
        path = file_chunks.path
        for row_group in file_chunks.row_groups:
            self._place(
                BinItem(
                    path=path,
                    rg_idx=row_group.rg_idx,
                    uncompressed_size=row_group.uncompressed_size,
                    num_rows=row_group.num_rows,
                    fully_matched=row_group.fully_matched,
                    rg_count=row_group.rg_count,
                    rg_sizes=row_group.rg_sizes,
                    rg_rows=row_group.rg_rows,
                )
            )

    def _units(self, item: BinItem) -> List[int]:
        # The row-group boundaries an item may be cut between, as unit sizes. A
        # splittable coalesced run (split_coalesced and rg_count > 1) yields one
        # unit per physical row group; anything else yields a single indivisible
        # unit (the whole item). Placement only ever cuts at unit boundaries.
        if self._split_coalesced and item.rg_count > 1:
            return list(item.rg_sizes)
        return [item.uncompressed_size]

    def _place(self, item: BinItem) -> None:
        item_bytes = item.uncompressed_size
        seen_bytes = self._seen_bytes_by_path.get(item.path, 0)
        self._seen_bytes_by_path[item.path] = seen_bytes + item_bytes

        if item_bytes > self._cap and len(self._units(item)) == 1:
            # Relaxation: an indivisible chunk bigger than a whole bin gets its own
            # bin. A splittable oversized run instead falls through and is cut into
            # bin-sized pieces by the placers.
            self._output.append(Bin((item,), item_bytes))
        elif seen_bytes < self._cap:
            self._place_light(item)
        else:
            self._place_heavy(item)

    def _seal_if_full(self, bin_: _OpenBin) -> None:
        # A shared bin at (or over) cap can never take another positive-size item:
        # ``_best_open_bin`` gives it end == start and the whole-item fast path
        # fails its ``used_bytes + total <= cap`` test. Leaving it in the pool just
        # burns one of the ``_max_shared_open_bins`` slots, so seal and evict it.
        if bin_.used_bytes >= self._cap:
            self._shared_bins.remove(bin_)
            self._output.append(bin_.seal())

    def _place_light(self, item: BinItem) -> None:
        # LIGHT colour -> shared First-Fit bins. First try to place the WHOLE item
        # in the first bin it fits. If it fits nowhere, cut it at unit boundaries
        # and best-fit each piece into the tightest open bin, opening a fresh bin
        # only when no open bin can take even one unit. (With splitting off the
        # item is a single unit, so this reduces to the original First Fit.)
        cap = self._cap
        units = self._units(item)
        prefix = _prefix_sums(units)
        total = prefix[-1]
        target = next(
            (b for b in self._shared_bins if b.used_bytes + total <= cap), None
        )
        if target is not None:
            target.add(item)
            self._seal_if_full(target)
            return
        start = 0
        while start < len(units):
            target, end = _best_open_bin(self._shared_bins, prefix, start, cap)
            if target is None:
                if len(self._shared_bins) >= self._max_shared_open_bins:
                    fullest = max(self._shared_bins, key=lambda b: b.used_bytes)
                    self._shared_bins.remove(fullest)
                    self._output.append(fullest.seal())
                target = _OpenBin()
                self._shared_bins.append(target)
                # A lone unit larger than a whole bin gets its own (over-sized) bin.
                end = max(_largest_prefix_fit(prefix, start, cap), start + 1)
            target.add(_subitem(item, len(units), start, end))
            self._seal_if_full(target)
            start = end

    def _place_heavy(self, item: BinItem) -> None:
        # HEAVY colour -> dedicated monochromatic bins. Fill the open bin at a unit
        # boundary, seal it once full, and carry any remnant into the next bin.
        # (With splitting off the item is a single unit, so this reduces to the
        # original Next Fit.)
        cap = self._cap
        if self._heavy_path != item.path or self._heavy_bin is None:
            if self._heavy_bin is not None:
                self._output.append(self._heavy_bin.seal())
            self._heavy_path = item.path
            self._heavy_bin = _OpenBin()
        heavy_bin = self._heavy_bin
        units = self._units(item)
        prefix = _prefix_sums(units)
        start = 0
        while start < len(units):
            end = _largest_prefix_fit(prefix, start, cap - heavy_bin.used_bytes)
            if end == start:  # nothing fits the open bin
                if heavy_bin.items:  # seal it and retry on a fresh bin
                    self._output.append(heavy_bin.seal())
                    heavy_bin = _OpenBin()
                    self._heavy_bin = heavy_bin
                    continue
                end = start + 1  # empty bin, lone unit > cap -> relaxation
            heavy_bin.add(_subitem(item, len(units), start, end))
            start = end
            if start < len(units):  # remnant remains -> bin is full, seal
                self._output.append(heavy_bin.seal())
                heavy_bin = _OpenBin()
                self._heavy_bin = heavy_bin

    # === Draining ===

    def has_partition(self) -> bool:
        return len(self._output) > 0

    def next_partition(self) -> FileManifest:
        return self._bin_to_manifest(self._output.popleft())

    def finalize(self) -> None:
        # Flush everything still open.
        if self._heavy_bin is not None and self._heavy_bin.items:
            self._output.append(self._heavy_bin.seal())
        self._heavy_bin = None
        self._heavy_path = None
        for open_bin in self._shared_bins:
            if open_bin.items:
                self._output.append(open_bin.seal())
        self._shared_bins = []

    @staticmethod
    def _bin_to_manifest(bin_: Bin) -> FileManifest:
        # One manifest row per distinct file in the bin. A file's (possibly-split)
        # items cover disjoint contiguous runs, so union their physical row-group
        # ids into the read unit for that file.
        ids_by_path: defaultdict = defaultdict(list)
        rows_by_path: defaultdict = defaultdict(int)
        size_by_path: defaultdict = defaultdict(int)
        for item in bin_.items:
            ids_by_path[item.path].extend(
                range(item.rg_idx, item.rg_idx + item.rg_count)
            )
            rows_by_path[item.path] += item.num_rows
            size_by_path[item.path] += item.uncompressed_size

        paths: List[str] = []
        sizes: List[int] = []
        chunk_metadatas: List[ParquetRowGroupChunkMetadata] = []
        for path, ids in ids_by_path.items():
            paths.append(path)
            sizes.append(size_by_path[path])
            chunk_metadatas.append(
                create_chunk_metadata(
                    ParquetRowGroupChunkMetadata,
                    row_group_ids=tuple(sorted(ids)),
                    num_rows=rows_by_path[path],
                    uncompressed_size=size_by_path[path],
                )
            )
        # TypedDict invariance: ``ParquetRowGroupChunkMetadata`` has extra keys
        # beyond the empty ``ChunkMetadata`` base, so the concrete list is not
        # assignable to ``List[Optional[ChunkMetadata]]`` without a cast.
        return FileManifest.construct_manifest(
            paths, sizes, cast(List[Optional[ChunkMetadata]], chunk_metadatas)
        )
