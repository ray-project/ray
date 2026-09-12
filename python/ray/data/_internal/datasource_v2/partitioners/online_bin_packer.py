from __future__ import annotations

import bisect
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Tuple, cast

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    ParquetRowGroupChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_decoded_size import (
    decoded_size_or_fallback,
    sum_exact,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    Bin,
    BinItem,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)

logger = logging.getLogger(__name__)


def _decoded_bytes(item: BinItem) -> int:
    """The item's size in the units the bin budget is measured in.

    Parquet's uncompressed size is still dictionary/RLE/bit-packed, so it is a
    poor proxy for the Arrow block a read task materializes -- which is what the
    budget is really bounding. Falls back to the pre-existing scaled estimate when
    the footer could not produce an exact decoded size.
    """
    return decoded_size_or_fallback(item.decoded_size, item.uncompressed_size)


@dataclass
class _OpenBin:
    items: List[BinItem] = field(default_factory=list)
    # Decoded bytes, which is what the cap is enforced against.
    used_bytes: int = 0
    # Tracked only so a sealed bin can report both numbers; never compared to the
    # cap.
    uncompressed_bytes: int = 0

    def add(self, item: BinItem) -> None:
        self.items.append(item)
        self.used_bytes += _decoded_bytes(item)
        self.uncompressed_bytes += item.uncompressed_size

    def seal(self) -> Bin:
        return Bin(
            items=tuple(self.items),
            total_uncompressed_size=self.uncompressed_bytes,
            total_decoded_size=self.used_bytes,
        )


def _prefix_sums(unit_sizes: List[int]) -> List[int]:
    # prefix[i] == sum of the first i unit sizes (prefix[0] == 0).
    prefix = [0]
    for size in unit_sizes:
        prefix.append(prefix[-1] + size)
    return prefix


def _largest_prefix_fit(prefix_sums: List[int], start: int, capacity_left: int) -> int:
    # Largest end (exclusive), end >= start, such that the row groups [start, end)
    # sum to <= capacity_left. prefix_sums[i] is the cumulative size of the first i row
    # groups, so sum(sizes[start:end]) == prefix_sums[end] - prefix_sums[start]. Binary
    # search for the largest end with prefix_sums[end] <= capacity_left + prefix_sums[start].
    # Returns ``start`` when not even one row group fits (caller treats that as
    # "nothing fits here").
    end = bisect.bisect_right(prefix_sums, capacity_left + prefix_sums[start]) - 1
    return max(end, start)


def _fit_at_least_one(prefix_sums: List[int], start: int, capacity_left: int) -> int:
    # Like ``_largest_prefix_fit``, but never returns ``start``: a bin that is
    # known to be empty must swallow at least one unit, even an over-sized one
    # (the relaxation for a lone unit larger than a whole bin).
    return max(_largest_prefix_fit(prefix_sums, start, capacity_left), start + 1)


def _slice_bin_item(item: BinItem, a: int, b: int) -> BinItem:
    # Row groups [a, b) of a coalesced item as a new contiguous BinItem. Uses the
    # exact per-RG sizes/rows so num_rows stays an exact survivor count for the
    # limit push-down, and rg_idx shifts by ``a`` because the run is contiguous.
    sizes = item.rg_sizes[a:b]
    rows = item.rg_rows[a:b]
    # Empty when the run had no exact decoded sizing, in which case the slice
    # inherits that and keeps falling back.
    decoded_sizes = item.rg_decoded_sizes[a:b]
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
        decoded_size=sum(decoded_sizes) if decoded_sizes else None,
        rg_decoded_sizes=decoded_sizes if count > 1 else (),
    )


def _subitem(item: BinItem, num_units: int, start: int, end: int) -> BinItem:
    # The item covering units [start, end). When that is the whole item, return it
    # unchanged (so a non-split item keeps its original rg_count/rg_sizes);
    # otherwise carve out the row-group range -- only reached for splittable runs.
    if start == 0 and end == num_units:
        return item
    return _slice_bin_item(item, start, end)


def _best_open_bin(
    bins: List[_OpenBin], prefix_sums: List[int], start: int, cap: int
) -> Tuple[Optional[_OpenBin], int]:
    # Among open bins, the one that swallows the largest prefix of units[start:]
    # with the least leftover space (best fit). Returns (bin, end); (None, start)
    # if no open bin can take even one unit.
    best: Optional[_OpenBin] = None
    best_end, best_gap = start, 0
    for bin in bins:
        room = cap - bin.used_bytes
        end = _largest_prefix_fit(prefix_sums, start, room)
        if end > start:
            gap = room - (prefix_sums[end] - prefix_sums[start])
            if best is None or end > best_end or (end == best_end and gap < best_gap):
                best, best_end, best_gap = bin, end, gap
    return best, best_end


class _BinPool:
    """Where a placer puts an item's units, and when it seals a bin.

    Placement is the same walk for every item -- acquire a bin plus the run of
    units it can take, add that run, let the pool decide whether the bin is done
    -- so the pool is the only thing that differs between light and heavy files.
    Sealed bins go straight into the packer's output deque.
    """

    def __init__(self, cap: int, output: Deque[Bin]):
        self._cap = cap
        self._output = output

    def acquire(self, prefix_sums: List[int], start: int) -> Tuple[_OpenBin, int]:
        # The bin to place into, and the exclusive end of the unit run it takes.
        # Always makes progress: the returned end is > start.
        raise NotImplementedError

    def after_add(self, bin_: _OpenBin) -> None:
        # Seal ``bin_`` if it can never take another unit.
        raise NotImplementedError

    def flush(self) -> None:
        # Seal everything still open.
        raise NotImplementedError

    def _seal(self, bin_: _OpenBin) -> None:
        self._output.append(bin_.seal())


class _SharedBinPool(_BinPool):
    """LIGHT files -> a pool of open bins holding chunks from mixed files."""

    def __init__(self, cap: int, output: Deque[Bin], max_open_bins: int):
        super().__init__(cap, output)
        self._max_open_bins = max_open_bins
        self._bins: List[_OpenBin] = []

    def try_whole(self, item: BinItem) -> bool:
        # First Fit on the WHOLE item: place it unsplit in the first bin it fits.
        # Returns False when it fits nowhere, leaving the caller to fall back to
        # the best-fit-per-unit walk.
        total = _decoded_bytes(item)
        target = next(
            (b for b in self._bins if b.used_bytes + total <= self._cap), None
        )
        if target is None:
            return False
        target.add(item)
        self.after_add(target)
        return True

    def acquire(self, prefix_sums: List[int], start: int) -> Tuple[_OpenBin, int]:
        # Best fit: the open bin that swallows the largest prefix of the remaining
        # units with the least leftover space. Open a fresh bin only when no open
        # bin can take even one unit.
        target, end = _best_open_bin(self._bins, prefix_sums, start, self._cap)
        if target is None:
            target = self._open_bin()
            end = _fit_at_least_one(prefix_sums, start, self._cap)
        return target, end

    def after_add(self, bin_: _OpenBin) -> None:
        # A shared bin at (or over) cap can never take another positive-size item:
        # ``_best_open_bin`` gives it end == start and ``try_whole`` fails its
        # ``used_bytes + total <= cap`` test. Leaving it in the pool just burns one
        # of the ``_max_open_bins`` slots, so seal and evict it.
        if bin_.used_bytes >= self._cap:
            self._bins.remove(bin_)
            self._seal(bin_)

    def flush(self) -> None:
        for bin_ in self._bins:
            if bin_.items:
                self._seal(bin_)
        self._bins = []

    def _open_bin(self) -> _OpenBin:
        if len(self._bins) >= self._max_open_bins:
            fullest = max(self._bins, key=lambda b: b.used_bytes)
            self._bins.remove(fullest)
            self._seal(fullest)
        bin_ = _OpenBin()
        self._bins.append(bin_)
        return bin_


class _SingleFileBinPool(_BinPool):
    """HEAVY files -> dedicated bins holding chunks of one file at a time.

    Next Fit over a single open bin: fill it at a unit boundary, seal it once
    full, and carry any remnant into the next bin. The open bin is allocated
    lazily, so "sealed a full bin" and "no heavy file yet" are the same state.
    """

    def __init__(self, cap: int, output: Deque[Bin]):
        super().__init__(cap, output)
        self._path: Optional[str] = None
        self._bin: Optional[_OpenBin] = None

    def switch_to(self, path: str) -> None:
        # Bins never mix heavy files, so moving to a new one seals the old bin.
        if self._path != path:
            self.flush()
            self._path = path

    def acquire(self, prefix_sums: List[int], start: int) -> Tuple[_OpenBin, int]:
        if self._bin is None:
            self._bin = _OpenBin()
        end = _largest_prefix_fit(prefix_sums, start, self._cap - self._bin.used_bytes)
        if end == start:  # nothing fits the open bin
            if self._bin.items:  # seal it and retry on a fresh bin
                self._seal(self._bin)
                self._bin = _OpenBin()
            end = _fit_at_least_one(prefix_sums, start, self._cap)
        return self._bin, end

    def after_add(self, bin_: _OpenBin) -> None:
        # Seal a full bin right away so it can be scheduled early; ``acquire``
        # opens the next one only if more units follow.
        if bin_.used_bytes >= self._cap:
            self._seal(bin_)
            self._bin = None

    def flush(self) -> None:
        if self._bin is not None and self._bin.items:
            self._seal(self._bin)
        self._bin = None
        self._path = None


def _bin_items(manifest: FileManifest) -> List[BinItem]:
    """Read a listing manifest as bin items, one per row.

    A row carrying :class:`ParquetRowGroupChunkMetadata` becomes a row-group run
    with exact stats. A row with no chunk metadata -- the plain whole-file
    listing path -- becomes a single indivisible unit sized by the file itself,
    which is what lets this partitioner sit behind any indexer rather than only
    a footer-reading one.
    """
    items: List[BinItem] = []
    for path, file_size, md in zip(
        manifest.paths, manifest.file_sizes, manifest.file_chunk_metadatas
    ):
        if md is None or "row_group_ids" not in md:
            # No footer stats, so the only size available is the file's on-disk
            # (compressed) length. Leaving ``decoded_size`` unset routes it
            # through the same fallback as a chunk whose footer had no size
            # statistics, which is the coherent reading now that the bin budget
            # counts decoded bytes.
            items.append(
                BinItem(
                    path=str(path),
                    rg_idx=0,
                    uncompressed_size=int(file_size),
                    num_rows=0,
                )
            )
            continue
        ids = md["row_group_ids"]
        # ``.get`` rather than ``[]``: a manifest produced by another partitioner,
        # or by a chunker predating decoded sizing, need not carry these keys.
        # Tested against None explicitly because manifest columns arrive as numpy
        # arrays, whose truthiness is ambiguous.
        rg_decoded_sizes = md.get("rg_decoded_sizes")
        items.append(
            BinItem(
                path=str(path),
                rg_idx=ids[0],
                uncompressed_size=md["uncompressed_size"],
                num_rows=md["num_rows"],
                fully_matched=md["fully_matched"],
                rg_count=len(ids),
                rg_sizes=tuple(md["rg_sizes"]),
                rg_rows=tuple(md["rg_rows"]),
                decoded_size=md.get("decoded_size"),
                rg_decoded_sizes=(
                    () if rg_decoded_sizes is None else tuple(rg_decoded_sizes)
                ),
            )
        )
    return items


class OnlineBinPacker(FilePartitioner):
    """Streaming coloured bin packer over listing rows.

    Feed manifests via :meth:`add_input`; drain sealed bins via
    :meth:`has_partition` / :meth:`next_partition` as they become available;
    call :meth:`finalize` once all input is added to flush the still-open bins.

    Packs globally -- one pool of open bins keyed by file -- so it needs every
    listing row, hence ``requires_global_input``.
    """

    def __init__(
        self,
        max_bin_bytes: int,
        *,
        max_shared_open_bins: int = 16,
        split_coalesced: bool = False,
    ):
        # ``max_bin_bytes`` doubles as the "file turns heavy" isolate threshold.
        self._cap = max_bin_bytes
        # When True, a coalesced item (rg_count > 1) that does not fit whole is
        # split at physical-row-group boundaries to fill residual bin space
        # instead of opening a fresh bin. Single row groups stay atomic, so with
        # coalescing off (every rg_count == 1) this is a no-op and the packer
        # behaves exactly as when the flag is False.
        self._split_coalesced = split_coalesced

        self._seen_bytes_by_path: dict = {}  # running w(f) per file
        self._output: Deque[Bin] = deque()  # sealed bins awaiting drain
        self._shared = _SharedBinPool(
            self._cap, self._output, max_shared_open_bins
        )  # non-isolated bins (mixed files)
        self._heavy = _SingleFileBinPool(self._cap, self._output)

    @property
    def requires_global_input(self) -> bool:
        return True

    @property
    def max_partition_decoded_bytes(self) -> Optional[int]:
        # The cap is enforced in decoded bytes (see ``_decoded_bytes``), so it is
        # a real bound rather than an estimate -- except for the relaxation that
        # lets a lone row group larger than a whole bin through, which a caller
        # sizing memory off this should treat as the known outlier it is.
        return self._cap

    # === Feeding ===

    def add_input(self, input_manifest: FileManifest) -> None:
        for item in _bin_items(input_manifest):
            self._place(item)

    def _units(self, item: BinItem) -> List[int]:
        # The row-group boundaries an item may be cut between, as unit sizes in
        # decoded bytes (the units the cap is in). A splittable coalesced run
        # (split_coalesced and rg_count > 1) yields one unit per physical row
        # group; anything else yields a single indivisible unit (the whole item).
        # Placement only ever cuts at unit boundaries.
        if self._split_coalesced and item.rg_count > 1:
            if item.rg_decoded_sizes:
                return list(item.rg_decoded_sizes)
            # Apply the fallback per row group so the units still sum to the
            # item's own decoded size.
            return [decoded_size_or_fallback(None, size) for size in item.rg_sizes]
        return [_decoded_bytes(item)]

    def _place(self, item: BinItem) -> None:
        item_bytes = _decoded_bytes(item)
        seen_bytes = self._seen_bytes_by_path.get(item.path, 0)
        self._seen_bytes_by_path[item.path] = seen_bytes + item_bytes

        if item_bytes > self._cap and len(self._units(item)) == 1:
            # Relaxation: an indivisible chunk bigger than a whole bin gets its own
            # bin. A splittable oversized run instead falls through and is cut into
            # bin-sized pieces by the placers.
            self._output.append(
                Bin(
                    items=(item,),
                    total_uncompressed_size=item.uncompressed_size,
                    total_decoded_size=item_bytes,
                )
            )
        elif seen_bytes < self._cap:
            # Prefer keeping a light item whole; fall back to splitting it across
            # the shared bins. This also lets a first, splittable oversized
            # coalesced run fill residual shared-bin space. (With splitting off
            # the item is a single unit, so this reduces to the original First
            # Fit.)
            if not self._shared.try_whole(item):
                self._pack(item, self._shared)
        else:
            # Once earlier chunks from a file already fill a bin, route its
            # subsequent chunks to the heavy pool. This preserves per-file
            # isolation for large files while still allowing their first
            # splittable coalesced run to fill residual shared-bin capacity.
            self._heavy.switch_to(item.path)
            self._pack(item, self._heavy)

    def _pack(self, item: BinItem, pool: _BinPool) -> None:
        # Walk the item's units, handing each run the pool accepts to the bin it
        # picked. Cutting only at unit boundaries keeps every piece a contiguous
        # row-group run with exact sizes and row counts.
        units = self._units(item)
        prefix_sums = _prefix_sums(units)
        start = 0
        while start < len(units):
            bin_, end = pool.acquire(prefix_sums, start)
            bin_.add(_subitem(item, len(units), start, end))
            pool.after_add(bin_)
            start = end

    # === Draining ===

    def has_partition(self) -> bool:
        return len(self._output) > 0

    def next_partition(self) -> FileManifest:
        bin_ = self._output.popleft()
        # Both totals: the decoded one is what the cap bounded, and the pair makes
        # the effective inflation ratio visible when a bin looks mis-sized.
        logger.debug(
            "Emitting bin with %d decoded bytes (%d uncompressed): %s",
            bin_.total_decoded_size,
            bin_.total_uncompressed_size,
            [
                (item.path, item.rg_idx, item.rg_idx + item.rg_count - 1)
                for item in bin_.items
            ],
        )
        return self._bin_to_manifest(bin_)

    def finalize(self) -> None:
        # Flush everything still open, heavy bins first so they drain ahead of the
        # shared ones.
        self._heavy.flush()
        self._shared.flush()

    @staticmethod
    def _bin_to_manifest(bin_: Bin) -> FileManifest:
        # One manifest row per distinct file in the bin. A file's (possibly-split)
        # items cover disjoint contiguous runs, so union their physical row-group
        # ids into the read unit for that file.
        items_by_path: defaultdict = defaultdict(list)
        for item in bin_.items:
            items_by_path[item.path].append(item)

        paths: List[str] = []
        sizes: List[int] = []
        chunk_metadatas: List[ParquetRowGroupChunkMetadata] = []
        for path, items in items_by_path.items():
            ids = [
                rg_id
                for item in items
                for rg_id in range(item.rg_idx, item.rg_idx + item.rg_count)
            ]
            uncompressed_size = sum(item.uncompressed_size for item in items)
            paths.append(path)
            sizes.append(uncompressed_size)
            chunk_metadatas.append(
                create_chunk_metadata(
                    ParquetRowGroupChunkMetadata,
                    row_group_ids=tuple(sorted(ids)),
                    num_rows=sum(item.num_rows for item in items),
                    uncompressed_size=uncompressed_size,
                    fully_matched=all(item.fully_matched for item in items),
                    rg_sizes=(),
                    rg_rows=(),
                    # A file's decoded size is exact only if every one of its
                    # items was (sum_exact); one fallback item makes the whole
                    # row fall back rather than reporting a mixed total.
                    decoded_size=sum_exact(item.decoded_size for item in items),
                    rg_decoded_sizes=(),
                )
            )
        # TypedDict invariance: ``ParquetRowGroupChunkMetadata`` has extra keys
        # beyond the empty ``ChunkMetadata`` base, so the concrete list is not
        # assignable to ``List[Optional[ChunkMetadata]]`` without a cast.
        return FileManifest.construct_manifest(
            paths=paths,
            sizes=sizes,
            chunk_metadatas=cast(List[Optional[ChunkMetadata]], chunk_metadatas),
        )
