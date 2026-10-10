from __future__ import annotations

import bisect
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import FilePartitioner

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BinItem:
    """One listing row placed into a bin: a file's run of read units, or the
    whole file. ``file.path`` is the packer's "colour"."""

    file: FileInfo
    # ``None`` for a whole-file listing row: indivisible, sized by the listing.
    run: Optional[FileChunk] = None

    @property
    def size_bytes(self) -> int:
        # What the packer budgets: the run's bytes for a run, else the on-disk
        # file size.
        if self.run is not None:
            return self.run.size_bytes
        assert self.file.size is not None
        return self.file.size


@dataclass(frozen=True)
class Bin:
    """A sealed bin: a set of items (across one or more files) whose combined
    size targets one bin budget. Becomes one ``FileManifest`` block == one
    downstream read task."""

    items: Tuple[BinItem, ...]
    total_bytes: int


@dataclass
class _OpenBin:
    items: List[BinItem] = field(default_factory=list)
    used_bytes: int = 0

    def add(self, item: BinItem) -> None:
        self.items.append(item)
        self.used_bytes += item.size_bytes

    def seal(self) -> Bin:
        return Bin(tuple(self.items), self.used_bytes)


def _prefix_sums(unit_sizes: List[int]) -> List[int]:
    # prefix[i] == sum of the first i unit sizes (prefix[0] == 0).
    prefix = [0]
    for size in unit_sizes:
        prefix.append(prefix[-1] + size)
    return prefix


def _largest_prefix_fit(prefix_sums: List[int], start: int, capacity_left: int) -> int:
    # Largest end (exclusive), end >= start, such that the units [start, end)
    # sum to <= capacity_left. prefix_sums[i] is the cumulative size of the first i
    # units, so sum(sizes[start:end]) == prefix_sums[end] - prefix_sums[start]. Binary
    # search for the largest end with prefix_sums[end] <= capacity_left + prefix_sums[start].
    # Returns ``start`` when not even one unit fits (caller treats that as
    # "nothing fits here").
    end = bisect.bisect_right(prefix_sums, capacity_left + prefix_sums[start]) - 1
    return max(end, start)


def _fit_at_least_one(prefix_sums: List[int], start: int, capacity_left: int) -> int:
    # Like ``_largest_prefix_fit``, but never returns ``start``: a bin that is
    # known to be empty must swallow at least one unit, even an over-sized one
    # (the relaxation for a lone unit larger than a whole bin).
    return max(_largest_prefix_fit(prefix_sums, start, capacity_left), start + 1)


def _slice_bin_item(item: BinItem, a: int, b: int) -> BinItem:
    # Units [a, b) of a multi-unit item as a new BinItem. Uses the exact
    # per-unit sizes/rows so num_rows stays an exact survivor count for the
    # limit push-down.
    run = item.run
    assert run is not None
    sizes = run.unit_sizes[a:b]
    rows = run.unit_rows[a:b]
    piece = FileChunk(
        unit_ids=run.unit_ids[a:b],
        num_rows=sum(rows),
        size_bytes=sum(sizes),
        fully_matched=run.fully_matched,
        unit_sizes=sizes if b - a > 1 else (),
        unit_rows=rows if b - a > 1 else (),
    )
    return BinItem(file=item.file, run=piece)


def _subitem(item: BinItem, num_units: int, start: int, end: int) -> BinItem:
    # The item covering units [start, end). When that is the whole item, return it
    # unchanged (so a non-split item keeps its original run); otherwise carve
    # out the unit range -- only reached for splittable runs.
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
        total = item.size_bytes
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

    A row carrying a :class:`FileChunk` becomes a run with exact stats. A row
    with no chunk metadata -- the plain whole-file listing path -- becomes a
    single indivisible item sized by the file itself, which is what lets this
    partitioner sit behind any indexer rather than only one that knows the
    file's units.
    """
    items: List[BinItem] = []
    for path, file_size, md in zip(
        manifest.paths, manifest.file_sizes, manifest.file_chunk_metadatas
    ):
        file = FileInfo(path=str(path), size=int(file_size))
        if md is None or "unit_ids" not in md:
            items.append(BinItem(file=file))
        else:
            items.append(BinItem(file=file, run=FileChunk.from_metadata(md)))
    return items


class OnlineBinPacker(FilePartitioner):
    """Streaming coloured bin packer over listing rows.

    Works for any format: a row is either a whole file or a :class:`FileChunk`
    of the file's read units, and the packer only sums ``size_bytes`` against
    its budget. Feed manifests via :meth:`add_input`; drain sealed bins via
    :meth:`has_partition` / :meth:`next_partition` as they become available;
    call :meth:`finalize` once all input is added to flush the still-open bins.

    By default it packs globally, with one pool of open bins keyed by file, so
    it needs every listing row (``requires_global_input=True``). With
    ``requires_global_input=False`` each listing task packs only its own shard
    of files, and ``partition_files`` flushes the open bins when the task ends.
    Listing then runs as parallel tasks, at the cost of at most one under-filled
    bin per shard. A file's rows never cross shards, because shards split the
    path list.
    """

    def __init__(
        self,
        max_bin_bytes: int,
        *,
        max_shared_open_bins: int = 16,
        split_coalesced: bool = False,
        requires_global_input: bool = True,
    ):
        # ``max_bin_bytes`` doubles as the "file turns heavy" isolate threshold.
        self._cap = max_bin_bytes
        # Whether the planner must feed every listing row to one instance
        # (global packing) or may run one instance per listing shard.
        self._requires_global_input = requires_global_input
        # When True, a multi-unit run that does not fit whole is split at unit
        # boundaries to fill residual bin space instead of opening a fresh bin.
        # Single units stay atomic, so with coalescing off (every run is one
        # unit) this is a no-op and the packer behaves exactly as when the flag
        # is False.
        self._split_coalesced = split_coalesced

        self._seen_bytes_by_path: dict = {}  # running w(f) per file
        self._output: Deque[Bin] = deque()  # sealed bins awaiting drain
        self._shared = _SharedBinPool(
            self._cap, self._output, max_shared_open_bins
        )  # non-isolated bins (mixed files)
        self._heavy = _SingleFileBinPool(self._cap, self._output)

    @property
    def requires_global_input(self) -> bool:
        return self._requires_global_input

    # === Feeding ===

    def add_input(self, input_manifest: FileManifest) -> None:
        for item in _bin_items(input_manifest):
            self._place(item)

    def _units(self, item: BinItem) -> List[int]:
        # The boundaries an item may be cut between, as unit sizes. A splittable
        # run (split_coalesced and more than one unit) yields one entry per
        # unit; anything else yields a single indivisible entry (the whole
        # item). Placement only ever cuts at unit boundaries.
        run = item.run
        if self._split_coalesced and run is not None and len(run.unit_ids) > 1:
            return list(run.unit_sizes)
        return [item.size_bytes]

    def _place(self, item: BinItem) -> None:
        item_bytes = item.size_bytes
        seen_bytes = self._seen_bytes_by_path.get(item.file.path, 0)
        self._seen_bytes_by_path[item.file.path] = seen_bytes + item_bytes

        if item_bytes > self._cap and len(self._units(item)) == 1:
            # Relaxation: an indivisible chunk bigger than a whole bin gets its own
            # bin. A splittable oversized run instead falls through and is cut into
            # bin-sized pieces by the placers.
            self._output.append(Bin((item,), item_bytes))
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
            self._heavy.switch_to(item.file.path)
            self._pack(item, self._heavy)

    def _pack(self, item: BinItem, pool: _BinPool) -> None:
        # Walk the item's units, handing each run the pool accepts to the bin it
        # picked. Cutting only at unit boundaries keeps every piece a run with
        # exact sizes and row counts.
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
        logger.debug(
            "Emitting bin with %d bytes: %s",
            bin_.total_bytes,
            [
                item.file.path
                if item.run is None
                else (item.file.path, item.run.unit_ids)
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
        # A whole-file item is one manifest row with no chunk metadata, as it
        # came in. A file's runs cover disjoint unit ranges, so union them into
        # one row per distinct file. Every row keeps the on-disk file size the
        # listing gave it; a run's bytes live in its chunk metadata.
        files: List[FileInfo] = []
        chunk_metadatas: List[Optional[ChunkMetadata]] = []
        runs_by_file: Dict[FileInfo, List[FileChunk]] = defaultdict(list)
        for item in bin_.items:
            if item.run is None:
                files.append(item.file)
                chunk_metadatas.append(None)
            else:
                runs_by_file[item.file].append(item.run)

        for file, runs in runs_by_file.items():
            merged = FileChunk(
                unit_ids=tuple(sorted(i for run in runs for i in run.unit_ids)),
                num_rows=sum(run.num_rows for run in runs),
                size_bytes=sum(run.size_bytes for run in runs),
                fully_matched=all(run.fully_matched for run in runs),
            )
            files.append(file)
            chunk_metadatas.append(merged.to_metadata())
        sizes: List[int] = []
        for file in files:
            assert file.size is not None
            sizes.append(file.size)
        return FileManifest.construct_manifest(
            paths=[file.path for file in files],
            sizes=sizes,
            chunk_metadatas=chunk_metadatas,
        )
