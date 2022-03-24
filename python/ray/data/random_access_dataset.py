import bisect
import logging
import random
import time
from collections import defaultdict
from typing import List, Any, Generic, Optional, TYPE_CHECKING

import ray
from ray.types import ObjectRef
from ray.data.block import T, BlockAccessor
from ray.data.impl.remote_fn import cached_remote_fn

if TYPE_CHECKING:
    from ray.data import Dataset

logger = logging.getLogger(__name__)


class RandomAccessDataset(Generic[T]):
    """A class that provides distributed, random access to a Dataset.

    See: ``Dataset.to_random_access_dataset()``.
    """

    def __init__(
        self,
        dataset: "Dataset[T]",
        key: str,
        num_workers: int,
    ):
        """Construct a RandomAccessDataset (internal API).

        The constructor is a private API. Use ``dataset.to_random_access_dataset()``
        to construct a RandomAccessDataset.
        """
        self._format = dataset._dataset_format()
        if self._format not in ["arrow", "pandas"]:
            raise ValueError("RandomAccessDataset only supports Arrow-format datasets.")

        start = time.perf_counter()
        logger.info("[setup] Indexing dataset by sort key.")
        sorted_ds = dataset.sort(key)
        get_bounds = cached_remote_fn(_get_bounds)
        blocks = sorted_ds.get_internal_block_refs()

        logger.info("[setup] Computing block range bounds.")
        bounds = ray.get([get_bounds.remote(b, key, self._format) for b in blocks])
        self._non_empty_blocks = []
        self._lower_bound = None
        self._upper_bounds = []
        for i, b in enumerate(bounds):
            if b:
                self._non_empty_blocks.append(blocks[i])
                if self._lower_bound is None:
                    self._lower_bound = b[0]
                self._upper_bounds.append(b[1])

        logger.info("[setup] Creating {} random access workers.".format(num_workers))
        self._workers = [
            _RandomAccessWorker.options(scheduling_strategy="SPREAD").remote(
                key, self._format
            )
            for _ in range(num_workers)
        ]
        (
            self._block_to_workers_map,
            self._worker_to_blocks_map,
        ) = self._compute_block_to_worker_assignments()

        logger.info(
            "[setup] Worker to blocks assignment: {}".format(self._worker_to_blocks_map)
        )
        ray.get(
            [
                w.assign_blocks.remote(
                    {
                        i: self._non_empty_blocks[i]
                        for i in self._worker_to_blocks_map[w]
                    }
                )
                for w in self._workers
            ]
        )

        logger.info("[setup] Finished assigning blocks to workers.")
        self._build_time = time.perf_counter() - start

    def _compute_block_to_worker_assignments(self):
        # Return values.
        block_to_workers: dict[int, List["ray.ActorHandle"]] = defaultdict(list)
        worker_to_blocks: dict["ray.ActorHandle", List[int]] = defaultdict(list)

        # Aux data structures.
        loc_to_workers: dict[str, List["ray.ActorHandle"]] = defaultdict(list)
        locs = ray.get([w.ping.remote() for w in self._workers])
        for i, loc in enumerate(locs):
            loc_to_workers[loc].append(self._workers[i])
        block_locs = ray.experimental.get_object_locations(self._non_empty_blocks)

        # First, try to assign all blocks to all workers at its location.
        for block_idx, block in enumerate(self._non_empty_blocks):
            block_info = block_locs[block]
            locs = block_info.get("node_ids", [])
            for loc in locs:
                for worker in loc_to_workers[loc]:
                    block_to_workers[block_idx].append(worker)
                    worker_to_blocks[worker].append(block_idx)

        # Randomly assign any leftover blocks to at least one worker.
        # TODO: the load balancing here could be improved.
        for block_idx, block in enumerate(self._non_empty_blocks):
            if len(block_to_workers[block_idx]) == 0:
                worker = random.choice(self._workers)
                block_to_workers[block_idx].append(worker)
                worker_to_blocks[worker].append(block_idx)

        return block_to_workers, worker_to_blocks

    def get_async(self, key: Any) -> ObjectRef[Optional[T]]:
        """Asynchronously finds the record for a single key.

        Args:
            key: The key of the record to find.

        Returns:
            ObjectRef containing the record (in pydict form), or None if not found.
        """
        block_index = self._find_le(key)
        if block_index is None:
            return ray.put(None)
        return self._worker_for(block_index).get.remote(block_index, key)

    def multiget(self, keys: List[Any]) -> List[Optional[T]]:
        """Synchronously find the records for a list of keys.

        Args:
            keys: List of keys to find the records for.

        Returns:
            List of found records (in pydict form), or None for missing records.
        """
        batches = defaultdict(list)
        for k in keys:
            batches[self._find_le(k)].append(k)
        futures = {}
        for index, keybatch in batches.items():
            if index is None:
                continue
            fut = self._worker_for(index).multiget.remote(
                [index] * len(keybatch), keybatch
            )
            futures[index] = fut
        results = {}
        for i, fut in futures.items():
            keybatch = batches[i]
            values = ray.get(fut)
            for k, v in zip(keybatch, values):
                results[k] = v
        return [results.get(k) for k in keys]

    def stats(self) -> str:
        """Returns a string containing access timing information."""
        stats = ray.get([w.stats.remote() for w in self._workers])
        total_time = sum(s["total_time"] for s in stats)
        accesses = [s["num_accesses"] for s in stats]
        blocks = [s["num_blocks"] for s in stats]
        msg = "RandomAccessDataset:\n"
        msg += "- Build time: {}s\n".format(round(self._build_time, 2))
        msg += "- Num workers: {}\n".format(len(stats))
        msg += "- Blocks per worker: {} min, {} max, {} mean\n".format(
            min(blocks), max(blocks), int(sum(blocks) / len(blocks))
        )
        msg += "- Accesses per worker: {} min, {} max, {} mean\n".format(
            min(accesses), max(accesses), int(sum(accesses) / len(accesses))
        )
        msg += "- Mean access time: {}us\n".format(
            int(total_time / (1 + sum(accesses)) * 1e6)
        )
        return msg

    def _worker_for(self, block_index: int):
        return random.choice(self._block_to_workers_map[block_index])

    def _find_le(self, x: Any) -> int:
        i = bisect.bisect_left(self._upper_bounds, x)
        if i >= len(self._upper_bounds) or x < self._lower_bound:
            return None
        return i


@ray.remote(num_cpus=0, placement_group=None)
class _RandomAccessWorker:
    def __init__(self, key_field, dataset_format):
        self.blocks = None
        self.key_field = key_field
        self.dataset_format = dataset_format
        self.num_accesses = 0
        self.total_time = 0

    def assign_blocks(self, block_ref_dict):
        self.blocks = {k: ray.get(ref) for k, ref in block_ref_dict.items()}

    def get(self, block_index, key):
        start = time.perf_counter()
        result = self._get(block_index, key)
        self.total_time += time.perf_counter() - start
        self.num_accesses += 1
        return result

    def multiget(self, block_indices, keys):
        start = time.perf_counter()
        result = [self._get(i, k) for i, k in zip(block_indices, keys)]
        self.total_time += time.perf_counter() - start
        self.num_accesses += 1
        return result

    def ping(self):
        return ray.get_runtime_context().node_id.hex()

    def stats(self) -> dict:
        return {
            "num_blocks": len(self.blocks),
            "num_accesses": self.num_accesses,
            "total_time": self.total_time,
        }

    def _get(self, block_index, key):
        if block_index is None:
            return None
        block = self.blocks[block_index]
        column = block[self.key_field]
        if self.dataset_format == "arrow":
            column = _ArrowListWrapper(column)
        i = _binary_search_find(column, key)
        if i is None:
            return None
        acc = BlockAccessor.for_block(block)
        return acc._create_table_row(acc.slice(i, i + 1, copy=True))


def _binary_search_find(column, x):
    i = bisect.bisect_left(column, x)
    if i != len(column) and column[i] == x:
        return i
    return None


class _ArrowListWrapper:
    def __init__(self, arrow_col):
        self.arrow_col = arrow_col

    def __getitem__(self, i):
        return self.arrow_col[i].as_py()

    def __len__(self):
        return len(self.arrow_col)


def _get_bounds(block, key, dataset_format):
    if len(block) == 0:
        return None
    b = (block[key][0], block[key][len(block) - 1])
    if dataset_format == "arrow":
        b = (b[0].as_py(), b[1].as_py())
    return b
