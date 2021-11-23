from typing import TypeVar, Any, Union, Callable, List, Iterable, Tuple

import ray
from ray.types import ObjectRef
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockPartition
from ray.data.context import DatasetContext
from ray.data.impl.arrow_block import DelegatingArrowBlockBuilder
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.remote_fn import cached_remote_fn

T = TypeVar("T")
U = TypeVar("U")

# A class type that implements __call__.
CallableClass = type


class ComputeStrategy:
    def apply(self, fn: Any, blocks: BlockList) -> BlockList:
        raise NotImplementedError


def _map_block_split(block: Block, fn: Any,
                     input_files: List[str]) -> BlockPartition:
    output = []
    for new_block in fn(block):
        accessor = BlockAccessor.for_block(new_block)
        new_meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=accessor.schema(),
            input_files=input_files)
        owner = DatasetContext.get_current().block_owner
        output.append((ray.put(new_block, _owner=owner), new_meta))
    return output


def _map_block_nosplit(block: Block, fn: Any, input_files: List[str]
                       ) -> Iterable[Tuple[ObjectRef[Block], BlockMetadata]]:
    builder = DelegatingArrowBlockBuilder()
    for new_block in fn(block):
        builder.add_block(new_block)
    new_block = builder.build()
    accessor = BlockAccessor.for_block(new_block)
    return new_block, accessor.get_metadata(input_files=input_files)


class TaskPool(ComputeStrategy):
    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList) -> BlockList:
        # Handle empty datasets.
        if blocks.initial_num_blocks() == 0:
            return blocks

        blocks = list(blocks.iter_blocks_with_metadata())
        context = DatasetContext.get_current()
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        if context.block_splitting_enabled:
            map_block = cached_remote_fn(_map_block_split).options(
                **remote_args)
            refs = [map_block.remote(b, fn, m.input_files) for b, m in blocks]
        else:
            map_block = cached_remote_fn(_map_block_nosplit).options(
                **dict(remote_args, num_returns=2))
            all_refs = [
                map_block.remote(b, fn, m.input_files) for b, m in blocks
            ]
            data_refs = [r[0] for r in all_refs]
            refs = [r[1] for r in all_refs]

        # Common wait for non-data refs.
        try:
            results = map_bar.fetch_until_complete(refs)
        except (ray.exceptions.RayTaskError, KeyboardInterrupt) as e:
            # One or more mapper tasks failed, or we received a SIGINT signal
            # while waiting; either way, we cancel all map tasks.
            for ref in refs:
                ray.cancel(ref)
            # Wait until all tasks have failed or been cancelled.
            for ref in refs:
                try:
                    ray.get(ref)
                except (ray.exceptions.RayTaskError,
                        ray.exceptions.TaskCancelledError):
                    pass
            # Reraise the original task failure exception.
            raise e from None

        new_blocks, new_metadata = [], []
        if context.block_splitting_enabled:
            for result in results:
                for block, metadata in result:
                    new_blocks.append(block)
                    new_metadata.append(metadata)
        else:
            for block, metadata in zip(data_refs, results):
                new_blocks.append(block)
                new_metadata.append(metadata)
        return BlockList(list(new_blocks), list(new_metadata))


class ActorPool(ComputeStrategy):
    def __init__(self):
        self.workers = []

    def __del__(self):
        for w in self.workers:
            w.__ray_terminate__.remote()

    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList) -> BlockList:

        blocks_in = list(blocks.iter_blocks_with_metadata())
        orig_num_blocks = len(blocks_in)
        results = []
        map_bar = ProgressBar("Map Progress", total=orig_num_blocks)

        class BlockWorker:
            def ready(self):
                return "ok"

            def process_block(self, block: Block, input_files: List[str]
                              ) -> Iterable[Tuple[Block, BlockMetadata]]:
                output = []
                for new_block in fn(block):
                    accessor = BlockAccessor.for_block(new_block)
                    new_metadata = BlockMetadata(
                        num_rows=accessor.num_rows(),
                        size_bytes=accessor.size_bytes(),
                        schema=accessor.schema(),
                        input_files=input_files)
                    owner = DatasetContext.get_current().block_owner
                    output.append((ray.put(new_block, _owner=owner),
                                   new_metadata))
                return output

        if not remote_args:
            remote_args["num_cpus"] = 1

        BlockWorker = ray.remote(**remote_args)(BlockWorker)

        self.workers = [BlockWorker.remote()]
        tasks = {w.ready.remote(): w for w in self.workers}
        ready_workers = set()

        while len(results) < orig_num_blocks:
            ready, _ = ray.wait(
                list(tasks), timeout=0.01, num_returns=1, fetch_local=False)
            if not ready:
                if len(ready_workers) / len(self.workers) > 0.8:
                    w = BlockWorker.remote()
                    self.workers.append(w)
                    tasks[w.ready.remote()] = w
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers),
                            len(self.workers) - len(ready_workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                results.append(obj_id)
                map_bar.update(1)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                block, meta = blocks_in.pop()
                ref = worker.process_block.remote(block, meta.input_files)
                tasks[ref] = worker

        map_bar.close()
        new_blocks, new_metadata = [], []
        for result in ray.get(results):
            for block, metadata in result:
                new_blocks.append(block)
                new_metadata.append(metadata)
        return BlockList(new_blocks, new_metadata)


def cache_wrapper(fn: Union[CallableClass, Callable[[Any], Any]]
                  ) -> Callable[[Any], Any]:
    """Implements caching of stateful callables.

    Args:
        fn: Either a plain function or class of a stateful callable.

    Returns:
        A plain function with per-process initialization cached as needed.
    """
    if isinstance(fn, CallableClass):

        def _fn(item: Any) -> Any:
            if ray.data._cached_fn is None or ray.data._cached_cls != fn:
                ray.data._cached_cls = fn
                ray.data._cached_fn = fn()
            return ray.data._cached_fn(item)

        return _fn
    else:
        return fn


def get_compute(compute_spec: Union[str, ComputeStrategy]) -> ComputeStrategy:
    if not compute_spec or compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    elif isinstance(compute_spec, ComputeStrategy):
        return compute_spec
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")
