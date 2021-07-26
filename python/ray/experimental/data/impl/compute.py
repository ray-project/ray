from typing import TypeVar, Iterable, Any, Union, Callable

import ray
from ray.types import ObjectRef
from ray.experimental.data.block import Block, BlockAccessor, BlockMetadata
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.progress_bar import ProgressBar

T = TypeVar("T")
U = TypeVar("U")

# A class type that implements __call__.
CallableClass = type


class ComputeStrategy:
    def apply(self, fn: Any,
              blocks: Iterable[Block]) -> Iterable[ObjectRef[Block]]:
        raise NotImplementedError


class TaskPool(ComputeStrategy):
    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList[Any]) -> BlockList[Any]:
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        kwargs = remote_args.copy()
        kwargs["num_returns"] = 2

        @ray.remote(**kwargs)
        def wrapped_fn(block: Block, meta: BlockMetadata):
            new_block = fn(block)
            accessor = BlockAccessor.for_block(new_block)
            new_meta = BlockMetadata(
                num_rows=accessor.num_rows(),
                size_bytes=accessor.size_bytes(),
                schema=accessor.schema(),
                input_files=meta.input_files)
            return new_block, new_meta

        refs = [
            wrapped_fn.remote(b, m)
            for b, m in zip(blocks, blocks.get_metadata())
        ]
        new_blocks, new_metadata = zip(*refs)

        map_bar.block_until_complete(list(new_blocks))
        new_metadata = ray.get(list(new_metadata))
        return BlockList(list(new_blocks), list(new_metadata))


class ActorPool(ComputeStrategy):
    def apply(self, fn: Any, remote_args: dict,
              blocks: Iterable[Block]) -> Iterable[ObjectRef[Block]]:

        map_bar = ProgressBar("Map Progress", total=len(blocks))

        class Worker:
            def ready(self):
                return "ok"

            @ray.method(num_returns=2)
            def process_block(self, block: Block,
                              meta: BlockMetadata) -> (Block, BlockMetadata):
                new_block = fn(block)
                accessor = BlockAccessor.for_block(new_block)
                new_metadata = BlockMetadata(
                    num_rows=accessor.num_rows(),
                    size_bytes=accessor.size_bytes(),
                    schema=accessor.schema(),
                    input_files=meta.input_files)
                return new_block, new_metadata

        if "num_cpus" not in remote_args:
            remote_args["num_cpus"] = 1
        Worker = ray.remote(**remote_args)(Worker)

        workers = [Worker.remote()]
        metadata_mapping = {}
        tasks = {w.ready.remote(): w for w in workers}
        ready_workers = set()
        blocks_in = [(b, m) for (b, m) in zip(blocks, blocks.get_metadata())]
        blocks_out = []

        while len(blocks_out) < len(blocks):
            ready, _ = ray.wait(
                list(tasks), timeout=0.01, num_returns=1, fetch_local=False)
            if not ready:
                if len(ready_workers) / len(workers) > 0.8:
                    w = Worker.remote()
                    workers.append(w)
                    tasks[w.ready.remote()] = w
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers),
                            len(workers) - len(ready_workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                blocks_out.append(obj_id)
                map_bar.update(1)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                block_ref, meta_ref = worker.process_block.remote(
                    *blocks_in.pop())
                metadata_mapping[block_ref] = meta_ref
                tasks[block_ref] = worker

        new_metadata = ray.get([metadata_mapping[b] for b in blocks_out])
        map_bar.close()
        return BlockList(blocks_out, new_metadata)


cached_cls = None
cached_fn = None


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
            global cached_cls, cached_fn
            if cached_fn is None or cached_cls != fn:
                cached_cls = fn
                cached_fn = fn()
            return cached_fn(item)

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
