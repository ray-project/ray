from typing import TypeVar, Iterable, Any

import ray
from ray.experimental.data.impl.block import Block, BlockMetadata, ObjectRef
from ray.experimental.data.impl.block_list import BlockList
from ray.experimental.data.impl.progress_bar import ProgressBar

T = TypeVar("T")
U = TypeVar("U")


class ComputePool:
    def apply(self, fn: Any,
              blocks: Iterable[Block[T]]) -> Iterable[ObjectRef[Block]]:
        raise NotImplementedError


class TaskPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: BlockList[Any]) -> BlockList[Any]:
        map_bar = ProgressBar("Map Progress", total=len(blocks))

        kwargs = remote_args.copy()
        kwargs["num_returns"] = 2

        @ray.remote(**kwargs)
        def wrapped_fn(block: Block, meta: BlockMetadata):
            new_block = fn(block)
            new_meta = BlockMetadata(
                num_rows=new_block.num_rows(),
                size_bytes=new_block.size_bytes(),
                schema=new_block.schema(),
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


class ActorPool(ComputePool):
    def apply(self, fn: Any, remote_args: dict,
              blocks: Iterable[Block[T]]) -> Iterable[ObjectRef[Block]]:

        map_bar = ProgressBar("Map Progress", total=len(blocks))

        class Worker:
            def ready(self):
                return "ok"

            @ray.method(num_returns=2)
            def process_block(self, block: Block[T], meta: BlockMetadata
                              ) -> (Block[U], BlockMetadata):
                new_block = fn(block)
                new_metadata = BlockMetadata(
                    num_rows=new_block.num_rows(),
                    size_bytes=new_block.size_bytes(),
                    schema=new_block.schema(),
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


def get_compute(compute_spec: str) -> ComputePool:
    if compute_spec == "tasks":
        return TaskPool()
    elif compute_spec == "actors":
        return ActorPool()
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`]")
