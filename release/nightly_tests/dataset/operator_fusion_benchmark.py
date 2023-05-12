import argparse
import json
import time
from typing import Dict, List, Any

import pandas as pd
import pyarrow as pa
import numpy as np

import ray
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, DEFAULT_TARGET_MAX_BLOCK_SIZE
from ray.data.datasource import Datasource, ReadTask, Reader


class BlockDatasource(Datasource):
    def create_reader(
        self,
        num_blocks_per_task: int,
        block_size: int,
        data_format: str,
        num_columns: int,
    ):
        return BlockReader(num_blocks_per_task, block_size, data_format, num_columns)


class BlockReader(Reader):
    def __init__(
        self,
        num_blocks_per_task: int,
        block_size: int,
        data_format: str,
        num_columns: int,
    ):
        self.num_blocks_per_task = num_blocks_per_task
        self.block_size = block_size
        self.data_format = data_format
        self.num_columns = num_columns

    def estimate_inmemory_data_size(self):
        return None

    def get_read_tasks(self, parallelism: int):
        def _blocks_generator():
            values = [1] * self.block_size
            columns = {str(i): values for i in range(self.num_columns)}
            for _ in range(self.num_blocks_per_task):
                if self.data_format == "pandas":
                    yield pd.DataFrame(columns)
                elif self.data_format == "simple":
                    assert len(columns) == 1
                    yield columns["0"]
                elif self.data_format == "pyarrow":
                    yield pa.table(columns)

        size_bytes = self.num_blocks_per_task * self.num_columns * self.block_size * 8

        return parallelism * [
            ReadTask(
                lambda: _blocks_generator(),
                BlockMetadata(
                    num_rows=self.num_blocks_per_task * self.block_size,
                    size_bytes=size_bytes,
                    schema=None,
                    input_files=None,
                    exec_stats=None,
                ),
            )
        ]


def make_ds(
    num_tasks: int,
    num_blocks_per_task: int,
    block_size: int,
    data_format: str,
    num_columns: int,
    ops_spec: List[Dict[str, Any]],
    target_max_block_size: int,
) -> ray.data.Datastream:
    ds = ray.data.read_datasource(
        BlockDatasource(),
        num_blocks_per_task=num_blocks_per_task,
        block_size=block_size,
        data_format=data_format,
        num_columns=num_columns,
        parallelism=num_tasks,
    )
    for op_spec in ops_spec:
        op = op_spec.pop("op")
        if op == "flat_map":
            fn = lambda x: [x, x]  # noqa: E731
        else:
            fn = lambda x: x  # noqa: E731
        ds = getattr(ds, op)(fn, **op_spec)
    return ds


def execute_ds(ds: ray.data.Datastream):
    ds = ds.fully_executed()


def _summarize_results(results: List[Dict[str, float]]) -> Dict[str, float]:
    if len(results) == 1:
        return results[0]
    execution_times = [trial_results["execution_time"] for trial_results in results]
    return {
        "mean_execution_time": np.mean(execution_times),
        "max_execution_time": np.max(execution_times),
        "min_execution_time": np.min(execution_times),
        "std_execution_time": np.std(execution_times),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-tasks", type=int, default=1)
    parser.add_argument("--num-blocks-per-task", type=int, default=1024)
    parser.add_argument("--block-size", type=int, default=8**1024)
    parser.add_argument("--data-format", type=str, default="simple")
    parser.add_argument("--num-columns", type=int, default=1)
    parser.add_argument(
        "--ops-spec",
        type=str,
        default=(
            '[{"op": "map_batches", "batch_size": 1024, "batch_format": "pandas"}]'
        ),
    )
    parser.add_argument("--target-max-block-size", type=int, default=None)
    parser.add_argument("--disable-optimizer", action="store_true", default=False)
    parser.add_argument("--num-trials", type=int, default=1)
    args = parser.parse_args()

    # Only allow num_columns > 0 when not using the simple data format.
    assert args.num_columns == 1 or args.data_format != "simple"

    # Load the ops spec JSON.
    ops_spec = json.loads(args.ops_spec)

    target_max_block_size = args.target_max_block_size
    if target_max_block_size is None:
        target_max_block_size = DEFAULT_TARGET_MAX_BLOCK_SIZE

    print(
        f"\nRunning zero-copy batching benchmark for {args.num_trials} trials:\n"
        f"num_tasks={args.num_tasks}\nnum_blocks_per_task={args.num_blocks_per_task}\n"
        f"block_size={args.block_size}\ndata_format={args.data_format}\n"
        f"num_columns={args.num_columns}\n"
        f"target_max_block_size={target_max_block_size}\nray_commit={ray.__commit__}\n"
        f"ops_spec:\n{json.dumps(ops_spec, indent=4)}"
    )

    ray.init()

    ctx = DataContext.get_current()
    ctx.target_max_block_size = target_max_block_size
    if args.disable_optimizer:
        ctx.optimizer_enabled = False
    else:
        ctx.optimizer_enabled = True
    results = []
    for trial in range(args.num_trials):
        print(f"\n\nRunning trial {trial}\n")
        print("\tCreating dataset.\n")
        start = time.perf_counter()
        ds = make_ds(
            args.num_tasks,
            args.num_blocks_per_task,
            args.block_size,
            args.data_format,
            args.num_columns,
            ops_spec,
            target_max_block_size,
        )
        print("\tExecuting dataset.\n")
        execute_ds(ds)
        execution_time = time.perf_counter() - start
        trial_results = {"execution_time": execution_time}
        print(f"\tTrial {trial} done: ", trial_results)
        results.append(trial_results)
    result_summary = _summarize_results(results)
    print("\n\nResults: ", result_summary)
