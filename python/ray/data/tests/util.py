import functools
import os
import tempfile
from contextlib import contextmanager
from typing import Any, Callable, Iterable, List, Optional

import pandas as pd

import ray
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    DeferredEmit,
    MetadataOpTask,
    PhysicalOperator,
    RefBundle,
    _emit_deferred_entry,
    _fire_task_done,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformCallable,
    MapTransformer,
)
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data.block import Block
from ray.data.expressions import Expr


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

    def get(self):
        return self.count

    def reset(self):
        self.count = 0


@contextmanager
def gen_bin_files(n):
    with tempfile.TemporaryDirectory() as temp_dir:
        paths = []
        for i in range(n):
            path = os.path.join(temp_dir, f"{i}.bin")
            paths.append(path)
            with open(path, "wb") as fp:
                to_write = str(i) * 500
                fp.write(to_write.encode())
        yield (temp_dir, paths)


def column_udf(col, udf):
    @functools.wraps(udf)
    def wraps(row):
        return {col: udf(row[col])}

    return wraps


def column_udf_class(col, udf):
    class UDFClass:
        def __call__(self, row):
            return {col: udf(row[col])}

    return UDFClass


# Ex: named_values("id", [1, 2, 3])
# Ex: named_values(["id", "id2"], [(1, 1), (2, 2), (3, 3)])
def named_values(col_names, tuples):
    output = []
    if isinstance(col_names, list):
        for t in tuples:
            output.append(dict(zip(col_names, t)))
    else:
        for t in tuples:
            output.append({col_names: t})
    return output


def extract_values(col_name, tuples):
    return [t[col_name] for t in tuples]


def assert_exprs_equal(actual: List[Expr], expected: List[Expr]):
    """Assert two expression lists match element-wise.

    ``Expr`` overloads ``==`` to build a comparison expression (e.g.
    ``col("a") == 5``), so it can't be used to compare exprs for equality;
    use ``structurally_equals`` instead.
    """
    actual_names = [e.name for e in actual]
    expected_names = [e.name for e in expected]
    assert len(actual) == len(expected), (actual_names, expected_names)
    assert all(a.structurally_equals(b) for a, b in zip(actual, expected)), (
        actual_names,
        expected_names,
    )


def drain_and_emit(task: DataOpTask, max_bytes_to_read: Optional[int]) -> int:
    """Synchronously drive one ``DataOpTask``: pull its ready pairs, fetch
    their metadata, emit the ``RefBundle``s, and fire any postponed done
    callback. Test-only stand-in for the streaming executor's
    ``MetadataPrefetcher`` pipeline (the production deferred-emit mode).
    """
    deferred: List[DeferredEmit] = []
    bytes_read = task.on_data_ready(max_bytes_to_read, deferred)
    if deferred:
        metas = ray.get([d.meta_ref for d in deferred])
        for d, meta_bytes in zip(deferred, metas):
            _emit_deferred_entry(d, meta_bytes)
    if task._task_done_pending:
        _fire_task_done(task)
    return bytes_read


def run_op_tasks_sync(op: PhysicalOperator, only_existing=False):
    """Run tasks of a PhysicalOperator synchronously.

    By default, this function will run until the op no longer has any active tasks.
    If only_existing is True, this function will only run the currently existing tasks.
    """
    tasks = op.get_active_tasks()
    while tasks:
        ref_to_task = {task.get_waitable(): task for task in tasks}
        ready, _ = ray.wait(
            [task.get_waitable() for task in tasks],
            num_returns=len(tasks),
            fetch_local=False,
            timeout=0.1,
        )

        for ref in ready:
            task = ref_to_task[ref]
            if isinstance(task, DataOpTask):
                # Read all currently available output from the streaming generator
                drain_and_emit(task, max_bytes_to_read=None)
                # Only remove the task when the generator has been fully exhausted
                if task.has_finished:
                    tasks.remove(task)
            else:
                assert isinstance(task, MetadataOpTask)
                task.on_task_finished()
                tasks.remove(task)

        # NOTE: If only existing tasks need to be handled skip refreshing list
        #       of outstanding tasks
        if only_existing:
            pass
        else:
            tasks = op.get_active_tasks()


def run_one_op_task(op):
    """Run one task of a PhysicalOperator."""
    tasks = op.get_active_tasks()

    while tasks:
        waitable_to_tasks = {task.get_waitable(): task for task in tasks}

        # Block, until 1 task is ready
        ready, _ = ray.wait(
            list(waitable_to_tasks.keys()), num_returns=1, fetch_local=False
        )

        task = waitable_to_tasks[ready[0]]
        # Reset tasks to track just 1 task
        tasks = [task]

        if isinstance(task, DataOpTask):
            drain_and_emit(task, None)
            if task.has_finished:
                tasks.remove(task)
        else:
            assert isinstance(task, MetadataOpTask)
            task.on_task_finished()
            tasks.remove(task)


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block_ref in bundle.block_refs:
        output_list.append(list(ray.get(block_ref)["id"]))


def _mul2_transform(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
    for block in block_iter:
        yield pd.DataFrame({"id": [b * 2 for b in block["id"]]})


def create_map_transformer_from_block_fn(
    block_fn: MapTransformCallable[Block, Block],
    init_fn: Optional[Callable[[], None]] = None,
    output_block_size_option: Optional[OutputBlockSizeOption] = None,
    disable_block_shaping: bool = False,
):
    """Create a MapTransformer from a single block-based transform function.

    This method should only be used for testing and legacy compatibility.
    """
    return MapTransformer(
        [
            BlockMapTransformFn(
                block_fn,
                output_block_size_option=output_block_size_option,
                disable_block_shaping=disable_block_shaping,
            ),
        ],
        init_fn=init_fn,
    )


def _take_outputs(op: PhysicalOperator) -> List[Any]:
    output = []
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)
    return output
