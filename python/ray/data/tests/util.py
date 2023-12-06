import functools
import os
import tempfile
from contextlib import contextmanager

import ray
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    PhysicalOperator,
)


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
            fp = open(path, "wb")
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
            output.append({name: value for (name, value) in zip(col_names, t)})
    else:
        for t in tuples:
            output.append({name: value for (name, value) in zip((col_names,), (t,))})
    return output


def extract_values(col_name, tuples):
    return [t[col_name] for t in tuples]


def run_op_tasks_sync(op: PhysicalOperator, only_existing=False):
    """Run tasks of a PhysicalOperator synchronously.

    By default, this function will run until the op no longer has any active tasks.
    If only_existing is True, this function will only run the currently existing tasks.
    """
    tasks = op.get_active_tasks()
    while tasks:
        ray.wait(
            [task.get_waitable() for task in tasks],
            num_returns=len(tasks),
            fetch_local=False,
        )
        for task in tasks:
            if isinstance(task, DataOpTask):
                task.on_data_ready(None)
            else:
                assert isinstance(task, MetadataOpTask)
                task.on_task_finished()
        if only_existing:
            return
        tasks = op.get_active_tasks()


def run_one_op_task(op):
    """Run one task of a PhysicalOperator."""
    tasks = op.get_active_tasks()
    waitable_to_tasks = {task.get_waitable(): task for task in tasks}
    ready, _ = ray.wait(
        list(waitable_to_tasks.keys()), num_returns=1, fetch_local=False
    )
    task = waitable_to_tasks[ready[0]]
    if isinstance(task, DataOpTask):
        task.on_data_ready(None)
    else:
        assert isinstance(task, MetadataOpTask)
        task.on_task_finished()
