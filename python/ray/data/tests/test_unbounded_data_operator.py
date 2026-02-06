import pyarrow as pa

import ray
from ray.data._internal.execution.operators.unbounded_data_operator import (
    UnboundedDataOperator,
)
from ray.data._internal.logical.operators.unbound_data_operator import StreamingTrigger
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask


class FakeStreamingDatasource(Datasource):
    def __init__(self, batches):
        # batches is list[list[pyarrow.Table]]  (microbatches)
        self._batches = list(batches)
        self._idx = 0

    def get_name(self):
        return "FakeStreamingDatasource"

    def initial_checkpoint(self):
        return 0

    def get_read_tasks(self, parallelism: int, *, checkpoint=None, trigger=None, batch_id=0):
        # each microbatch returns up to 1 task for simplicity
        if checkpoint is None:
            checkpoint = 0
        if checkpoint >= len(self._batches):
            return [], checkpoint
        tables = self._batches[checkpoint]
        next_ckpt = checkpoint + 1

        def read_fn():
            # yield each table as a block
            for t in tables:
                yield t

        task = ReadTask(read_fn=read_fn, metadata=None)
        return [task], next_ckpt

    def commit_checkpoint(self, checkpoint):
        self._idx = checkpoint


def _drain(op, max_iters=1000):
    out = []
    iters = 0
    while iters < max_iters and op.has_next():
        try:
            b = op._get_next_inner()
            out.extend([ref for ref, _ in b.blocks])
        except StopIteration:
            pass
        iters += 1
    return out


def test_available_now_stops_after_empty():
    ray.init(ignore_reinit_error=True)
    ds = FakeStreamingDatasource(
        batches=[
            [pa.table({"a": [1, 2]})],
            [pa.table({"a": [3]})],
        ]
    )
    ctx = DataContext.get_current()
    trigger = StreamingTrigger.available_now()

    op = UnboundedDataOperator(ctx, ds, trigger, parallelism=1)
    op.start(None)

    refs = _drain(op)
    assert len(refs) == 2  # two tables yielded across microbatches
    assert op.completed()

    ray.shutdown()


if __name__ == "__main__":
    test_available_now_stops_after_empty()
    print("Test passed!")
