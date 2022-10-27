import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask, Reader

from ray.tests.conftest import *  # noqa


def test_read_large_data(ray_start_cluster):
    # Test 20G input with single task
    num_batch = 20
    ctx = ray.data.context.DatasetContext.get_current()
    block_splitting_enabled = ctx.block_splitting_enabled
    ctx.block_splitting_enabled = True

    try:
        cluster = ray_start_cluster
        cluster.add_node(num_cpus=1)

        ray.init(cluster.address)

        # Data source generates multiple 1G random bytes data
        class LargeBytesDatasource(Datasource):
            def create_reader(self, **read_args):
                return LargeBytesReader()

        class LargeBytesReader(Reader):
            def estimate_inmemory_data_size(self):
                return None

            def get_read_tasks(self, parallelism: int):
                def _1g_batches_generator():
                    for _ in range(num_batch):
                        yield pd.DataFrame(
                            {"one": [np.random.bytes(1024 * 1024 * 1024)]}
                        )

                return parallelism * [
                    ReadTask(
                        lambda: _1g_batches_generator(),
                        BlockMetadata(
                            num_rows=None,
                            size_bytes=None,
                            schema=None,
                            input_files=None,
                            exec_stats=None,
                        ),
                    )
                ]

        def foo(batch):
            return pd.DataFrame({"one": [1]})

        ds = ray.data.read_datasource(
            LargeBytesDatasource(),
            parallelism=1,
        )

        ds = ds.map_batches(foo, batch_size=None)
        assert ds.count() == num_batch
    finally:
        ctx.block_splitting_enabled = block_splitting_enabled


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
