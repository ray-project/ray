import numpy as np
import pytest
import torch
import torch.distributed as dist

import ray
from ray.air.util.torch_dist import (
    init_torch_dist_process_group,
    TorchDistributedWorker,
)


def test_torch_process_group_gloo():
    @ray.remote
    class TestWorker(TorchDistributedWorker):
        def run(self):
            tensor = torch.tensor([1.0])
            dist.all_reduce(tensor)
            return tensor.numpy()

    workers = [TestWorker.remote() for _ in range(5)]

    init_torch_dist_process_group(workers, backend="gloo", init_method="env")

    reduced = ray.get([w.run.remote() for w in workers])

    # One tensor from each worker.
    assert len(reduced) == 5
    for r in reduced:
        assert len(r) == 1
        assert r.dtype == np.float32
        # All-reduce. Each tensor contributed 1.0. 5 tensors in total.
        assert r[0] == 5.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
