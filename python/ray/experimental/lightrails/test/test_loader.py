from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
import torch
from ray.experimental.lightrails.util import BlockBatchLoader


def test_loader():
    loader = BlockBatchLoader()

    loader.push_batch(torch.rand(1, 2))
    loader.push_batch(torch.rand(2, 3))
    it = iter(loader)
    assert next(it)[0].shape == (1, 2)
    assert next(it)[0].shape == (2, 3)

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(lambda: next(it))

    assert future.done() is False
    loader.push_batch(torch.rand(3, 4))
    assert future.result()[0].shape == (3, 4)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
