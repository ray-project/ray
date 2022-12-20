# flake8: noqa
# __single_sample_begin__
from ray import serve
import ray


@serve.deployment
class Model:
    def __call__(self, single_sample: int) -> int:
        return single_sample * 2


handle = serve.run(Model.bind())
assert ray.get(handle.remote(1)) == 2
# __single_sample_end__


# __batch_begin__
from typing import List
import numpy as np
from ray import serve
import ray


@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, multiple_samples: List[int]) -> List[int]:
        # Use numpy's vectorized computation to efficiently process a batch.
        return np.array(multiple_samples) * 2


handle = serve.run(Model.bind())
assert ray.get([handle.remote(i) for i in range(8)]) == [i * 2 for i in range(8)]
# __batch_end__
