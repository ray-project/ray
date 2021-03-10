import asyncio
from functools import wraps
from inspect import iscoroutinefunction

import ray
from ray.serve.backend_worker import BatchQueue

class _ExceptionWrapper:
    def __init__(self, underlying):
        assert isinstance(underlying, Exception)
        self._underlying = underlying

    def raise_underlying(self):
        raise self._underlying

async def handle_batches(batch_queue, obj, method):
    while True:
        batch = await batch_queue.wait_for_batch()
        assert len(batch) > 0
        requests = [item[0] for item in batch]
        futures = [item[1] for item in batch]

        try:
            results = await method(obj, requests)
        except Exception as e:
            results = [_ExceptionWrapper(e)] * len(requests)

        for i, result in enumerate(results):
            futures[i].set_result(result)

def batch(method, max_batch_size=10, batch_wait_timeout_s=0.1):
    # TODO(edoakes): how to check that only methods are passed into this?
    # We get an unbound function type, which I'm not sure how to validate.
    if not iscoroutinefunction(method):
        raise TypeError(
            "Methods decorated with @serve.batch must be 'async def'.")

    @wraps(method)
    async def batch_wrapper(self, request):
        batch_queue_attr = f"_serve_batch_queue_{method.__name__}"
        if not hasattr(self, batch_queue_attr):
            batch_queue = BatchQueue(max_batch_size, batch_wait_timeout_s)
            setattr(self, batch_queue_attr, batch_queue)
            asyncio.get_event_loop().create_task(handle_batches(batch_queue, self, method))
        else:
            batch_queue = getattr(self, batch_queue_attr)

        future = asyncio.get_event_loop().create_future()
        batch_queue.put((request, future))
        result = await future
        if isinstance(result, _ExceptionWrapper):
            result.raise_underlying()
        
        return result

    return batch_wrapper

class A:
    def __init__(self, size, timeout):
        self.size = size
        self.timeout = timeout

    @batch
    async def handle_batch(self, requests):
        if "raise" in requests:
            raise Exception("uh oh")
        return requests

    async def single(self, request):
        return await self.handle_batch(request)

async def main():
    a = A(2, 5)
    #print(await a.single("hi"))
    #print(await a.single("raise"))

    t1 = asyncio.create_task(a.single("hi"))
    t2 = asyncio.create_task(a.single("raise"))
    print(await t1)
    print(await t2)

asyncio.run(main())
