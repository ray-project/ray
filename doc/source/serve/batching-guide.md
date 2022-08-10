# Requests Batching

Serve offers a request batching feature that can improve your service throughput with sacrificing latency.

This guide will teach you:
- How to use Serve's `@serve.batch` decorator
- How to configure `@serve.batch` decorator

ML frameworks like Tensorflow, PyTorch, and Scikit-Learn are built to support efficiently evaluating multiple samples at the same time.
Ray Serve allows you to take advantage of this feature via dynamic request batching.
When a request arrives, it will wait in a queue. This queue buffers requests to form a batch of sample. The batch will then be picked up by the model for evaluation. After the evaluation, the result batch will be split up and send to the response individually.

## Enable batching for your deployment
You can enable batching by using the {mod}`ray.serve.batch` decorator. Let's take a look at an simple example. We will transform the MyModel class to accept a batch.
```{literalinclude} doc_code/batching_guide.py
---
start-after: __single_sample_begin__
end-before: __single_sample_end__
---
```

The batching decorators expect you to make the following changes in your method signature:
- The function should be an async method because the decorator batches in asyncio event loop.
- The function should accept a list of its original input types as input. For example, `arg1: int, arg2: str` should be changed to `arg1: List[int], arg2: List[str]`.
- The function should return a list as well. The returned list should have exactly the same length as the input list, so that the decorator can split the output back to its individual sender.

```{literalinclude} doc_code/batching_guide.py
---
start-after: __batch_begin__
end-before: __batch_end__
emphasize-lines: 6-9
---
```

You can supply two optional parameter the decorators.
- `batch_wait_timeout_s` controls how long should Serve wait for a batch once the first request arrives.
- `max_batch_size` controls the size of the batch.
Once the first request arrives, the batching decorator will wait for a full batch (up to `max_batch_size`) until `batch_wait_timeout_s` is reached. If the timeout is reached, the batch will be sent to the model regardless the batch size.

## Tips for fine-tuning batching parameters

`max_batch_size` ideally should be power of 2s (2, 4, 8, 16, ...) because CPUs and GPUs are both optimized for data of these shapes. Large batch sizes incur a high memory cost as well as latency penalty for the first few requests.

`batch_wait_timeout_s` should be set considering the end to end latency SLO (Service Level Objective). After all, the first requests will wait at-most this time for a full batch, adding to its latency cost. For example, if your latency target is 150ms, and the model takes 100ms to evaluate the batch, the `batch_wait_timeout_s` should be way lower than 50ms.

When using batching in Serve Deployment Graph, relationship between upstream node and downstream node might affect the performance as well. Consider a chain of two models, first model sets `max_batch_size=8` and second model sets `max_batch_size=6`. In this scenario, the second model will always evaluate twice when the first model finishes a full batch. The batch size of downstream models should ideally be multiples or dividers of the upstream models to ensure the batches play well together.