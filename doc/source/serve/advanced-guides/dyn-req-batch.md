---
myst:
  html_meta:
    description: "Batch concurrent requests in a Serve deployment with @serve.batch, custom batch size functions, and streaming batched responses."
---

(serve-performance-batching-requests)=
# Dynamic request batching

Dynamic request batching in Ray Serve can improve your service's throughput without sacrificing latency. Batching helps because machine learning (ML) models can use efficient vectorized computation to process a batch of requests at once. Batching is also necessary when your model is expensive to use and you want to maximize hardware utilization.

ML frameworks such as TensorFlow, PyTorch, and scikit-learn support evaluating multiple samples at the same time, and dynamic request batching takes advantage of that capability. When a request arrives, Serve puts the request in a queue, which buffers requests to form a batch. The deployment picks up the batch and evaluates it. After the evaluation, Serve splits up the resulting batch and returns each response individually.

## Enable batching for your deployment
Enable batching with the {mod}`ray.serve.batch` decorator. The following example modifies the `Model` class to accept a batch:
```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __single_sample_begin__
end-before: __single_sample_end__
---
```

The batching decorators expect three changes to your method signature:
- Declare the method as an async method, because the decorator batches in the `asyncio` event loop.
- Modify the method to accept a list of its original input types. For example, change `arg1: int, arg2: str` to `arg1: List[int], arg2: List[str]`.
- Modify the method to return a list. The return list must be the same length as the input list so the decorator can split the output evenly and return each response to its corresponding request.

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __batch_begin__
end-before: __batch_end__
emphasize-lines: 11-12
---
```

The decorators accept four optional parameters:
- `max_batch_size` controls the size of the batch. The default value is 10.
- `batch_wait_timeout_s` controls how long Serve waits for a batch once the first request arrives. The default value is 0.01, or 10 milliseconds.
- `max_concurrent_batches` sets the maximum number of batches that can run concurrently. The default value is 1.
- `batch_size_fn` is an optional function that computes the effective batch size. It takes a list of items and returns an integer that represents the batch size. Use it to batch based on custom metrics such as total nodes in graphs or total tokens in sequences. When `batch_size_fn` is `None`, the default, Serve computes the batch size as `len(batch)`.

Once the first request arrives, the batching decorator waits until the batch fills to `max_batch_size` or `batch_wait_timeout_s` elapses. When the timeout elapses, Serve sends the batch to the model regardless of its size.

:::{tip}
You can reconfigure the `batch_wait_timeout_s` and `max_batch_size` parameters with the `set_batch_wait_timeout_s` and `set_max_batch_size` methods:

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __batch_params_update_begin__
end-before: __batch_params_update_end__
---
```

Call these methods in the constructor or in the [`reconfigure` method](serve-user-config) to control the `@serve.batch` parameters through your Serve config file.
:::

(custom-batch-size-functions)=
## Define a custom batch size function

By default, Ray Serve measures batch size as the number of items in the batch, `len(batch)`. In many workloads, however, the computational cost depends on properties of the items rather than on their count, as in the following examples:

- **Graph neural networks (GNNs)**: The cost depends on the total number of nodes across all graphs, not the number of graphs.
- **Natural language processing (NLP)**: Transformer models batch by total token count, not the number of sequences.
- **Variable-resolution images**: Memory usage depends on total pixels, not the number of images.

Use the `batch_size_fn` parameter to define a custom metric for batch size. Serve calls `batch_size_fn` with a list that holds each request's value for the method's first parameter after `self`. Pass that value as the first argument of each call, either positionally or by keyword. For example, with `async def predict(self, items, *, tag)`, both `await self.predict(text, tag="t")` and `await self.predict(items=text, tag="t")` pass `text` to `batch_size_fn`.

:::{note}
Serve raises a `TypeError` for a call that leaves out the method's first parameter or passes another keyword argument before it. It raises the same error when that parameter is `**kwargs`, or is `*args` and the call passes more than one value.
:::

### Graph neural network example

The following example shows how to batch graph data by total node count:

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __batch_size_fn_begin__
end-before: __batch_size_fn_end__
emphasize-lines: 20
---
```

In this example, `batch_size_fn=lambda graphs: sum(g.num_nodes for g in graphs)` ensures that the batch contains at most 10,000 total nodes. This limit prevents GPU memory overflow regardless of how many graphs the batch holds.

### NLP token batching example

The following example shows how to batch text sequences by total token count:

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __batch_size_fn_nlp_begin__
end-before: __batch_size_fn_nlp_end__
emphasize-lines: 12
---
```

This pattern ensures that the total number of tokens doesn't exceed the model's context window or memory limits.

(serve-streaming-batched-requests-guide)=

(streaming-batched-requests)=
## Stream batched requests

Use an async generator to stream the outputs from your batched requests. The following example converts the `StreamingResponder` class to accept a batch.

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __single_stream_begin__
end-before: __single_stream_end__
---
```

Decorate async generator functions with the {mod}`ray.serve.batch` decorator. As with non-streaming methods, the function takes a `List` of inputs, and in each iteration it `yield`s an iterable of outputs with the same length as the input batch size.

```{literalinclude} ../doc_code/batching_guide.py
---
start-after: __batch_stream_begin__
end-before: __batch_stream_end__
---
```

Calling the `serve.batch`-decorated function returns an async generator that you can `await` to receive results.

Some inputs within a batch might generate fewer outputs than others. When an input has nothing left to yield, pass a `StopIteration` object into the output iterable. This action stops the generator that Serve returns when it calls the `serve.batch` function with that input. When `serve.batch`-decorated functions return streaming generators over HTTP, the end client's connection can then close as soon as its call finishes, instead of waiting for the entire batch to finish.

(tips-for-fine-tuning-batching-parameters)=
## Tune batching parameters

Ideally, set `max_batch_size` to a power of 2, such as 2, 4, 8, or 16, because CPUs and GPUs are both optimized for data of these shapes. Large batch sizes incur a high memory cost and a latency penalty for the first few requests.

When you use `batch_size_fn`, set `max_batch_size` based on your custom metric rather than item count. For example, if you batch by total nodes in graphs, set `max_batch_size` to your GPU's maximum node capacity, such as 10,000 nodes, rather than to a count of graphs.

Set `batch_wait_timeout_s` based on your end-to-end latency service level objective (SLO). For example, if your latency target is 150 ms and the model takes 100 ms to evaluate the batch, set `batch_wait_timeout_s` to a value much lower than 150 ms - 100 ms = 50 ms.

When you batch requests in an application that chains deployments, the relationship between an upstream deployment and a downstream deployment might also affect performance. Consider a chain of two models where the first model sets `max_batch_size=8` and the second sets `max_batch_size=6`. When the first model finishes a full batch of 8, the second model finishes one batch of 6. Serve then only partially fills the next batch with the remaining 8 - 6 = 2 requests, which incurs latency costs. Ideally, make each downstream model's batch size a multiple or divisor of the upstream model's batch size so the batches work optimally together.
