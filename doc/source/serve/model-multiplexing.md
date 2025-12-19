(serve-model-multiplexing)=

# Model Multiplexing

This section helps you understand how to write multiplexed deployment by using the `serve.multiplexed` and `serve.get_multiplexed_model_id` APIs.

## Why model multiplexing?

Model multiplexing is a technique used to efficiently serve multiple models with similar input types from a pool of replicas. Traffic is routed to the corresponding model based on the request header. To serve multiple models with a pool of replicas, 
model multiplexing optimizes cost and load balances the traffic. This is useful in cases where you might have many models with the same shape but different weights that are sparsely invoked. If any replica for the deployment has the model loaded, incoming traffic for that model (based on request header) will automatically be routed to that replica avoiding unnecessary load time.

## Writing a multiplexed deployment

To write a multiplexed deployment, use the `serve.multiplexed` and `serve.get_multiplexed_model_id` APIs.

Assuming you have multiple PyTorch models inside an AWS S3 bucket with the following structure:
```
s3://my_bucket/1/model.pt
s3://my_bucket/2/model.pt
s3://my_bucket/3/model.pt
s3://my_bucket/4/model.pt
...
```

Define a multiplexed deployment:
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_deployment_example_begin__
:end-before: __serve_deployment_example_end__
```

:::{note}
The `serve.multiplexed` API also has a `max_num_models_per_replica` parameter. Use it to configure how many models to load in a single replica. If the number of models is larger than `max_num_models_per_replica`, Serve uses the LRU policy to evict the least recently used model.
:::

:::{tip}
This code example uses the PyTorch Model object. You can also define your own model class and use it here. To release resources when the model is evicted, implement the `__del__` method. Ray Serve internally calls the `__del__` method to release resources when the model is evicted.
:::


`serve.get_multiplexed_model_id` retrieves the model ID from the request header. This ID is then passed to the `get_model` function. If the model is not already cached in the replica, Serve loads it from the S3 bucket. Otherwise, the cached model is returned.

:::{note}
Internally, the Serve router uses the model ID in the request header to route traffic to a corresponding replica. If all replicas that have the model are over-subscribed, Ray Serve routes the request to a new replica, which then loads and caches the model from the S3 bucket.
:::

To send a request to a specific model, include the `serve_multiplexed_model_id` field in the request header, and set the value to the model ID to which you want to send the request.
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_request_send_example_begin__
:end-before: __serve_request_send_example_end__
```
:::{note}
`serve_multiplexed_model_id` is required in the request header, and the value should be the model ID you want to send the request to.

If the `serve_multiplexed_model_id` is not found in the request header, Serve will treat it as a normal request and route it to a random replica.
:::

After you run the above code, you should see the following lines in the deployment logs:
```
INFO 2023-05-24 01:19:03,853 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:442 - Started executing request CUpzhwUUNw
INFO 2023-05-24 01:19:03,854 default_Model default_Model#EjYmnQ CUpzhwUUNw / default multiplex.py:131 - Loading model '1'.
INFO 2023-05-24 01:19:04,859 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:542 - __CALL__ OK 1005.8ms
```

If you continue to load more models and exceed the `max_num_models_per_replica`, the least recently used model will be evicted and you will see the following lines in the deployment logs::
```
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default replica.py:442 - Started executing request WzjTbJvbPN
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default multiplex.py:145 - Unloading model '3'.
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default multiplex.py:131 - Loading model '4'.
INFO 2023-05-24 01:19:16,993 default_Model default_Model#rimNjA WzjTbJvbPN / default replica.py:542 - __CALL__ OK 1005.7ms
```

You can also send a request to a specific model by using handle {mod}`options <ray.serve.handle.DeploymentHandle>` API.
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_handle_send_example_begin__
:end-before: __serve_handle_send_example_end__
```

When using model composition, you can send requests from an upstream deployment to a multiplexed deployment using the Serve DeploymentHandle. You need to set the `multiplexed_model_id` in the options. For example:
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_model_composition_example_begin__
:end-before: __serve_model_composition_example_end__
```

## Using model multiplexing with batching

You can combine model multiplexing with the `@serve.batch` decorator for efficient batched inference. When you use both features together, Ray Serve automatically splits batches by model ID to ensure each batch contains only requests for the same model. This prevents issues where a single batch would contain requests targeting different models.

The following example shows how to combine multiplexing with batching:

```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_multiplexed_batching_example_begin__
:end-before: __serve_multiplexed_batching_example_end__
```

:::{note}
`serve.get_multiplexed_model_id()` works correctly inside functions decorated with `@serve.batch`. Ray Serve guarantees that all requests in a batch have the same `multiplexed_model_id`, so you can safely use this value to load and apply the appropriate model for the entire batch.
:::
