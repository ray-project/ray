(serve-model-multiplexing)=

# Model Multiplexing

This section helps you understand how to write multiplexed deployment by using the `serve.multiplexed` and `serve.get_multiplexed_model_id` APIs.

This is an experimental feature and the API may change in the future. You are welcome to try it out and give us feedback!

## Why model multiplexing?

Model multiplexing is a technique used to efficiently serve multiple models with similar input types from a pool of replicas. Traffic is routed to the corresponding model based on the request header. To serve multiple models with a pool of replicas, 
model multiplexing optimizes cost and load balances the traffic. This is useful in cases where you might have many models with the same shape but different weights that are sparsely invoked. If any replica for the deployment has the model loaded, incoming traffic for that model (based on request header) will automatically be routed to that replica avoiding unnecessary load time.

## Writing a multiplexed deployment

To write a multiplexed deployment, use the `serve.multiplexed` and `serve.get_multiplexed_model_id` APIs.

Assuming you have multiple Torch models inside an aws s3 bucket with the following structure:
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
This code example uses the Pytorch Model object. You can also define your own model class and use it here. To release resources when the model is evicted, implement the `__del__` method. Ray Serve internally calls the `__del__` method to release resources when the model is evicted.
:::


`serve.get_multiplexed_model_id` is used to retrieve the model id from the request header, and the model_id is then passed into the `get_model` function. If the model id is not found in the replica, Serve will load the model from the s3 bucket and cache it in the replica. If the model id is found in the replica, Serve will return the cached model.

:::{note}
Internally, serve router will route the traffic to the corresponding replica based on the model id in the request header.
If all replicas holding the model are over-subscribed, ray serve sends the request to a new replica that doesn't have the model loaded. The replica will load the model from the s3 bucket and cache it.
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
