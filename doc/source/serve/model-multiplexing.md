# Model Multiplexing

This section helps you understand how to write multiplexed deployment by using new introduced API `serve.multiplexed` and `serve.get_multiplexed_model_id`.

This is experimental feature and the API may change in the future, welcome to try it out and give us feedback!

## Why do we need model multiplexing?

Model multiplexing is a technique to serve multiple models in a single replica, traffic will be routed to the corresponding model based on the request header.

In the previous version of Ray Serve, if you want to serve many models, you have to deploy multiple applications or write routing logics inside your own deployment codes. This is not efficient when you have many models with similar input types and you don't need to load them all unless there are requests asking for them.

## How to write a multiplexed deployment?

To write a multiplexed deployment, you need to use the new introduced API `serve.multiplexed` and `serve.get_multiplexed_model_id`.

Assume you have multiple torch models inside aws s3:
```
s3://my_bucket/1/model.pt
s3://my_bucket/2/model.pt
s3://my_bucket/3/model.pt
s3://my_bucket/4/model.pt
...
```

Define a Multiplexed deployment:
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_deployment_example_begin__
:end-before: __serve_deployment_example_end__
```

:::{note}
serve.multiplexed API is also parameterized by `max_num_models_per_replica`. You can use them to configure how many models you want to load in a single replica. If the number of models is larger than `max_num_models_per_replica`, Serve will use LRU policy to evict the least recently used model.
:::

:::{tips}
In the code example, we are using the Pytorch Model object, you can also define your own model class and use it here. You must make sure the model class has `__call__` method, and if you want to release resources when the model is evicted, you can implement `__del__` method. Ray Serve internally will call `__call__` method to execute the model, and call `__del__` method to release resources when the model is evicted.
:::


In the code, you can use `serve.get_multiplexed_model_id` to get the model id from each request, ray serve will use this model id to route the request to the corresponding replica.

To send a request to a specific model, you can use the following code:
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_request_send_example_begin__
:end-before: __serve_request_send_example_end__
```

:::{note}
`serve_multiplexed_model_id` is required in the request header, and the value should be the model id you want to send the request to.
:::

After you run the above code, you will see the following output inside the deployment log:
```
INFO 2023-05-24 01:19:03,853 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:442 - Started executing request CUpzhwUUNw
INFO 2023-05-24 01:19:03,854 default_Model default_Model#EjYmnQ CUpzhwUUNw / default multiplex.py:131 - Loading model '1'.
INFO 2023-05-24 01:19:04,859 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:542 - __CALL__ OK 1005.8ms
```

If you load multiple different models in a single replica, and exceed the `max_num_models_per_replica`, you will see the following output inside the deployment log, the least recently used model will be evicted:
```
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default replica.py:442 - Started executing request WzjTbJvbPN
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default multiplex.py:145 - Unloading model '3'.
INFO 2023-05-24 01:19:15,988 default_Model default_Model#rimNjA WzjTbJvbPN / default multiplex.py:131 - Loading model '4'.
INFO 2023-05-24 01:19:16,993 default_Model default_Model#rimNjA WzjTbJvbPN / default replica.py:542 - __CALL__ OK 1005.7ms
```
