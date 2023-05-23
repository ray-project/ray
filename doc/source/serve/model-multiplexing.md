# Model Multiplexing

This section helps you understand how to write multiplexed deployment by using new introduced API `serve.multiplexed` and `serve.get_multiplexed_model_id`.

This is alpha feature and the API may change in the future, welcome to try it out and give us feedback!

## What is model multiplexing?

Model multiplexing is a technique to serve multiple models in a single replica. It is useful when you want to serve multiple models with similar input/output types, and you want to save resources by sharing the same replica.

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

With the above code, you can use `serve.get_multiplexed_model_id` to get the model id from each request.

To send a request to the above deployment, you can use the following code:
```{literalinclude} doc_code/multiplexed.py
:language: python
:start-after: __serve_request_send_example_begin__
:end-before: __serve_request_send_example_end__
```

With the above code, you can use special keyword `serve_multiplexed_model_id` in request headers to specify the model id you want to use.

After you run the above code, you will see the following output inside the deployment log:
```
INFO 2023-05-24 01:19:03,853 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:442 - Started executing request CUpzhwUUNw
INFO 2023-05-24 01:19:03,854 default_Model default_Model#EjYmnQ CUpzhwUUNw / default multiplex.py:131 - Loading model '1'.
INFO 2023-05-24 01:19:04,859 default_Model default_Model#EjYmnQ CUpzhwUUNw / default replica.py:542 - __CALL__ OK 1005.8ms
```
