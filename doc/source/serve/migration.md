(serve-migration)=

# 1.x to 2.x API Migration Guide

This section covers what to consider or change in your application when migrating from Ray versions 1.x to 2.x.

## What has been changed?

In Ray Serve 2.0, we released a [new deployment API](converting-to-ray-serve-deployment). The 1.x deployment API can still be used, but it will be deprecated in the future version.


## Migrating the 1.x Deployment

### Migrating handle pass between deployments
In the 1.x deployment, we usually pass handle of deployment to chain the deployments.
```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __raw_handle_graph_start__
:end-before: __raw_handle_graph_end__
:language: python
```

With the 2.0 deployment API, you can use the following code to update the above one.
```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __graph_with_new_api_start__
:end-before: __graph_with_new_api_end__
:language: python
```

:::{note}
- `get_handle` can be replaced by `bind()` function to fulfill same functionality.
- `serve.run` will return the entry point deployment handle for your whole chained deployments.
:::

### Migrating a single deployment to the new deployment API 

In the 1.x deployment API, we usually have the following code for deployment.
```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __single_deployment_old_api_start__
:end-before: __single_deployment_old_api_end__
:language: python
```

With the 2.0 deployment API, you can use the following code to update the above one.
```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __single_deployment_new_api_start__
:end-before: __single_deployment_new_api_end__
:language: python
```


### Migrate Multiple deployment to new deployment API

When you have multiple deployments, here is the normal code for 1.x API

```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __multi_deployments_old_api_start__
:end-before: __multi_deployments_old_api_end__
:language: python
```

With the 2.0 deployment API, you can use the following code to update the above one.

```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __multi_deployments_new_api_start__
:end-before: __multi_deployments_new_api_end__
:language: python
```


:::{note}
- `predict` method is defined inside `DAGDriver` class as an entry point to fulfil requests
- Similar to `predict` method, `predict_with_route` method is defined inside `DAGDriver` class as an entry point to fulfil requests.
- `DAGDriver` is a special class to handle multi entry points for different deployments 
- `DAGDriver.bind` can accept dictionary and each key is represented as entry point route path.
- `predict_with_route` accepts a route path as the first argument to select which model to use.
- In the example, you can also use an HTTP request to fulfill your request. Different models will bind with different route paths based on the user inputs; e.g. http://localhost:8000/model1 and http://localhost:8000/model2
:::


### Migrate deployments with route prefixes

Sometimes, you have a customized route prefix for each deployment:

```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __customized_route_old_api_start__
:end-before: __customized_route_old_api_end__
:language: python
```

With the 2.0 deployment API, you can use the following code to update the above one.

```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __customized_route_old_api_1_start__
:end-before: __customized_route_old_api_1_end__
:language: python
```

Or if you have multiple deployments and want to customize the HTTP route prefix for each model, you can use the following code:

```{literalinclude} ../serve/doc_code/migration_example.py
:start-after: __customized_route_old_api_2_start__
:end-before: __customized_route_old_api_2_end__
:language: python
```
