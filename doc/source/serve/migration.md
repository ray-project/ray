(serve-migration)=

# Serve 1.x to 2.x API Migration Guide

This section is to help you migrate the deployment API from 1.x to 2.x.

## What has been changed?

In Ray Serve 2.0, we released a [new deployment API](converting-to-ray-serve-deployment). The 1.x deployment API can still be used, but with a deprecation warning.


## Migrating the 1.x Deployment

### Migrating a single deployment to the new deployment API 

In the 1.x deployment API, we usually have the following code for deployment.
```python
@serve.deployment
class Model:
    def forward(self, input):
        # some inference work
        print(input)
        return

serve.start()
Model.deploy()
handle = Model.get_handle()
handle.forward.remote(1)
```

With the 2.0 deployment API, you can use the following code to update the above one.
```python
@serve.deployment
class Model:
    def forward(self, input):
        # some inference work
        print(input)
        return

with InputNode() as dag_input:
    model = Model.bind()
    d = DAGDriver.bind(model.forward.bind(dag_input))
handle = serve.run(d)
handle.predict.remote(1)
```

:::{note}
`predict` method is defined inside `DAGDriver` class as an entry point to fulfil requests
:::

### Migrate Multiple deployment to new deployment API

When you have multiple deployments, here is the normal code for 1.x API

```python
@serve.deployment
class Model:
    def forward(self, input):
        # some inference work
        print(input)
        return

@serve.deployment
class Model2:
    def forward(self, input):
        # some inference work
        print(input)
        return

serve.start()
Model.deploy()
Model2.deploy()
handle = Model.get_handle()
handle.forward.remote(1)

handle2 = Model2.get_handle()
handle2.forward.remote(1)
```

With the 2.0 deployment API, you can use the following code to update the above one.

```python
@serve.deployment
class Model:
    def forward(self, input):
        # some inference work
        print(input)
        return

@serve.deployment
class Model2:
    def forward(self, input):
        # some inference work
        print(input)
        return

with InputNode() as dag_input:
    model = Model.bind()
    model2 = Model2.bind()
    d = DAGDriver.bind({"/model1": model.forward.bind(dag_input), "/model2": model2.forward.bind(dag_input)})
handle = serve.run(d)
handle.predict_with_route.remote("/model1", 1)
handle.predict_with_route.remote("/model2", 1)

resp = requests.get("http://localhost:8000/model1", data="1")
resp = requests.get("http://localhost:8000/model2", data="1")
```


:::{note}
- Similar to `predict` method, `predict_with_route` method is defined inside `DAGDriver` class as an entry point to fulfil requests.
- `DAGDriver.bind` can accept dictionary and each key is represented as entry point route path.
- `predict_with_route` accepts a route path as the first argument to select which model to use.
- In the example, you can also use an HTTP request to fulfill your request. Different models will bind with different route paths based on the user inputs; e.g. http://localhost:8000/model1 and http://localhost:8000/model2
:::


### Migrate deployments with route prefixes

Sometimes, you have a customized route prefix for each deployment:

```python
@serve.deployment(route_prefix="/my_model1")
class Model:
    def __call__(self, req):
        # some inference work
        return "done"

serve.start()
Model.deploy()
resp = requests.get("http://localhost:8000/my_model1", data="321")
```

With the 2.0 deployment API, you can use the following code to update the above one.

```python
@serve.deployment
class Model:
    def __call__(self, req):
        # some inference work
        return "done"

d = DAGDriver.options(route_prefix="/my_model1").bind(Model.bind())
handle = serve.run(d)
resp = requests.get("http://localhost:8000/my_model1", data="321")
```

Or if you have multiple deployments and want to customize http route prefix for each model you can have the following code

```python
@serve.deployment
class Model:
    def __call__(self, req):
        # some inference work
        return "done"

@serve.deployment
class Model2:
    def __call__(self, req):
        # some inference work
        return "done"

d = DAGDriver.bind({"/my_model1": Model.bind(), "/my_model2": Model2.bind()})
handle = serve.run(d)
resp = requests.get("http://localhost:8000/my_model1", data="321")
resp = requests.get("http://localhost:8000/my_model2", data="321")
```

:::
