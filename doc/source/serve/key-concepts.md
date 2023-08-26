(serve-key-concepts)=

# Key Concepts

(serve-key-concepts-deployment)=

## Deployment

Deployments are the central concept in Ray Serve.
A deployment contains business logic or an ML model to handle incoming requests and can be scaled up to run across a Ray cluster.
At runtime, a deployment consists of a number of *replicas*, which are individual copies of the class or function that are started in separate Ray Actors (processes).
The number of replicas can be scaled up or down (or even autoscaled) to match the incoming request load.

To define a deployment, use the {mod}`@serve.deployment <ray.serve.api.deployment>` decorator on a Python class (or function for simple use cases).
Then, `bind` the deployment with optional arguments to the constructor (see below).
Finally, deploy the resulting "bound deployment" using `serve.run` (or the equivalent `serve run` CLI command, see [Development Workflow](serve-dev-workflow) for details).

```{literalinclude} doc_code/key_concepts.py
:start-after: __my_first_deployment_start__
:end-before: __my_first_deployment_end__
:language: python
```

(serve-key-concepts-application)=

## Application

An application is the unit of upgrade in a Ray Serve cluster. An application consists of one or more deployments. One of these deployments is considered the [“ingress” deployment](serve-key-concepts-ingress-deployment), which handles all inbound traffic.

Applications can be called via HTTP at the specified route_prefix or in Python by retrieving a handle to the application by name.
 
(serve-key-concepts-query-deployment)=

## ServeHandle (composing deployments)

Ray Serve enables flexible model composition and scaling by allowing multiple independent deployments to call into each other.
When binding a deployment, you can include references to _other bound deployments_.
Then, at runtime each of these arguments is converted to a {mod}`ServeHandle <ray.serve.handle.RayServeHandle>` that can be used to query the deployment using a Python-native API.
Below is a basic example where the `Driver` deployment can call into two downstream models.

```{literalinclude} doc_code/key_concepts.py
:start-after: __driver_start__
:end-before: __driver_end__
:language: python
```

(serve-key-concepts-ingress-deployment)=

## Ingress Deployment (HTTP handling)

A Serve application can consist of multiple deployments that can be combined to perform model composition or complex business logic.
However, there is always one "top-level" deployment, the one that will be passed to `serve.run` or `serve.build` to deploy the application.
This deployment is called the "ingress deployment" because it serves as the entrypoint for all traffic to the application.
Often, it will then route to other deployments or call into them using the `ServeHandle` API and compose the results before returning to the user.

The ingress deployment defines the HTTP handling logic for the application.
By default, the `__call__` method of the class will be called and passed in a `Starlette` request object.
The response will be serialized as JSON, but other `Starlette` response objects can also be returned directly.
Here's an example:

```{literalinclude} doc_code/key_concepts.py
:start-after: __most_basic_ingress_start__
:end-before: __most_basic_ingress_end__
:language: python
```

After binding the deployment and running `serve.run()`, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model to verify that it's working.

```{literalinclude} doc_code/key_concepts.py
:start-after: __request_get_start__
:end-before: __request_get_end__
:language: python
```

For more expressive HTTP handling, Serve also comes with a built-in integration with `FastAPI`.
This allows you to use the full expressiveness of FastAPI to define more complex APIs:

```{literalinclude} doc_code/key_concepts.py
:start-after: __fastapi_start__
:end-before: __fastapi_end__
:language: python
```

(serve-key-concepts-deployment-graph)=

## Deployment Graph

Building on top of the deployment concept, Ray Serve also provides a first-class API for composing multiple models into a graph structure and orchestrating the calls to each deployment automatically. In this case, the `DAGDriver` is the ingress deployment.

Here's a simple example combining a preprocess function and model.

```{literalinclude} doc_code/key-concepts-deployment-graph.py
```

## What's Next?
Now that you have learned the key concepts, you can dive into these guides:
- [Scaling and allocating resources](scaling-and-resource-allocation)
- [Configuring HTTP logic and integrating with FastAPI](http-guide)
- [Development workflow for Serve applications](serve-dev-workflow)
- [Composing deployments to perform model composition](serve-model-composition)
