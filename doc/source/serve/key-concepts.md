(serve-key-concepts)=

# Key Concepts

(serve-key-concepts-deployment)=

## Deployment

Deployments are the central concept in Ray Serve.
A deployment contains business logic or an ML model to handle incoming requests and can be scaled up to run across a Ray cluster.
At runtime, a deployment consists of a number of *replicas*, which are individual copies of the class or function that are started in separate Ray Actors (processes).
The number of replicas can be scaled up or down (or even autoscaled) to match the incoming request load.

To define a deployment, use the {mod}`@serve.deployment <ray.serve.deployment>` decorator on a Python class (or function for simple use cases).
Then, `bind` the deployment with optional arguments to the constructor to define an [application](serve-key-concepts-application).
Finally, deploy the resulting application using `serve.run` (or the equivalent `serve run` CLI command, see [Development Workflow](serve-dev-workflow) for details).

```{literalinclude} ../serve/doc_code/key_concepts.py
:start-after: __start_my_first_deployment__
:end-before: __end_my_first_deployment__
:language: python
```

(serve-key-concepts-application)=

## Application

An application is the unit of upgrade in a Ray Serve cluster. An application consists of one or more deployments. One of these deployments is considered the [“ingress” deployment](serve-key-concepts-ingress-deployment), which handles all inbound traffic.

Applications can be called via HTTP at the specified `route_prefix` or in Python using a `DeploymentHandle`.
 
(serve-key-concepts-deployment-handle)=

## DeploymentHandle (composing deployments)

Ray Serve enables flexible model composition and scaling by allowing multiple independent deployments to call into each other.
When binding a deployment, you can include references to _other bound deployments_.
Then, at runtime each of these arguments is converted to a {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` that can be used to query the deployment using a Python-native API.
Below is a basic example where the `Ingress` deployment can call into two downstream models.
For a more comprehensive guide, see the [model composition guide](serve-model-composition).

```{literalinclude} ../serve/doc_code/key_concepts.py
:start-after: __start_deployment_handle__
:end-before: __end_deployment_handle__
:language: python
```

(serve-key-concepts-ingress-deployment)=

## Ingress deployment (HTTP handling)

A Serve application can consist of multiple deployments that can be combined to perform model composition or complex business logic.
However, one deployment is always the "top-level" one that is passed to `serve.run` to deploy the application.
This deployment is called the "ingress deployment" because it serves as the entrypoint for all traffic to the application.
Often, it then routes to other deployments or calls into them using the `DeploymentHandle` API, and composes the results before returning to the user.

The ingress deployment defines the HTTP handling logic for the application.
By default, the `__call__` method of the class is called and passed in a `Starlette` request object.
The response will be serialized as JSON, but other `Starlette` response objects can also be returned directly.
Here's an example:

```{literalinclude} ../serve/doc_code/key_concepts.py
:start-after: __start_basic_ingress__
:end-before: __end_basic_ingress__
:language: python
```

After binding the deployment and running `serve.run()`, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model using `requests` to verify that it's working.

For more expressive HTTP handling, Serve also comes with a built-in integration with `FastAPI`.
This allows you to use the full expressiveness of FastAPI to define more complex APIs:

```{literalinclude} ../serve/doc_code/key_concepts.py
:start-after: __start_fastapi_ingress__
:end-before: __end_fastapi_ingress__
:language: python
```

## What's next?
Now that you have learned the key concepts, you can dive into these guides:
- [Resource allocation](serve-resource-allocation)
- [Autoscaling guide](serve-autoscaling)
- [Configuring HTTP logic and integrating with FastAPI](http-guide)
- [Development workflow for Serve applications](serve-dev-workflow)
- [Composing deployments to perform model composition](serve-model-composition)
