(serve-model-composition)=

# Deploy Compositions of Models

With this guide, you can:

* Compose multiple {ref}`deployments <serve-key-concepts-deployment>` containing ML models or business logic into a single {ref}`application <serve-key-concepts-application>`
* Independently scale and configure each of your ML models and business logic steps

:::{note}
{mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` is now the default handle API.
You can continue using the legacy `RayServeHandle` and `RayServeSyncHandle` APIs using `handle.options(use_new_handle_api=False)` or `export RAY_SERVE_ENABLE_NEW_HANDLE_API=0`, but this support will be removed in a future version.
:::

## Compose deployments using DeploymentHandles

When building an application, you can `.bind()` multiple deployments and pass them to each other's constructors.
At runtime, inside the deployment code Ray Serve substitutes the bound deployments with 
{ref}`DeploymentHandles <serve-key-concepts-deployment-handle>` that you can use to call methods of other deployments.
This capability lets you divide your application's steps, such as preprocessing, model inference, and post-processing, into independent deployments that you can independently scale and configure.

Use {mod}`handle.remote <ray.serve.handle.DeploymentHandle.remote>` to send requests to a deployment.
These requests can contain ordinary Python args and kwargs, which DeploymentHandles can pass  directly to the method.
The method call returns a {mod}`DeploymentResponse <ray.serve.handle.DeploymentResponse>` that represents a future to the output.
You can `await` the response to retrieve its result or pass it to another downstream {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` call.

(serve-model-composition-deployment-handles)=
## Basic DeploymentHandle example

This example has two deployments:

```{literalinclude} doc_code/model_composition/language_example.py
:start-after: __hello_start__
:end-before: __hello_end__
:language: python
:linenos: true
```

In line 44, the `LanguageClassifier` deployment takes in the `spanish_responder` and `french_responder` as constructor arguments. At runtime, Ray Serve converts these arguments into `DeploymentHandles`. `LanguageClassifier` can then call the `spanish_responder` and `french_responder`'s deployment methods using this handle.

For example, the `LanguageClassifier`'s `__call__` method uses the HTTP request's values to decide whether to respond in Spanish or French. It then forwards the request's name to the `spanish_responder` or the `french_responder` on lines 20 and 23 using the `DeploymentHandle`s. The format of the calls is as follows:

```python
response: DeploymentResponse = self.spanish_responder.say_hello.remote(name)
```

This call has a few parts:
* `self.spanish_responder` is the `SpanishResponder` handle taken in through the constructor.
* `say_hello` is the `SpanishResponder` method to invoke.
* `remote` indicates that this is a `DeploymentHandle` call to another deployment.
* `name` is the argument for `say_hello`. You can pass any number of arguments or keyword arguments here.

This call returns a `DeploymentResponse` object, which is a reference to the result, rather than the result itself.
This pattern allows the call to execute asynchronously.
To get the actual result, `await` the response.
`await` blocks until the asynchronous call executes and then returns the result.
In this example, line 23 calls `await response` and returns the resulting string.

(serve-model-composition-await-warning)=
:::{warning}
You can use the `response.result()` method to get the return value of remote `DeploymentHandle` calls.
However, avoid calling `.result()` from inside a deployment because it blocks the deployment from executing any other code until the remote method call finishes.
Using `await` lets the deployment process other requests while waiting for the remote method call to finish.
You should use `await` instead of `.result()` inside deployments.
:::

You can copy the preceding `hello.py` script and run it with `serve run`. Make sure to run the command from a directory containing `hello.py`, so it can locate the script:

```console
$ serve run hello:language_classifier
```

You can use this client script to interact with the example:

```{literalinclude} doc_code/model_composition/language_example.py
:start-after: __hello_client_start__
:end-before: __hello_client_end__
:language: python
```

While the `serve run` command is running, open a separate terminal window and run the script:

```console
$ python hello_client.py

Hola Dora
```

:::{note}
Composition lets you break apart your application and independently scale each part. For instance, suppose this `LanguageClassifier` application's requests were 75% Spanish and 25% French. You could scale your `SpanishResponder` to have 3 replicas and your `FrenchResponder` to have 1 replica, so you can meet your workload's demand. This flexibility also applies to reserving resources like CPUs and GPUs, as well as any other configurations you can set for each deployment.

With composition, you can avoid application-level bottlenecks when serving models and business logic steps that use different types and amounts of resources.
:::

## Chaining DeploymentHandle calls

Ray Serve can directly pass the `DeploymentResponse` object that a `DeploymentHandle` returns, to another `DeploymentHandle` call to chain together multiple stages of a pipeline.
You don't need to `await` the first response, Ray Serve
manages the `await` behavior under the hood. When the first call finishes, Ray Serve passes the output of the first call, instead of the `DeploymentResponse` object, directly to the second call.

For example, the code sample below defines three deployments in an application:

- An `Adder` deployment that increments a value by its configured increment.
- A `Multiplier` deployment that multiplies a value by its configured multiple.
- An `Ingress` deployment that chains calls to the adder and multiplier together and returns the final response.

Note how the response from the `Adder` handle passes directly to the `Multiplier` handle, but inside the multiplier, the input argument resolves to the output of the `Adder` call.

```{literalinclude} doc_code/model_composition/chaining_example.py
:start-after: __chaining_example_start__
:end-before: __chaining_example_end__
:language: python
```

## Streaming DeploymentHandle calls

You can also use `DeploymentHandles` to make streaming method calls that return multiple outputs.
To make a streaming call, the method must be a generator and you must set `handle.options(stream=True)`.
Then, the handle call returns a {mod}`DeploymentResponseGenerator <ray.serve.handle.DeploymentResponseGenerator>` instead of a unary `DeploymentResponse`.
You can use `DeploymentResponseGenerators` as a sync or async generator, like in an `async for` code block.
Similar to `DeploymentResponse.result()`, avoid using a `DeploymentResponseGenerator` as a sync generator within a deployment, as that blocks other requests from executing concurrently on that replica.
Note that you can't pass `DeploymentResponseGenerators` to other handle calls.

Example:

```{literalinclude} doc_code/model_composition/streaming_example.py
:start-after: __streaming_example_start__
:end-before: __streaming_example_end__
:language: python
```

## Advanced: Pass a DeploymentResponse "by reference"

By default, when you pass a `DeploymentResponse` to another `DeploymentHandle` call, Ray Serve passes the result of the `DeploymentResponse` directly to the downstream method once it's ready.
However, in some cases you might want to start executing the downstream call before the result is ready. For example, to do some preprocessing or fetch a file from remote storage.
To accomplish this behavior, pass the `DeploymentResponse` "by reference" by embedding it in another Python object, such as a list or dictionary.
When you pass responses by reference, Ray Serve replaces them with Ray `ObjectRef`s instead of the resulting value and they can start executing before the result is ready.

The example below has two deployments: a preprocessor and a downstream model that takes the output of the preprocessor.
The downstream model has two methods:

- `pass_by_value` takes the output of the preprocessor "by value," so it doesn't execute until the preprocessor finishes.
- `pass_by_reference` takes the output "by reference," so it gets an `ObjectRef` and executes eagerly.

```{literalinclude} doc_code/model_composition/response_by_reference_example.py
:start-after: __response_by_reference_example_start__
:end-before: __response_by_reference_example_end__
:language: python
```

## Advanced: Convert a DeploymentResponse to a Ray ObjectRef

Under the hood, each `DeploymentResponse` corresponds to a Ray `ObjectRef`, or an `ObjectRefGenerator` for streaming calls.
To compose `DeploymentHandle` calls with Ray Actors or Tasks, you may want to resolve the response to its `ObjectRef`.
For this purpose, you can use the {mod}`DeploymentResponse._to_object_ref <ray.serve.handle.DeploymentResponse>` and {mod}`DeploymentResponse._to_object_ref_sync <ray.serve.handle.DeploymentResponse>` developer APIs.

Example:

```{literalinclude} doc_code/model_composition/response_to_object_ref_example.py
:start-after: __response_to_object_ref_example_start__
:end-before: __response_to_object_ref_example_end__
:language: python
```
