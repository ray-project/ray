(serve-model-composition)=

# Deploy Compositions of Models

This section helps you:

* compose multiple deployments containing ML models or business logic into a single application
* independently scale and configure each of your ML models and business logic steps

:::{note}
Ray 2.7 introduces a new {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` API that will replace the existing `RayServeHandle` and `RayServeSyncHandle` APIs.
Existing code will continue to work, but you are encouraged to opt-in to the new API to avoid breakages in the future.
To opt into the new API, you can either use `handle.options(use_new_handle_api=True)` on each handle or set it globally via environment variable: `export RAY_SERVE_ENABLE_NEW_HANDLE_API=1`.
:::

## Composing Deployments using DeploymentHandles

When building an application, you can `.bind()` multiple deployments and pass them to each other's constructors.
At runtime, inside the deployment code the bound deployments are substituted with {mod}`DeploymentHandles <ray.serve.handle.DeploymentHandle>` that you can use to call methods of the other deployment.
This lets you divide your application's steps (such as preprocessing, model inference, and post-processing) into independent deployments that can be independently scaled and configured.

Use {mod}`handle.remote <ray.serve.handle.DeploymentHandle.remote>` to send requests to a deployment.
These requests can contain ordinary Python args and kwargs that are passed directly to the method.
The method call returns a {mod}`DeploymentResponse <ray.serve.handle.DeploymentResponse>` that represents a future to the output.
The response can be `await`ed to retrieve its result or passed to another downstream `DeploymentHandle` call.

(serve-model-composition-deployment-handles)=
### Basic DeploymentHandle Example

Here's an example:

```{literalinclude} doc_code/model_composition/language_example.py
:start-after: __hello_start__
:end-before: __hello_end__
:language: python
:linenos: true
```

In line 40, the `LanguageClassifier` deployment takes in the `spanish_responder` and `french_responder` as constructor arguments. At runtime, these arguments are converted into `DeploymentHandles`. `LanguageClassifier` can then call the `spanish_responder` and `french_responder`'s deployment methods using this handle.

For example, the `LanguageClassifier`'s `__call__` method uses the HTTP request's values to decide whether to respond in Spanish or French. It then forwards the request's name to the `spanish_responder` or the `french_responder` on lines 17 and 19 using the `DeploymentHandle`s. The format of the calls is as follows:

```python
response: DeploymentResponse = self.spanish_responder.say_hello.remote(name)
```

This call has a few parts:
* `self.spanish_responder` is the `SpanishResponder` handle taken in through the constructor.
* `say_hello` is the `SpanishResponder` method to invoke.
* `remote` indicates that this is a `DeploymentHandle` call to another deployment.
* `name` is the argument for `say_hello`. You can pass any number of arguments or keyword arguments here.

This call returns a `DeploymentResponse`, which is a reference to the result, not the result itself.
This pattern allows the call to execute asynchronously.
To get the actual result, `await` the response.
`await` blocks until the asynchronous call executes and then returns the result.
In this example, line 23 calls `await response` and returns the resulting string.

(serve-model-composition-await-warning)=
:::{warning}
You can use the `response.result()` method to get the return value of remote `DeploymentHandle` calls.
However, calling `.result()` from inside a deployment is an antipattern because it blocks the deployment from executing any other code until the remote method call is finished.
Using `await` lets the deployment process other requests while waiting for the remote method call to finish.
You should use `await` instead of `.result()` inside deployments.
:::

You can copy the `hello.py` script above and run it with `serve run`. Make sure to run the command from a directory containing `hello.py`, so it can locate the script:

```console
$ serve run hello:language_classifier
```

You can use this client script to interact with the example:

```{literalinclude} doc_code/model_composition/language_example.py
:start-after: __hello_client_start__
:end-before: __hello_client_end__
:language: python
```

While the `serve run` command is running, open a separate terminal window and run this script:

```console
$ python hello_client.py

Hola Dora
```

:::{note}
Composition lets you break apart your application and independently scale each part. For instance, suppose this `LanguageClassifier` application's requests were 75% Spanish and 25% French. You could scale your `SpanishResponder` to have 3 replicas and your `FrenchResponder` to have 1 replica, so you could meet your workload's demand. This flexibility also applies to reserving resources like CPUs and GPUs, as well as any other configurations you can set for each deployment.

With composition, you can avoid application-level bottlenecks when serving models and business logic steps that use different types and amounts of resources.
:::

### Chaining DeploymentHandle Calls

The `DeploymentResponse` object returned from a `DeploymentHandle` can be directly passed to another `DeploymentHandle` call to chain together multiple stages of a pipeline.
There is no need to `await` the first response; when the first call finishes, its output is passed to the second call and substituted for the `DeploymentResponse` that was passed in.

For example, the code sample below defines three deployments in an application:

- An `Adder` deployment that increments the passed value by its configured increment.
- A `Multiplier` deployment that multiplies the passed value by its configured multiple.
- An `Ingress` deployment that chains calls to the adder and multiplier together and returns the final response.

Note how the response from the `Adder` handle is passed directly to the `Multiplier` handle, but inside the multiplier the input argument is resolved to the output of the `Adder` call.

```{literalinclude} doc_code/model_composition/chaining_example.py
:start-after: __chaining_example_start__
:end-before: __chaining_example_end__
:language: python
```

### Streaming DeploymentHandle Calls

`DeploymentHandles` can also be used to make streaming method calls that return multiple outputs.
To make a streaming call, the method being called must be a generator and `handle.options(stream=True)` must be set.
Then, the handle call will return a {mod}`DeploymentResponseGenerator <ray.serve.handle.DeploymentResponseGenerator>` instead of a unary `DeploymentResponse`.
`DeploymentResponseGenerators` can be used as a sync or async generator (e.g., in an `async for` code block).
Similar to `DeploymentResponse.result()`, it's an anti-pattern to use a `DeploymentResponseGenerator` as a sync generator within a deployment, as that will block other requests from executing concurrently on that replica.
`DeploymentResponseGenerators` cannot currently be passed to other handle calls.

Example:

```{literalinclude} doc_code/model_composition/streaming_example.py
:start-after: __streaming_example_start__
:end-before: __streaming_example_end__
:language: python
```

### Advanced: Pass a DeploymentResponse "by reference"

By default, when you pass a `DeploymentResponse` to another `DeploymentHandle` call, the downstream method will be passed the result of the `DeploymentResponse` directly once it's ready.
However, in some cases you might want to start executing the downstream call before the result is ready (for example, to do some preprocessing or fetch a file from remote storage).
To accomplish this, pass the `DeploymentResponse` "by reference" by embedding it in another Python object (such as a list or dictionary).
When responses are passed by reference, they'll be replaced with Ray `ObjectRef`s instead of the resulting value and can start executing before the result is ready.

In the below example, there are two deployments: a preprocessor and a downstream model that takes the output of the preprocessor.
The downstream model has two methods:

- `pass_by_value` takes the output of the preprocessor "by value," so it doesn't execute until the preprocessor is done.
- `pass_by_reference` takes the output "by reference," so it gets an `ObjectRef` and executes eagerly.

```{literalinclude} doc_code/model_composition/response_by_reference_example.py
:start-after: __response_by_reference_example_start__
:end-before: __response_by_reference_example_end__
:language: python
```

### Advanced: Convert a DeploymentResponse to a Ray ObjectRef

Under the hood, each `DeploymentResponse` corresponds to a Ray `ObjectRef` (or a `StreamingObjectRefGenerator` for streaming calls).
In order to compose `DeploymentHandle` calls with Ray Actors or Tasks, you may want to resolve the response to its `ObjectRef`.
For this purpose, you can use the {mod}`DeploymentResponse._to_object_ref <ray.serve.handle.DeploymentResponse._to_object_ref>` and {mod}`DeploymentResponse._to_object_ref <ray.serve.handle.DeploymentResponse._to_object_ref_gen>` developer APIs.

Example:

```{literalinclude} doc_code/model_composition/response_to_object_ref_example.py
:start-after: __response_to_object_ref_example_start__
:end-before: __response_to_object_ref_example_end__
:language: python
```
