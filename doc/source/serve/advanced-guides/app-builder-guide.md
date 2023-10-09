(serve-app-builder-guide)=
# Pass Arguments to Applications

This section describes how to pass arguments to your applications using an application builder function.

## Defining an application builder

When writing an application, there are often parameters that you want to be able to easily change in development or production.
For example, you might have a path to trained model weights and want to test out a newly trained model.
In Ray Serve, these parameters are typically passed to the constructor of your deployments using `.bind()`.
This pattern allows you to be configure deployments using ordinary Python code but it requires modifying the code anytime one of the parameters needs to change.

To pass arguments without changing the code, define an "application builder" function that takes an arguments dictionary (or [Pydantic object](typed-app-builders)) and returns the built application to be run.

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_untyped_builder__
:end-before: __end_untyped_builder__
:language: python
```

You can use this application builder function as the import path in the `serve run` CLI command or the config file (as shown below).
To avoid writing code to handle type conversions and missing arguments, use a [Pydantic object](typed-app-builders) instead.

### Passing arguments via `serve run`

Pass arguments to the application builder from `serve run` using the following syntax:

```bash
$ serve run hello:app_builder key1=val1 key2=val2
```

The arguments are passed to the application builder as a dictionary, in this case `{"key1": "val1", "key2": "val2"}`.
For example, to pass a new message to the `HelloWorld` app defined above (with the code saved in `hello.py`):

```bash
% serve run hello:app_builder message="Hello from CLI"
2023-05-16 10:47:31,641 INFO scripts.py:404 -- Running import path: 'hello:app_builder'.
2023-05-16 10:47:33,344 INFO worker.py:1615 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ServeController pid=56826) INFO 2023-05-16 10:47:35,115 controller 56826 deployment_state.py:1244 - Deploying new version of deployment default_HelloWorld.
(ServeController pid=56826) INFO 2023-05-16 10:47:35,141 controller 56826 deployment_state.py:1483 - Adding 1 replica to deployment default_HelloWorld.
(ProxyActor pid=56828) INFO:     Started server process [56828]
(ServeReplica:default_HelloWorld pid=56830) Message: Hello from CLI
2023-05-16 10:47:36,131 SUCC scripts.py:424 -- Deployed Serve app successfully.
```

Notice that the "Hello from CLI" message is printed from within the deployment constructor.

### Passing arguments via config file

Pass arguments to the application builder in the config file's `args` field:

```yaml
applications:
  - name: MyApp
    import_path: hello:app_builder
    args:
      message: "Hello from config"
```

For example, to pass a new message to the `HelloWorld` app defined above (with the code saved in `hello.py` and the config saved in `config.yaml`):

```bash
% serve run config.yaml
2023-05-16 10:49:25,247 INFO scripts.py:351 -- Running config file: 'config.yaml'.
2023-05-16 10:49:26,949 INFO worker.py:1615 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
2023-05-16 10:49:28,678 SUCC scripts.py:419 -- Submitted deploy config successfully.
(ServeController pid=57109) INFO 2023-05-16 10:49:28,676 controller 57109 controller.py:559 - Building application 'MyApp'.
(ProxyActor pid=57111) INFO:     Started server process [57111]
(ServeController pid=57109) INFO 2023-05-16 10:49:28,940 controller 57109 application_state.py:202 - Built application 'MyApp' successfully.
(ServeController pid=57109) INFO 2023-05-16 10:49:28,942 controller 57109 deployment_state.py:1244 - Deploying new version of deployment MyApp_HelloWorld.
(ServeController pid=57109) INFO 2023-05-16 10:49:29,016 controller 57109 deployment_state.py:1483 - Adding 1 replica to deployment MyApp_HelloWorld.
(ServeReplica:MyApp_HelloWorld pid=57113) Message: Hello from config
```

Notice that the "Hello from config" message is printed from within the deployment constructor.

(typed-app-builders)=
### Typing arguments with Pydantic

To avoid writing logic to parse and validate the arguments by hand, define a [Pydantic model](https://pydantic-docs.helpmanual.io/usage/models/) as the single input parameter's type to your application builder function (the parameter must be type annotated).
Arguments are passed the same way, but the resulting dictionary is used to construct the Pydantic model using `model.parse_obj(args_dict)`.

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_typed_builder__
:end-before: __end_typed_builder__
:language: python
```

```bash
% serve run hello:app_builder message="Hello from CLI"
2023-05-16 10:47:31,641 INFO scripts.py:404 -- Running import path: 'hello:app_builder'.
2023-05-16 10:47:33,344 INFO worker.py:1615 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ServeController pid=56826) INFO 2023-05-16 10:47:35,115 controller 56826 deployment_state.py:1244 - Deploying new version of deployment default_HelloWorld.
(ServeController pid=56826) INFO 2023-05-16 10:47:35,141 controller 56826 deployment_state.py:1483 - Adding 1 replica to deployment default_HelloWorld.
(ProxyActor pid=56828) INFO:     Started server process [56828]
(ServeReplica:default_HelloWorld pid=56830) Message: Hello from CLI
2023-05-16 10:47:36,131 SUCC scripts.py:424 -- Deployed Serve app successfully.
```

## Common patterns

### Multiple parametrized applications using the same builder

You can use application builders to run multiple applications with the same code but different parameters.
For example, multiple applications may share preprocessing and HTTP handling logic but use many different trained model weights.
The same application builder `import_path` can take different arguments to define multiple applications as follows:

```yaml
applications:
  - name: Model1
    import_path: my_module:my_model_code
    args:
      model_uri: s3://my_bucket/model_1
  - name: Model2
    import_path: my_module:my_model_code
    args:
      model_uri: s3://my_bucket/model_2
  - name: Model3
    import_path: my_module:my_model_code
    args:
      model_uri: s3://my_bucket/model_3
```

### Configuring multiple composed deployments

You can use the arguments passed to an application builder to configure multiple deployments in a single application.
For example a model composition application might take weights to two different models as follows:

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_composed_builder__
:end-before: __end_composed_builder__
:language: python
```
