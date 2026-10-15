---
myst:
  html_meta:
    description: "Pass arguments to Serve applications with an application builder function, supplied through serve run or a config file and typed with Pydantic."
---

(serve-app-builder-guide)=
# Pass arguments to applications

This page describes how to pass arguments to your applications with an application builder function.

(defining-an-application-builder)=
## Define an application builder

An application often has parameters that you want to change in development or production. For example, you might have a path to trained model weights and want to test a newly trained model. In Ray Serve, you typically pass these parameters to the constructors of your deployments with `.bind()`. This pattern configures deployments in ordinary Python code, but you have to modify the code whenever a parameter changes.

To pass arguments without changing the code, define an *application builder* function. It takes a dictionary of arguments or a [Pydantic object](typed-app-builders), and returns the built application to run.

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_untyped_builder__
:end-before: __end_untyped_builder__
:language: python
```

You can use this application builder function as the import path in the `serve run` CLI command or in the config file, as the following sections show. To avoid writing code to handle type conversions and missing arguments, use a [Pydantic object](typed-app-builders) instead.

(passing-arguments-via-serve-run)=
### Pass arguments through `serve run`

Pass arguments to the application builder from `serve run` using the following syntax:

```bash
$ serve run hello:app_builder key1=val1 key2=val2
```

The application builder receives the arguments as a dictionary, in this case `{"key1": "val1", "key2": "val2"}`. For example, with the code saved in `hello.py`, run the following command to pass a new message to the `HelloWorld` application defined earlier:

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

The deployment constructor prints the "Hello from CLI" message.

(passing-arguments-via-config-file)=
### Pass arguments through a config file

Pass arguments to the application builder in the config file's `args` field:

```yaml
applications:
  - name: MyApp
    import_path: hello:app_builder
    args:
      message: "Hello from config"
```

For example, with the code saved in `hello.py` and the config saved in `config.yaml`, run the following command to pass a new message to the `HelloWorld` application defined earlier:

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

The deployment constructor prints the "Hello from config" message.

(typed-app-builders)=
(typing-arguments-with-pydantic)=
### Type arguments with Pydantic

:::{warning}
**Pydantic v1 deprecation notice:** Pydantic v1 is deprecated and Ray will drop support for it in version 2.56. If you're using Pydantic v1, upgrade to Pydantic v2 by running `pip install -U pydantic`. See [GitHub issue #58876](https://github.com/ray-project/ray/issues/58876) for details.
:::

To avoid parsing and validating the arguments by hand, define a [Pydantic model](https://pydantic-docs.helpmanual.io/usage/models/) and use it as the type of your application builder function's single input parameter. The parameter must have a type annotation. You pass arguments the same way, but Ray Serve constructs the Pydantic model from the resulting dictionary with `model.parse_obj(args_dict)`.

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_typed_builder__
:end-before: __end_typed_builder__
:language: python
```

```bash
% serve run hello:typed_app_builder message="Hello from CLI"
2023-05-16 10:47:31,641 INFO scripts.py:404 -- Running import path: 'hello:typed_app_builder'.
2023-05-16 10:47:33,344 INFO worker.py:1615 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ServeController pid=56826) INFO 2023-05-16 10:47:35,115 controller 56826 deployment_state.py:1244 - Deploying new version of deployment default_HelloWorld.
(ServeController pid=56826) INFO 2023-05-16 10:47:35,141 controller 56826 deployment_state.py:1483 - Adding 1 replica to deployment default_HelloWorld.
(ProxyActor pid=56828) INFO:     Started server process [56828]
(ServeReplica:default_HelloWorld pid=56830) Message: Hello from CLI
2023-05-16 10:47:36,131 SUCC scripts.py:424 -- Deployed Serve app successfully.
```

## Common patterns

The following sections show common ways to use application builders.

(multiple-parametrized-applications-using-the-same-builder)=
### Run multiple parametrized applications with the same builder

You can use application builders to run multiple applications with the same code but different parameters. For example, multiple applications might share preprocessing and HTTP handling logic but use many different trained model weights. The following config passes different arguments to the same application builder `import_path` to define three applications:

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

(configuring-multiple-composed-deployments)=
### Configure multiple composed deployments

You can use the arguments passed to an application builder to configure multiple deployments in a single application. For example, the following model composition application takes weights for two different models:

```{literalinclude} ../doc_code/app_builder.py
:start-after: __begin_composed_builder__
:end-before: __end_composed_builder__
:language: python
```
