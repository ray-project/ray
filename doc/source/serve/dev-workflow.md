% TODO(architkulkarni): Add in the appropriate sidebars and table of contents

# Sample Development Workflow

This page describes the recommended development workflow for Ray Serve applications when iterating in development. Once you're ready to go to production, you can jump to the [Production Guide](production.md) section.

## Local Development using Python handles

You can use `serve.run` in a Python script to run and test your application locally, using a handle to send requests rather than HTTP.

Benefits:

- Self-contained Python is convenient for writing local integration tests.
- No need to deploy to a cloud provider or manage infrastructure.

Drawbacks:

- Doesn't test HTTP endpoints.
- Can't use GPUs if your local machine doesn't have them.

Let's see a simple example.

```python
# Filename: local_dev.py
from ray import serve
from ray.dag.input_node import InputNode
import starlette.requests

@serve.deployment
def double(s: str):
    return s + " " + s

@serve.deployment
class HelloDeployment:
    def __call__(self, request: starlette.requests.Request):
        return "Hello, {}!".format(request.params["name"])

hello_deployment = HelloDeployment.bind()
with InputNode() as http_request:
    hello_output = hello_deployment.bind(http_request)
    double_output = double.bind(hello_output)

ref = double_output.execute("Ray")
result = ray.get(ref)
assert result = "Hello, Ray! Hello, Ray!"
```

Here we used the `execute` method to directly send a Python object to the deployment, rather than an HTTP request.

% TODO: We didn't use serve.run() like we said at the begining; should we?


## Local Development with HTTP requests

You can use the `serve run` CLI command to run and test your application locally using HTTP to send requests (similar to how you might use `uvicorn run` if you're familiar with Uvicorn):

```bash
serve run local_dev:HelloDeployment
```

This command will block the terminal, and can be canceled with Ctrl-C.

Now that Serve is running, we can pass in HTTP requests to the application and see the output.
For simplicity, we'll just use the `curl` command to send requests from another terminal.

```bash
curl -X GET http://localhost:8000/hello?name=Ray
# Hello, Ray! Hello, Ray!
```



## Testing on a remote cluster

When making the transition from your local machine to a remote cluster, you'll need to make sure your cluster has a similar environment to your local machine--files, environment variables, and Python packages, for example.  During development, you can use {ref}`Runtime Environments<runtime-environments>` to manage this in a flexible way.

See [Ray Client](ray-client-under-construction) for more information on the Ray address specified here by the `--address` parameter.

Let's see a simple example.

```bash
serve run  --address=ray://<cluster-ip-address>:10001 --runtime-env-json='{"env_vars": {"MY_ENV_VAR": "my-value"}, "working_dir": "./project/src", "pip": ["requests", "chess"]}' local_dev:HelloDeployment
```

This will connect to the remote cluster via Ray Client and run your serve application.  Here, the directory specified by `working_dir` should contain `local_dev.py`.

Once this is up and running, we can send requests to the application and see the output.

```bash
curl -X GET http://<cluster-ip-address>:8000/hello?name=Ray
# Hello, Ray! Hello, Ray!
```

For more complex runtime environments, you can pass in a YAML file; see [serve run](serve_cli.md#serve-run) for details.

:::{tip}
If you need to upload local modules that reside in a directory that's not a subdirectory of your `working_dir`, use the `"py_modules"` field of `runtime_env`; e.g. `--runtime-env-json='{"working_dir": "/dir1", "py_modules": ["/dir2/my_module"]}'`.
:::

:::{tip}
If you're only using the `working_dir` field, you can use a simpler command:

```bash
serve run  --address=ray://<cluster-ip-address>:10001 --working_dir="./project/src" local_dev:HelloDeployment
```

:::


Or for a runtime environment using fields other than `working_dir`:

:::{tip}
You can also specify runtime environments on a per-deployment basis; see TODO link for details.
:::

% TODO: Can you actually upload local files to a remote cluster using `serve run --runtime-env`? In other words can you use a Ray Client address as the `--address` parameter, and Serve will wait for the files to be downloaded to the cluster before running the deployment?

A common pattern is to use the root directory of your project as the `working_dir` of your runtime environment when testing on a remote cluster.

## What's Next?

Once you are ready to deploy to production, see the [Production Guide](production.md).
