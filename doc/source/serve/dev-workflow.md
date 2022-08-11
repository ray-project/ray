# Sample Development Workflow

This page describes the recommended workflow for developing Ray Serve applications. If you're ready to go to production, jump to the [Production Guide](production.md) section.

## Local Development using `serve.run`

You can use `serve.run` in a Python script to run and test your application locally, using a handle to send requests rather than HTTP.

Benefits:

- Self-contained Python is convenient for writing local integration tests.
- No need to deploy to a cloud provider or manage infrastructure.

Drawbacks:

- Doesn't test HTTP endpoints.
- Can't use GPUs if your local machine doesn't have them.

Let's see a simple example.

```{literalinclude} ../serve/doc_code/local_dev.py
:start-after: __local_dev_start__
:end-before: __local_dev_end__
:language: python
```

We can add the testing code below:

```{literalinclude} ../serve/doc_code/local_dev.py
:start-after: __local_dev_handle_start__
:end-before: __local_dev_handle_end__
:language: python
```

This test code could be in the same file or a different file.  Before moving to the next section, remove or comment out the local testing code above if you added it in the same file.

## Local Development with HTTP requests

You can use the `serve run` CLI command to run and test your application locally using HTTP to send requests (similar to how you might use the `uvicorn` command if you're familiar with [Uvicorn](https://www.uvicorn.org/)).

Recall our example above:

```{literalinclude} ../serve/doc_code/local_dev.py
:start-after: __local_dev_start__
:end-before: __local_dev_end__
:language: python
```

Now run the following command in your terminal:

```bash
serve run local_dev:HelloDeployment
```

This will output something similar to the following:

```console
2022-08-11 11:31:47,692 INFO scripts.py:294 -- Deploying from import path: "local_dev:graph".
2022-08-11 11:31:50,372 INFO worker.py:1481 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265.
(ServeController pid=9865) INFO 2022-08-11 11:31:54,039 controller 9865 http_state.py:129 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' on node 'dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' listening on '127.0.0.1:8000'
(ServeController pid=9865) INFO 2022-08-11 11:31:55,373 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'Doubler'.
(ServeController pid=9865) INFO 2022-08-11 11:31:55,389 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'HelloDeployment'.
(HTTPProxyActor pid=9872) INFO:     Started server process [9872]
2022-08-11 11:31:57,383 SUCC scripts.py:315 -- Deployed successfully.
```

The `serve run` command blocks the terminal and can be canceled with Ctrl-C.

Now that Serve is running, we can pass in HTTP requests to the application and see the output.
For simplicity, we'll just use the `curl` command to send requests from another terminal.

```bash
curl -X PUT "http://localhost:8000/?name=Ray"   
```

This will output:

```console
Hello, Ray! Hello, Ray!
```

After you're done testing, you can shut down Ray Serve with Ctrl-C:

```console
^C2022-08-11 11:47:19,829       INFO scripts.py:323 -- Got KeyboardInterrupt, shutting down...
(ServeController pid=9865) INFO 2022-08-11 11:47:19,926 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'Doubler'.
(ServeController pid=9865) INFO 2022-08-11 11:47:19,929 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'HelloDeployment'.
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
curl -X PUT http://<cluster-ip-address>:8000/?name=Ray
# Hello, Ray! Hello, Ray!
```

For more complex runtime environments, you can specify the `runtime_env` via a YAML file; see [serve run](serve_cli.md#serve-run) for details.

:::{tip}
If you need to upload local modules that reside in a directory that's not a subdirectory of your `working_dir`, use the `"py_modules"` field of `runtime_env`; e.g. `--runtime-env-json='{"working_dir": "/dir1", "py_modules": ["/dir2/my_module"]}'`.
:::

:::{tip}
If you're only using the `working_dir` field, you can use a simpler command:

```bash
serve run  --address=ray://<cluster-ip-address>:10001 --working_dir="./project/src" local_dev:HelloDeployment
```

:::

A common pattern is to use the root directory of your project as the `working_dir` of your runtime environment when testing on a remote cluster.

## What's Next?

Once you are ready to deploy to production, see the [Production Guide](production.md).
