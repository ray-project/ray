(serve-dev-workflow)=

# Development Workflow

This page describes the recommended workflow for developing Ray Serve applications. If you're ready to go to production, jump to the [Production Guide](serve-in-production) section.

## Local Development using `serve.run`

You can use `serve.run` in a Python script to run and test your application locally, using a handle to send requests programmatically rather than over HTTP.

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

We can add the code below to deploy and test Serve locally.

```{literalinclude} ../serve/doc_code/local_dev.py
:start-after: __local_dev_handle_start__
:end-before: __local_dev_handle_end__
:language: python
```

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
serve run local_dev:graph
# 2022-08-11 11:31:47,692 INFO scripts.py:294 -- Deploying from import path: "local_dev:graph".
# 2022-08-11 11:31:50,372 INFO worker.py:1481 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265.
# (ServeController pid=9865) INFO 2022-08-11 11:31:54,039 controller 9865 http_state.py:129 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' on node 'dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' listening on '127.0.0.1:8000'
# (ServeController pid=9865) INFO 2022-08-11 11:31:55,373 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'Doubler'.
# (ServeController pid=9865) INFO 2022-08-11 11:31:55,389 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'HelloDeployment'.
# (HTTPProxyActor pid=9872) INFO:     Started server process [9872]
# 2022-08-11 11:31:57,383 SUCC scripts.py:315 -- Deployed successfully.
```

The `serve run` command blocks the terminal and can be canceled with Ctrl-C. Typically, `serve run` should not be run simultaneously from multiple terminals, unless each `serve run` is targeting a separate running Ray cluster.

Now that Serve is running, we can send HTTP requests to the application.
For simplicity, we'll just use the `curl` command to send requests from another terminal.

```bash
curl -X PUT "http://localhost:8000/?name=Ray"
# Hello, Ray! Hello, Ray!
```

After you're done testing, you can shut down Ray Serve by interrupting the `serve run` command (e.g., with Ctrl-C):

```console
^C2022-08-11 11:47:19,829       INFO scripts.py:323 -- Got KeyboardInterrupt, shutting down...
(ServeController pid=9865) INFO 2022-08-11 11:47:19,926 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'Doubler'.
(ServeController pid=9865) INFO 2022-08-11 11:47:19,929 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'HelloDeployment'.
```

Note that rerunning `serve run` will redeploy all deployments. To prevent redeploying those deployments whose code hasn't changed, you can use `serve deploy`; see the [Production Guide](serve-in-production) for details.

## Testing on a remote cluster

To test on a remote cluster, you'll use `serve run` again, but this time you'll pass in an `--address` argument to specify the address of the Ray cluster to connect to.  For remote clusters, this address has the form `ray://<head-node-ip-address>:10001`; see [Ray Client](ray-client-ref) for more information.

When making the transition from your local machine to a remote cluster, you'll need to make sure your cluster has a similar environment to your local machine--files, environment variables, and Python packages, for example.  

Let's see a simple example that just packages the code. Run the following command on your local machine, with your remote cluster head node IP address substituted for `<head-node-ip-address>` in the command:

```bash
serve run  --address=ray://<head-node-ip-address>:10001 --working-dir="./project/src" local_dev:graph
```

This will connect to the remote cluster via Ray Client, upload the `working_dir` directory, and run your serve application.  Here, the local directory specified by `working_dir` must contain `local_dev.py` so that it can be uploaded to the cluster and imported by Ray Serve.

Once this is up and running, we can send requests to the application:

```bash
curl -X PUT http://<head-node-ip-address>:8000/?name=Ray
# Hello, Ray! Hello, Ray!
```

For more complex dependencies, including files outside the working directory, environment variables, and Python packages, you can use {ref}`Runtime Environments<runtime-environments>`. Here is an example using the --runtime-env-json argument:

```bash
serve run  --address=ray://<head-node-ip-address>:10001 --runtime-env-json='{"env_vars": {"MY_ENV_VAR": "my-value"}, "working_dir": "./project/src", "pip": ["requests", "chess"]}' local_dev:graph
```

You can also specify the `runtime_env` via a YAML file; see [serve run](serve_cli.html#serve-run) for details.

## What's Next?

View details about your Serve application in the [Ray dashboard](dash-serve-view).
Once you are ready to deploy to production, see the [Production Guide](serve-in-production).
