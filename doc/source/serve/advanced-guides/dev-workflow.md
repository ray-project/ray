---
myst:
  html_meta:
    description: "Recommended Ray Serve development loop: iterate locally with serve.run, send HTTP requests, use local testing mode, then test on a cluster."
---

(serve-dev-workflow)=

# Development workflow

This page describes the recommended workflow for developing Ray Serve applications. If you're ready to go to production, see the [Production Guide](serve-in-production).

(local-development-using-serve-run)=
## Develop locally with `serve.run`

You can call `serve.run` in a Python script to run and test your application locally, then send requests programmatically through a handle rather than over HTTP.

This approach has the following benefits:

- Self-contained Python is convenient for writing local integration tests.
- You don't need to deploy to a cloud provider or manage infrastructure.

It also has the following drawbacks:

- It doesn't test HTTP endpoints.
- You can't use GPUs if your local machine doesn't have them.

The following example defines an application with two deployments:

```{literalinclude} ../doc_code/local_dev.py
:start-after: __local_dev_start__
:end-before: __local_dev_end__
:language: python
```

Add the following code to deploy and test Serve locally:

```{literalinclude} ../doc_code/local_dev.py
:start-after: __local_dev_handle_start__
:end-before: __local_dev_handle_end__
:language: python
```

(local-development-with-http-requests)=
## Develop locally with HTTP requests

You can use the `serve run` CLI command to run and test your application locally and send requests to it over HTTP. If you're familiar with [Uvicorn](https://uvicorn.dev/), you use `serve run` much as you'd use the `uvicorn` command.

Recall the preceding example:

```{literalinclude} ../doc_code/local_dev.py
:start-after: __local_dev_start__
:end-before: __local_dev_end__
:language: python
```

Run the following command in your terminal:

```bash
serve run local_dev:app
# 2022-08-11 11:31:47,692 INFO scripts.py:294 -- Deploying from import path: "local_dev:app".
# 2022-08-11 11:31:50,372 INFO worker.py:1481 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265.
# (ServeController pid=9865) INFO 2022-08-11 11:31:54,039 controller 9865 proxy_state.py:129 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' on node 'dff7dc5b97b4a11facaed746f02448224aa0c1fb651988ba7197e949' listening on '127.0.0.1:8000'
# (ServeController pid=9865) INFO 2022-08-11 11:31:55,373 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'Doubler'.
# (ServeController pid=9865) INFO 2022-08-11 11:31:55,389 controller 9865 deployment_state.py:1232 - Adding 1 replicas to deployment 'HelloDeployment'.
# (HTTPProxyActor pid=9872) INFO:     Started server process [9872]
# 2022-08-11 11:31:57,383 SUCC scripts.py:315 -- Deployed successfully.
```

The `serve run` command blocks the terminal until you cancel it with Ctrl-C. Typically, you shouldn't run `serve run` simultaneously from multiple terminals unless each `serve run` targets a separate running Ray cluster.

With Serve running, send HTTP requests to the application. This example uses the `curl` command to send requests from another terminal:

```bash
curl -X PUT "http://localhost:8000/?name=Ray"
# Hello, Ray! Hello, Ray!
```

When you finish testing, you can shut down Ray Serve by interrupting the `serve run` command, for example with Ctrl-C:

```console
^C2022-08-11 11:47:19,829       INFO scripts.py:323 -- Got KeyboardInterrupt, shutting down...
(ServeController pid=9865) INFO 2022-08-11 11:47:19,926 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'Doubler'.
(ServeController pid=9865) INFO 2022-08-11 11:47:19,929 controller 9865 deployment_state.py:1257 - Removing 1 replicas from deployment 'HelloDeployment'.
```

Rerunning `serve run` redeploys all deployments. To avoid redeploying the deployments whose code hasn't changed, you can use `serve deploy`. See the [Production Guide](serve-in-production) for details.

(local-testing-mode)=
### Use local testing mode

:::{note}
Local testing mode is an experimental feature.
:::

In local testing mode, Ray Serve runs your deployments locally in a single process. Use this mode to unit test and debug your application logic without the overhead of a full Ray cluster. To enable it, pass the `_local_testing_mode` flag to the `serve.run` function:

```{literalinclude} ../doc_code/local_dev.py
:start-after: __local_dev_testing_start__
:end-before: __local_dev_testing_end__
:language: python
```

This mode runs each deployment in a background thread and supports most of the same features as running on a full Ray cluster. Local testing mode doesn't support some features, such as converting `DeploymentResponses` to `ObjectRefs`. If you encounter limitations, consider filing a feature request on GitHub.

(testing-on-a-remote-cluster)=
## Test on a remote cluster

To test on a remote cluster, run `serve run` with the `--address` argument set to the address of the Ray cluster to connect to. For remote clusters, the address has the form `ray://<head-node-ip-address>:10001`. See [Ray Client](ray-client-ref) for more information.

When you move from your local machine to a remote cluster, make sure the cluster's environment is similar to your local machine's, including files, environment variables, and Python packages.

The following example packages only the code. Run this command on your local machine, replacing `<head-node-ip-address>` with the IP address of your remote cluster's head node:

```bash
serve run  --address=ray://<head-node-ip-address>:10001 --working-dir="./project/src" local_dev:app
```

This command connects to the remote cluster with the Ray Client, uploads the `working_dir` directory, and runs your Serve application. The local directory that `working_dir` specifies must contain `local_dev.py` so that the command can upload it to the cluster and Ray Serve can import it.

When the application is running, send requests to it:

```bash
curl -X PUT http://<head-node-ip-address>:8000/?name=Ray
# Hello, Ray! Hello, Ray!
```

For more complex dependencies, including files outside the working directory, environment variables, and Python packages, you can use {ref}`runtime environments<runtime-environments>`. This example uses the `--runtime-env-json` argument:

```bash
serve run  --address=ray://<head-node-ip-address>:10001 --runtime-env-json='{"env_vars": {"MY_ENV_VAR": "my-value"}, "working_dir": "./project/src", "pip": ["requests", "chess"]}' local_dev:app
```

You can also specify the `runtime_env` in a YAML file. See [`serve run`](#serve-cli) for details.

## What's next?

View details about your Serve application in the [Ray dashboard](dash-serve-view). When you're ready to deploy to production, see the [Production Guide](serve-in-production).
