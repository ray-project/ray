(serve-in-production-deploying)=

# Deploy on VM

You can deploy your Serve application to production on a Ray cluster using the Ray Serve CLI.
`serve deploy` takes in a config file path and it deploys that file to a Ray cluster over HTTP.
This could either be a local, single-node cluster as in this example or a remote, multi-node cluster started with the [Ray Cluster Launcher](cloud-vm-index).

This section should help you:

- understand how to deploy a Ray Serve config file using the  CLI.
- understand how to update your application using the CLI.
- understand how to deploy to a remote cluster started with the [Ray Cluster Launcher](cloud-vm-index).

Let's start by deploying the [config for the `FruitStand` example](fruit-config-yaml):

```console
$ ls
fruit.py
fruit_config.yaml

$ ray start --head
...

$ serve deploy fruit_config.yaml
2022-06-20 17:26:31,106	SUCC scripts.py:139 --
Sent deploy request successfully!
 * Use `serve status` to check deployments' statuses.
 * Use `serve config` to see the running app's config.
```

`ray start --head` starts a long-lived Ray cluster locally. `serve deploy fruit_config.yaml` deploys the `fruit_config.yaml` file to this local cluster. To stop your Ray cluster, you can run the CLI command `ray stop`.

The message `Sent deploy request successfully!` means:
* The Ray cluster has received your config file successfully.
* It will start a new Serve application if one hasn't already started.
* The Serve application will deploy the deployments from your deployment graph, updated with the configurations from your config file.

It does **not** mean that your Serve application, including your deployments, has already started running successfully. This happens asynchronously as the Ray cluster attempts to update itself to match the settings from your config file. See [Inspect an application](serve-in-production-inspecting) for how to get the current status.

(serve-in-production-remote-cluster)=

## Using a remote cluster

By default, `serve deploy` deploys to a cluster running locally. However, you should also use `serve deploy` whenever you want to deploy your Serve application to a remote cluster. `serve deploy` takes in an optional `--address/-a` argument where you can specify your remote Ray cluster's dashboard agent address. This address should be of the form:

```
[RAY_CLUSTER_URI]:[DASHBOARD_AGENT_PORT]
```

As an example, the address for the local cluster started by `ray start --head` is `http://127.0.0.1:52365`. We can explicitly deploy to this address using the command

```console
$ serve deploy config_file.yaml -a http://127.0.0.1:52365
```

The Ray Dashboard agent's default port is 52365. To set it to a different value, use the `--dashboard-agent-listen-port` argument when running `ray start`.

:::{note}
When running on a remote cluster, you need to ensure that the import path is accessible. See [Handle Dependencies](serve-handling-dependencies) for how to add a runtime environment.
:::

:::{note}
If the port 52365 (or whichever port you specify with `--dashboard-agent-listen-port`) is unavailable when Ray starts, the dashboard agent’s HTTP server will fail. However, the dashboard agent and Ray will continue to run.
You can check if an agent’s HTTP server is running by sending a curl request: `curl http://{node_ip}:{dashboard_agent_port}/api/serve/deployments/`. If the request succeeds, the server is running on that node. If the request fails, the server is not running on that node. To launch the server on that node, terminate the process occupying the dashboard agent’s port, and restart Ray on that node.
:::

:::{tip}
By default, all the Serve CLI commands assume that you're working with a local cluster. All Serve CLI commands, except `serve start` and `serve run` use the Ray agent address associated with a local cluster started by `ray start --head`. However, if the `RAY_AGENT_ADDRESS` environment variable is set, these Serve CLI commands will default to that value instead.

Similarly, `serve start` and `serve run`, use the Ray head node address associated with a local cluster by default. If the `RAY_ADDRESS` environment variable is set, they will use that value instead.

You can check `RAY_AGENT_ADDRESS`'s value by running:

```console
$ echo $RAY_AGENT_ADDRESS
```

You can set this variable by running the CLI command:

```console
$ export RAY_AGENT_ADDRESS=[YOUR VALUE]
```

You can unset this variable by running the CLI command:

```console
$ unset RAY_AGENT_ADDRESS
```

Check for this variable in your environment to make sure you're using your desired Ray agent address.
:::

To inspect the status of the Serve application in production, see [Inspect an application](serve-in-production-inspecting).

Make heavyweight code updates (like `runtime_env` changes) by starting a new Ray Cluster, updating your Serve config file, and deploying the file with `serve deploy` to the new cluster. Once the new deployment is finished, switch your traffic to the new cluster.
