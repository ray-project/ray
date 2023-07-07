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

It does **not** mean that your Serve application, including your deployments, has already started running successfully. This happens asynchronously as the Ray cluster attempts to update itself to match the settings from your config file. Check out the [next section](serve-in-production-inspecting) to learn more about how to get the current status.

## Adding a runtime environment

The import path (e.g., `fruit:deployment_graph`) must be importable by Serve at runtime.
When running locally, this might be in your current working directory.
However, when running on a cluster you also need to make sure the path is importable.
You can achieve this either by building the code into the cluster's container image (see [Cluster Configuration](kuberay-config) for more details) or by using a `runtime_env` with a [remote URI](remote-uris) that hosts the code in remote storage.

As an example, we have [pushed a copy of the FruitStand deployment graph to GitHub](https://github.com/ray-project/test_dag/blob/40d61c141b9c37853a7014b8659fc7f23c1d04f6/fruit.py). You can use this config file to deploy the `FruitStand` deployment graph to your own Ray cluster even if you don't have the code locally:

```yaml
import_path: fruit:deployment_graph

runtime_env:
    working_dir: "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
```

:::{note}
As a side note, you could also package your deployment graph into a standalone Python package that can be imported using a [PYTHONPATH](https://docs.python.org/3.10/using/cmdline.html#envvar-PYTHONPATH) to provide location independence on your local machine. However, it's still best practice to use a `runtime_env`, to ensure consistency across all machines in your cluster.
:::

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

The Ray dashboard agent's default port is 52365. You can set it to a different value using the `--dashboard-agent-listen-port` argument when running `ray start`."

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

(serve-in-production-inspecting)=

## Inspecting the application with `serve config` and `serve status`

The Serve CLI also offers two commands to help you inspect your Serve application in production: `serve config` and `serve status`.
If you're working with a remote cluster, `serve config` and `serve status` also offer an `--address/-a` argument to access your cluster. Check out [the previous section](serve-in-production-remote-cluster) for more info on this argument.

`serve config` gets the latest config file the Ray cluster received. This config file represents the Serve application's goal state. The Ray cluster will constantly attempt to reach and maintain this state by deploying deployments, recovering failed replicas, and more.

Using the `fruit_config.yaml` example from [an earlier section](fruit-config-yaml):

```console
$ ray start --head
$ serve deploy fruit_config.yaml
...

$ serve config
import_path: fruit:deployment_graph

runtime_env: {}

deployments:

- name: MangoStand
  num_replicas: 2
  route_prefix: null
...
```

`serve status` gets your Serve application's current status. It's divided into two parts: the `app_status` and the `deployment_statuses`.

The `app_status` contains three fields:
* `status`: a Serve application has four possible statuses:
    * `"NOT_STARTED"`: no application has been deployed on this cluster.
    * `"DEPLOYING"`: the application is currently carrying out a `serve deploy` request. It is deploying new deployments or updating existing ones.
    * `"RUNNING"`: the application is at steady-state. It has finished executing any previous `serve deploy` requests, and it is attempting to maintain the goal state set by the latest `serve deploy` request.
    * `"DEPLOY_FAILED"`: the latest `serve deploy` request has failed.
* `message`: provides context on the current status.
* `deployment_timestamp`: a unix timestamp of when Serve received the last `serve deploy` request. This is calculated using the `ServeController`'s local clock.

The `deployment_statuses` contains a list of dictionaries representing each deployment's status. Each dictionary has three fields:
* `name`: the deployment's name.
* `status`: a Serve deployment has three possible statuses:
    * `"UPDATING"`: the deployment is updating to meet the goal state set by a previous `deploy` request.
    * `"HEALTHY"`: the deployment is at the latest requests goal state.
    * `"UNHEALTHY"`: the deployment has either failed to update, or it has updated and has become unhealthy afterwards. This may be due to an error in the deployment's constructor, a crashed replica, or a general system or machine error.
* `message`: provides context on the current status.

You can use the `serve status` command to inspect your deployments after they are deployed and throughout their lifetime.

Using the `fruit_config.yaml` example from [an earlier section](fruit-config-yaml):

```console
$ ray start --head
$ serve deploy fruit_config.yaml
...

$ serve status
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1655771534.835145
deployment_statuses:
- name: MangoStand
  status: HEALTHY
  message: ''
- name: OrangeStand
  status: HEALTHY
  message: ''
- name: PearStand
  status: HEALTHY
  message: ''
- name: FruitMarket
  status: HEALTHY
  message: ''
- name: DAGDriver
  status: HEALTHY
  message: ''
```

`serve status` can also be used with KubeRay ({ref}`kuberay-index`), a Kubernetes operator for Ray Serve, to help deploy your Serve applications with Kubernetes. There's also work in progress to provide closer integrations between some of the features from this document, like `serve status`, with Kubernetes to provide a clearer Serve deployment story.
