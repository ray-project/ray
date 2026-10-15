---
myst:
  html_meta:
    description: "Deploy a Ray Serve application to a VM-based cluster with serve deploy, including targeting a remote cluster."
---

(serve-in-production-deploying)=

# Deploy on VM

You can deploy your Serve application to production on a Ray cluster with the Ray Serve CLI. `serve deploy` takes the path to a Serve config file and deploys that file to a Ray cluster over HTTP. The cluster can be a local, single-node cluster, as in this example, or a remote, multi-node cluster that you start with the [Ray Cluster Launcher](cloud-vm-index).

This section shows how to deploy a Serve config file with the CLI, how to update your application with the CLI, and how to deploy to a remote cluster that you start with the Ray Cluster Launcher.

Start by deploying the [Serve config file](production-config-yaml) for the [Text ML application example](serve-in-production-example):

```console
$ ls
text_ml.py
serve_config.yaml

$ ray start --head
...

$ serve deploy serve_config.yaml
2022-06-20 17:26:31,106	SUCC scripts.py:139 --
Sent deploy request successfully!
 * Use `serve status` to check deployments' statuses.
 * Use `serve config` to see the running app's config.
```

`ray start --head` starts a long-lived Ray cluster locally. `serve deploy serve_config.yaml` deploys the `serve_config.yaml` file to this local cluster. To stop the Ray cluster, run `ray stop`.

The message `Sent deploy request successfully!` means the following:

- The Ray cluster received your config file.
- The Ray cluster starts a new Serve application if one hasn't already started.
- The Serve application deploys its deployments, updated with the configurations from your config file.

:::{note}
The message doesn't mean that your Serve application, including its deployments, has already started running successfully. That happens asynchronously as the Ray cluster attempts to update itself to match the settings from your config file. To get the current status, see [Inspect an application](serve-in-production-inspecting).
:::

(serve-in-production-remote-cluster)=

(using-a-remote-cluster)=
## Deploy to a remote cluster

By default, `serve deploy` deploys to a cluster running locally. Use `serve deploy` to deploy your Serve application to a remote cluster too. To specify the remote Ray cluster's dashboard address, pass the optional `--address` or `-a` argument. Use the following form for the address:

```
[RAY_CLUSTER_URI]:[DASHBOARD_PORT]
```

For example, the address of the local cluster that `ray start --head` starts is `http://127.0.0.1:8265`. To deploy to this address explicitly, run the following command:

```console
$ serve deploy config_file.yaml -a http://127.0.0.1:8265
```

The Ray dashboard's default port is 8265. To set a different port, pass the `--dashboard-port` argument to `ray start`.

:::{note}
When you run on a remote cluster, make sure the import path is accessible. To add a runtime environment, see [Handle dependencies](serve-handling-dependencies).
:::

:::{tip}
By default, all Serve CLI commands assume that you're working with a local cluster. All Serve CLI commands except `serve start` and `serve run` use the Ray dashboard address of a local cluster that `ray start --head` starts. If the `RAY_DASHBOARD_ADDRESS` environment variable is set, these commands default to its value instead.

Similarly, `serve start` and `serve run` use the Ray head node address of a local cluster by default. If the `RAY_ADDRESS` environment variable is set, they use its value instead.

To check the value of `RAY_DASHBOARD_ADDRESS`, run the following command:

```console
$ echo $RAY_DASHBOARD_ADDRESS
```

To set this variable, run the following command:

```console
$ export RAY_DASHBOARD_ADDRESS=[YOUR VALUE]
```

To unset this variable, run the following command:

```console
$ unset RAY_DASHBOARD_ADDRESS
```

Check for this variable in your environment to make sure you're using the Ray dashboard address you want.
:::

To inspect the status of the Serve application in production, see [Inspect an application](serve-in-production-inspecting).

To make heavyweight code updates, such as `runtime_env` changes, start a new Ray cluster, update your Serve config file, and deploy the file to the new cluster with `serve deploy`. After your application finishes deploying on the new cluster, switch your traffic to it.
