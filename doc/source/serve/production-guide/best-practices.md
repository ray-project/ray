(serve-best-practices)=

# Best practices

This section summarizes the best practices when deploying to production using the Serve CLI:

* Use `serve run` to manually test and improve your deployment graph locally.
* Use `serve build` to create a Serve config file for your deployment graph.
    * Put your deployment graph's code in a remote repository and manually configure the `working_dir` or `py_modules` fields in your Serve config file's `runtime_env` to point to that repository.
* Use `serve status` to track your Serve application's health and deployment progress.
* Use `serve config` to check the latest config that your Serve application received. This is its goal state.
* Make lightweight configuration updates (e.g. `num_replicas` or `user_config` changes) by modifying your Serve config file and redeploying it with `serve deploy`.

(serve-in-production-inspecting)=

## Inspect an application with `serve config` and `serve status`

Two Serve CLI commands help you inspect a Serve application in production: `serve config` and `serve status`.
If you have a remote cluster, `serve config` and `serve status` also has an `--address/-a` argument to access the cluster. See [VM deployment](serve-in-production-remote-cluster) for more information on this argument.

`serve config` gets the latest config file that the Ray Cluster received. This config file represents the Serve application's goal state. The Ray Cluster constantly strives to reach and maintain this state by deploying deployments, and recovering failed replicas, and performing other relevant actions.

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

`serve status` gets your Serve application's current status. The status has two parts per application: the `app_status` and the `deployment_statuses`.

The `app_status` contains three fields:
* `status`: A Serve application has four possible statuses:
    * `"NOT_STARTED"`: No application has been deployed on this cluster.
    * `"DEPLOYING"`: The application is currently carrying out a `serve deploy` request. It is deploying new deployments or updating existing ones.
    * `"RUNNING"`: The application is at steady-state. It has finished executing any previous `serve deploy` requests, and is attempting to maintain the goal state set by the latest `serve deploy` request.
    * `"DEPLOY_FAILED"`: The latest `serve deploy` request has failed.
* `message`: Provides context on the current status.
* `deployment_timestamp`: A UNIX timestamp of when Serve received the last `serve deploy` request. The timestamp is calculated using the `ServeController`'s local clock.

The `deployment_statuses` contains a list of dictionaries representing each deployment's status. Each dictionary has three fields:
* `name`: The deployment's name.
* `status`: A Serve deployment has three possible statuses:
    * `"UPDATING"`: The deployment is updating to meet the goal state set by a previous `deploy` request.
    * `"HEALTHY"`: The deployment achieved the latest requests goal state.
    * `"UNHEALTHY"`: The deployment has either failed to update, or has updated and has become unhealthy afterwards. This condition may be due to an error in the deployment's constructor, a crashed replica, or a general system or machine error.
* `message`: Provides context on the current status.

Use the `serve status` command to inspect your deployments after they are deployed and throughout their lifetime.

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

For Kubernetes deployments with KubeRay, tighter integrations of `serve status` with Kubernetes are available. See [Getting the status of Serve applications in Kubernetes](serve-getting-status-kubernetes).