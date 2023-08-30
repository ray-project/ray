(serve-best-practices)=

# Best practices in production

This section helps you:

* Understand best practices when operating Serve in production
* Learn more about managing Serve with the Serve CLI
* Configure your HTTP requests when querying Serve

## CLI best practices

This section summarizes the best practices for deploying to production using the Serve CLI:

* Use `serve run` to manually test and improve your Serve application locally.
* Use `serve build` to create a Serve config file for your Serve application.
    * For development, put your Serve application's code in a remote repository and manually configure the `working_dir` or `py_modules` fields in your Serve config file's `runtime_env` to point to that repository.
    * For production, put your Serve application's code in a custom Docker image instead of a `runtime_env`. See [this tutorial](serve-custom-docker-images) to learn how to create custom Docker images and deploy them on KubeRay.
* Use `serve status` to track your Serve application's health and deployment progress.
* Use `serve config` to check the latest config that your Serve application received. This is its goal state.
* Make lightweight configuration updates (e.g., `num_replicas` or `user_config` changes) by modifying your Serve config file and redeploying it with `serve deploy`.

(serve-in-production-inspecting)=

### Inspect an application with `serve config` and `serve status`

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

`serve status` gets your Serve application's current status. This command reports the status of the `proxies` and the `applications` running on the Ray cluster.

`proxies` lists each proxy's status. Each proxy is identified by the node ID of the node that it runs on. A proxy has three possible statuses:
* `STARTING`: The proxy is starting up and is not yet ready to serve requests.
* `HEALTHY`: The proxy is capable of serving requests. It is behaving normally.
* `UNHEALTHY`: The proxy has failed its health-checks. It will be killed, and a new proxy will be started on that node.
* `DRAINING`: The proxy is healthy but is closed to new requests. It may contain pending requests that are still being processed.
* `DRAINED`: The proxy is closed to new requests. There are no pending requests.

`applications` contains a list of applications, their overall statuses, and their deployments' statuses. Each entry in `applications` maps an application's name to four fields:
* `status`: A Serve application has four possible overall statuses:
    * `"NOT_STARTED"`: No application has been deployed on this cluster.
    * `"DEPLOYING"`: The application is currently carrying out a `serve deploy` request. It is deploying new deployments or updating existing ones.
    * `"RUNNING"`: The application is at steady-state. It has finished executing any previous `serve deploy` requests, and is attempting to maintain the goal state set by the latest `serve deploy` request.
    * `"DEPLOY_FAILED"`: The latest `serve deploy` request has failed.
* `message`: Provides context on the current status.
* `deployment_timestamp`: A UNIX timestamp of when Serve received the last `serve deploy` request. The timestamp is calculated using the `ServeController`'s local clock.
* `deployments`: A list of entries representing each deployment's status. Each entry maps a deployment's name to three fields:
    * `status`: A Serve deployment has three possible statuses:
        * `"UPDATING"`: The deployment is updating to meet the goal state set by a previous `deploy` request.
        * `"HEALTHY"`: The deployment achieved the latest requests goal state.
        * `"UNHEALTHY"`: The deployment has either failed to update, or has updated and has become unhealthy afterwards. This condition may be due to an error in the deployment's constructor, a crashed replica, or a general system or machine error.
    * `replica_states`: A list of the replicas' states and the number of replicas in that state. Each replica has five possible states:
        * `STARTING`: The replica is starting and not yet ready to serve requests.
        * `UPDATING`: The replica is undergoing a `reconfigure` update.
        * `RECOVERING`: The replica is recovering its state.
        * `RUNNING`: The replica is running normally and able to serve requests.
        * `STOPPING`: The replica is being stopped.
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

(serve-best-practices-http-requests)=

## Best practices for HTTP requests

Most examples in these docs use straightforward `get` or `post` requests using Python's `requests` library, such as:

```{literalinclude} ../doc_code/requests_best_practices.py
:start-after: __prototype_code_start__
:end-before: __prototype_code_end__
:language: python
```

This pattern is useful for prototyping, but it isn't sufficient for production. In production, HTTP requests should use:

* Retries: Requests may occasionally fail due to transient issues (e.g., slow network, node failure, power outage, spike in traffic, etc.). Retry failed requests a handful of times to account for these issues.
* Exponential backoff: To avoid bombarding the Serve application with retries during a transient error, apply an exponential backoff on failure. Each retry should wait exponentially longer than the previous one before running. For example, the first retry may wait 0.1s after a failure, and subsequent retries wait 0.4s (4 x 0.1), 1.6s, 6.4s, 25.6s, etc. after the failure.
* Timeouts: Add a timeout to each retry to prevent requests from hanging. The timeout should be longer than the application's latency to give your application enough time to process requests. Additionally, set an [end-to-end timeout](serve-performance-e2e-timeout) in the Serve application, so slow requests don't bottleneck replicas.

```{literalinclude} ../doc_code/requests_best_practices.py
:start-after: __production_code_start__
:end-before: __production_code_end__
:language: python
```
