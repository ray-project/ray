(kuberay-logging)=

# Log Persistence

This page provides tips on how to collect logs from
Ray clusters running on Kubernetes.

:::{tip}
Skip to {ref}`the deployment instructions <kuberay-logging-tldr>`
for a sample configuration showing how to extract logs from a Ray pod.
:::

## The Ray log directory
Each Ray pod runs several component processes, such as the Raylet, object manager, dashboard agent, etc.
These components log to files in the directory `/tmp/ray/session_latest/logs` in the pod's file system.
Extracting and persisting these logs requires some setup.

## Log processing tools
There are a number of log processing tools available within the Kubernetes
ecosystem. This page will shows how to extract Ray logs using [Fluent Bit][FluentBit].
Other popular tools include [Fluentd][Fluentd], [Filebeat][Filebeat], and [Promtail][Promtail].

## Log collection strategies
We mention two strategies for collecting logs written to a pod's filesystem,
**sidecar containers** and **daemonsets**. You can read more about these logging
patterns in the [Kubernetes documentation][KubDoc].

### Sidecar containers
We will provide an {ref}`example <kuberay-fluentbit>` of the sidecar strategy in this guide.
You can process logs by configuring a log-processing sidecar
for each Ray pod. Ray containers should be configured to share the `/tmp/ray`
directory with the logging sidecar via a volume mount.

You can configure the sidecar to do either of the following:
* Stream Ray logs to the sidecar's stdout.
* Export logs to an external service.

### Daemonset
Alternatively, it is possible to collect logs at the Kubernetes node level.
To do this, one deploys a log-processing daemonset onto the Kubernetes cluster's
nodes. With this strategy, it is key to mount
the Ray container's `/tmp/ray` directory to the relevant `hostPath`.

(kuberay-fluentbit)=
## Setting up logging sidecars with Fluent Bit
In this section, we give an example of how to set up log-emitting
[Fluent Bit][FluentBit] sidecars for Ray pods.

See the full config for a single-pod RayCluster with a logging sidecar [here][ConfigLink].
We now discuss this configuration and show how to deploy it.

### Configuring log processing
The first step is to create a ConfigMap with configuration
for Fluent Bit.

Here is a minimal ConfigMap which tells a Fluent Bit sidecar to
* Tail Ray logs.
* Output the logs to the container's stdout.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Fluent Bit ConfigMap
:end-before: ---
```
A few notes on the above config:
- In addition to streaming logs to stdout, you can use an [OUTPUT] clause to export logs to any
  [storage backend][FluentBitStorage] supported by Fluent Bit.
- The `Path_Key true` line above ensures that file names are included in the log records
  emitted by Fluent Bit.
- The `Refresh_Interval 5` line asks Fluent Bit to refresh the list of files
  in the log directory once per 5 seconds, rather than the default 60.
  The reason is that the directory `/tmp/ray/session_latest/logs/` does not exist
  initially (Ray must create it first). Setting the `Refresh_Interval` low allows us to see logs
  in the Fluent Bit container's stdout sooner.


### Adding logging sidecars to your RayCluster CR

#### Adding log and config volumes
For each pod template in our RayCluster CR, we
need to add two volumes: One volume for Ray's logs
and another volume to store Fluent Bit configuration from the ConfigMap
applied above.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Log and config volumes
```

#### Mounting the Ray log directory
Add the following volume mount to the Ray container's configuration.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Share logs with Fluent Bit
:end-before: Fluent Bit sidecar
```

#### Adding the Fluent Bit sidecar
Finally, add the Fluent Bit sidecar container to each Ray pod config
in your RayCluster CR.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Fluent Bit sidecar
:end-before: Log and config volumes
```
Mounting the `ray-logs` volume gives the sidecar container access to Ray's logs.
The <nobr>`fluentbit-config`</nobr> volume gives the sidecar access to logging configuration.

#### Putting everything together
Putting all of the above elements together, we have the following yaml configuration
for a single-pod RayCluster will a log-processing sidecar.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
```

### Deploying a RayCluster with logging CR
(kuberay-logging-tldr)=
Now, we will see how to deploy the configuration described above.

Deploy the KubeRay Operator if you haven't yet.
Refer to the {ref}`Getting Started guide <kuberay-operator-deploy>`
for instructions on this step.

Now, run the following commands to deploy the Fluent Bit ConfigMap and a single-pod RayCluster with
a Fluent Bit sidecar.
```shell
kubectl apply -f https://raw.githubusercontent.com/ray-project/ray/releases/2.4.0/doc/source/cluster/kubernetes/configs/ray-cluster.log.yaml
```

Determine the Ray pod's name with
```shell
kubectl get pod | grep raycluster-complete-logs
```

Examine the FluentBit sidecar's STDOUT to see logs for Ray's component processes.
```shell
# Substitute the name of your Ray pod.
kubectl logs raycluster-complete-logs-head-xxxxx -c fluentbit
```

[FluentBit]: https://docs.fluentbit.io/manual
[FluentBitStorage]: https://docs.fluentbit.io/manual
[Filebeat]: https://www.elastic.co/guide/en/beats/filebeat/7.17/index.html
[Fluentd]: https://docs.fluentd.org/
[Promtail]: https://grafana.com/docs/loki/latest/clients/promtail/
[KubDoc]: https://kubernetes.io/docs/concepts/cluster-administration/logging/
[ConfigLink]: https://raw.githubusercontent.com/ray-project/ray/releases/2.4.0/doc/source/cluster/kubernetes/configs/ray-cluster.log.yaml

## Using structured logging

The metadata of tasks or actors may be obtained by Ray's :ref:`runtime_context APIs <runtime-context-apis>`.
Runtime context APIs help you to add metadata to your logging messages, making your logs more structured.

```python
import ray
# Initiate a driver.
ray.init()

 @ray.remote
def task():
    print(f"task_id: {ray.get_runtime_context().task_id}")

ray.get(task.remote())
```

```bash
(pid=47411) task_id: TaskID(a67dc375e60ddd1affffffffffffffffffffffff01000000)
```
## Redirecting Ray logs to stderr

By default, Ray logs are written to files under the ``/tmp/ray/session_*/logs`` directory. If you wish to redirect all internal Ray logging and your own logging within tasks/actors to stderr of the host nodes, you can do so by ensuring that the ``RAY_LOG_TO_STDERR=1`` environment variable is set on the driver and on all Ray nodes. This practice is not recommended but may be useful if you are using a log aggregator that needs log records to be written to stderr in order for them to be captured.

Redirecting logging to stderr will also cause a ``({component})`` prefix, e.g. ``(raylet)``, to be added to each of the log record messages.

```bash
[2022-01-24 19:42:02,978 I 1829336 1829336] (gcs_server) grpc_server.cc:103: GcsServer server started, listening on port 50009.
[2022-01-24 19:42:06,696 I 1829415 1829415] (raylet) grpc_server.cc:103: ObjectManager server started, listening on port 40545.
2022-01-24 19:42:05,087 INFO (dashboard) dashboard.py:95 -- Setup static dir for dashboard: /mnt/data/workspace/ray/python/ray/dashboard/client/build
2022-01-24 19:42:07,500 INFO (dashboard_agent) agent.py:105 -- Dashboard agent grpc address: 0.0.0.0:49228
```

This should make it easier to filter the stderr stream of logs down to the component of interest. Note that multi-line log records will **not** have this component marker at the beginning of each line.

When running a local Ray cluster, this environment variable should be set before starting the local cluster:

```python
os.environ["RAY_LOG_TO_STDERR"] = "1"
ray.init()
```

When starting a local cluster via the CLI or when starting nodes in a multi-node Ray cluster, this environment variable should be set before starting up each node:

```bash
env RAY_LOG_TO_STDERR=1 ray start
```

If using the Ray cluster launcher, you would specify this environment variable in the Ray start commands:

```bash
head_start_ray_commands:
    - ray stop
    - env RAY_LOG_TO_STDERR=1 ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

worker_start_ray_commands:
    - ray stop
    - env RAY_LOG_TO_STDERR=1 ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076
```

When connecting to the cluster, be sure to set the environment variable before connecting:

```python
os.environ["RAY_LOG_TO_STDERR"] = "1"
ray.init(address="auto")
```

## Rotating logs

Ray supports log rotation of log files. Note that not all components are currently supporting log rotation. (Raylet and Python/Java worker logs are not rotating).

By default, logs are rotating when it reaches to 512MB (maxBytes), and there could be up to 5 backup files (backupCount). Indexes are appended to all backup files (e.g., `raylet.out.1`)
If you'd like to change the log rotation configuration, you can do it by specifying environment variables. For example,

```bash
RAY_ROTATION_MAX_BYTES=1024; ray start --head # Start a ray instance with maxBytes 1KB.
RAY_ROTATION_BACKUP_COUNT=1; ray start --head # Start a ray instance with backupCount 1.
```