(vms-logging)=

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

### Configure log processing
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


### Add logging sidecars to your RayCluster CR

#### Add log and config volumes
For each pod template in our RayCluster CR, we
need to add two volumes: One volume for Ray's logs
and another volume to store Fluent Bit configuration from the ConfigMap
applied above.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Log and config volumes
```

#### Mount the Ray log directory
Add the following volume mount to the Ray container's configuration.
```{literalinclude} ../configs/ray-cluster.log.yaml
:language: yaml
:start-after: Share logs with Fluent Bit
:end-before: Fluent Bit sidecar
```

#### Add the Fluent Bit sidecar
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

## Setting up loggers

When using Ray, all of the tasks and actors are executed remotely in Ray's worker processes. 
Since Python logger module creates a singleton logger per process, loggers should be configured on per task/actor basis. 

:::{note}
To stream logs to a driver, they should be flushed to stdout and stderr.
:::

```python
import ray
import logging
# Initiate a driver.
ray.init()

@ray.remote
class Actor:
     def __init__(self):
        # Basic config automatically configures logs to
        # be streamed to stdout and stderr.
        # Set the severity to INFO so that info logs are printed to stdout.
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logging.info(msg)

actor = Actor.remote()
ray.get(actor.log.remote("A log message for an actor."))

@ray.remote
def f(msg):
    logging.basicConfig(level=logging.INFO)
    logging.info(msg)

ray.get(f.remote("A log message for a task"))
```

```bash
(pid=95193) INFO:root:A log message for a task
(pid=95192) INFO:root:A log message for an actor.
```
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
## Logging directory structure

By default, Ray logs are stored in a ``/tmp/ray/session_*/logs`` directory. 

:::{note}
The default temp directory is ``/tmp/ray`` (for Linux and Mac OS). If you'd like to change the temp directory, you can specify it when ``ray start`` or ``ray.init()`` is called. 
:::

A new Ray instance creates a new session ID to the temp directory. The latest session ID is symlinked to ``/tmp/ray/session_latest``.

Here's a Ray log directory structure. Note that ``.out`` is logs from stdout/stderr and ``.err`` is logs from stderr. The backward compatibility of log directories is not maintained.

- ``dashboard.[log|err]``: A log file of a Ray dashboard. ``log.`` file contains logs generated from the dashboard's logger. ``.err`` file contains stdout and stderr printed from the dashboard. They are usually empty except when the dashboard crashes unexpectedly.
- ``dashboard_agent.log``: Every Ray node has one dashboard agent. This is a log file of the agent.
- ``gcs_server.[out|err]``: The GCS server is a stateless server that manages Ray cluster metadata. It exists only in the head node.
- ``io-worker-[worker_id]-[pid].[out|err]``: Ray creates IO workers to spill/restore objects to external storage by default from Ray 1.3+. This is a log file of IO workers.
- ``job-driver-[submission_id].log``: The stdout of a job submitted via the :ref:`Ray Jobs API <jobs-overview>`.
- ``log_monitor.[log|err]``: The log monitor is in charge of streaming logs to the driver. ``log.`` file contains logs generated from the log monitor's logger. ``.err`` file contains the stdout and stderr printed from the log monitor. They are usually empty except when the log monitor crashes unexpectedly.
- ``monitor.[out|err]``: Stdout and stderr of a cluster launcher.
- ``monitor.log``: Ray's cluster launcher is operated with a monitor process. It also manages the autoscaler.
- ``plasma_store.[out|err]``: Deprecated.
- ``python-core-driver-[worker_id]_[pid].log``: Ray drivers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``python-core-worker-[worker_id]_[pid].log``: Ray workers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``raylet.[out|err]``: A log file of raylets.
- ``redis-shard_[shard_index].[out|err]``: Redis shard log files.
- ``redis.[out|err]``: Redis log files.
- ``runtime_env_agent.log``: Every Ray node has one agent that manages :ref:`runtime environment <runtime-environments>` creation, deletion and caching.
  This is the log file of the agent containing logs of create/delete requests and cache hits and misses.
  For the logs of the actual installations (including e.g. ``pip install`` logs), see the ``runtime_env_setup-[job_id].log`` file (see below).
- ``runtime_env_setup-[job_id].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a task, actor or job.  This file will only be present if a runtime environment is installed.
- ``runtime_env_setup-ray_client_server_[port].log``: Logs from installing :ref:`runtime environments <runtime-environments>` for a job when connecting via :ref:`Ray Client <ray-client-ref>`.
- ``worker-[worker_id]-[job_id]-[pid].[out|err]``: Python/Java part of Ray drivers and workers. All of stdout and stderr from tasks/actors are streamed here. Note that job_id is an id of the driver. 

## Rotating logs

Ray supports log rotation of log files. Note that not all components are currently supporting log rotation. (Raylet and Python/Java worker logs are not rotating).

By default, logs are rotating when it reaches to 512MB (maxBytes), and there could be up to 5 backup files (backupCount). Indexes are appended to all backup files (e.g., `raylet.out.1`)
If you'd like to change the log rotation configuration, you can do it by specifying environment variables. For example,

```bash
RAY_ROTATION_MAX_BYTES=1024; ray start --head # Start a ray instance with maxBytes 1KB.
RAY_ROTATION_BACKUP_COUNT=1; ray start --head # Start a ray instance with backupCount 1.
```