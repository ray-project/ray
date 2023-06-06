(kuberay-logging)=

# Log Persistence

Logs (both system and application logs) are useful for troubleshooting Ray applications and Clusters. For example, you may want to access system logs if a node terminates unexpectedly.

Similar to Kubernetes, Ray does not provide a native storage solution for log data. Users need to manage the lifecycle of the logs by themselves. This page provides instructions on how to collect logs from Ray Clusters that are running on Kubernetes.

:::{tip}
Skip to {ref}`the deployment instructions <kuberay-logging-tldr>`
for a sample configuration showing how to extract logs from a Ray pod.
:::

## Ray log directory
By default, Ray writes logs to files in the directory `/tmp/ray/session_*/logs` on each Ray pod's file system, including application and system logs. Learn more about the {ref}`log directory and log files <logging-directory>` and the {ref}`log rotation configuration <log-rotation>` before you start to collect the logs.

## Log processing tools
There are a number of open source log processing tools available within the Kubernetes ecosystem. This page shows how to extract Ray logs using [Fluent Bit][FluentBit].
Other popular tools include [Vector][Vector], [Fluentd][Fluentd], [Filebeat][Filebeat], and [Promtail][Promtail].

## Log collection strategies
Collect logs written to a pod's filesystem using one of two logging strategies:
**sidecar containers** or **daemonsets**. Read more about these logging
patterns in the [Kubernetes documentation][KubDoc].

### Sidecar containers
We provide an {ref}`example <kuberay-fluentbit>` of the sidecar strategy in this guide.
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


### Adding logging sidecars to RayCluster Custom Resource (CR)

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

(kuberay-logging-tldr)=
### Deploying a RayCluster with logging sidecar


To deploy the configuration described above, deploy the KubeRay Operator if you haven't yet:
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

[Vector]: https://vector.dev/
[FluentBit]: https://docs.fluentbit.io/manual
[FluentBitStorage]: https://docs.fluentbit.io/manual
[Filebeat]: https://www.elastic.co/guide/en/beats/filebeat/7.17/index.html
[Fluentd]: https://docs.fluentd.org/
[Promtail]: https://grafana.com/docs/loki/latest/clients/promtail/
[KubDoc]: https://kubernetes.io/docs/concepts/cluster-administration/logging/
[ConfigLink]: https://raw.githubusercontent.com/ray-project/ray/releases/2.4.0/doc/source/cluster/kubernetes/configs/ray-cluster.log.yaml


(redirect-to-stderr)=
## Redirecting Ray logs to stderr

By default, Ray writes logs to files under the ``/tmp/ray/session_*/logs`` directory. If you prefer to redirect logs to stderr of the host pods instead, set the environment variable ``RAY_LOG_TO_STDERR=1`` on all Ray nodes. This practice is not recommended but may be useful if your log processing tool only captures log records written to stderr.

```{admonition} Alert
:class: caution
There are known issues with this feature. For example, it may break features like {ref}`Worker log redirection to Driver <log-redirection-to-driver>`. If those features are wanted, use the {ref}`Fluent Bit solution <kuberay-fluentbit>` above.

For Clusters on VMs, do not redirect logs to stderr. Follow {ref}`this guide <vm-logging>` to persist logs.
```

Redirecting logging to stderr also prepends a ``({component})`` prefix, for example ``(raylet)``, to each log record messages.

```bash
[2022-01-24 19:42:02,978 I 1829336 1829336] (gcs_server) grpc_server.cc:103: GcsServer server started, listening on port 50009.
[2022-01-24 19:42:06,696 I 1829415 1829415] (raylet) grpc_server.cc:103: ObjectManager server started, listening on port 40545.
2022-01-24 19:42:05,087 INFO (dashboard) dashboard.py:95 -- Setup static dir for dashboard: /mnt/data/workspace/ray/python/ray/dashboard/client/build
2022-01-24 19:42:07,500 INFO (dashboard_agent) agent.py:105 -- Dashboard agent grpc address: 0.0.0.0:49228
```

These prefixes allow you to filter the stderr stream of logs down to the component of interest. Note that multi-line log records do **not** have this component marker at the beginning of each line.

Follow the steps below to set the environment variable ``RAY_LOG_TO_STDERR=1`` on all Ray nodes

  ::::{tab-set}

  :::{tab-item} Single-node local cluster
  **Start the cluster explicitly with CLI** <br/>
  ```bash
  env RAY_LOG_TO_STDERR=1 ray start
  ```

  **Start the cluster implicitly with `ray.init`** <br/>
  ```python
  os.environ["RAY_LOG_TO_STDERR"] = "1"
  ray.init()
  ```
  :::

  :::{tab-item} KubeRay
  Set `RAY_LOG_TO_STDERR` to `1` in `spec.headGroupSpec.template.spec.containers.env` and `spec.workerGroupSpec.template.spec.containers.env`. Check out this [example YAML file](https://gist.github.com/scottsun94/da4afda045d6e1cc32f9ccd6c33281c2)

  :::


  ::::



