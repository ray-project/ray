(kuberay-logging)=

# Logging

This page provides tips on how to collect logs from
Ray clusters running on Kubernetes.

:::{tip}
Skip to {ref}`the deployment instructions<kuberay-logging-tldr>`
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
We will provide an {ref}`example<kuberay-fluentbit>` of the sidecar strategy in this guide.
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
# Setting up logging sidecars with Fluent Bit.
In this section, we give an example of how to set up log-emitting
[Fluent Bit][FluentBit] sidecars for Ray pods.

## Configure log processing
The first step is to create a ConfigMap with configuration
for FluentBit.

Here is a minimal ConfigMap which tells a Fluent Bit sidecar to
* Tail Ray logs.
* Output the logs to the container's stdout.
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluentbit-config
data:
  fluent-bit.conf: |
    [INPUT]
        Name tail
        Path /tmp/ray/session_latest/logs/*
        Tag ray
        Path_Key true
        Refresh_Interval 5
    [OUTPUT]
        Name stdout
        Match *
```
In addition to streaming logs to stdout, you can export logs to any
[storage backend][FluentBitStorage] supported by Fluent Bit.

## Add logging sidecars to your RayCluster CR.

### Add log and config volumes.
For each pod template in our RayCluster CR, we
need to add two volumes: One volume for Ray's logs
and another volume to store Fluent Bit configuration from the ConfigMap
applied above.
```yaml
volumes:
- name: ray-logs
  emptyDir: {}
- name: fluentbit-config
  configMap:
    name: fluentbit-config
```

### Mount the Ray log directory
Add the following volume mount to the Ray container's configuration.
```yaml
volumeMounts:
- mountPath: /tmp/ray
  name: ray-logs
```

### Add the Fluent Bit sidecar
Finally, add the Fluent Bit sidecar container to each Ray pod config
in your RayCluster CR.
```yaml
- name: fluentbit
  image: fluent/fluent-bit:1.9.6
  volumeMounts:
  - mountPath: /tmp/ray
    name: ray-logs
  - mountPath: /fluent-bit/etc/fluent-bit.conf
    name: fluentbit-config
```
Mounting the `ray-logs` volume gives the sidecar container access to Ray's logs.
The `fluentbit-config` volume gives the sidecar access to logging configuration.

### Putting everything together
Putting all of the above elements together, we have the following yaml configuration
for a single-pod RayCluster will a log-processing sidecar:
```{literalinclude} ../config/ray-cluster.log.yaml
:language: yaml
```


## Deploying a RayCluster with logging CR.
(kuberay-logging-tldr)=
Now, we will see how to deploy the configuration described above.

Deploy the KubeRay Operator if you haven't yet.
Refer to the {ref}`Getting Started guide<kuberay-operator-deploy>`
for instructions on this step.

Now, run the following commands to deploy the Fluent Bit ConfigMap and a single-pod RayCluster with
a Fluent Bit sidecar.
```shell
# Starting from the parent of cloned Ray master.
pushd ray/doc/source/cluster/cluster_under_construction/ray-clusters-on-kubernetes/configs/
kubectl apply -f ray-cluster.log.yaml
popd
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
