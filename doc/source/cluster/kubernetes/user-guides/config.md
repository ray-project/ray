(kuberay-config)=

# RayCluster Configuration

This guide covers the key aspects of Ray cluster configuration on Kubernetes.

## Introduction

Deployments of Ray on Kubernetes follow the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/). The key players are
- A [custom resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)
    called a `RayCluster` describing the desired state of a Ray cluster.
- A [custom controller](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#custom-controllers),
    the KubeRay operator, which manages Ray pods in order to match the `RayCluster`'s spec.

To deploy a Ray cluster, one creates a `RayCluster` custom resource (CR):
```shell
kubectl apply -f raycluster.yaml
```

This guide covers the salient features of `RayCluster` CR configuration.

For reference, here is a condensed example of a `RayCluster` CR in yaml format.
```yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: raycluster-complete
spec:
  rayVersion: "2.3.0"
  enableInTreeAutoscaling: true
  autoscalerOptions:
     ...
  headGroupSpec:
    serviceType: ClusterIP # Options are ClusterIP, NodePort, and LoadBalancer
    rayStartParams:
      dashboard-host: "0.0.0.0"
      ...
    template: # Pod template
        metadata: # Pod metadata
        spec: # Pod spec
            containers:
            - name: ray-head
              image: rayproject/ray-ml:2.3.0
              resources:
                limits:
                  cpu: 14
                  memory: 54Gi
                requests:
                  cpu: 14
                  memory: 54Gi
              ports: # Optional service port overrides
              - containerPort: 6379
                name: gcs
              - containerPort: 8265
                name: dashboard
              - containerPort: 10001
                name: client
              - containerPort: 8000
                name: serve
                ...
  workerGroupSpecs:
  - groupName: small-group
    replicas: 1
    minReplicas: 1
    maxReplicas: 5
    rayStartParams:
        ...
    template: # Pod template
      spec:
        ...
  # Another workerGroup
  - groupName: medium-group
    ...
  # Yet another workerGroup, with access to special hardware perhaps.
  - groupName: gpu-group
    ...
```

The rest of this guide will discuss the `RayCluster` CR's config fields.
See also the [guide](kuberay-autoscaling-config) on configuring Ray autoscaling with KubeRay.

(kuberay-config-ray-version)=
## The Ray Version
The field `rayVersion` specifies the version of Ray used in the Ray cluster.
The `rayVersion` is used to fill default values for certain config fields.
The Ray container images specified in the RayCluster CR should carry
the same Ray version as the CR's `rayVersion`. If you are using a nightly or development
Ray image, it is fine to set `rayVersion` to the latest release version of Ray.

## Pod configuration: headGroupSpec and workerGroupSpecs

At a high level, a RayCluster is a collection of Kubernetes pods, similar to a Kubernetes Deployment or StatefulSet.
Just as with the Kubernetes built-ins, the key pieces of configuration are
* Pod specification
* Scale information (how many pods are desired)

The key difference between a Deployment and a `RayCluster` is that a `RayCluster` is
specialized for running Ray applications. A Ray cluster consists of

* One **head pod** which hosts global control processes for the Ray cluster.
  The head pod can also run Ray tasks and actors.
* Any number of **worker pods**, which run Ray tasks and actors.
  Workers come in **worker groups** of identically configured pods.
  For each worker group, we must specify **replicas**, the number of
  pods we want of that group.

The head pod’s configuration is
specified under `headGroupSpec`, while configuration for worker pods is
specified under `workerGroupSpecs`. There may be multiple worker groups,
each group with its own configuration. The `replicas` field
of a `workerGroupSpec` specifies the number of worker pods of that group to
keep in the cluster. Each `workerGroupSpec` also has optional `minReplicas` and
`maxReplicas` fields; these fields are important if you wish to enable {ref}`autoscaling <kuberay-autoscaling-config>`.

### Pod templates
The bulk of the configuration for a `headGroupSpec` or
`workerGroupSpec` goes in the `template` field. The `template` is a Kubernetes Pod
template which determines the configuration for the pods in the group.
Here are some of the subfields of the pod `template` to pay attention to:

#### containers
A Ray pod template specifies at minimum one container, namely the container
that runs the Ray processes. A Ray pod template may also specify additional sidecar
containers, for purposes such as {ref}`log processing <persist-kuberay-custom-resource-logs>`. However, the KubeRay operator assumes that
the first container in the containers list is the main Ray container.
Therefore, make sure to specify any sidecar containers
**after** the main Ray container. In other words, the Ray container should be the **first**
in the `containers` list.

#### resources
It’s important to specify container CPU and memory requests and limits for
each group spec. For GPU workloads, you may also wish to specify GPU
limits. For example, set `nvidia.com/gpu:2` if using an Nvidia GPU device plugin
and you wish to specify a pod with access to 2 GPUs.
See {ref}`this guide <kuberay-gpu>` for more details on GPU support.

It's ideal to size each Ray pod to take up the
entire Kubernetes node on which it is scheduled. In other words, it’s
best to run one large Ray pod per Kubernetes node.
In general, it is more efficient to use a few large Ray pods than many small ones.
The pattern of fewer large Ray pods has the following advantages:
- more efficient use of each Ray pod's shared memory object store
- reduced communication overhead between Ray pods
- reduced redundancy of per-pod Ray control structures such as Raylets

The CPU, GPU, and memory **limits** specified in the Ray container config
will be automatically advertised to Ray. These values will be used as
the logical resource capacities of Ray pods in the head or worker group.
Note that CPU quantities will be rounded up to the nearest integer
before being relayed to Ray.
The resource capacities advertised to Ray may be overridden in the {ref}`rayStartParams`.

On the other hand CPU, GPU, and memory **requests** will be ignored by Ray.
For this reason, it is best when possible to **set resource requests equal to resource limits**.

#### nodeSelector and tolerations
You can control the scheduling of worker groups' Ray pods by setting the `nodeSelector` and
`tolerations` fields of the pod spec. Specifically, these fields determine on which Kubernetes
nodes the pods may be scheduled.
See the [Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
for more about Pod-to-Node assignment.

#### image
The Ray container images specified in the `RayCluster` CR should carry
the same Ray version as the CR's `spec.rayVersion`.
If you are using a nightly or development Ray image, you can specify Ray's
latest release version under `spec.rayVersion`.

For Apple M1 or M2 MacBooks, see [Use ARM-based docker images for Apple M1 or M2 MacBooks](docker-image-for-apple-macbooks) to specify the
correct image.

You must install code dependencies for a given Ray task or actor on each Ray node that
might run the task or actor.
The simplest way to achieve this configuration is to use the same Ray image for the Ray head and all worker groups.
In any case, do make sure that all Ray images in your CR carry the same Ray version and
Python version.
To distribute custom code dependencies across your cluster, you can build a custom container image,
using one of the [official Ray images](https://hub.docker.com/r/rayproject/ray) as the base.
See {ref}`this guide <docker-images>` to learn more about the official Ray images.
For dynamic dependency management geared towards iteration and development,
you can also use {ref}`Runtime Environments <runtime-environments>`.

For `kuberay-operator` versions 1.1.0 and later, the Ray container image must have `wget` installed in it.

#### metadata.name and metadata.generateName
The KubeRay operator will ignore the values of `metadata.name` and `metadata.generateName` set by users.
The KubeRay operator will generate a `generateName` automatically to avoid name conflicts.
See [KubeRay issue #587](https://github.com/ray-project/kuberay/pull/587) for more details.

(rayStartParams)=
## Ray Start Parameters
The ``rayStartParams`` field of each group spec is a string-string map of arguments to the Ray
container’s `ray start` entrypoint. For the full list of arguments, refer to
the documentation for {ref}`ray start <ray-start-doc>`. We make special note of the following arguments:

### dashboard-host
For most use-cases, this field should be set to "0.0.0.0" for the Ray head pod.
This is required to expose the Ray dashboard outside the Ray cluster. (Future versions might set
this parameter by default.)

(kuberay-num-cpus)=
### num-cpus
This optional field tells the Ray scheduler and autoscaler how many CPUs are
available to the Ray pod. The CPU count can be autodetected from the
Kubernetes resource limits specified in the group spec’s pod
`template`. However, it is sometimes useful to override this autodetected
value. For example, setting `num-cpus:"0"` for the Ray head pod will prevent Ray
workloads with non-zero CPU requirements from being scheduled on the head.
Note that the values of all Ray start parameters, including `num-cpus`,
must be supplied as **strings**.

### num-gpus
This field specifies the number of GPUs available to the Ray container.
In future KubeRay versions, the number of GPUs will be auto-detected from Ray container resource limits.
Note that the values of all Ray start parameters, including `num-gpus`,
must be supplied as **strings**.

### memory
The memory available to the Ray is detected automatically from the Kubernetes resource
limits. If you wish, you may override this autodetected value by setting the desired memory value,
in bytes, under `rayStartParams.memory`.
Note that the values of all Ray start parameters, including `memory`,
must be supplied as **strings**.

### resources
This field can be used to specify custom resource capacities for the Ray pod.
These resource capacities will be advertised to the Ray scheduler and Ray autoscaler.
For example, the following annotation will mark a Ray pod as having 1 unit of `Custom1` capacity
and 5 units of `Custom2` capacity.
```yaml
rayStartParams:
    resources: '"{\"Custom1\": 1, \"Custom2\": 5}"'
```
You can then annotate tasks and actors with annotations like `@ray.remote(resources={"Custom2": 1})`.
The Ray scheduler and autoscaler will take appropriate action to schedule such tasks.

Note the format used to express the resources string. In particular, note
that the backslashes are present as actual characters in the string.
If you are specifying a `RayCluster` programmatically, you may have to
[escape the backslashes](https://github.com/ray-project/ray/blob/cd9cabcadf1607bcda1512d647d382728055e688/python/ray/tests/kuberay/test_autoscaling_e2e.py#L92) to make sure they are processed as part of the string.

The field `rayStartParams.resources` should only be used for custom resources. The keys
`CPU`, `GPU`, and `memory` are forbidden. If you need to specify overrides for those resource
fields, use the Ray start parameters `num-cpus`, `num-gpus`, or `memory`.

(kuberay-networking)=
## Services and Networking
### The Ray head service.
The KubeRay operator automatically configures a Kubernetes Service exposing the default ports
for several services of the Ray head pod, including
- Ray Client (default port 10001)
- Ray Dashboard (default port 8265)
- Ray GCS server (default port 6379)
- Ray Serve (default port 8000)
- Ray Prometheus metrics (default port 8080)

The name of the configured Kubernetes Service is the name, `metadata.name`, of the RayCluster
followed by the suffix <nobr>`head-svc`</nobr>. For the example CR given on this page, the name of
the head service will be
<nobr>`raycluster-example-head-svc`</nobr>. Kubernetes networking (`kube-dns`) then allows us to address
the Ray head's services using the name <nobr>`raycluster-example-head-svc`</nobr>.
For example, the Ray Client server can be accessed from a pod
in the same Kubernetes namespace using
```python
ray.init("ray://raycluster-example-head-svc:10001")
```
The Ray Client server can be accessed from a pod in another namespace using
```python
ray.init("ray://raycluster-example-head-svc.default.svc.cluster.local:10001")
```
(This assumes the Ray cluster was deployed into the default Kubernetes namespace.
If the Ray cluster is deployed in a non-default namespace, use that namespace in
place of `default`.)

### Specifying non-default ports.
If you wish to override the ports exposed by the Ray head service, you may do so by specifying
the Ray head container's `ports` list, under `headGroupSpec`.
Here is an example of a list of non-default ports for the Ray head service.
```yaml
ports:
- containerPort: 6380
  name: gcs
- containerPort: 8266
  name: dashboard
- containerPort: 10002
  name: client
```
If the head container's `ports` list is specified, the Ray head service will expose precisely
the ports in the list. In the above example, the head service will expose just three ports;
in particular there will be no port exposed for Ray Serve.

For the Ray head to actually use the non-default ports specified in the ports list,
you must also specify the relevant `rayStartParams`. For the above example,
```yaml
rayStartParams:
  port: "6380"
  dashboard-port: "8266"
  ray-client-server-port: "10002"
  ...
```

[IngressDoc]: kuberay-ingress
