(kuberay-config)=

# RayCluster Configuration

This guide covers the key aspects of Ray cluster configuration on Kubernetes.

## Introduction

Deployments of Ray on Kubernetes follow the standard [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) consisting of a
- A [Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)
    called a `RayCluster` describing the desired state of a Ray Cluster.
- A [Custom Controller](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#custom-controllers),
    the KubeRay operator, which processes `RayCluster` resources and manages Ray pods.

To deploy a Ray cluster, one creates a `RayCluster` custom resource (CR):
```shell
kubectl apply -f raycluster.yaml
```

This guide covers the salient features of the `RayCluster` CR configuration.

For reference, here is a condensed example of a `RayCluster` CR in yaml format.
```yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: raycluster-complete
spec:
  rayVersion: "2.0.0"
  enableInTreeAutoscaling: True
  autoscalerOptions:
     ...
  headGroupSpec:
    rayStartParams:
      block: True
      dashboard-host: "0.0.0.0"
      ...
    template: # Pod template
        metadata: # Pod metadata
        spec: # Pod spec
            containers:
            - name: ray-head
              image: rayproject/ray-ml:2.0.0
              resources:
                limits:
                  cpu: 14
                  memory: 54Gi
                requests:
                  cpu: 14
                  memory: 54Gi
              ports:
              - containerPort: 6379
                name: gcs
              - containerPort: 8265
                name: dashboard
              - containerPort: 10001
                name: client
  workerGroupSpecs:
  - groupName: small-group
    replicas: 1
    minReplicas: 1
    maxReplicas: 5
    rayStartParams:
        ...
    template: # Pod template
        ...
  # Another workerGroup
  - groupName: medium-group
    ...
  # Yet another workerGroup, with access to special hardware perhaps.
  - groupName: gpu-group
    ...
```

## The Ray version
The field `rayVersion` specifies the version of Ray used in the Ray cluster.
The Ray version is used when filling out default values for certain config fields.
The Ray container images specified in the RayCluster CR should carry
the same Ray version as the CR's `rayVersion`. If you are using a nightly or development
Ray image, it is fine to specify the latest release version of Ray.

## Pod configuration and scale: headGroupSpec and workerGroupSpecs

At a high level, a RayCluster is a collection of Kubernetes pods, similar to a Kubernetes Deployment or StatefulSet.
Just as with the Kubernetes built-ins, the key pieces of configuration are
* Pod specification
* Scale information (how many pods are desired)

The difference is that a RayCluster is specialized for running Ray applications.
A RayCluster consists of

* One **head pod** which hosts global control processes for the Ray cluster.
  The head pod can also run Ray tasks and actors.
* Any number of **worker pods**, which run Ray tasks and actors.
  Workers come in **worker groups** of identically configured pods.

The head pod’s configuration is
specified under `headGroupSpec`, while configuration for worker pods is
specified under `workerGroupSpecs`. There may be multiple worker groups,
each group with its own configuration. The `replicas` field
of a `workerGroupSpec` specifies the number of worker pods of that group to
keep in the cluster.

### template
The bulk of the configuration of a `headGroupSpec` or
`workerGroupSpec`'s goes in the `template` field. The `template` is a Kubernetes Pod
template which determines the configuration for the pods in the group.
Here are some of the subfields of `template` to pay attention to:

#### ports
Under `headGroupSpec`, the Ray head container should list the ports for the services it exposes.
```yaml
ports:
- containerPort: 6379
name: gcs
- containerPort: 8265
name: dashboard
- containerPort: 10001
name: client
```
The KubeRay operator will configure a Kubernetes Service exposing these ports.
The name of the configured Kubernetes service is the name, `metadata.name`, of the RayCluster
followed by the suffix `-head-svc`. For the example CR given on this page, the name will
be `raycluster-example-head-svc`.
(Sentence about KubeDNS.)
For example, the Ray Client server will be accessible from a pod
in the same Kubernetes namespace using `ray.init("ray://raycluster-example-head-svc:10001")`.
The Ray Client server will be accessible from a pod in another namespace using
`ray.init("ray://raycluster-example-head-svc.default.svc.cluster.local:10001")`.
(If the Ray cluster is a non-default namespace, use that namespace in
place of `default`.)
Ray Client and other services can be made accessible from outside the Kubernetes cluster
using port-forwarding or an ingress. (Networking notes.)

#### resources
It’s important to specify container CPU and memory requests and limits for
each group spec. For GPU workloads, you may also wish to specify GPU
limits e.g. `nvidia.com/gpu: 1` if using an nvidia GPU device plugin. See {ref}`kuberay-gpu`.

It is ideal when possible to size each Ray pod such that it takes up the
entire Kubernetes node on which it is scheduled. In other words, it’s
best to run one large Ray pod per Kubernetes node.
Broadly speaking, it is more efficient to use a few large Ray pods than many small ones.
The pattern of fewer large Ray pods has the following advantages:
- enables more efficient use of each Ray pod's shared memory object store
- reduces communication overhead between Ray pods
- reduces redundancy of per-pod Ray control processes such as Raylets.

#### nodeSelector and tolerations
You can control the scheduling of worker groups' Ray pods by setting the `nodeSelector` and
`tolerations` fields of the pod spec. Specifically, these fields determine on which Kubernetes
nodes the pods may be scheduled.
See the [Kubernetes docs](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)
for more about Pod-to-Node assignment.

#### image
The Ray container images specified in the RayCluster CR should carry
the same Ray version as the CR's `spec.rayVersion`.
(If you are using a nightly or development Ray image, it is fine to specify Ray's
latest release version under `spec.rayVersion`.)

Code dependencies for a given Ray task or actor must be installed on each Ray node that
might run the task or actor.
To achieve this, it is simplest to use the same Ray image for the Ray head and all worker groups.
In any case, do make sure that all Ray images in your CR carry the same Ray version and
Python version.
To distribute custom code dependencies across your cluster, you can build a custom container image,
using one of the `official Ray images <https://hub.docker.com/r/rayproject/ray>`_ as the base.
Read more about the official Ray images at {ref}`docker-images`.
For dynamic dependency management geared towards iteration and developement,
you can also use {ref}`Runtime Environments<runtime-environments>`.

## Ray Start Parameters
The ``rayStartParams`` field of each group spec is a string-string map of arguments to the Ray
container’s `ray start` entrypoint. For the full list of arguments, refer to
the documentation for {ref}`ray start<ray-start-doc>`. We make special note of the following arguments:

#### block
For most use-cases, this field should be set to "true" for all Ray pod. The container's Ray
entrypoint will then block forever until a Ray process exits, at which point the container
will exit. If this field is omitted, `ray start` will start Ray processes in the background and the container
will subsequently sleep forever until terminated. (Future versions of KubeRay may set
block to true by default. See [KubeRay issue #368](https://github.com/ray-project/kuberay/issues/368).)

#### dashboard-host
For most use-cases, this field should be set to "0.0.0.0" for the Ray head pod.
This is required to expose the Ray dashboard outside the Ray cluster. (Future versions might set
this parameter by default.)

#### num-cpus
This optional field tells the Ray scheduler and autoscaler how many CPUs are
available to the Ray pod. The CPU count can be autodetected from the
Kubernetes resource limits specified in the group spec’s pod
`template`. However, it is sometimes useful to override this autodetected
value. For example, setting `num-cpus:"0"` for the Ray head pod will prevent Ray
workloads with non-zero CPU requirements from being scheduled on the head.

#### num-gpus
This optional field specifies the number of GPUs available to the Ray container.
In KubeRay versions since 0.3.0, the number of GPUs can be auto-detected from Ray container resource limits.
For certain advanced use-cases, you may wish to use `num-gpus` to set an {ref}`override<kuberay-gpu-override>`.

#### memory
The memory available to the Ray is detected automatically from the Kubernetes resource
limits. If you wish, you may override this autodetected value.

#### resources
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
that the backslashes are present as literal characters in the string.
If you are specifying a RayCluster programmatically, you may have to
[escape the backslashes](https://github.com/ray-project/ray/blob/cd9cabcadf1607bcda1512d647d382728055e688/python/ray/tests/kuberay/test_autoscaling_e2e.py#L92) to make sure they are processed as part of the string.

The field `rayStartParams.resources` should only be used for custom resources. The keys
`CPU`, `GPU`, and `memory` are forbidden. If you need to specify overrides for those resource
fields, use the Ray start parameters `num-cpus`, `num-gpus`, or `memory`.


(kuberay-autoscaling-config)=
## Autoscaler configuration
```{note}
If you are deciding whether to use autoscaling for a particular Ray application,
check out the discussion {ref}`Should I enable autoscaling?`
```
To enable the optional Ray Autoscaler support, set `enableInTreeAutoscaling:true`.
The KubeRay operator will then automatically configure an autoscaling sidecar container
for the Ray head pod. The autoscaler container collects resource metrics from the Ray head container
and automatically adjusts the `replicas` field of each `workerGroupSpec` as needed to fulfill
the requirements of your Ray application.

Use the fields `minReplicas` and `maxReplicas` to constrain the `replicas` of an autoscaling
`workerGroup`. When deploying an autoscaling cluster, one typically sets `replicas` and `minReplicas` to the same value.
The Ray autoscaler will then take over and modify the `replicas` field as needed by
the Ray application.

### Autoscaler operation

#### Scale up
The autoscaler scales worker pods up when...

The autoscaler scales Ray worker pods up by...

#### Scale down
The autoscaler scales a worker pod down when the pod has not been using resources
for a {ref}`set period of time`. Resources means...

The autoscaler scales Ray worker pods down by adding the Ray pods' names to the RayCluster CR's
`scaleStrategy.workersToDelete` list and decrementing the `replicas` field of the relevant
`workerGroupSpec`.

Note that you may manually adjust the scale by editing the `replicas` or workersTo
. (It is also possible to implement custom scaling
logic that adjusts the scale on your behalf.)
It is however, not recommended to manually edit these fields for a RayCluster with
autoscaling enabled.

### autoscalerOptions
To enable Ray autoscaler support, it is enough to set `enableInTreeAutoscaling:true`.
Should you need to adjust autoscaling behavior or change the autoscaler container's configuration,
you can use the RayCluster CR's `autoscalerOptions` field. The `autoscalerOptions` field
carries the following subfields:

#### upscalingMode
The `upscalingMode` field can be used to control the rate of Ray pod upscaling.

UpscalingMode is "Conservative", "Default", or "Aggressive."
- `Conservative`: Upscaling is rate-limited; the number of pending worker pods is at most the size of the Ray cluster.
- `Default`: Upscaling is not rate-limited.
- `Aggressive`: An alias for Default; upscaling is not rate-limited.

You may wish to use `Conservative` upscaling if you plan to submit many short-lived tasks
to your RayCluster. Otherwise, you may observe the following `thrashing` behavior:
- The autoscaler sees resource demands from the submitted short-lived tasks.
- The autoscaler immediately creates Ray pods to accomodate the demand.
- By the time the additional Ray pods are provisioned, the tasks have already run to completion.
- The additional Ray pods are unused and scale down after a period of idleness.

Note, however, that it is generally not recommended to over-parallelize with Ray.
Since running a Ray task incurs scheduling overhead, it is usually preferable to use
a few long-running tasks over many short-running tasks. Ensuring that each task has
a non-trivial amount of work to do will also help prevent the autoscaler from over-provisioning
Ray pods.

#### idleTimeoutSeconds
`IdleTimeoutSeconds` is the number of seconds to wait before scaling down a worker pod
which is not using resources. Resources in this context are the logical Ray resources
(such as CPU, GPU, memory, and custom resources) specified in Ray task and actor annotations.

`IdleTimeoutSeconds` defaults to 60 seconds.

#### resources
The `resources` subfield of `autoscalerOptions` sets optional resource overrides
for the autoscaler sidecar container. These overrides
should be specified in the standard [container resource
spec format](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#resources).
The default values are as indicated below:
```
resources:
  limits:
    cpu: "500m"
    memory: "512Mi"
  requests:
    cpu: "500m"
    memory: "512Mi"
```
These defaults should be suitable for most use-cases.
However, we do recommend monitoring autoscaler container resource usage and adjusting as needed.

#### image and imagePullPolicy
The `image` subfield of `autoscalerOptions` optionally overrides the autoscaler container image.
If your RayCluster's `spec.RayVersion` is at least `2.0.0`, the autoscaler will default to using
**the same image** as the Ray container. (Ray autoscaler code is bundled with the rest of Ray.)
For older Ray versions, the autoscaler will default to the image `rayproject/ray:2.0.0`.

The `imagePullPolicy` subfield of `autoscalerOptions` optionally overrides the autoscaler container's
image pull policy. The default is `Always`.

The `image` and `imagePullPolicy` overrides are provided primarily for the purposes of autoscaler testing and
development.

#### env and envFrom

The `env` and `envFrom` fields specify autoscaler container
environment variables, for debugging and development purposes.
These fields should be formatted following the
[Kuberentes API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#environment-variables)
for container environment variables.
