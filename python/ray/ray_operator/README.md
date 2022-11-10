# The Legacy Ray Operator

This directory carries source code for the legacy Ray Operator.

Using the [KubeRay operator](https://ray-project.github.io/kuberay/components/operator/)
is the preferred way to deploy Ray on Kubernetes. Refer to the latest [Ray documentation](https://docs.ray.io/en/latest/)
for up-to-date guides on Ray's Kubernetes support.

For documentation on the legacy Ray Operator,
refer to the [Ray 1.13.0 documentation](https://docs.ray.io/en/releases-1.13.0/cluster/kubernetes.html#deploying-on-kubernetes).

## Deprecation timeline

This section outlines the deprecation timeline for the legacy Ray Operator.

- As of Ray 2.0.0, the legacy Ray Operator is in maintenance mode.
- The operator will still be compatible with Ray 2.1.0.
- **The legacy Ray Operator will be removed in Ray 2.2.0.**

_In other words, it will not be possible to use the legacy Ray Operator with Ray versions
2.2.0 or greater._

The rest of this document
- Compares the KubeRay operator to the legacy Ray Operator hosted in the Ray repo.
- Provides migration notes for users switching to the KubeRay operator.

## KubeRay vs. Legacy Ray Operator: Similarities

### Purpose
The two operators have the same purpose: managing clusters of Ray pods deployed on Kubernetes.

### High-level interface structure
Both operators rely on a user-specified custom resource specifying Ray pod configuration and
Ray worker pod quantities.

## KubeRay vs. Legacy Ray Operator: Differences

The two operators differ primarily in internal design and implementation.
There are also some differences in configuration details.

### Implementation and architecture
**Legacy Ray Operator** The legacy Ray Operator is implemented in Python.
Kubernetes event handling is implemented using the [Kopf](https://kopf.readthedocs.io/en/stable/) framework.
The operator invokes Ray cluster launcher and autoscaler code to manage Ray clusters.
The operator forks an autoscaler subprocess for each Ray cluster it manages.
The Ray autoscaler subprocesses create and delete Ray pods directly.

**KubeRay Operator** The KubeRay operator is implemented in Golang using standard tools
for building Kubernetes operators, including the [KubeBuilder](https://github.com/kubernetes-sigs/kubebuilder)
operator framework
and the [client-go](https://github.com/kubernetes/client-go) client library.
The KubeRay operator is structurally simpler than the legacy Ray Operator;
rather than running many Ray autoscalers in subprocesses, the KubeRay operator implements a simple
reconciliation loop. The reconciliation loop creates and deletes Ray pods to match the desired
state expressed in each RayCluster CR.
Each Ray cluster runs its own autoscaler as a sidecar to the Ray head pod.
The Ray autoscaler communicates desired scale to the KubeRay operator by writing to the RayCluster
custom resource.

### Scalability
The KubeRay operator is more scalable than the legacy Ray Operator. Specifically, the
KubeRay operator can simultaneously manage more Ray clusters.

**Legacy Ray Operator** Each Ray autoscaler consumes nontrivial memory and CPU resources.
Since the legacy Ray Operator runs many autoscalers in one pod, it cannot manage many Ray clusters.

**KubeRay Operator** The KubeRay operator does not run Ray autoscaler processes.
Each Ray autoscaler runs as a sidecar to the Ray head. Since managing each Ray cluster is cheap,
the KubeRay operator can manage many Ray clusters.

### Ray version compatibility

**Legacy Ray Operator**
It is recommended to use the same Ray version in the legacy Ray operator
as in all of the Ray pods managed by the operator.
Matching Ray versions is required to maintain compatibility between autoscaler code
running in the operator pod and Ray code running in the Ray cluster.

**KubeRay Operator**
The KubeRay operator is compatible with many Ray versions.
Compatibility of KubeRay v0.3.0 with Ray versions 1.11, 1.12, 1.13, and 2.0 is tested explicitly.

Note however that autoscaling with KubeRay is supported only with Ray versions
at least as new as 1.11.0.

### Configuration details
Some details of Ray cluster configuration are different with KubeRay; see the next section
for migration notes. Refer to the [Ray documentation](https://docs.ray.io/en/latest/) for comprehensive
information on KubeRay configuration.

## Migration Notes

Take note of the following configuration differences when switching to KubeRay
deployment.

### Delete the Old CRD!
The legacy Ray Operator's RayCluster CRD has the fully-qualified name
`rayclusters.cluster.ray.io`, while KubeRay's RayCluster CRD is named `rayclusters.ray.io`.
To avoid unexpected name conflicts, be sure to delete the old CRD before using KubeRay:
```shell
kubectl delete crd rayclusters.cluster.ray.io
```


### Autoscaling is optional
Ray Autoscaler support is optional with KubeRay. Set `spec.enableInTreeAutoscaling:true`
in the RayCluster CR to enable autoscaling. The KubeRay operator will then automatically
configure a Ray Autoscaler sidecar for the Ray head pod.
The autoscaler container requests 500m CPU and 512Mi memory by default.
Autoscaler container configuration is accessible via `spec.autoscalerOptions`.
Note that autoscaling with KubeRay is supported only with Ray versions at least as new 1.11.

### No need to specify /dev/shm
The KubeRay operator automatically configures a `/dev/shm` volume for each Ray pod's object store.
There is no need to specify this volume in the RayCluster CR.

### Don't specify metadata.name in pod templates
Do not specify `metadata.name` in the pod templates specifying head pod and worker pod
configuration. As of KubeRay 0.3.0, specifying pod template names may prevent the KubeRay operator from
launching multiple worker pods. See [KubeRay issue 582][KubeRay582].

The KubeRay operator chooses unique pod names using the Ray cluster name
and worker group `groupName`.

### Namespace-scoped operation.
Similar to the legacy Ray Operator, it is possible to run the KubeRay operator at single-namespace scope.
See the [KubeRay documentation][KubeRaySingleNamespace] for details.

Note that the KubeRay operator can manage many Ray clusters running at different Ray versions.
Thus, from a scalability and compatibility perspective, there is no need to run
one KubeRay operator per Kubernetes namespace. Run a namespace-scoped KubeRay operator
only if necessary, e.g. to accommodate permissions constraints in your Kubernetes cluster.

### Specifying resource quantities.
Ray pod CPU, GPU, and memory capacities are detected from container resource limits and advertised
to Ray.

The interface for overriding the resource capacities advertised to Ray is different:
Resource overrides must be specified in `rayStartParams`.
For example, you may wish to prevent the Ray head pod
from running Ray workloads by labelling the head as having 0 CPU capacity.
To achieve this with KubeRay, include the following in the `headGroupSpec`'s configuration:
```yaml
rayStartParams:
    num-cpus: "0"
```
To advertise custom resource capacities to Ray, one uses the field `rayStartParams.resources`.

[KuberaySingleNamespace]: https://github.com/ray-project/kuberay#single-namespace-version

### Ray Version
The Ray version (e.g. 2.0.0) should be supplied under the RayCluster CR's `spec.rayVersion`.

### Init Containers and Pre-Stop hooks
There are two pieces of configuration that should be included in all KubeRay RayCluster CRs.
- Worker pods need an init container that awaits creation of the Ray head service.
- Ray containers for the Ray head and worker should include a preStop hook with a `ray stop`
  command.
While future versions of KubeRay may inject this configuration automatically,
currently these elements must be included in all RayCluster CRs.

## Migration: Example
This section presents an example of the migration process.
Specifically, we translate a Helm values.yaml configuration for the legacy Ray Operator into
an example RayCluster CR for KubeRay.
We also recommend taking a look at example RayCluster CRs in the [Ray docs][RayExamples]
and in the [KubeRay docs][KubeRayExamples].

### Legacy Ray Operator values.yaml
Here is a `values.yaml` for the legacy Ray Operator's Helm chart which specifies a Ray cluster
with the following features
- A head pod annotated with a `"CPU":0` override to prevent scheduling Ray workloads on the head.
- A CPU worker group annotated with custom resource capacities.
- A GPU worker group.
```yaml
image: rayproject/ray-ml:2.0.0-gpu
headPodType: rayHeadType
podTypes:
    rayHeadType:
        CPU: 14
        memory: 54Gi
        # Annotate the head pod as having 0 CPU
        # to prevent the head pod from scheduling Ray workloads.
        rayResources: {"CPU": 0}
    rayCPUWorkerType:
        # Start with 2 CPU workers. Allow scaling up to 3 CPU workers.
        minWorkers: 2
        maxWorkers: 3
        memory: 54Gi
        CPU: 14
        # Annotate the Ray worker pod as having 1 unit of Custom capacity and 5 units of "Custom2" capacity
        rayResources: {"Custom": 1, "Custom2": 5}
    rayGPUWorkerType:
        minWorkers: 0
        maxWorkers: 5
        CPU: 3
        GPU: 1
        memory: 50Gi

operatorImage: rayproject/ray:2.0.0
```

### KubeRay RayCluster CR
In this section, we present an annotated KubeRay RayCluster CR equivalent to the above legacy Ray Operator Helm configuration.

> **_NOTE:_** The configuration below is more verbose, as it does not employ Helm.
> Helm support for KubeRay is in progress; to try it, out read KubeRay's [Helm docs][KubeRayHelm].
> KubeRay's Helm charts can be found on GitHub [here][KubeRayHelmCode].
> Currently, we recommend directly deploying KubeRay RayCluster CRs without Helm.

Here is a [link][ConfigLink] to the configuration shown below.

```yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  labels:
    controller-tools.k8s.io: "1.0"
  name: raycluster-example
spec:
  # To use autoscaling, the following field must be included.
  enableInTreeAutoscaling: true
  # The Ray version must be supplied.
  rayVersion: '2.0.0'
  headGroupSpec:
    serviceType: ClusterIP
    rayStartParams:
      dashboard-host: '0.0.0.0'
      block: 'true'
      # Annotate the head pod as having 0 CPU
      # to prevent the head pod from scheduling Ray workloads.
      num-cpus: 0
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray-ml:2.0.0-gpu
          resources:
            limits:
              cpu: "14"
              memory: "54Gi"
            requests:
              cpu: "14"
              memory: "54Gi"
          # Keep this in container configs.
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","ray stop"]
  workerGroupSpecs:
  # Start with 2 CPU workers. Allow scaling up to 3 CPU workers.
  - replicas: 2
    minReplicas: 2
    maxReplicas: 3
    groupName: rayCPUWorkerType
    rayStartParams:
      block: 'true'
      # Annotate the Ray worker pod as having 1 unit of "Custom" capacity and 5 units of "Custom2" capacity
      resources: '"{\"Custom\": 1, \"Custom2\": 5}"'
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray-ml:2.0.0-gpu
          resources:
            limits:
              cpu: "14"
              memory: "54Gi"
            requests:
              cpu: "14"
              memory: "54Gi"
          # Keep the lifecycle block in Ray container configs.
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","ray stop"]
        # Keep the initContainers block in worker pod configs.
        initContainers:
        - name: init-myservice
          image: busybox:1.28
          command: ['sh', '-c', "until nslookup $RAY_IP.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local; do echo waiting for myservice; sleep 2; done"]
  # Start with 0 GPU workers. Allow scaling up to 5 GPU workers.
  - replicas: 0
    minReplicas: 0
    maxReplicas: 5
    groupName: rayGPUWorkerType
    rayStartParams:
      block: 'true'
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray-ml:2.0.0-gpu
          resources:
            limits:
              cpu: "3"
              memory: "50Gi"
              nvidia.com/gpu: 1
            requests:
              cpu: "3"
              memory: "50Gi"
              nvidia.com/gpu: 1
          # Keep the lifecycle block in Ray container configs.
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","ray stop"]
        # Keep the initContainers block in worker pod configs.
        initContainers:
        - name: init-myservice
          image: busybox:1.28
          command: ['sh', '-c', "until nslookup $RAY_IP.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local; do echo waiting for myservice; sleep 2; done"]
# Operator configuration is not specified here -- the KubeRay operator should be deployed before creating Ray clusters.
```
<!-- TODO: fix this  -->
<!-- [RayExamples]: https://github.com/ray-project/ray/tree/master/doc/source/cluster/kubernetes/configs -->
[RayExamples]: https://github.com/ray-project/ray/tree/master/doc/source/cluster/kubernetes/configs
[KubeRayExamples]: https://ray-project.github.io/kuberay/components/operator/#running-an-example-cluster
[ConfigLink]: https://github.com/ray-project/ray/tree/master/doc/source/cluster/kubernetes/configs/migration-example.yaml
<!-- [ConfigLink]: https://raw.githubusercontent.com/ray-project/ray/7aeb1ab9cf7adb58fd9418c0e08984ff0fe6d018/doc/source/cluster/ray-clusters-on-kubernetes/configs/migration-example.yaml -->
[KubeRayHelm]: https://ray-project.github.io/kuberay/deploy/helm/
[KubeRayHelmCode]: https://github.com/ray-project/kuberay/tree/master/helm-chart
[KubeRay582]: https://github.com/ray-project/kuberay/issues/582
