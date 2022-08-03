(kuberay-config)=

# RayCluster Configuration

This guide covers the key aspects of Ray cluster configuration on Kubernetes.

## Introduction

Deployments of Ray on Kubernetes follow the standard Operator pattern.

This guide covers the salient features of RayCluster CR configuration.

```yaml
apiVersion: ray.io/v1alpha1
    kind: RayCluster
    metadata:
      name: raycluster-complete
    spec:
      enableInTreeAutoscaling: True
      autoscalerOptions:
         ...
      headGroupSpec:
        rayStartParams:
          port: '6379'
          num-cpus: '1'
          ...
        template: # Pod template
            metadata: # Pod metadata
            spec: # Pod spec
                containers:
                - name: ray-head
                  image: rayproject/ray:1.12.1
                  resources:
                    limits:
                      cpu: 14
                      memory: 54Gi
                    requests:
                      cpu: "14"
                      memory: "1024Mi"
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

## Pod configuration and scale: headGroupSpec and workerGroupSpecs

At a high level, a RayCluster is a collection of Kubernetes pods, similar to a Kubernetes Deployment or StatefulSet.
Just as with the Kubernetes built-ins, the key pieces of configuration are
* Pod specification
* Scale information (how many pods are desired)

The key difference is that a RayCluster is specialized for running Ray applications.
A RayCluster consists of

* One **head pod** which hosts global control processes for the Ray cluster. The head pod can also run Ray tasks and actors.
* Any number of **worker pods**, which run Ray tasks and actors. Workers come in **groups** of identically configured pods.

The head pod’s configuration is
specified under `headGroupSpec`, while configuration for worker pods is
specified under `workerGroupSpecs`. There may be multiple worker groups,
each group with its own configuration `template`. The `replicas` field
of a `workerGroupSpec` specifies the number of worker pods of each group to
keep in the cluster.

## Autoscaling
To enable the optional Ray Autoscaler support, set `enableInTreeAutoscaling:true`.
The KubeRay operator will then automatically configure an autoscaling sidecar container
for the Ray head pod. The autoscaler container collects resource metrics from the Ray head container
and automatically adjusts the `replicas` field of each `workerGroupSpec` as needed to fulfill
the requirements of your Ray application.

Use the fields `minReplicas` and `maxReplicas` to constrain the `replicas` of an autoscaling
`workerGroup`. When deploying an autoscaling cluster, it is recommended to set `replicas` and `minReplicas` to the same value.
The Ray autoscaler will then take over and modify the replicas field as needed by
your Ray application.


`workersToDelete`

(autoscaler-pro-con)=
### Should I enable autoscaling?
Ray autoscaler support is optional.
Here are some considerations to keep in mind when choosing whether to use autoscaling.

#### Autoscaling: Pros
**Cope with unknown resource requirements** If you don't know how much compute your Ray
workload will require, autoscaling can adjust your Ray cluster to the right size.
**Save on costs** Idle compute is automatically scaled down, potentially leading to cost savings,
especially when you are using expensive resources like GPUs.

#### Autoscaling: Cons
**Less predictable when resource requirements are known.** If you already know exactly
how much compute your workload requires, it makes sense to provision a statically-sized Ray cluster.

**Longer end-to-end runtime.** Autoscaling entails provisioning compute for Ray workers
while the Ray application is running. On the other hand, if you pre-provision a fixed
number of Ray nodes,
all of the Ray nodes can be started in parallel, potentially reducing your application's
runtime.

### Ray Autoscaler vs. other autoscalers
We describe the relationship of the Ray autoscaler to other autoscalers in the Kubernetes
ecosystem.

#### Ray Autoscaler vs. Horizontal Pod Autoscaler
The Ray Autoscaler adjusts the number of Ray nodes in a Ray cluster.
On Kubernetes, we run one Ray node per Kubernetes pod. Thus in the context of Kubernetes,
the Ray autoscaler scales Ray **pod quantities**. In this sense, the relationship between
the Ray autoscaler and KubeRay operator is similar to the Horizontal Pod Autoscaler and Deployment controller.
However, the Ray Autoscaler has two key aspects which distinguish it from the HPA.
1. *Application-specific load metrics*
The Horizontal Pod Autoscaler determines scale based on actual usage metrics like CPU
and memory. By contrast, the Ray autoscaler uses Ray's logical resources.
...
In terms of the metrics it takes into account, the Ray autoscaler is thus similar
to the Kubernetes cluster autoscaler based on the logical resources specified
in container resource requests.
2. *Fine-grained control of scale-down*
To accommodate the statefulness of Ray applications, the Ray autoscaler has more
fine-grained control over scale-down than the Horizontal Pod Autoscaler -- in addition to
determining desired scale, the Ray autoscaler is able to select.
3. *Architecture*
Horizontal Pod Autoscaling is centrally managed by a manager in the Kubernetes control plane;
the manager controls the scale of many Kubernetes Objects.
By contrast, each Ray cluster is managed by its own Ray Autoscaler
attached as a sidecar container in the Ray head pod.

#### Ray Autoscaler with Kubernetes cluster autoscaler
The Ray autoscaler and Kubernetes cluster autoscaler complement each other.
When the Ray autoscaler decides to schedule a Ray pod, a Kubernetes node must be made.
(Karpenter, GKE)

#### Vertical pod autoscaler
At the moment, there is no integration between Ray and the Vertical Pod Autoscaler.

### autoscalerOptions
To enable Ray autoscaler support, it is enough to set `enableInTreeAutoscaling:true`.
Should you need to adjust autoscaling behavior or change the autoscaler container's configuration,
you can use the RayCluster CR's `autoscalerOptions` field. The `autoscalerOptions` field
carries the following subfields:

#### upscalingMode

#### idleTimeoutSeconds

#### resources

#### image

#### imagePullPolicy

#### env

#### envFrom


## Autoscaling, workersToDelete

## Ray Start Parameters

## Managing compute resources

## Ports, exposing Ray services

## Volume mounts, logging
