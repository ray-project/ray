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

(kuberay-autoscaling-config)=
## Autoscaler configuration
```{note}
If you are deciding whether to use autoscaling, check out the discussion {ref}`Should I enable autoscaling?`
```
To enable the optional Ray Autoscaler support, set `enableInTreeAutoscaling:true`.
The KubeRay operator will then automatically configure an autoscaling sidecar container
for the Ray head pod. The autoscaler container collects resource metrics from the Ray head container
and automatically adjusts the `replicas` field of each `workerGroupSpec` as needed to fulfill
the requirements of your Ray application.

Use the fields `minReplicas` and `maxReplicas` to constrain the `replicas` of an autoscaling
`workerGroup`. When deploying an autoscaling cluster, you would typically set `replicas` and `minReplicas` to the same value.
The Ray autoscaler will then take over and modify the `replicas` field as needed by
your Ray application.

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
to your RayCluster. Otherwise, you may observe the following thrashing behavior:
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

#### image

#### imagePullPolicy

#### env

#### envFrom

## Ray Start Parameters

## Managing compute resources

## Ports, exposing Ray services
The Ray container should expose

## Volume mounts, logging
