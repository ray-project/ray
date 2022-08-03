(kuberay-config)=

# RayCluster Configuration

This guide covers the key aspects of Ray cluster configuration on Kubernetes.

## Introduction

Deployments of Ray on Kubernetes follow the standard Operator pattern.

This guide covers the salient features of RayCluster CR configuration.

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
      - groupName: medium-group
        ...

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

### Should I enable autoscaling?

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
