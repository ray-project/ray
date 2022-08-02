(kuberay-config)=

# RayCluster Configuration

This guide covers the key aspects of Ray cluster configuration on Kubernetes.

## Introduction

Deployments of Ray on Kubernetes follow the standard Operator pattern.

This guide covers the salient features of RayCluster CR configuration.

## The headGroupSpec and workerGroupSpecs

A RayCluster is a grouping of Kubernetes pods, similar to a RayCluster Deployment or StatefulSet.
The bulk of the configuration goes into determining.

The key difference is that a RayCluster is specialized for running Ray applications.
In particular, a RayCluster consists of 

- One **head pod** which hosts global control processes for the Ray cluster. The head pod can also run Ray tasks and actors.
- Any number of **worker pods**, which run Ray tasks and actors. Workers come in **groups** of identically configured pods.

The field `headGroupSpec` carries configuration for the head pod.

A RayCluster

```{note}
The word "group" in `headGroupSpec` is misleading:
a RayCluster has only one head pod.
The field `headGroupSpec.replicas` is optional and deprecated in KubeRay versions
since 0.3.0.
```

## Autoscaling, workersToDelete

## Ray Start Parameters

## Managing compute resources

## Ports, exposing Ray services

## Volume mounts, logging
