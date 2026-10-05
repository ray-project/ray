---
myst:
  html_meta:
    description: "Run Ray on Kubernetes with KubeRay, the officially supported operator, and find KubeRay guides that apply more broadly, such as storage, image pull latency, and scheduling."
---

(ray-on-kubernetes)=

# Ray on Kubernetes

This page describes how to run Ray on Kubernetes and points to KubeRay guides that apply beyond KubeRay. On Kubernetes, each Ray node runs as a Pod.

## Deploy Ray on Kubernetes

KubeRay is the officially supported way to run Ray on Kubernetes. KubeRay is an open source Kubernetes operator that deploys and manages Ray clusters through custom resources such as RayCluster, RayJob, and RayService. To get started, see {ref}`kuberay-index`.

You can also deploy Ray on Kubernetes manually, without an operator. A manual deployment isn't officially supported, and you create and manage the Ray cluster's Pods yourself.

Anyscale, the managed Ray platform, runs Ray on Kubernetes through its own operator. See [Anyscale on Kubernetes](https://docs.anyscale.com/clouds/kubernetes?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=ray-on-kubernetes).

## KubeRay guides that apply more broadly

The following KubeRay guides cover topics that don't depend on the KubeRay operator, so they can also help with other Ray on Kubernetes deployments. Their examples use KubeRay.

* {ref}`kuberay-storage`: Where to put code, artifacts, and package dependencies for interactive development and for production.
* {ref}`reduce-image-pull-latency`: Strategies for shortening Ray container image pulls, such as a DaemonSet that pulls images onto every node ahead of time.
* {ref}`kuberay-k8s-setup`: Create managed Kubernetes clusters with GPU or TPU nodes on Google Cloud, AWS, Azure, and Alibaba Cloud.
* {ref}`kuberay-scheduling`: How the Kubernetes scheduler and the Ray scheduler divide placement decisions, and which one to investigate when a workload stalls.
