# Ray on Kubernetes
(kuberay-index)=
## Overview

In this section we cover how to execute your distributed Ray programs on a Kubernetes cluster.

Using the [KubeRay Operator](https://ray-project.github.io/kuberay/components/operator/) is the
recommended way to do so. The operator provides a Kubernetes-native way to manage Ray clusters.
Each Ray cluster consists of a head node pod and a collection of worker node pods. Optional
autoscaling support allows the KubeRay Operator to size your Ray clusters according to the
requirements of your Ray workload, adding and removing Ray pods as needed. KubeRay supports
heterogenous compute nodes (including GPUs) as well as running multiple Ray clusters with
different Ray versions in the same Kubernetes cluster.

```{eval-rst}
.. image:: images/ray_on_kubernetes.png
    :align: center
..
  Find source document here: https://docs.google.com/drawings/d/1E3FQgWWLuj8y2zPdKXjoWKrfwgYXw6RV_FWRwK8dVlg/edit
```


Concretely, you will learn how to:

- Set up and configure Ray on a Kubernetes cluster
- Deploy and monitor Ray applications
- Integrate Ray applications with Kubernetes networking

## Learn More

The Ray docs present all the information you need to start running Ray workloads on Kubernetes.

```{eval-rst}
.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Getting Started**
    ^^^

    Learn how to start a Ray cluster and deploy Ray applications on Kubernetes.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Get Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block
    ---
    **Examples**
    ^^^

    Try example Ray workloads on Kubernetes.

    +++
    .. link-button:: kuberay-examples
        :type: ref
        :text: Try example workloads
        :classes: btn-outline-info btn-block
    ---
    **User Guides**
    ^^^

    Learn best practices for configuring Ray clusters on Kubernetes.

    +++
    .. link-button:: kuberay-guides
        :type: ref
        :text: Read the User Guides
        :classes: btn-outline-info btn-block
    ---
    **API Reference**
    ^^^

    Find API references on RayCluster configuration.

    +++
    .. link-button:: kuberay-api-reference
        :type: ref
        :text: Check API references
        :classes: btn-outline-info btn-block
```
## About KubeRay

Ray's Kubernetes support is developed at the [KubeRay GitHub repository](https://github.com/ray-project/kuberay), under the broader [Ray project](https://github.com/ray-project/). KubeRay is used by several companies to run production Ray deployments.

- Visit the [KubeRay GitHub repo](https://github.com/ray-project/kuberay) to track progress, report bugs, propose new features, or contribute to
the project.
- Check out the [KubeRay docs](https://ray-project.github.io/kuberay/) for further technical information, developer guides,
and discussion of new and upcoming features.

```{note}
The KubeRay operator replaces the older Ray operator previously hosted in the [Ray repository](https://github.com/ray-project/ray/tree/releases/2.1.0/python/ray/ray_operator).
The legacy Ray operator is compatible with Ray versions up to Ray 2.1.0.
However, **the legacy Ray operator cannot be used with Ray 2.2.0 or newer.**
Check the linked README for migration notes.

If you have used the legacy Ray operator in the past,
make sure to de-register that operator's CRD before
using KubeRay:
```shell
kubectl delete crd rayclusters.cluster.ray.io
```
