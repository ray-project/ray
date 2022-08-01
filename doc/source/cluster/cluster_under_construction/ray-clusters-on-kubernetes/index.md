# Ray on Kubernetes
(kuberay-index)=
## Overview

You can execute your distributed Ray programs on a Kubernetes cluster.

The [KubeRay Operator](https://ray-project.github.io/kuberay/components/operator/) provides a Kubernetes-native
interface for managing Ray clusters. Each Ray cluster consist of a head pod and collection of worker pods.
Optional autoscaling support allows the KubeRay Operator to size your Ray clusters according to the requirements
of your Ray workload, adding and removing Ray pods as needed.

## Learn More

The Ray docs present all the information you need to start running Ray workloads on Kubernetes.

```{eval-rst}
.. panels::
    :container: text-center
    :column: col-lg-12 p-2
    :card:

    **Getting started**
    ^^^

    Learn how to start a Ray cluster and deploy Ray applications on Kubernetes.

    +++
    .. link-button:: kuberay-quickstart
        :type: ref
        :text: Get Started with Ray on Kubernetes
        :classes: btn-outline-info btn-block
```
## The KubeRay project

Ray's Kubernetes support is developed at the [KubeRay GitHub repository](https://github.com/ray-project/kuberay), under the broader [Ray project](https://github.com/ray-project/).

- Visit the [KubeRay GitHub repo](https://github.com/ray-project/kuberay) to track progress, report bugs, propose new features, or contribute to
the project.
- Check out the [KubeRay docs](https://ray-project.github.io/kuberay/) for further technical information, developer guides,
and discussion of new and upcoming features.
