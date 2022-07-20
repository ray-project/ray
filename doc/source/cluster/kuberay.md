# Ray on Kubernetes

(kuberay-index)=

## Overview

You can leverage your Kubernetes cluster as a substrate for execution of distributed Ray programs.

The KubeRay Operator provides a Kubernetes-native interface for managing Ray clusters. Each Ray
cluster consist of a head pod and collection of worker pods. Optional Ray Autoscaler support allows
the KubeRay Operator to size your Ray clusters according to the needs of your Ray workload, adding
and removing Ray pods as needed.

## Learn More

The Ray docs present all the information you need to start running Ray workloads on Kubernetes.

.. panels::
    :container: text-center
    :column: col-lg-6 px-2 py-2
    :card:

    **Quick start**
    ^^^

    Learn how to start a Ray cluster and deploy Ray applications on Kubernetes.


## The KubeRay project

Ray's Kubernetes support is developed at the KubeRay GitHub repository, under the broader Ray project.

- Visit the KubeRay GitHub repo to track progress, report bugs, propose new features, or contribute to
the project.
- Check out the KubeRay documentation for further technical information, developer guides,
and discussion of new and upcoming features.
