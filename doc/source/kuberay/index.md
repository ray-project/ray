---
myst:
  html_meta:
    description: "KubeRay, the officially supported Kubernetes operator for Ray: the RayCluster, RayJob, RayService, and RayCronJob custom resources, autoscaling, and GPU support."
---

(kuberay-index)=

# KubeRay

```{toctree}
:hidden:

getting-started/index
user-guides/index
examples/index
ecosystem/index
benchmarks/index
troubleshooting/index
references/index
```

[KubeRay](https://github.com/ray-project/kuberay) is the officially supported Kubernetes operator for Ray. It provides a Kubernetes-native way to deploy and manage Ray clusters. KubeRay runs each Ray node as a Kubernetes Pod, so each Ray cluster consists of a head Pod and a collection of worker Pods.

```{eval-rst}
.. image:: images/ray_on_kubernetes.png
    :align: center
..
  Find source document here: https://docs.google.com/drawings/d/1E3FQgWWLuj8y2zPdKXjoWKrfwgYXw6RV_FWRwK8dVlg/edit
```

KubeRay adds four custom resources:

* **RayCluster**: Creates a Ray cluster and manages its lifecycle, including autoscaling and fault tolerance.
* **RayJob**: Creates a RayCluster, submits a Ray job when the cluster is ready, and can delete the RayCluster when the job finishes.
* **RayService**: Runs Ray Serve applications on a RayCluster, with zero-downtime upgrades and high availability.
* **RayCronJob**: Creates RayJobs on a recurring cron schedule. RayCronJob is in alpha and requires KubeRay 1.6.0 or later.

With optional autoscaling, KubeRay sizes your Ray clusters to the requirements of your Ray workload, adding and removing Pods as needed. KubeRay supports heterogeneous compute nodes, including GPUs, and runs multiple Ray clusters with different Ray versions in the same Kubernetes cluster.

To learn the basics of KubeRay and run your first Ray application with it, see {ref}`kuberay-quickstart` and the following quickstart guides:

* [RayCluster Quick Start](kuberay-raycluster-quickstart)
* [RayJob Quick Start](kuberay-rayjob-quickstart)
* [RayService Quick Start](kuberay-rayservice-quickstart)
* [RayCronJob Quick Start](kuberay-raycronjob-quickstart)

For guidance that applies to Ray on Kubernetes in general, such as storage and container image pull latency, see {ref}`ray-on-kubernetes`.

Additionally, [Anyscale](https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=deploy-ray-on-k8s) is the managed Ray platform developed by the creators of Ray. It offers an easy path to deploy Ray clusters on your existing Kubernetes infrastructure, including EKS, GKE, AKS, or self-hosted Kubernetes.

## Learn More

The Ray docs present all the information you need to start running Ray workloads on Kubernetes.

::::{grid} 1 2 2 2
:gutter: 1
:class-container: container pb-3

:::{grid-item-card}

**Getting Started**
^^^

Learn how to start a Ray cluster and deploy Ray applications on Kubernetes.

+++
```{button-ref} kuberay-quickstart
:color: primary
:outline:
:expand:

Get Started with Ray on Kubernetes
```
:::

:::{grid-item-card}

**User Guides**
^^^

Learn best practices for configuring Ray clusters on Kubernetes.

+++
```{button-ref} kuberay-guides
:color: primary
:outline:
:expand:

Read the User Guides
```
:::

:::{grid-item-card}

**Examples**
^^^

Try example Ray workloads on Kubernetes.

+++
```{button-ref} kuberay-examples
:color: primary
:outline:
:expand:

Try example workloads
```
:::

:::{grid-item-card}

**Ecosystem**
^^^

Integrate KubeRay with third party Kubernetes ecosystem tools.

+++
```{button-ref} kuberay-ecosystem-integration
:color: primary
:outline:
:expand:

Ecosystem Guides
```
:::

:::{grid-item-card}

**Benchmarks**
^^^

Check the KubeRay benchmark results.

+++
```{button-ref} kuberay-benchmarks
:color: primary
:outline:
:expand:

Benchmark results
```
:::

:::{grid-item-card}

**Troubleshooting**
^^^

Consult the KubeRay troubleshooting guides.

+++
```{button-ref} kuberay-troubleshooting
:color: primary
:outline:
:expand:

Troubleshooting guides
```
:::
::::
## About KubeRay

Ray's Kubernetes support is developed at the [KubeRay GitHub repository](https://github.com/ray-project/kuberay), under the broader [Ray project](https://github.com/ray-project/). KubeRay is used by several companies to run production Ray deployments.

- Visit the [KubeRay GitHub repo](https://github.com/ray-project/kuberay) to track progress, report bugs, propose new features, or contribute to the project.
