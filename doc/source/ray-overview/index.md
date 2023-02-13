```{include} /_includes/overview/announcement.md
```

## Overview

Ray minimizes the complexity of running your distributed individual and end-to-end machine learning workflows with these components:
* Scalable libraries for common machine learning tasks such as data preprocessing, distributed training, hyperparameter tuning, reinforcement learning, and model serving. 
* Pythonic distributed computing primitives for parallelizing and scaling Python applications.
* Integrations and utilities for integrating and deploying your Ray cluster with existing tools and infrastructure such as Kubernetes, AWS, GCP, and Azure.

Data scientists and machine learning practitioners can scale jobs without needing infrastructure expertise using Ray:
* Easily parallelize and distribute workloads across multiple nodes and GPUs.
* Quickly configure and access cloud compute resources.
* Leverage the ML ecosystem with native and extensible integrations.

Ray automatically handles key processes for distributed systems engineers:
* **Orchestration**--Managing the various components of a distributed system.
* **Scheduling**--Coordinating when and where tasks are executed.
* **Fault tolerance**--Ensuring tasks complete regardless of inevitable points of failure.
* **Auto-scaling**--Adjusting the number of resources allocated to dynamic demand.


## Ray framework

|<img src="../images/map-of-ray.png" width="70%" loading="lazy">|
|:--:|
|Stack of Ray libraries - unified toolkit for ML workloads.|

Ray's unified compute framework comprises of three layers:

1. **Ray AI Runtime**--An open-source, Python, domain-specific set of libraries that equip ML engineers, data scientists, and researchers with a scalable and unified toolkit for ML applications.
1. **Ray Core**--An open-source, Python, general purpose, distributed computing library that enables ML engineers and Python developers to scale Python applications and accelerate machine learning workloads.
1. **Ray cluster**--A set of worker nodes connected to a common Ray head node. Ray clusters can be fixed-size, or they can autoscale up and down according to the resources requested by applications running on the cluster.

## What you can do with Ray

See how Ray scales these common ML workloads:
* [Batch inference on CPUs and GPUs](workloads.html#batch-inference-on-cpus-and-gpus)
* [Model serving](workloads.html#model-serving)
* [Parallel training](workloads.html#parallel-training-of-many-models)
* [Distributed training of large models](workloads.html#distributed-training-of-large-models)
* [Parallel hyperparameter tuning experiments](workloads.html#parallel-hyperparameter-tuning-experiment)
* [Reinforcement learning](workloads.html#reinforcement-learning)
* [ML platform](workloads.html#ml-platform)

Ray has a range of libraries for different levels of control and flexibility. 

````{panels}
:container: text-left
:column: col-lg-4 px-2 py-2
:card:

**Scale machine learning workloads**
^^^
Build ML applications with a toolkit of libraries for distributed 
[data processing](data/dataset.rst), 
[model training](train/train.rst), 
[tuning](tune/index.rst), 
[reinforcement learning](rllib/index.rst), 
[model serving](serve/index.rst), 
and [more](ray-more-libs/index.rst).
+++
```{link-button} ../ray-air/getting-started
:type: ref
:text: Ray AIR
:classes: btn-outline-info btn-block
```
---

**Build distributed applications**
^^^
Build and run distributed applications with a [simple and flexible API](ray-core/walkthrough.rst).
[Parallelize](ray-core/walkthrough.rst) single machine code with little to zero code changes.

+++
```{link-button} ../ray-core/walkthrough
:type: ref
:text: Ray Core
:classes: btn-outline-info btn-block
```
---

**Deploy large-scale workloads**
^^^
Deploy workloads on [AWS, GCP, Azure](cluster/getting-started) or 
[on premise](cluster/vms/user-guides/launching-clusters/on-premises).
Use Ray cluster managers to run Ray on existing
[Kubernetes](cluster/kubernetes/index),
[YARN](cluster/vms/user-guides/community/yarn),
or [Slurm](cluster/vms/user-guides/community/slurm) clusters.
+++

```{link-button} ../cluster/getting-started
:type: ref
:text: Ray Clusters
:classes: btn-outline-info btn-block
```
````

Each of [Ray AIR's](../ray-air/getting-started) five native libraries, distributes a specific ML task:
- [Data](../data/dataset): Scalable, framework-agnostic data loading and transformation across training, tuning, and prediction.
- [Train](../train/train): Distributed multi-node and multi-core model training with fault tolerance that integrates with popular training libraries.
- [Tune](../tune/index): Scalable hyperparameter tuning to optimize model performance.
- [Serve](../serve/index): Scalable and programmable serving to deploy models for online inference, with optional microbatching to improve performance.
- [RLlib](../rllib/index): Scalable distributed reinforcement learning workloads that integrate with the other Ray AIR libraries.

For more advanced users, [Ray Core](../ray-core/walkthrough) is the library that enables Python developers to easily build scalable, distributed systems that can run on a laptop, cluster, cloud or Kubernetes. It's the foundation that Ray's ML libraries (Ray AIR) and third-party integrations (Ray ecosystem) are built on.

Ray gives you flexibility with easy-to-use primitives in native Python code for composing distributed applications. Getting started with Ray Core involves a few key abstractions:

- [Tasks](../ray-core/tasks): Stateless Python functions executed in the cluster.
- [Actors](../ray-core/actors): Stateful Python classes (worker processes) created in the cluster.
- [Objects](../ray-core/objects): Immutable values accessible across the cluster; cached in Ray's distributed [shared-memory](https://en.wikipedia.org/wiki/Shared_memory) object store.

Ray runs on any machine, cluster, cloud provider, and Kubernetes, and features a growing
[ecosystem of community integrations](ray-libraries).

