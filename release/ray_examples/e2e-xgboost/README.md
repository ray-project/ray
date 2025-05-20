# End-to-end XGBoost on Anyscale

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/anyscale/e2e-xgboost" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>


## Overview

<div align="center">
  <img src="https://raw.githubusercontent.com/anyscale/e2e-xgboost/refs/heads/main/images/overview.png" width=800>
</div>

This tutorial implements an end-to-end XGBoost application on Anyscale, covering:


- **Distributed data preprocessing and model training**: Ingest and preprocess data at scale using [Ray Data](https://docs.ray.io/en/latest/data/data.html). Then, train a distributed [XGBoost model](https://xgboost.readthedocs.io/en/stable/python/index.html) using [Ray Train](https://docs.ray.io/en/latest/train/train.html) `notebooks/01-Distributed_Training.ipynb`
- **Model Validation using Offline Inference**: Evaluate the model using Ray Data offline batch inference `notebooks/02-Validation.ipynb`.
- **Online Model Serving**: Deploy the model as a scalable online service using [Ray Serve](https://docs.ray.io/en/latest/serve/index.html). For example, `notebooks/03-Serving.ipynb`
- **Production Deployment**: Create production batch [**Jobs**](https://docs.anyscale.com/platform/jobs/) for offline workloads (data prep, training, batch prediction) and potentially online [**Services**](https://docs.anyscale.com/platform/services/).


## Dependencies
Install the dependencies using `pip`:

```bash
pip install -r requirements.txt
# For development only, install the dev dependencies:
# pip install -r requirements_dev.txt
# Install the dist_xgboost package
pip install -e .
```

## Development

This application was developed on [Anyscale Workspaces](https://docs.anyscale.com/platform/workspaces/), enabling development without thinking about infrastructure, just like on a laptop. Workspaces come with:
- **Development tools**: Spin up a remote session from your local IDE (cursor, VSCode) and start coding, using the same tools you love but with the power of Anyscale's compute.
- **Dependencies**: Continue to install dependencies using familiar tools like pip. Anyscale ensures proper dependency distribution to your cluster.
- **Scalable Compute**: Leverage any reserved instance capacity, spot instance from any compute provider of your choice by deploying Anyscale into your account. Alternatively, you can use the Anyscale cloud for a full cloud-native experience.
  - Under the hood, Anyscale creates and smartly manages a cluster.
- **Debugging**: Leverage a [distributed debugger](https://docs.anyscale.com/platform/workspaces/workspaces-debugging/#distributed-debugger) to get the same VSCode-like debugging experience.

Learn more about Anyscale Workspaces through the [official documentation](https://docs.anyscale.com/platform/workspaces/).

<div align="center">
  <img src="https://raw.githubusercontent.com/anyscale/e2e-xgboost/refs/heads/main/images/compute.png" width=600>
</div>

## Production
Seamlessly integrate with your existing CI/CD pipelines by leveraging the Anyscale [CLI](https://docs.anyscale.com/reference/quickstart-cli) or [SDK](https://docs.anyscale.com/reference/quickstart-sdk) to deploy [highly available services](https://docs.anyscale.com/platform/services) and run [reliable batch jobs](https://docs.anyscale.com/platform/jobs). Development in an environment that's almost identical to production (multi-node cluster) drastically speeds up dev → prod velocity. This includes proprietary RayTurbo features to optimize workloads for performance, fault tolerance, scale, and observability.


## No infrastructure headaches
Abstract away infrastructure from your ML/AI developers so they can focus on their core ML development. You can additionally better manage compute resources and costs with the [enterprise governance and observability](https://www.anyscale.com/blog/enterprise-governance-observability) and [administrator capabilities](https://docs.anyscale.com/administration/overview) so you can set [resource quotas](https://docs.anyscale.com/reference/resource-quotas/), set [priorities for different workloads](https://docs.anyscale.com/administration/cloud-deployment/global-resource-scheduler) and gain [observability of your utilization across your entire compute fleet](https://docs.anyscale.com/administration/resource-management/telescope-dashboard).
If you're already on a Kubernetes cloud (Amazon Elastic Kubernetes Service, Google Kubernetes Engine, etc.), then you can still leverage the proprietary optimizations from RayTubo you can see in action in these tutorials through the [Anyscale K8s Operator](https://docs.anyscale.com/administration/cloud-deployment/kubernetes/).

Below is a list of infrastructure headaches Anyscale removes so you can focus on your ML development.👇

<details>
  <summary>Click <b>here</b> to see the infrastructure pains Anyscale removes</summary>

**🚀 1. Fast Workload Launch**
* With Kubernetes (Amazon Elastic Kubernetes Service/Google Kubernetes Engine), you must manually create a cluster before launching anything.
* This includes setting up VPCs, Identity and Access Management roles, node pools, autoscaling, etc.
* Anyscale handles all of this automatically-- you just define your job or endpoint and run it.

**⚙️ 2. No GPU Driver Hassles**
* Kubernetes requires you to install and manage Nvidia drivers and the device plugin for GPU workloads.
* On Anyscale, GPU environments just work—drivers, libraries, and runtime are pre-configured.

**📦 3. No KubeRay or CRD Management**
* Running Ray on K8s needs:
    * Installing KubeRay
    * Writing and maintaining custom YAML manifests
    * Managing Custom Resource Definitions
    * Tuning stateful sets and pod configs
* On Anyscale, this is all abstracted—you launch Ray clusters without writing a single YAML file.

**🧠 4. No Need to Learn K8s Internals**
* With Kubernetes, users must:
    * Inspect pods/logs
    * Navigate dashboards
    * Manually send http requests to Ray endpoints
* Anyscale users never touch pods. Everything is accessible through the CLI, SDK, or UI.

**💸 5. Spot Instance Handling Just Works**
* Kubernetes requires custom node pools and lifecycle handling for spot instance interruptions.
* With Anyscale, preemptible VMs are automatically managed with node draining and rescheduling.

</details>

<div></div>
