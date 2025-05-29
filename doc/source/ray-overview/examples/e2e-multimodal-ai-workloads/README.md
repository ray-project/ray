# Image Semantic Search and Classification

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/anyscale/foundational-ray-app" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>


### Overview

In this tutorial, we'll be implementing an image semantic search application that leverages batch inference, distributed training and online serving at scale.

- [**`01-Batch-Inference.ipynb`**](https://github.com/anyscale/foundational-ray-app/tree/main/notebooks/01-Batch-Inference.ipynb): ingest and preprocess data at scale using [Ray Data](https://docs.ray.io/en/latest/data/data.html) to generate embeddings for an image dataset (e.g., different dog breeds) and store them.
- [**`02-Distributed-Training.ipynb`**](https://github.com/anyscale/foundational-ray-app/tree/main/notebooks/02-Distributed-Training.ipynb): reprocess the same data to train an image classifier using [Ray Train](https://docs.ray.io/en/latest/train/train.html) and saving model artifacts to a model registry (MLOps).
- [**`03-Online-Serving.ipynb`**](https://github.com/anyscale/foundational-ray-app/tree/main/notebooks/03-Online-Serving.ipynb): serve a semantic search application using [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) that uses model predictions to filter and retrieve the most relevant images based on input queries.
- Create production batch [**Jobs**](https://docs.anyscale.com/platform/jobs/) for offline workloads (embedding generation, model training, etc.) and production online [**Services**](https://docs.anyscale.com/platform/services/) that can scale.

<img src="https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/overview.png" width=900>

### Development

We're developing our application on [Anyscale Workspaces](https://docs.anyscale.com/platform/workspaces/), which enables us to develop without thinking about infrastructure, just like we would on a laptop. Workspaces come with:
- **Development tools**: Spin up a remote session from your local IDE (cursor, vscode, etc.) and start coding, using the same tools you love but with the power of Anyscale's compute.
- **Dependencies**: Continue to install dependencies using familiar tools like pip. Anyscale will ensure dependencies are being propagated to your cluster.

```bash
pip install -q "matplotlib==3.10.0" "torch==2.5.1" "transformers==4.47.1" "scikit-learn==1.6.0" "mlflow==2.19.0" "ipywidgets"
```

- **Compute**: Leverage any reserved instance capacity, spot instance from any compute provider of your choice by deploying Anyscale into your account. Alternatively, you can use the Anyscale cloud for a full serverless experience.
  - Under the hood, a cluster will be spun up and smartly managed by Anyscale.
- **Debugging**: Leverage a [distributed debugger](https://docs.anyscale.com/platform/workspaces/workspaces-debugging/#distributed-debugger) to get the same VSCode-like debugging experience.

Learn more about Anyscale Workspaces through the [official documentation](https://docs.anyscale.com/platform/workspaces/).

<div align="center">
  <img src="https://raw.githubusercontent.com/anyscale/foundational-ray-app/refs/heads/main/images/compute.png" width=600>
</div>

**Note**: If you're on [Anyscale](https://console.anyscale.com/), you can run this entire tutorial for free (all dependencies are setup and the necessary compute will autoscale). Otherwise be sure to install the dependencies from the [`containerfile`](https://github.com/anyscale/foundational-ray-app/tree/main/containerfile) and provision the appropriate GPU resources.

### Production
Seamlessly integrate with your existing CI/CD pipelines by leveraging the Anyscale [CLI](https://docs.anyscale.com/reference/quickstart-cli) or [SDK](https://docs.anyscale.com/reference/quickstart-sdk) to deploy [highly available services](https://docs.anyscale.com/platform/services) and run [reliable batch jobs](https://docs.anyscale.com/platform/jobs). Given we've been developing in an environment that's almost identical to production (multinode cluster), this should drastically speed up our dev → prod velocity. We'll also learn about proprietary RayTurbo features to optimize our workloads for performance, fault tolerance, scale and observability.


### No infrastructure headaches
Abstract away infrastructure from your ML/AI developers so they can focus on their core ML development. You can additionally better manage compute resources and costs with our [enterprise governance and observability](https://www.anyscale.com/blog/enterprise-governance-observability) and [admin capabilities](https://docs.anyscale.com/administration/overview) so you can set [resource quotas](https://docs.anyscale.com/reference/resource-quotas/), set [priorities for different workloads](https://docs.anyscale.com/administration/cloud-deployment/global-resource-scheduler) and gain [observability of your utilization across your entire compute fleet](https://docs.anyscale.com/administration/resource-management/telescope-dashboard).
If you're already on a kubernetes cloud (EKS, GKE, etc.), then you can still leverage the proprietary optimizations from RayTubo you'll see in action in these tutorials through our [Anyscale K8s Operator](https://docs.anyscale.com/administration/cloud-deployment/kubernetes/).
