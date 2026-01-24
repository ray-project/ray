<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify README.ipynb instead, then regenerate this file with:
jupyter nbconvert "README.ipynb" --to markdown --output "README.md"
-->

# Ray Train workloads

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/ray_train_workloads"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/train/tutorials/content" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This tutorial series provides hands-on learning for **Ray Train** and its ecosystem (Ray Data, Anyscale Workspaces).  
The tutorials walk through common ML workload patterns—vision, tabular, time series, generative, policy learning, and recommendation—showing how to scale them **from single-node to fully distributed training and inference** with minimal code changes.

---

## Tutorial index

### 1. Getting started
- [**Introduction to Ray Train**](https://docs.ray.io/en/latest/train/tutorials/content/getting-started/01_02_03_intro_to_ray_train.html)  
  Your starting point. Learn the basics of distributed training with PyTorch and Ray Train:
  - Why and when to use Ray Train vs. raw Distributed Data Parallel (DDP).  
  - Wrapping models/data loaders with [`prepare_model`](https://docs.ray.io/en/latest/train/api/doc/ray.train.torch.prepare_model.html) / [`prepare_data_loader`](https://docs.ray.io/en/latest/train/api/doc/ray.train.torch.prepare_data_loader.html).  
  - Using [`ScalingConfig`](https://docs.ray.io/en/latest/train/api/doc/ray.train.ScalingConfig.html) and [`RunConfig`](https://docs.ray.io/en/latest/train/api/doc/ray.train.RunConfig.html) for scale and checkpointing.  
  - Reporting metrics, saving checkpoints, and inspecting results with [`train.report`](https://docs.ray.io/en/latest/train/api/doc/ray.train.report.html).  
  - Running fully distributed end-to-end training on Anyscale.

---

### 2. Workload patterns (independent, work in any order)

- [**Vision workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04a_vision_pattern.html)  
  Real-world computer vision with Food-101, preprocessing with Ray Data, fault-tolerant ResNet training, and scalable inference tasks.  

- [**Tabular workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04b_tabular_workload_pattern.html)  
  Tabular ML with CoverType dataset, XGBoost + Ray Train, checkpoint-aware training, feature importance, and distributed inference.  

- [**Time series workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04c_time_series_workload_pattern.html)  
  New York City taxi demand forecasting with a Transformer model, scaling across GPUs, epoch-level fault tolerance, and remote inference from checkpoints.  

- [**Generative computer vision workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04d1_generative_cv_pattern.html)  
  A mini diffusion pipeline (Food-101-Lite), showcasing Ray Data preprocessing, PyTorch Lightning integration, checkpointing, and image generation.  

- [**Policy learning workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04d2_policy_learning_pattern.html)  
  Diffusion-policy pipeline on Gymnasium's Pendulum-v1 dataset, scaling across GPUs, checkpoint-per-epoch, and direct policy rollout in-notebook.  

- [**Recommendation system workloads**](https://docs.ray.io/en/latest/train/tutorials/content/workload-patterns/04e_rec_sys_workload_pattern.html)  
  Matrix-factorization recommendation system with MovieLens 100K, streaming batches with [`iter_torch_batches`](https://docs.ray.io/en/latest/data/api/doc/ray.data.DataIterator.iter_torch_batches.html), custom training loop with checkpointing, and modular separation of training/eval/inference.

---

## Key benefits
- **Unified abstraction:** One training loop works across CPU, GPU, and multi-node clusters.
- **Fault tolerance:** Resume from checkpoints on failures or pre-emptions.
- **Scalability:** Move from laptop prototyping to cluster-scale training without code changes.
- **Observability:** Access metrics, logs, and checkpoints through Ray and Anyscale tooling.
- **Flexibility:** Mix and match workload patterns for real-world ML pipelines.  

```{toctree}
:hidden:

getting-started/01_02_03_intro_to_ray_train.ipynb
workload-patterns/04a_vision_pattern.ipynb
workload-patterns/04b_tabular_workload_pattern.ipynb
workload-patterns/04c_time_series_workload_pattern.ipynb
workload-patterns/04d1_generative_cv_pattern.ipynb
workload-patterns/04d2_policy_learning_pattern.ipynb
workload-patterns/04e_rec_sys_workload_pattern.ipynb
```
