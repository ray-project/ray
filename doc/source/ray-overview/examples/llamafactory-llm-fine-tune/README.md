# Fine-tuning LLMs with LLaMA-Factory on Anyscale

This repository provides ready-to-run templates for fine-tuning Large Language Models (LLMs) on Anyscale using [LLaMA-Factory](https://github.com/hiyouga/LLaMA-Factory). These templates demonstrate instruction tuning and preference alignment at scale (multi-GPU, multi-node), with configurations that you can reuse across different cloud providers.

Each template is an executable notebook that guides you through setup, configuration, and distributed execution. It also includes corresponding YAML/JSON configurations for repeatable runs and automation.

## Why LLaMA-Factory?

LLaMA-Factory is an easy-to-use, open-source framework. Its simple, declarative configs and consistent CLI allow you to define Continued Pre-Training (CPT), Supervised Fine-Tuning (SFT), Direct Preference Optimization (DPO), and Kahneman-Tversky Optimization (KTO) runs once and reuse them across environments. It supports popular adapters like LoRA and QLoRA using Parameter-Efficient Fine-Tuning and integrates with DeepSpeed for efficient multi-GPU training. This enables reproducible, composable workflows that start small and scale on demand.

## Templates

### [SFT with LoRA and DeepSpeed](./notebooks/sft_lora_deepspeed.ipynb)
Supervised instruction tuning with **LoRA** and **DeepSpeed ZeRO** for efficient, reproducible multi-GPU training.

---

### [DPO with QLoRA](./notebooks/dpo_qlora.ipynb)
Preference alignment on pairwise data with **DPO** and **QLoRA** for memory-efficient, scalable training.

---

### [KTO with LoRA](./notebooks/kto_lora.ipynb)
Single-signal feedback alignment with **KTO** and **LoRA** for lightweight, scalable preference tuning.

---

### [CPT with DeepSpeed](./notebooks/cpt_deepspeed.ipynb)
Continued pre-training on raw text with **full fine-tuning** and **DeepSpeed ZeRO** for efficient, reproducible multi-GPU training.

## Repository layout

- **`notebooks/`**: End-to-end executable templates for SFT, DPO, and KTO.
- **`train-configs/`**: Configuration files for models, adapters, and hyperparameters.
- **`dataset-configs/`**: Dataset metadata and registries that the templates reference.
- **`deepspeed-configs/`**: DeepSpeed ZeRO presets for scaling and memory efficiency.

## Development with Anyscale workspaces

Develop as you would on your laptop. Attach your IDE remotely and install dependencies with `pip` that automatically propagate to the cluster. Debug distributed training with the [distributed debugger](https://docs.anyscale.com/platform/workspaces/workspaces-debugging/#distributed-debugger). For more details, see [Anyscale workspaces](https://docs.anyscale.com/platform/workspaces/).

## Production with Anyscale jobs

Transition from development to production by submitting your configurations as an **Anyscale job**. This allows for reliable execution on managed clusters and seamless integration with CI/CD pipelines. See more [Anyscale jobs](https://docs.anyscale.com/platform/jobs/) to learn more.

