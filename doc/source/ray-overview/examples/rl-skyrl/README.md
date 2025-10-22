# Reinforcement Learning for LLMs with SkyRL

**⏱️ Time to complete**: ~40 mins, including the time to train the models


This template walks through running [GRPO](https://arxiv.org/pdf/2402.03300) on Anyscale using the [SkyRL](https://github.com/NovaSky-AI/SkyRL) framework. 
SkyRL is a modular full-stack RL library for LLMs developed at the Berkeley Sky Computing Lab in collaboration with Anyscale, providing a flexible framework 
for training LLMs on tool-use tasks and multi-turn agentic workflows using popular RL algorithms (PPO, GRPO, DAPO). SkyRL uses [Ray](https://github.com/ray-project/ray) extensively for managing training and generation workers, and for orchestration of the RL training loop, allowing it to easily scale to multiple GPUs and nodes within a Ray cluster.

This template will first show a basic example of training a model to solve math word problems from the GSM8K dataset using GRPO. Next, the template will
show how you can create your own new environment to train on your specific task using the SkyRL-Gym.


## Setup
SkyRL uses the [uv + Ray integration](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters) for dependency management, ensuring a consistent set of dependencies get shipped to all Ray workers. This template uses the `novaskyai/skyrl-train-ray-2.48.0-py3.12-cu12.8` docker image to ensure all necessary system depedencies are installed. The exact Dockerfile can be found at [SkyRL/docker/Dockerfile](https://github.com/NovaSky-AI/SkyRL/blob/skyrl_train-v0.2.0/docker/Dockerfile).

First, clone SkyRL and cd to `skyrl-train/`.

```bash
git clone --branch skyrl_train-v0.2.0 https://github.com/NovaSky-AI/SkyRL.git
cd SkyRL/skyrl-train/
```

## GRPO for solving math problems (GSM8K)
### Dataset preparation
To download and prepare the GSM8K dataset from HuggingFace, run the following command:

```bash
uv run --isolated examples/gsm8k/gsm8k_dataset.py --output_dir  /mnt/cluster_storage/data/gsm8k
```

This script converts the Huggingface GSM8K dataset to two Parquet files with the [schema required by SkyRL](https://skyrl.readthedocs.io/en/latest/datasets/dataset-preparation.html).
- `train.parquet` - Training data.
- `validation.parquet` - Validation data.

### Launching your training run

Now you're ready to launch a training run! If you choose to use the W&B logger (`trainer.logger="wandb"`), first set the `WANDB_API_KEY` environment variable in the [Dependencies tab](https://docs.anyscale.com/development#environment-variables). Otherwise, you can set `trainer.logger="console"` to print training logs to console. 


```bash
SKYRL_RAY_PG_TIMEOUT_IN_S=90 bash examples/gsm8k/run_gsm8k.sh \
   data.train_data="['/mnt/cluster_storage/data/gsm8k/train.parquet']" \
   data.val_data="['/mnt/cluster_storage/data/gsm8k/validation.parquet']" \
   trainer.ckpt_path="/mnt/cluster_storage/ckpts/gsm8k_1.5B_ckpt" \
   trainer.micro_forward_batch_size_per_gpu=16 \
   trainer.micro_train_batch_size_per_gpu=16 \
   trainer.epochs=1 \
   trainer.logger="console"
```

If using W&B, you should see logs like the ones shown below, with detailed metric tracking and timing breakdowns for each stage of the RL pipeline.
<img src="https://raw.githubusercontent.com/anyscale/templates/main/templates/rl-skyrl/assets/gsm8k_wandb.png" width=1500px />


## Creating a new environment or task

Now that you've run a basic example to teach an LLM to solve math word problems, you might want to start training on your own custom task! Check out the SkyRL docs for [creating a new environment or task](https://skyrl.readthedocs.io/en/latest/tutorials/new_env.html) for a full walkthrough of the simple steps to implement a custom multi-turn environment using the SkyRL-Gym interface. The commands needed to run the multi-turn example in the linked tutorial on Anyscale are shown below.

### Preparing your data

```bash
uv run --isolated examples/multiply/multiply_dataset.py \
  --output_dir /mnt/cluster_storage/data/multiply \
  --num_digits 4 \
  --train_size 10000 \
  --test_size 200
```

### Training your model
```bash
SKYRL_RAY_PG_TIMEOUT_IN_S=90 bash examples/multiply/run_multiply.sh \
   data.train_data="['/mnt/cluster_storage/data/multiply/train.parquet']" \
   data.val_data="['/mnt/cluster_storage/data/multiply/validation.parquet']" \
   trainer.ckpt_path="/mnt/cluster_storage/ckpts/multiply_1.5B_ckpt" \
   trainer.micro_forward_batch_size_per_gpu=16 \
   trainer.micro_train_batch_size_per_gpu=16 \
   trainer.epochs=1 \
   trainer.logger="console"
```

## Next steps

After completing this template, you can:
- Explore more advanced algorithms, like [PPO](https://github.com/NovaSky-AI/SkyRL/tree/main/skyrl-train/examples/ppo) or [DAPO](https://skyrl.readthedocs.io/en/latest/algorithms/dapo.html)
- Explore more advanced tasks like [SWE-Bench](https://skyrl.readthedocs.io/en/latest/examples/mini_swe_agent.html), or [agentic search (Search-R1)](https://skyrl.readthedocs.io/en/latest/examples/search.html).
- Optimize your training pipeline using [Async Training](https://skyrl.readthedocs.io/en/latest/tutorials/async.html)
- Deploy your trained LLM using [Ray Serve LLM on Anyscale](https://console.anyscale.com/template-preview/deployment-serve-llm?utm_source=anyscale_docs&utm_medium=docs&utm_campaign=examples_page&utm_content=deployment-serve-llm?utm_source=anyscale&utm_medium=docs&utm_campaign=examples_page&utm_content=deployment-serve-llm).
