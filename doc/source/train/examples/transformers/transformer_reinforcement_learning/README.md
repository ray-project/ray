# RL Post-Training using Hugging Face TRL with GRPO

This notebook builds on the TRL GRPO Example, [GRPO Trainer](https://huggingface.co/docs/trl/en/grpo_trainer), to fine-tune a Qwen2.5 0.5B model to answer math questions using the DeepMath-103k dataset and Group Relative Policy Optimization (GRPO) algorithm.

Ray Train scales this implementation efficiently across multiple GPUs without changing training logic.

This notebook consists of the following steps:

1. Quick Summary of GRPO and the DeepMath dataset
2. Package and Environment Setup
3. Running TRL with Ray Train
4. Scaling to more GPUs

<div id="anyscale-note" class="alert alert-block alert-warning">

  <strong>Anyscale Specific Configuration</strong>

  <p><strong>Note:</strong> This tutorial is optimized for the Anyscale platform. When running on open source Ray, you need additional configuration. For example, you would need to manually:</p>

  <ul>
    <li><strong>Configure your Ray Cluster</strong>: Set up your multi-node environment and manage resource allocation without Anyscale's automation.</li>
    <li><strong>Manage Dependencies</strong>: Manually install and manage dependencies on each node.</li>
    <li><strong>Set Up Storage</strong>: Configure your own distributed or shared storage system for model checkpointing.</li>
  </ul>

  <p>All these configurations are handled automatically through the Anyscale platform.
</div>

<style>
  div#anyscale-note > p,
  div#anyscale-note > ul,
  div#anyscale-note > ul li {
    color: black;
  }

  div#anyscale-note {
    background-color: rgb(255, 243, 205);
  }

  div#anyscale-note {
    border: 1px solid #ccc;
    border-radius: 8px;
    padding: 15px;
  }

</style>

## 1. Summary of the DeepMath dataset and GRPO algorithm

### DeepMath dataset

This example uses the DeepMath-103k dataset by [He et al., 2025](https://arxiv.org/abs/2504.11456), consisting of 103,000 challenging questions across a wide range of mathematical subjects including Algebra, Calculus, Number Theory, Geometry, Probability, and Discrete Mathematics. These questions are more heavily distributed towards challenging questions, in particular Levels 5 to 9, compared to alternative datasets, making them highly complex and difficult to solve without specialist training.

Each example contains two fields:
- **`prompt`**: a list of chat-format message dicts; the question text is in `prompt[0]["content"]`.
- **`solution`**: a LaTeX-formatted ground-truth answer, used by the reward function to verify the model's response.

Using a structured LaTeX format ensures the reward function can reliably parse and compare both the model's output and the ground truth. Here are a few examples from the dataset:

| Subject | Prompt | Solution |
|---|---|---|
| Number Theory | Determine the number of 19th power residues modulo 229. | $12$ |
| Functional Analysis | Determine the norm $\lVert T \rVert$ of the linear operator $T: \ell^2 \rightarrow \ell^2$ given by $(Tx)_1 = 0$, $(Tx)_n = -x_n + \alpha x_{n+1}$ for $n \geq 2$, where $\alpha \in \mathbb{C}$. | $1 + \lvert\alpha\rvert$ |
| Analysis | Determine whether there exists a Schwartz function $g \in \mathcal{S}(\mathbb{R}^n)$ such that for a given continuous function $f: \mathbb{R}^n \to \mathbb{R}$ with $f \not\equiv 0$, the integral $\int_{\mathbb{R}^n} g^2 f \, dx \neq 0$. | Yes |

### Group Relative Policy Optimization Algorithm

To RL post-train the model to answer math questions, this example uses TRL's GRPO (Group Relative Policy Optimization) implementation introduced by [Shao et al., 2024](https://arxiv.org/abs/2402.03300). GRPO is an online algorithm that iteratively improves by training on data the model generates itself. Each iteration proceeds through four steps:

1. **Generate**: For each prompt in the batch, sample a group of G completions from the current policy.
2. **Score**: Apply the reward function to each completion, producing a scalar reward per completion.
3. **Advantage**: Normalize the rewards within each group by subtracting the group mean and dividing by the group standard deviation. This relative score becomes the advantage $\hat{A}_{i,t}$ where positive means better than peers.
4. **Loss**: Update the policy to increase the probability of high-advantage completions and decrease it for low-advantage ones, while a KL penalty keeps the updated policy close to the reference.

$$
\mathcal{L}_{\text{GRPO}}(\theta) = -\frac{1}{\sum_{i=1}^{G} |o_i|} \sum_{i=1}^{G} \sum_{t=1}^{|o_i|} \left[ \frac{\pi_\theta(o_{i,t} \mid q, o_{i,\mathopen{<} t})}{\left[\pi_\theta(o_{i,t} \mid q, o_{i,\mathopen{<} t})\right]_{\text{no grad}}} \hat{A}_{i,t} - \beta \mathbb{D}_{\text{KL}}\left[\pi_\theta \| \pi_{\text{ref}}\right] \right],
$$

For a deeper, more technical, description of the algorithm, see the [TRL GRPO page](https://huggingface.co/docs/trl/en/grpo_trainer#looking-deeper-into-the-grpo-method) and the [original paper](https://arxiv.org/abs/2402.03300).

## 2. Package and Environment Setup

Install the required dependencies


```python
# !pip install "trl[vllm]" "math_verify" "transformers==4.57.6"
```

Use `ray.init()` to initialize a local cluster. By default, this cluster contains only the machine you are running this notebook on. You can also run this notebook on an Anyscale cluster.


```python
import ray

ray.init()
```

Use `ray.cluster_resources()` to check which resources your cluster has access to.
If you're running this notebook on your local machine or Google Colab, you should see the number of CPU cores and GPUs available to you.


```python
from pprint import pprint

pprint(ray.cluster_resources())
```

Change these two variables to control whether the training uses CPUs or GPUs, and how many workers to spawn. Each worker claims one CPU or GPU, so make sure not to request more resources than are available. By default, the training runs with one GPU worker.


```python
use_gpu = True  # set this to `False` to run on CPUs
num_workers = 4  # set this to the number of GPUs or CPUs you want to use
```

## 3. Using Hugging Face TRL (Transformer Reinforcement Learning) with Ray Train

In comparison to supervised pre-training where models optimize to minimize the error for their next token, RL-based post-training aims to maximise their reward from a prompt. Therefore, it's crucial to define a reward function to measure the success and to train a model.

This example uses the `trl.rewards.accuracy_reward` function to check whether the model's answer matches the answer in the dataset. As the answers use the LaTeX format, you must parse responses and solution before comparing them. The default `trl.rewards.accuracy_reward` implementation applies timeouts to the `parse` and `verify` functions, which are incompatible with Ray. The version defined below disables these timeouts by setting `parsing_timeout=0` and `timeout_seconds=0`.


```python
from latex2sympy2_extended import NormalizationConfig
from math_verify import LatexExtractionConfig, parse, verify

def accuracy_reward(completions, solution, **kwargs):
    """Reward function that checks mathematical accuracy.

    This is a copy of `trl.rewards.accuracy_reward`.
    The only difference is `parse(..., parsing_timeout=0)` and `verify(..., timeout_seconds=0)`
    to avoid `signal.alarm()` issues with ray.
    """
    contents = [completion[0]["content"] for completion in completions]
    rewards = []
    for content, sol in zip(contents, solution, strict=True):
        gold_parsed = parse(sol, parsing_timeout=0)
        if len(gold_parsed) != 0:
            # We require the answer to be provided in correct latex (no malformed operators)
            answer_parsed = parse(
                content,
                extraction_config=[
                    LatexExtractionConfig(
                        normalization_config=NormalizationConfig(units=True),
                        # Ensures that boxed is tried first
                        boxed_match_priority=0,
                        try_extract_without_anchor=False,
                    )
                ],
                extraction_mode="first_match",
                parsing_timeout=0,
            )
            reward = float(verify(gold_parsed, answer_parsed, timeout_seconds=0))
        else:
            # If the gold solution cannot be parsed, we assign `None` to skip this example
            reward = None
        rewards.append(reward)

    return rewards
```

Next, build the training function that's distributed across all the workers. The `GRPOTrainer` implements the GRPO algorithm for rolling out the model, computing the advantage and backpropagation the losses.

With `vllm_mode="colocate"`, each worker runs a vLLM instance that consumes about 30% of the GPU memory. The alternative `"server"` mode dedicates one worker to generation with vLLM while the others perform learning, but this introduces inter-GPU communication overhead that can reduce throughput. See [this blog post](https://huggingface.co/blog/vllm-colocate) for more details.

Two Ray Train-specific additions integrate the HF Trainer into the Ray Train ecosystem:

- **`RayTrainReportCallback`** — hooks into the HF Trainer's logging to forward metrics and checkpoints to Ray Train after each training step. This is what populates the `Result` object returned by `trainer.fit()` with metrics like `reward` and the best checkpoint.
- **`prepare_trainer`** — configures the HF Trainer for distributed execution across Ray workers. It disables HF's built-in distributed setup so that Ray Train controls the process group instead, and ensures correct device placement on each worker.


```python
from ray.train.huggingface.transformers import RayTrainReportCallback, prepare_trainer
from trl import GRPOConfig, GRPOTrainer
from datasets import load_dataset

def train_func(config):
    # Load the DeepMath dataset with only 100 elements for this example.
    # For larger datasets, use Ray Data.
    dataset = load_dataset("trl-lib/DeepMath-103K", split="train").shuffle(seed=42).select(range(100))

    # Use vllm_mode="colocate" to allow easy scaling as each GPU handles their own vLLM instance
    training_args = GRPOConfig(
        # Compute a training batch size of 4 for each prompt
        per_device_train_batch_size=4,
        # Use vLLM, colocated on each GPU which uses 30% of the vRAM.
        use_vllm=True,
        vllm_mode="colocate",
        vllm_gpu_memory_utilization=0.3,
        # Run two training epochs over the whole dataset
        num_train_epochs=2,
        # Save checkpoints and log metrics every epoch
        save_strategy="epoch",
        logging_strategy="epoch",
    )

    # GRPO Trainer
    trainer = GRPOTrainer(
        model="Qwen/Qwen2.5-0.5B",
        args=training_args,
        reward_funcs=accuracy_reward,
        train_dataset=dataset,
    )

    # Report metrics and checkpoints to Ray Train
    trainer.add_callback(RayTrainReportCallback())

    # Prepare your trainer for Ray Data integration
    trainer = prepare_trainer(trainer)

    # Start Training
    trainer.train()
```

With your `train_func` complete, you can now instantiate the `TorchTrainer`. Aside from calling the function, set the `scaling_config`, which controls the number of workers and resources used, and the `run_config` to configure checkpointing.

The `CheckpointConfig` controls how Ray Train saves and selects checkpoints during training:

- **`num_to_keep=1`** — Ray Train retains only the single best checkpoint on disk, saving storage.
- **`checkpoint_score_attribute="reward"`** — Ray Train ranks checkpoints by the `"reward"` metric, which is the mean reward across the batch as reported by `RayTrainReportCallback`.
- **`checkpoint_score_order="max"`** — higher reward is better, so Ray Train keeps the checkpoint with the highest reward seen across all training steps.


```python
from ray.train.torch import TorchTrainer
from ray.train import RunConfig, ScalingConfig, CheckpointConfig

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    run_config=RunConfig(
        checkpoint_config=CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="reward",
            checkpoint_score_order="max",
        ),
    ),
)
```

Finally, call the `fit` method to start training with Ray Train. Save the `Result` object to a variable so you can access metrics and checkpoints.


```python
result = trainer.fit()
```

You can use the returned `Result` object to access metrics and the Ray Train `Checkpoint` associated with the last iteration.


```python
result
```

## Scaling to more GPUs or a larger model

The preceding example trains with 4 workers on a small 100-sample subset of the dataset. To scale up, adjust `num_workers` in `ScalingConfig` and the `num_workers` variable at the top of the notebook, along with the dataset size and model.

**Scaling workers:**
Each worker claims one GPU. With `vllm_mode="colocate"`, each worker runs its own vLLM instance for generation and its own training process — generation requires no inter-GPU communication. Adding more workers increases the effective batch size and reduces time-to-convergence without any changes to training logic.

**Scaling the model:**
Replace `"Qwen/Qwen2.5-0.5B"` with a larger checkpoint. Larger models require more GPU memory per worker. If a model doesn't fit on a single GPU, consider:
- Explore using DeepSpeed ZeRO to distribute the model weights across multiple GPUs.
- Reducing `vllm_gpu_memory_utilization` from `0.3` to leave more memory for model weights at the cost of a smaller vLLM KV cache.

**Example configuration for 8 GPUs with the full dataset:**

```python
# In train_func, use the full dataset:
dataset = load_dataset("trl-lib/DeepMath-103K", split="train").shuffle(seed=42)

training_args = GRPOConfig(
    per_device_train_batch_size=4,
    use_vllm=True,
    vllm_mode="colocate",
    vllm_gpu_memory_utilization=0.3,
    num_train_epochs=3,
)

# Launch with 8 workers
trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    run_config=RunConfig(
        checkpoint_config=CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="reward",
            checkpoint_score_order="max",
        ),
    ),
)
```

## Summary

This notebook demonstrated how to use Ray Train to scale RL post-training of a Qwen2.5 0.5B model across multiple GPUs using the TRL GRPO implementation and the DeepMath-103k dataset.

The key Ray Train integration points were:

- **`RayTrainReportCallback`** — forwards HF Trainer metrics and checkpoints to Ray Train after each step.
- **`prepare_trainer`** — configures the HF Trainer to run distributed training under Ray's process group.
- **`TorchTrainer`** — launches `train_func` on all workers with the configured resources.
- **`CheckpointConfig`** — automatically tracks and retains the best checkpoint based on the `"reward"` metric.

From here, you can:
- Scale to more GPUs or multiple nodes by increasing `num_workers` in `ScalingConfig`.
- Swap in a different base model or dataset by updating the `model` argument and `load_dataset` call.
- Experiment with different reward functions to train for other tasks.
