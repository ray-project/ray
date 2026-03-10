"""
Minimal Example adapted from https://huggingface.co/docs/trl/grpo_trainer

RL Post-training a Qwen2 0.5B Instruct model using the GPRO algorithm and the DeepMath 103k dataset.
"""
from datasets import load_dataset
from latex2sympy2_extended import NormalizationConfig
from math_verify import LatexExtractionConfig, parse, verify
from trl import GRPOConfig, GRPOTrainer

from ray.train import ScalingConfig
from ray.train.huggingface.transformers import RayTrainReportCallback, prepare_trainer
from ray.train.torch import TorchTrainer


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


# [1] Define a training function that includes all your training logic
# ====================================================================
def train_func():
    # Datasets
    dataset = load_dataset("trl-lib/DeepMath-103K", split="train")

    # Use vllm_mode="colocate" to allow easy scaling as each GPU handles their own vLLM instance
    training_args = GRPOConfig(
        per_device_train_batch_size=4, use_vllm=True, vllm_mode="colocate"
    )

    # GRPO Trainer
    trainer = GRPOTrainer(
        model="Qwen/Qwen2-0.5B-Instruct",
        args=training_args,
        reward_funcs=accuracy_reward,
        train_dataset=dataset,
    )

    # [2] Report metrics and checkpoints to Ray Train
    # ===============================================
    trainer.add_callback(RayTrainReportCallback())

    # [3] Prepare your trainer for Ray Data integration
    # =================================================
    trainer = prepare_trainer(trainer)

    # Start Training
    trainer.train()


if __name__ == "__main__":
    # [4] Build a Ray TorchTrainer to launch `train_func` on all workers
    # ==================================================================
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    )

    trainer.fit()
