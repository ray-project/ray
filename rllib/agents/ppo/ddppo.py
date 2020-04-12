from ray.rllib.agents.ppo import ppo
from ray.rllib.agents.trainer import with_base_config
from ray.rllib.optimizers import TorchDistributedDataParallelOptimizer
"""Decentralized Distributed PPO implementation.

Unlike APPO or PPO, learning is no longer done centralized in the trainer
process. Instead, gradients are computed remotely on each rollout worker and
all-reduced to sync them at each mini-batch. This allows each worker's GPU
to be used both for sampling and for training.

DD-PPO should be used if you have envs that require GPUs to function, or have
a very large model that cannot be effectively optimized with the GPUs available
on a single machine (DD-PPO allows scaling to arbitrary numbers of GPUs across
multiple nodes, unlike PPO/APPO which is limited to GPUs on a single node).

Paper reference: https://arxiv.org/abs/1911.00357
Note that unlike the paper, we currently do not implement straggler mitigation.
"""

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(ppo.DEFAULT_CONFIG, {
    # During the sampling phase, each rollout worker will collect a batch
    # `rollout_fragment_length * num_envs_per_worker` steps in size.
    "rollout_fragment_length": 100,
    # Vectorize the env (should enable by default since each worker has a GPU).
    "num_envs_per_worker": 5,
    # During the SGD phase, workers iterate over minibatches of this size.
    # The effective minibatch size will be `sgd_minibatch_size * num_workers`.
    "sgd_minibatch_size": 50,
    # Number of SGD epochs per optimization round.
    "num_sgd_iter": 10,

    # *** WARNING: configs below are DDPPO overrides over PPO; you
    #     shouldn't need to adjust them. ***
    "use_pytorch": True,  # DDPPO requires PyTorch distributed.
    "num_gpus": 0,  # Learning is no longer done on the driver process, so
                    # giving GPUs to the driver does not make sense!
    "num_gpus_per_worker": 1,  # Each rollout worker gets a GPU.
    "truncate_episodes": True,  # Require evenly sized batches. Otherwise,
                                # collective allreduce could fail.
    "train_batch_size": -1,  # This is auto set based on sample batch size.
})
# __sphinx_doc_end__
# yapf: enable


def validate_config(config):
    if config["train_batch_size"] == -1:
        # Auto set.
        config["train_batch_size"] = (
            config["rollout_fragment_length"] * config["num_envs_per_worker"])
    else:
        raise ValueError(
            "Set rollout_fragment_length instead of train_batch_size "
            "for DDPPO.")
    ppo.validate_config(config)


def make_distributed_allreduce_optimizer(workers, config):
    if not config["use_pytorch"]:
        raise ValueError(
            "Distributed data parallel is only supported for PyTorch")
    if config["num_gpus"]:
        raise ValueError(
            "When using distributed data parallel, you should set "
            "num_gpus=0 since all optimization "
            "is happening on workers. Enable GPUs for workers by setting "
            "num_gpus_per_worker=1.")
    if config["batch_mode"] != "truncate_episodes":
        raise ValueError(
            "Distributed data parallel requires truncate_episodes "
            "batch mode.")

    return TorchDistributedDataParallelOptimizer(
        workers,
        expected_batch_size=config["rollout_fragment_length"] *
        config["num_envs_per_worker"],
        num_sgd_iter=config["num_sgd_iter"],
        sgd_minibatch_size=config["sgd_minibatch_size"],
        standardize_fields=["advantages"])


DDPPOTrainer = ppo.PPOTrainer.with_updates(
    name="DDPPO",
    default_config=DEFAULT_CONFIG,
    make_policy_optimizer=make_distributed_allreduce_optimizer,
    validate_config=validate_config)
