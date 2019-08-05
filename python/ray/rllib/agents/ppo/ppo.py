from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.rllib.agents import with_common_config
from ray.rllib.agents.ppo.ppo_policy import PPOTFPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.optimizers import SyncSamplesOptimizer, LocalMultiGPUOptimizer

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.2,
    # Size of batches collected from each worker
    "sample_batch_size": 200,
    # Number of timesteps collected for each SGD round
    "train_batch_size": 4000,
    # Total SGD batch size across all devices for SGD
    "sgd_minibatch_size": 128,
    # Whether to shuffle sequences in the batch when training (recommended)
    "shuffle_sequences": True,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # Stepsize of SGD
    "lr": 5e-5,
    # Learning rate schedule
    "lr_schedule": None,
    # Share layers for value function. If you set this to True, it's important
    # to tune vf_loss_coeff.
    "vf_share_layers": False,
    # Coefficient of the value function loss. It's important to tune this if
    # you set vf_share_layers: True
    "vf_loss_coeff": 1.0,
    # Coefficient of the entropy regularizer
    "entropy_coeff": 0.0,
    # Decay schedule for the entropy regularizer
    "entropy_coeff_schedule": None,
    # PPO clip parameter
    "clip_param": 0.3,
    # Clip param for the value function. Note that this is sensitive to the
    # scale of the rewards. If your expected V is large, increase this.
    "vf_clip_param": 10.0,
    # If specified, clip the global norm of gradients by this amount
    "grad_clip": None,
    # Target value for KL divergence
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Uses the sync samples optimizer instead of the multi-gpu one. This does
    # not support minibatches.
    "simple_optimizer": False,
})
# __sphinx_doc_end__
# yapf: enable


def choose_policy_optimizer(workers, config):
    if config["simple_optimizer"]:
        return SyncSamplesOptimizer(
            workers,
            num_sgd_iter=config["num_sgd_iter"],
            train_batch_size=config["train_batch_size"])

    return LocalMultiGPUOptimizer(
        workers,
        sgd_batch_size=config["sgd_minibatch_size"],
        num_sgd_iter=config["num_sgd_iter"],
        num_gpus=config["num_gpus"],
        sample_batch_size=config["sample_batch_size"],
        num_envs_per_worker=config["num_envs_per_worker"],
        train_batch_size=config["train_batch_size"],
        standardize_fields=["advantages"],
        shuffle_sequences=config["shuffle_sequences"])


def update_kl(trainer, fetches):
    if "kl" in fetches:
        # single-agent
        trainer.workers.local_worker().for_policy(
            lambda pi: pi.update_kl(fetches["kl"]))
    else:

        def update(pi, pi_id):
            if pi_id in fetches:
                pi.update_kl(fetches[pi_id]["kl"])
            else:
                logger.debug("No data for {}, not updating kl".format(pi_id))

        # multi-agent
        trainer.workers.local_worker().foreach_trainable_policy(update)


def warn_about_bad_reward_scales(trainer, result):
    # Warn about bad clipping configs
    if trainer.config["vf_clip_param"] <= 0:
        rew_scale = float("inf")
    elif result["policy_reward_mean"]:
        rew_scale = 0  # punt on handling multiagent case
    else:
        rew_scale = round(
            abs(result["episode_reward_mean"]) /
            trainer.config["vf_clip_param"], 0)
    if rew_scale > 200:
        logger.warning(
            "The magnitude of your environment rewards are more than "
            "{}x the scale of `vf_clip_param`. ".format(rew_scale) +
            "This means that it will take more than "
            "{} iterations for your value ".format(rew_scale) +
            "function to converge. If this is not intended, consider "
            "increasing `vf_clip_param`.")


def validate_config(config):
    if config["entropy_coeff"] < 0:
        raise DeprecationWarning("entropy_coeff must be >= 0")
    if config["sgd_minibatch_size"] > config["train_batch_size"]:
        raise ValueError(
            "Minibatch size {} must be <= train batch size {}.".format(
                config["sgd_minibatch_size"], config["train_batch_size"]))
    if config["batch_mode"] == "truncate_episodes" and not config["use_gae"]:
        raise ValueError(
            "Episode truncation is not supported without a value "
            "function. Consider setting batch_mode=complete_episodes.")
    if config["multiagent"]["policies"] and not config["simple_optimizer"]:
        logger.info(
            "In multi-agent mode, policies will be optimized sequentially "
            "by the multi-GPU optimizer. Consider setting "
            "simple_optimizer=True if this doesn't work for you.")
    if config["simple_optimizer"]:
        logger.warning(
            "Using the simple non-minibatch optimizer. This will greatly "
            "reduce performance, consider simple_optimizer=False.")
    if not config["vf_share_layers"]:
        logger.warning(
            "FYI: By default, the value function will not share layers "
            "with the policy model ('vf_share_layers': False).")


PPOTrainer = build_trainer(
    name="PPO",
    default_config=DEFAULT_CONFIG,
    default_policy=PPOTFPolicy,
    make_policy_optimizer=choose_policy_optimizer,
    validate_config=validate_config,
    after_optimizer_step=update_kl,
    after_train_result=warn_about_bad_reward_scales)
