"""
Asynchronous Proximal Policy Optimization (APPO)
================================================

This file defines the distributed Trainer class for the asynchronous version
of proximal policy optimization (APPO).
See `appo_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#appo
"""
from typing import Optional, Type

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.ppo.appo_tf_policy import AsyncPPOTFPolicy
from ray.rllib.agents.ppo.ppo import UpdateKL
from ray.rllib.agents import impala
from ray.rllib.policy.policy import Policy
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES, _get_shared_metrics
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import PartialTrainerConfigDict, TrainerConfigDict

# yapf: disable
# __sphinx_doc_begin__

# Adds the following updates to the `IMPALATrainer` config in
# rllib/agents/impala/impala.py.
DEFAULT_CONFIG = impala.ImpalaTrainer.merge_trainer_configs(
    impala.DEFAULT_CONFIG,  # See keys in impala.py, which are also supported.
    {
        # Whether to use V-trace weighted advantages. If false, PPO GAE
        # advantages will be used instead.
        "vtrace": True,

        # == These two options only apply if vtrace: False ==
        # Should use a critic as a baseline (otherwise don't use value
        # baseline; required for using GAE).
        "use_critic": True,
        # If true, use the Generalized Advantage Estimator (GAE)
        # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
        "use_gae": True,
        # GAE(lambda) parameter
        "lambda": 1.0,

        # == PPO surrogate loss options ==
        "clip_param": 0.4,

        # == PPO KL Loss options ==
        "use_kl_loss": False,
        "kl_coeff": 1.0,
        "kl_target": 0.01,

        # == IMPALA optimizer params (see documentation in impala.py) ==
        "rollout_fragment_length": 50,
        "train_batch_size": 500,
        "min_iter_time_s": 10,
        "num_workers": 2,
        "num_gpus": 0,
        "num_multi_gpu_tower_stacks": 1,
        "minibatch_buffer_size": 1,
        "num_sgd_iter": 1,
        "replay_proportion": 0.0,
        "replay_buffer_num_slots": 100,
        "learner_queue_size": 16,
        "learner_queue_timeout": 300,
        "max_sample_requests_in_flight_per_worker": 2,
        "broadcast_interval": 1,
        "grad_clip": 40.0,
        "opt_type": "adam",
        "lr": 0.0005,
        "lr_schedule": None,
        "decay": 0.99,
        "momentum": 0.0,
        "epsilon": 0.1,
        "vf_loss_coeff": 0.5,
        "entropy_coeff": 0.01,
        "entropy_coeff_schedule": None,
    },
    _allow_unknown_configs=True,
)

# __sphinx_doc_end__
# yapf: enable


class UpdateTargetAndKL:
    def __init__(self, workers, config):
        self.workers = workers
        self.config = config
        self.update_kl = UpdateKL(workers)
        self.target_update_freq = config["num_sgd_iter"] \
            * config["minibatch_buffer_size"]

    def __call__(self, fetches):
        metrics = _get_shared_metrics()
        cur_ts = metrics.counters[STEPS_SAMPLED_COUNTER]
        last_update = metrics.counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update > self.target_update_freq:
            metrics.counters[NUM_TARGET_UPDATES] += 1
            metrics.counters[LAST_TARGET_UPDATE_TS] = cur_ts
            # Update Target Network
            self.workers.local_worker().foreach_trainable_policy(
                lambda p, _: p.update_target())
            # Also update KL Coeff
            if self.config["use_kl_loss"]:
                self.update_kl(fetches)


class APPOTrainer(impala.ImpalaTrainer):
    def __init__(self, config, *args, **kwargs):
        # Before init: Add the update target and kl hook.
        # This hook is called explicitly after each learner step in the
        # execution setup for IMPALA.
        config["after_train_step"] = UpdateTargetAndKL

        super().__init__(config, *args, **kwargs)

        # After init: Initialize target net.
        self.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def get_default_policy_class(self, config: PartialTrainerConfigDict) -> \
            Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.agents.ppo.appo_torch_policy import \
                AsyncPPOTorchPolicy
            return AsyncPPOTorchPolicy
        else:
            return AsyncPPOTFPolicy
