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
from ray.rllib.execution.common import (
    STEPS_SAMPLED_COUNTER,
    LAST_TARGET_UPDATE_TS,
    NUM_TARGET_UPDATES,
    _get_shared_metrics,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.typing import PartialTrainerConfigDict, TrainerConfigDict


class APPOConfig(impala.ImpalaConfig):
    """Defines a APPOTrainer configuration class from which a new Trainer can be built.

    Example:
        >>> from ray.rllib.agents.ppo import APPOConfig
        >>> config = APPOConfig().training(lr=0.01, grad_clip=30.0)\
        ...     .resources(num_gpus=1)\
        ...     .rollouts(num_rollout_workers=16)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.agents.ppo import APPOConfig
        >>> from ray import tune
        >>> config = APPOConfig()
        >>> # Print out some default values.
        >>> print(config.sample_async)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "APPO",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, trainer_class=None):
        """Initializes a APPOConfig instance."""
        super().__init__(trainer_class=trainer_class or APPOTrainer)

        # fmt: off
        # __sphinx_doc_begin__

        # APPO specific settings:
        self.vtrace = True
        self.use_critic = True
        self.use_gae = True
        self.lambda_ = 1.0
        self.clip_param = 0.4
        self.use_kl_loss = False
        self.kl_coeff = 1.0
        self.kl_target = 0.01

        # Override some of ImpalaConfig's default values with APPO-specific values.
        self.rollout_fragment_length = 50
        self.train_batch_size = 500
        self.min_time_s_per_reporting = 10
        self.num_workers = 2
        self.num_gpus = 0
        self.num_multi_gpu_tower_stacks = 1
        self.minibatch_buffer_size = 1
        self.num_sgd_iter = 1
        self.replay_proportion = 0.0
        self.replay_buffer_num_slots = 100
        self.learner_queue_size = 16
        self.learner_queue_timeout = 300
        self.max_sample_requests_in_flight_per_worker = 2
        self.broadcast_interval = 1
        self.grad_clip = 40.0
        self.opt_type = "adam"
        self.lr = 0.0005
        self.lr_schedule = None
        self.decay = 0.99
        self.momentum = 0.0
        self.epsilon = 0.1
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.01
        self.entropy_coeff_schedule = None

        self._disable_execution_plan_api = False
        # __sphinx_doc_end__
        # fmt: on

    @override(impala.ImpalaConfig)
    def training(
        self,
        *,
        vtrace: Optional[bool] = None,
        use_critic: Optional[bool] = None,
        use_gae: Optional[bool] = None,
        lambda_: Optional[float] = None,
        clip_param: Optional[float] = None,
        use_kl_loss: Optional[bool] = None,
        kl_coeff: Optional[float] = None,
        kl_target: Optional[float] = None,
        **kwargs,
    ) -> "APPOConfig":
        """Sets the training related configuration.

        Args:
            vtrace: Whether to use V-trace weighted advantages. If false, PPO GAE
                advantages will be used instead.
            use_critic: Should use a critic as a baseline (otherwise don't use value
                baseline; required for using GAE). Only applies if vtrace=False.
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
                Only applies if vtrace=False.
            lambda_: GAE (lambda) parameter.
            clip_param: PPO surrogate slipping parameter.
            use_kl_loss: Whether to use the KL-term in the loss function.
            kl_coeff: Coefficient for weighting the KL-loss term.
            kl_target: Target term for the KL-term to reach (via adjusting the
                `kl_coeff` automatically).

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if vtrace is not None:
            self.vtrace = vtrace
        if use_critic is not None:
            self.use_critic = use_critic
        if use_gae is not None:
            self.use_gae = use_gae
        if lambda_ is not None:
            self.lambda_ = lambda_
        if clip_param is not None:
            self.clip_param = clip_param
        if use_kl_loss is not None:
            self.use_kl_loss = use_kl_loss
        if kl_coeff is not None:
            self.kl_coeff = kl_coeff
        if kl_target is not None:
            self.kl_target = kl_target

        return self


class UpdateTargetAndKL:
    def __init__(self, workers, config):
        self.workers = workers
        self.config = config
        self.update_kl = UpdateKL(workers)
        self.target_update_freq = (
            config["num_sgd_iter"] * config["minibatch_buffer_size"]
        )

    def __call__(self, fetches):
        metrics = _get_shared_metrics()
        cur_ts = metrics.counters[STEPS_SAMPLED_COUNTER]
        last_update = metrics.counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update > self.target_update_freq:
            metrics.counters[NUM_TARGET_UPDATES] += 1
            metrics.counters[LAST_TARGET_UPDATE_TS] = cur_ts
            # Update Target Network
            self.workers.local_worker().foreach_policy_to_train(
                lambda p, _: p.update_target()
            )
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
        self.workers.local_worker().foreach_policy_to_train(
            lambda p, _: p.update_target()
        )

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return APPOConfig().to_dict()

    @override(Trainer)
    def get_default_policy_class(
        self, config: PartialTrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.agents.ppo.appo_torch_policy import AsyncPPOTorchPolicy

            return AsyncPPOTorchPolicy
        else:
            return AsyncPPOTFPolicy


# Deprecated: Use ray.rllib.agents.ppo.APPOConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(APPOConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.ppo.appo.DEFAULT_CONFIG",
        new="ray.rllib.agents.ppo.appo.APPOConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
