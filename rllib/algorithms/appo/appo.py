"""
Asynchronous Proximal Policy Optimization (APPO)
================================================

This file defines the distributed Algorithm class for the asynchronous version
of proximal policy optimization (APPO).
See `appo_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#appo
"""
from typing import Optional, Type
import logging

from ray.rllib.algorithms.impala.impala import Impala, ImpalaConfig
from ray.rllib.algorithms.ppo.ppo import UpdateKL
from ray.rllib.execution.common import _get_shared_metrics, STEPS_SAMPLED_COUNTER
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import (
    PartialAlgorithmConfigDict,
    ResultDict,
    AlgorithmConfigDict,
)

logger = logging.getLogger(__name__)


class APPOConfig(ImpalaConfig):
    """Defines a configuration class from which an APPO Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.appo import APPOConfig
        >>> config = APPOConfig().training(lr=0.01, grad_clip=30.0)\
        ...     .resources(num_gpus=1)\
        ...     .rollouts(num_rollout_workers=16)\
        ...     .environment("CartPole-v1")
        >>> print(config.to_dict())
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()
        >>> algo.train()

    Example:
        >>> from ray.rllib.algorithms.appo import APPOConfig
        >>> from ray import air
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
        >>> tune.Tuner(
        ...     "APPO",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a APPOConfig instance."""
        super().__init__(algo_class=algo_class or APPO)

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
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 50
        self.train_batch_size = 500
        self.min_time_s_per_iteration = 10
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
        # __sphinx_doc_end__
        # fmt: on

    @override(ImpalaConfig)
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
            This updated AlgorithmConfig object.
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


class APPO(Impala):
    def __init__(self, config, *args, **kwargs):
        """Initializes an APPO instance."""
        super().__init__(config, *args, **kwargs)

        # After init: Initialize target net.
        self.workers.local_worker().foreach_policy_to_train(
            lambda p, _: p.update_target()
        )

    @override(Impala)
    def setup(self, config: PartialAlgorithmConfigDict):
        # Before init: Add the update target and kl hook.
        # This hook is called explicitly after each learner step in the
        # execution setup for IMPALA.
        if config.get("_disable_execution_plan_api", True) is False:
            config["after_train_step"] = UpdateTargetAndKL

        super().setup(config)

        if self.config["_disable_execution_plan_api"] is True:
            self.update_kl = UpdateKL(self.workers)

    def after_train_step(self, train_results: ResultDict) -> None:
        """Updates the target network and the KL coefficient for the APPO-loss.

        This method is called from within the `training_iteration` method after each
        train update.

        The target network update frequency is calculated automatically by the product
        of `num_sgd_iter` setting (usually 1 for APPO) and `minibatch_buffer_size`.

        Args:
            train_results: The results dict collected during the most recent
                training step.
        """
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED if self._by_agent_steps else NUM_ENV_STEPS_SAMPLED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        target_update_freq = (
            self.config["num_sgd_iter"] * self.config["minibatch_buffer_size"]
        )
        if cur_ts - last_update > target_update_freq:
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

            # Update our target network.
            self.workers.local_worker().foreach_policy_to_train(
                lambda p, _: p.update_target()
            )

            # Also update the KL-coefficient for the APPO loss, if necessary.
            if self.config["use_kl_loss"]:

                def update(pi, pi_id):
                    assert LEARNER_STATS_KEY not in train_results, (
                        "{} should be nested under policy id key".format(
                            LEARNER_STATS_KEY
                        ),
                        train_results,
                    )
                    if pi_id in train_results:
                        kl = train_results[pi_id][LEARNER_STATS_KEY].get("kl")
                        assert kl is not None, (train_results, pi_id)
                        # Make the actual `Policy.update_kl()` call.
                        pi.update_kl(kl)
                    else:
                        logger.warning("No data for {}, not updating kl".format(pi_id))

                # Update KL on all trainable policies within the local (trainer)
                # Worker.
                self.workers.local_worker().foreach_policy_to_train(update)

    @override(Impala)
    def training_step(self) -> ResultDict:
        train_results = super().training_step()

        # Update KL, target network periodically.
        self.after_train_step(train_results)

        return train_results

    @classmethod
    @override(Impala)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return APPOConfig().to_dict()

    @override(Impala)
    def get_default_policy_class(
        self, config: PartialAlgorithmConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.appo.appo_torch_policy import APPOTorchPolicy

            return APPOTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF1Policy

            return APPOTF1Policy
        else:
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF2Policy

            return APPOTF2Policy


# Deprecated: Use ray.rllib.algorithms.appo.APPOConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(APPOConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.ppo.appo::DEFAULT_CONFIG",
        new="ray.rllib.algorithms.appo.appo::APPOConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
