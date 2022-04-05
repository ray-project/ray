"""
Proximal Policy Optimization (PPO)
==================================

This file defines the distributed Trainer class for proximal policy
optimization.
See `ppo_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#ppo
"""

import logging
from typing import List, Optional, Type, Union

from ray.rllib.agents import with_common_config
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    ConcatBatches,
    StandardizeFields,
    SelectExperiences,
)
from ray.rllib.execution.train_ops import TrainOneStep, MultiGPUTrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)


class PPOConfig(TrainerConfig):
    """Defines a PPOTrainer configuration class from which a PPOTrainer can be built.

    Example:
        >>> config = PPOConfig(kl_coeff=0.3).training(gamma=0.9, lr=0.01)\
                        .resources(num_gpus=0)\
                        .rollouts(num_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> config = PPOConfig()
        >>> # Print out some default values.
        >>> print(config.clip_param)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), clip_param=0.2)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "PPO",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self):
        """Initializes a PPOConfig instance."""
        super().__init__(trainer_class=PPOTrainer)

        # fmt: off
        # __sphinx_doc_begin__
        #
        self.lr_schedule = None
        self.use_critic = True
        self.use_gae = True
        self.lambda_ = 1.0
        self.kl_coeff = 0.2
        self.sgd_minibatch_size = 128
        self.num_sgd_iter = 30
        self.shuffle_sequences = True
        self.vf_loss_coeff = 1.0
        self.entropy_coeff = 0.0
        self.entropy_coeff_schedule = None
        self.clip_param = 0.3
        self.vf_clip_param = 10.0
        self.grad_clip = None
        self.kl_target = 0.01
        # __sphinx_doc_end__
        # fmt: on

        # Override some of TrainerConfig's default values with PPO-specific values.
        self.rollout_fragment_length = 200
        self.train_batch_size = 4000
        self.lr = 5e-5
        self.model["vf_share_layers"] = False

    @override(TrainerConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        use_critic: Optional[bool] = None,
        use_gae: Optional[bool] = None,
        lambda_: Optional[float] = None,
        kl_coeff: Optional[float] = None,
        sgd_minibatch_size: Optional[int] = None,
        num_sgd_iter: Optional[int] = None,
        shuffle_sequences: Optional[bool] = None,
        vf_loss_coeff: Optional[float] = None,
        entropy_coeff: Optional[float] = None,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None,
        clip_param: Optional[float] = None,
        vf_clip_param: Optional[float] = None,
        grad_clip: Optional[float] = None,
        kl_target: Optional[float] = None,
        **kwargs,
    ) -> "PPOConfig":
        """Sets the training related configuration.

        Args:
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            use_critic: Should use a critic as a baseline (otherwise don't use value
                baseline; required for using GAE).
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
            lambda_: The GAE (lambda) parameter.
            kl_coeff: Initial coefficient for KL divergence.
            sgd_minibatch_size: Total SGD batch size across all devices for SGD.
                This defines the minibatch size within each epoch.
            num_sgd_iter: Number of SGD iterations in each outer loop (i.e., number of
                epochs to execute per train batch).
            shuffle_sequences: Whether to shuffle sequences in the batch when training
                (recommended).
            vf_loss_coeff: Coefficient of the value function loss. IMPORTANT: you must
                tune this if you set vf_share_layers=True inside your model's config.
            entropy_coeff: Coefficient of the entropy regularizer.
            entropy_coeff_schedule: Decay schedule for the entropy regularizer.
            clip_param: PPO clip parameter.
            vf_clip_param: Clip param for the value function. Note that this is
                sensitive to the scale of the rewards. If your expected V is large,
                increase this.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            kl_target: Target value for KL divergence.

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if use_critic is not None:
            self.use_critic = use_critic
        if use_gae is not None:
            self.use_gae = use_gae
        if lambda_ is not None:
            self.lambda_ = lambda_
        if kl_coeff is not None:
            self.kl_coeff = kl_coeff
        if sgd_minibatch_size is not None:
            self.sgd_minibatch_size = sgd_minibatch_size
        if num_sgd_iter is not None:
            self.num_sgd_iter = num_sgd_iter
        if shuffle_sequences is not None:
            self.shuffle_sequences = shuffle_sequences
        if vf_loss_coeff is not None:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not None:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not None:
            self.entropy_coeff_schedule = entropy_coeff_schedule
        if clip_param is not None:
            self.clip_param = clip_param
        if vf_clip_param is not None:
            self.vf_clip_param = vf_clip_param
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if kl_target is not None:
            self.kl_target = kl_target

        return self


# Deprecated: Use ray.rllib.agents.ppo.PPOConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(
            with_common_config(
                {
                    # PPO specific keys:
                    "use_critic": True,
                    "use_gae": True,
                    "lambda": 1.0,
                    "kl_coeff": 0.2,
                    "sgd_minibatch_size": 128,
                    "shuffle_sequences": True,
                    "num_sgd_iter": 30,
                    "lr_schedule": None,
                    "vf_loss_coeff": 1.0,
                    "entropy_coeff": 0.0,
                    "entropy_coeff_schedule": None,
                    "clip_param": 0.3,
                    "vf_clip_param": 10.0,
                    "grad_clip": None,
                    "kl_target": 0.01,
                    "rollout_fragment_length": 200,
                    # TrainerConfig overrides:
                    "train_batch_size": 4000,
                    "lr": 5e-5,
                    "model": {
                        "vf_share_layers": False,
                    },
                }
            )
        )

    @Deprecated(
        old="ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG",
        new="ray.rllib.agents.ppo.ppo.PPOConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()


class UpdateKL:
    """Callback to update the KL based on optimization info.

    This is used inside the execution_plan function. The Policy must define
    a `update_kl` method for this to work. This is achieved for PPO via a
    Policy mixin class (which adds the `update_kl` method),
    defined in ppo_[tf|torch]_policy.py.
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, fetches):
        def update(pi, pi_id):
            assert LEARNER_STATS_KEY not in fetches, (
                "{} should be nested under policy id key".format(LEARNER_STATS_KEY),
                fetches,
            )
            if pi_id in fetches:
                kl = fetches[pi_id][LEARNER_STATS_KEY].get("kl")
                assert kl is not None, (fetches, pi_id)
                # Make the actual `Policy.update_kl()` call.
                pi.update_kl(kl)
            else:
                logger.warning("No data for {}, not updating kl".format(pi_id))

        # Update KL on all trainable policies within the local (trainer)
        # Worker.
        self.workers.local_worker().foreach_policy_to_train(update)


def warn_about_bad_reward_scales(config, result):
    if result["policy_reward_mean"]:
        return result  # Punt on handling multiagent case.

    # Warn about excessively high VF loss.
    learner_info = result["info"][LEARNER_INFO]
    if DEFAULT_POLICY_ID in learner_info:
        scaled_vf_loss = (
            config["vf_loss_coeff"]
            * learner_info[DEFAULT_POLICY_ID][LEARNER_STATS_KEY]["vf_loss"]
        )

        policy_loss = learner_info[DEFAULT_POLICY_ID][LEARNER_STATS_KEY]["policy_loss"]
        if config.get("model", {}).get("vf_share_layers") and scaled_vf_loss > 100:
            logger.warning(
                "The magnitude of your value function loss is extremely large "
                "({}) compared to the policy loss ({}). This can prevent the "
                "policy from learning. Consider scaling down the VF loss by "
                "reducing vf_loss_coeff, or disabling vf_share_layers.".format(
                    scaled_vf_loss, policy_loss
                )
            )

    # Warn about bad clipping configs
    if config["vf_clip_param"] <= 0:
        rew_scale = float("inf")
    else:
        rew_scale = round(
            abs(result["episode_reward_mean"]) / config["vf_clip_param"], 0
        )
    if rew_scale > 200:
        logger.warning(
            "The magnitude of your environment rewards are more than "
            "{}x the scale of `vf_clip_param`. ".format(rew_scale)
            + "This means that it will take more than "
            "{} iterations for your value ".format(rew_scale)
            + "function to converge. If this is not intended, consider "
            "increasing `vf_clip_param`."
        )

    return result


class PPOTrainer(Trainer):
    # TODO: Change the return value of this method to return a TrainerConfig object
    #  instead.
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return PPOConfig().to_dict()

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        """Validates the Trainer's config dict.

        Args:
            config (TrainerConfigDict): The Trainer's config to check.

        Raises:
            ValueError: In case something is wrong with the config.
        """
        # Call super's validation method.
        super().validate_config(config)

        if isinstance(config["entropy_coeff"], int):
            config["entropy_coeff"] = float(config["entropy_coeff"])

        if config["entropy_coeff"] < 0.0:
            raise DeprecationWarning("entropy_coeff must be >= 0.0")

        # SGD minibatch size must be smaller than train_batch_size (b/c
        # we subsample a batch of `sgd_minibatch_size` from the train-batch for
        # each `sgd_num_iter`).
        # Note: Only check this if `train_batch_size` > 0 (DDPPO sets this
        # to -1 to auto-calculate the actual batch size later).
        if (
            config["train_batch_size"] > 0
            and config["sgd_minibatch_size"] > config["train_batch_size"]
        ):
            raise ValueError(
                "`sgd_minibatch_size` ({}) must be <= "
                "`train_batch_size` ({}).".format(
                    config["sgd_minibatch_size"], config["train_batch_size"]
                )
            )

        # Check for mismatches between `train_batch_size` and
        # `rollout_fragment_length` and auto-adjust `rollout_fragment_length`
        # if necessary.
        # Note: Only check this if `train_batch_size` > 0 (DDPPO sets this
        # to -1 to auto-calculate the actual batch size later).
        num_workers = config["num_workers"] or 1
        calculated_min_rollout_size = (
            num_workers
            * config["num_envs_per_worker"]
            * config["rollout_fragment_length"]
        )
        if (
            config["train_batch_size"] > 0
            and config["train_batch_size"] % calculated_min_rollout_size != 0
        ):
            new_rollout_fragment_length = config["train_batch_size"] // (
                num_workers * config["num_envs_per_worker"]
            )
            logger.warning(
                "`train_batch_size` ({}) cannot be achieved with your other "
                "settings (num_workers={} num_envs_per_worker={} "
                "rollout_fragment_length={})! Auto-adjusting "
                "`rollout_fragment_length` to {}.".format(
                    config["train_batch_size"],
                    config["num_workers"],
                    config["num_envs_per_worker"],
                    config["rollout_fragment_length"],
                    new_rollout_fragment_length,
                )
            )
            config["rollout_fragment_length"] = new_rollout_fragment_length

        # Episodes may only be truncated (and passed into PPO's
        # `postprocessing_fn`), iff generalized advantage estimation is used
        # (value function estimate at end of truncated episode to estimate
        # remaining value).
        if config["batch_mode"] == "truncate_episodes" and not config["use_gae"]:
            raise ValueError(
                "Episode truncation is not supported without a value "
                "function (to estimate the return at the end of the truncated"
                " trajectory). Consider setting "
                "batch_mode=complete_episodes."
            )

        # Multi-agent mode and multi-GPU optimizer.
        if config["multiagent"]["policies"] and not config["simple_optimizer"]:
            logger.info(
                "In multi-agent mode, policies will be optimized sequentially"
                " by the multi-GPU optimizer. Consider setting "
                "simple_optimizer=True if this doesn't work for you."
            )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.agents.ppo.ppo_torch_policy import PPOTorchPolicy

            return PPOTorchPolicy
        else:
            return PPOTFPolicy

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "PPO execution_plan does NOT take any additional parameters"

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # Collect batches for the trainable policies.
        rollouts = rollouts.for_each(
            SelectExperiences(local_worker=workers.local_worker())
        )
        # Concatenate the SampleBatches into one.
        rollouts = rollouts.combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"]["count_steps_by"],
            )
        )
        # Standardize advantages.
        rollouts = rollouts.for_each(StandardizeFields(["advantages"]))

        # Perform one training step on the combined + standardized batch.
        if config["simple_optimizer"]:
            train_op = rollouts.for_each(
                TrainOneStep(
                    workers,
                    num_sgd_iter=config["num_sgd_iter"],
                    sgd_minibatch_size=config["sgd_minibatch_size"],
                )
            )
        else:
            train_op = rollouts.for_each(
                MultiGPUTrainOneStep(
                    workers=workers,
                    sgd_minibatch_size=config["sgd_minibatch_size"],
                    num_sgd_iter=config["num_sgd_iter"],
                    num_gpus=config["num_gpus"],
                    _fake_gpus=config["_fake_gpus"],
                )
            )

        # Update KL after each round of training.
        train_op = train_op.for_each(lambda t: t[1]).for_each(UpdateKL(workers))

        # Warn about bad reward scales and return training metrics.
        return StandardMetricsReporting(train_op, workers, config).for_each(
            lambda result: warn_about_bad_reward_scales(config, result)
        )
