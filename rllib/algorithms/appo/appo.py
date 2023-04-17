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

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.impala.impala import Impala, ImpalaConfig
from ray.rllib.algorithms.appo.tf.appo_tf_learner import AppoHPs, LEARNER_RESULTS_KL_KEY
from ray.rllib.algorithms.ppo.ppo import UpdateKL
from ray.rllib.execution.common import _get_shared_metrics, STEPS_SAMPLED_COUNTER
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    NUM_ENV_STEPS_TRAINED,
    NUM_AGENT_STEPS_TRAINED,
)
from ray.rllib.utils.metrics import ALL_MODULES, LEARNER_STATS_KEY
from ray.rllib.utils.typing import (
    ResultDict,
)

logger = logging.getLogger(__name__)


class APPOConfig(ImpalaConfig):
    """Defines a configuration class from which an APPO Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.appo import APPOConfig
        >>> config = APPOConfig().training(lr=0.01, grad_clip=30.0)
        >>> config = config.resources(num_gpus=1)
        >>> config = config.rollouts(num_rollout_workers=16)
        >>> config = config.environment("CartPole-v1")
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build an Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.appo import APPOConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = APPOConfig()
        >>> # Print out some default values.
        >>> print(config.sample_async)   # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(lr=tune.grid_search([0.001, 0.0001]))
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
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
        self._learner_hps = AppoHPs()
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
        self.target_update_frequency = 1
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
        self.tau = 1.0
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        # __sphinx_doc_end__
        # fmt: on

    @override(ImpalaConfig)
    def training(
        self,
        *,
        vtrace: Optional[bool] = NotProvided,
        use_critic: Optional[bool] = NotProvided,
        use_gae: Optional[bool] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        clip_param: Optional[float] = NotProvided,
        use_kl_loss: Optional[bool] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        kl_target: Optional[float] = NotProvided,
        tau: Optional[float] = NotProvided,
        target_update_frequency: Optional[int] = NotProvided,
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
            tau: The factor by which to update the target policy network towards
                the current policy network. Can range between 0 and 1.
                e.g. updated_param = tau * current_param + (1 - tau) * target_param
            target_update_frequency: The frequency to update the target policy and
                tune the kl loss coefficients that are used during training. After
                setting this parameter, the algorithm waits for at least
                `target_update_frequency * minibatch_size * num_sgd_iter` number of
                samples to be trained on by the learner group before updating the target
                networks and tuned the kl loss coefficients that are used during
                training.
                NOTE: this parameter is only applicable when using the learner api
                (_enable_learner_api=True and _enable_rl_module_api=True).


        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if vtrace is not NotProvided:
            self.vtrace = vtrace
        if use_critic is not NotProvided:
            self.use_critic = use_critic
        if use_gae is not NotProvided:
            self.use_gae = use_gae
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if clip_param is not NotProvided:
            self.clip_param = clip_param
            self._learner_hps.clip_param = clip_param
        if use_kl_loss is not NotProvided:
            self.use_kl_loss = use_kl_loss
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
            self._learner_hps.kl_coeff = kl_coeff
        if kl_target is not NotProvided:
            self.kl_target = kl_target
            self._learner_hps.kl_target = kl_target
        if tau is not NotProvided:
            self.tau = tau
            self._learner_hps.tau = tau
        if target_update_frequency is not NotProvided:
            self.target_update_frequency = target_update_frequency

        return self

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.appo.tf.appo_tf_learner import APPOTfLearner

            return APPOTfLearner
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> SingleAgentRLModuleSpec:
        if self.framework_str == "tf2":
            from ray.rllib.algorithms.appo.appo_catalog import APPOCatalog
            from ray.rllib.algorithms.appo.tf.appo_tf_rl_module import APPOTfRLModule

            return SingleAgentRLModuleSpec(
                module_class=APPOTfRLModule, catalog_class=APPOCatalog
            )
        else:
            raise ValueError(f"The framework {self.framework_str} is not supported.")

    @override(ImpalaConfig)
    def validate(self) -> None:
        super().validate()
        self._learner_hps.tau = self.tau
        self._learner_hps.kl_target = self.kl_target
        self._learner_hps.kl_coeff = self.kl_coeff
        self._learner_hps.clip_param = self.clip_param


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
            if self.config.use_kl_loss:
                self.update_kl(fetches)


class APPO(Impala):
    def __init__(self, config, *args, **kwargs):
        """Initializes an APPO instance."""
        super().__init__(config, *args, **kwargs)

        # After init: Initialize target net.

        # TODO(avnishn):
        # does this need to happen in __init__? I think we can move it to setup()
        if not self.config._enable_rl_module_api:
            self.workers.local_worker().foreach_policy_to_train(
                lambda p, _: p.update_target()
            )

    @override(Impala)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # TODO(avnishn):
        # this attribute isn't used anywhere else in the code. I think we can safely
        # delete it.
        if not self.config._enable_rl_module_api:
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

        last_update = self._counters[LAST_TARGET_UPDATE_TS]

        if self.config._enable_learner_api and train_results:
            # using steps trained here instead of sampled ... I'm not sure why the
            # other implemenetation uses sampled.
            # to be quite frank, im not sure if I understand how their target update
            # freq would work. The difference in steps sampled/trained is pretty
            # much always going to be larger than self.config.num_sgd_iter *
            # self.config.minibatch_buffer_size unless the number of steps collected
            # is really small. The thing is that the default rollout fragment length
            # is 50, so the minibatch buffer size * num_sgd_iter is going to be
            # have to be 50 to even meet the threshold of having delayed target
            # updates.
            # we should instead have the target / kl threshold update be based off
            # of the train_batch_size * some target update frequency * num_sgd_iter.
            cur_ts = self._counters[
                NUM_ENV_STEPS_TRAINED
                if self.config.count_steps_by == "env_steps"
                else NUM_AGENT_STEPS_TRAINED
            ]
            target_update_steps_freq = (
                self.config.train_batch_size
                * self.config.num_sgd_iter
                * self.config.target_update_frequency
            )
            if (cur_ts - last_update) >= target_update_steps_freq:
                kls_to_update = {}
                for module_id, module_results in train_results.items():
                    if module_id != ALL_MODULES:
                        kls_to_update[module_id] = module_results[LEARNER_STATS_KEY][
                            LEARNER_RESULTS_KL_KEY
                        ]
                self._counters[NUM_TARGET_UPDATES] += 1
                self._counters[LAST_TARGET_UPDATE_TS] = cur_ts
                self.learner_group.additional_update(sampled_kls=kls_to_update)

        else:
            cur_ts = self._counters[
                NUM_AGENT_STEPS_SAMPLED
                if self.config.count_steps_by == "agent_steps"
                else NUM_ENV_STEPS_SAMPLED
            ]
            target_update_freq = (
                self.config.num_sgd_iter * self.config.minibatch_buffer_size
            )
            if cur_ts - last_update > target_update_freq:
                self._counters[NUM_TARGET_UPDATES] += 1
                self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

                # Update our target network.
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, _: p.update_target()
                )

                # Also update the KL-coefficient for the APPO loss, if necessary.
                if self.config.use_kl_loss:

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
                            logger.warning(
                                "No data for {}, not updating kl".format(pi_id)
                            )

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
    def get_default_config(cls) -> AlgorithmConfig:
        return APPOConfig()

    @classmethod
    @override(Impala)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            if config._enable_rl_module_api:
                raise ValueError(
                    "APPO with the torch backend is not yet supported by "
                    " the RLModule and Learner API."
                )
            else:
                from ray.rllib.algorithms.appo.appo_torch_policy import APPOTorchPolicy

                return APPOTorchPolicy
        elif config["framework"] == "tf":
            if config._enable_rl_module_api:
                raise ValueError(
                    "RLlib's RLModule and Learner API is not supported for"
                    " tf1. Use "
                    "framework='tf2' instead."
                )
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF1Policy

            return APPOTF1Policy
        else:
            if config._enable_rl_module_api:
                # TODO(avnishn): This policy class doesn't work just yet
                from ray.rllib.algorithms.appo.tf.appo_tf_policy_rlm import (
                    APPOTfPolicyWithRLModule,
                )

                return APPOTfPolicyWithRLModule
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF2Policy

            return APPOTF2Policy
