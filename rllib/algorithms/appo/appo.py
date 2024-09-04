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
from ray.rllib.algorithms.impala.impala import IMPALA, IMPALAConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
)
from ray.rllib.utils.metrics import LEARNER_STATS_KEY
from ray.rllib.utils.typing import (
    ResultDict,
)

logger = logging.getLogger(__name__)


LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
OLD_ACTION_DIST_KEY = "old_action_dist"
OLD_ACTION_DIST_LOGITS_KEY = "old_action_dist_logits"


class APPOConfig(IMPALAConfig):
    """Defines a configuration class from which an APPO Algorithm can be built.

    .. testcode::

        from ray.rllib.algorithms.appo import APPOConfig
        config = APPOConfig().training(lr=0.01, grad_clip=30.0, train_batch_size=50)
        config = config.resources(num_gpus=0)
        config = config.env_runners(num_env_runners=1)
        config = config.environment("CartPole-v1")

        # Build an Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()
        del algo

    .. testcode::

        from ray.rllib.algorithms.appo import APPOConfig
        from ray import air
        from ray import tune

        config = APPOConfig()
        # Update the config object.
        config = config.training(lr=tune.grid_search([0.001,]))
        # Set the config object's env.
        config = config.environment(env="CartPole-v1")
        # Use to_dict() to get the old-style python config dict
        # when running with tune.
        tune.Tuner(
            "APPO",
            run_config=air.RunConfig(stop={"training_iteration": 1},
                                     verbose=0),
            param_space=config.to_dict(),

        ).fit()

    .. testoutput::
        :hide:

        ...
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
        # TODO (sven): Activate once v-trace sequences in non-RNN batch are solved.
        #  If we switch this on right now, the shuffling would destroy the rollout
        #  sequences (non-zero-padded!) needed in the batch for v-trace.
        # self.shuffle_batch_per_epoch = True

        # Override some of IMPALAConfig's default values with APPO-specific values.
        self.num_env_runners = 2
        self.min_time_s_per_iteration = 10
        self.target_network_update_freq = 1
        self.learner_queue_size = 16
        self.learner_queue_timeout = 300
        self.max_sample_requests_in_flight_per_worker = 2
        self.broadcast_interval = 1
        self.grad_clip = 40.0
        # Note: Only when using enable_rl_module_and_learner=True can the clipping mode
        # be configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"

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

        self.num_gpus = 0  # @OldAPIStack
        self.num_multi_gpu_tower_stacks = 1  # @OldAPIStack
        self.minibatch_buffer_size = 1  # @OldAPIStack
        self.replay_proportion = 0.0  # @OldAPIStack
        self.replay_buffer_num_slots = 100  # @OldAPIStack

        # __sphinx_doc_end__
        # fmt: on

        self.target_update_frequency = DEPRECATED_VALUE

    @override(IMPALAConfig)
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
        target_network_update_freq: Optional[int] = NotProvided,
        # Deprecated keys.
        target_update_frequency=DEPRECATED_VALUE,
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
            target_network_update_freq: The frequency to update the target policy and
                tune the kl loss coefficients that are used during training. After
                setting this parameter, the algorithm waits for at least
                `target_network_update_freq` number of environment samples to be trained
                on before updating the target networks and tune the kl loss
                coefficients. NOTE: This parameter is only applicable when using the
                Learner API (enable_rl_module_and_learner=True).

        Returns:
            This updated AlgorithmConfig object.
        """
        if target_update_frequency != DEPRECATED_VALUE:
            deprecation_warning(
                old="target_update_frequency",
                new="target_network_update_freq",
                error=False,
            )
            target_network_update_freq = target_update_frequency

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
        if use_kl_loss is not NotProvided:
            self.use_kl_loss = use_kl_loss
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
        if kl_target is not NotProvided:
            self.kl_target = kl_target
        if tau is not NotProvided:
            self.tau = tau
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq

        return self

    @override(IMPALAConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            from ray.rllib.algorithms.appo.torch.appo_torch_learner import (
                APPOTorchLearner,
            )

            return APPOTorchLearner
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.appo.tf.appo_tf_learner import APPOTfLearner

            return APPOTfLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(IMPALAConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.appo.torch.appo_torch_rl_module import (
                APPOTorchRLModule as RLModule,
            )
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.appo.tf.appo_tf_rl_module import (
                APPOTfRLModule as RLModule,
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

        from ray.rllib.algorithms.appo.appo_catalog import APPOCatalog

        return RLModuleSpec(module_class=RLModule, catalog_class=APPOCatalog)

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self):
        return super()._model_config_auto_includes | {"vf_share_layers": False}


class APPO(IMPALA):
    def __init__(self, config, *args, **kwargs):
        """Initializes an APPO instance."""
        super().__init__(config, *args, **kwargs)

        # After init: Initialize target net.

        # TODO(avnishn): Does this need to happen in __init__? I think we can move it
        #  to setup()
        if not self.config.enable_rl_module_and_learner:
            self.env_runner.foreach_policy_to_train(lambda p, _: p.update_target())

    @override(IMPALA)
    def training_step(self) -> ResultDict:
        train_results = super().training_step()

        # Update the target network and the KL coefficient for the APPO-loss.
        # The target network update frequency is calculated automatically by the product
        # of `num_epochs` setting (usually 1 for APPO) and `minibatch_buffer_size`.
        if self.config.enable_rl_module_and_learner:
            if NUM_TARGET_UPDATES in train_results:
                self._counters[NUM_TARGET_UPDATES] += train_results[NUM_TARGET_UPDATES]
                self._counters[LAST_TARGET_UPDATE_TS] = train_results[
                    LAST_TARGET_UPDATE_TS
                ]
        else:
            last_update = self._counters[LAST_TARGET_UPDATE_TS]
            cur_ts = self._counters[
                (
                    NUM_AGENT_STEPS_SAMPLED
                    if self.config.count_steps_by == "agent_steps"
                    else NUM_ENV_STEPS_SAMPLED
                )
            ]
            target_update_freq = (
                self.config.num_epochs * self.config.minibatch_buffer_size
            )
            if cur_ts - last_update > target_update_freq:
                self._counters[NUM_TARGET_UPDATES] += 1
                self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

                # Update our target network.
                self.env_runner.foreach_policy_to_train(lambda p, _: p.update_target())

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
                    self.env_runner.foreach_policy_to_train(update)

        return train_results

    @classmethod
    @override(IMPALA)
    def get_default_config(cls) -> AlgorithmConfig:
        return APPOConfig()

    @classmethod
    @override(IMPALA)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.appo.appo_torch_policy import APPOTorchPolicy

            return APPOTorchPolicy
        elif config["framework"] == "tf":
            if config.enable_rl_module_and_learner:
                raise ValueError(
                    "RLlib's RLModule and Learner API is not supported for"
                    " tf1. Use "
                    "framework='tf2' instead."
                )
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF1Policy

            return APPOTF1Policy
        else:
            from ray.rllib.algorithms.appo.appo_tf_policy import APPOTF2Policy

            return APPOTF2Policy
