"""
Proximal Policy Optimization (PPO)
==================================

This file defines the distributed Algorithm class for proximal policy
optimization.
See `ppo_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#ppo
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union, TYPE_CHECKING

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    standardize_fields,
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
    TIMERS,
    ALL_MODULES,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ResultDict
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.rllib.core.learner.learner import Learner


logger = logging.getLogger(__name__)

LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = "vf_loss_unclipped"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_var"
LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


class PPOConfig(AlgorithmConfig):
    """Defines a configuration class from which a PPO Algorithm can be built.

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig

        config = PPOConfig()
        config.environment("CartPole-v1")
        config.env_runners(num_env_runners=1)
        config.training(
            gamma=0.9, lr=0.01, kl_coeff=0.3, train_batch_size_per_learner=256
        )

        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray import tune

        config = (
            PPOConfig()
            # Set the config object's env.
            .environment(env="CartPole-v1")
            # Update the config object's training parameters.
            .training(
                lr=0.001, clip_param=0.2
            )
        )

        tune.Tuner(
            "PPO",
            run_config=tune.RunConfig(stop={"training_iteration": 1}),
            param_space=config,
        ).fit()

    .. testoutput::
        :hide:

        ...
    """

    def __init__(self, algo_class=None):
        """Initializes a PPOConfig instance."""
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        super().__init__(algo_class=algo_class or PPO)

        # fmt: off
        # __sphinx_doc_begin__
        self.lr = 5e-5
        self.rollout_fragment_length = "auto"
        self.train_batch_size = 4000

        # PPO specific settings:
        self.use_critic = True
        self.use_gae = True
        self.num_epochs = 30
        self.minibatch_size = 128
        self.shuffle_batch_per_epoch = True
        self.lambda_ = 1.0
        self.use_kl_loss = True
        self.kl_coeff = 0.2
        self.kl_target = 0.01
        self.vf_loss_coeff = 1.0
        self.entropy_coeff = 0.0
        self.clip_param = 0.3
        self.vf_clip_param = 10.0
        self.grad_clip = None

        # Override some of AlgorithmConfig's default values with PPO-specific values.
        self.num_env_runners = 2
        # __sphinx_doc_end__
        # fmt: on

        self.model["vf_share_layers"] = False  # @OldAPIStack
        self.entropy_coeff_schedule = None  # @OldAPIStack
        self.lr_schedule = None  # @OldAPIStack

        # Deprecated keys.
        self.sgd_minibatch_size = DEPRECATED_VALUE
        self.vf_share_layers = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
                DefaultPPOTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultPPOTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import (
                PPOTorchLearner,
            )

            return PPOTorchLearner
        elif self.framework_str in ["tf2", "tf"]:
            raise ValueError(
                "TensorFlow is no longer supported on the new API stack! "
                "Use `framework='torch'`."
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `framework='torch'`."
            )

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        use_critic: Optional[bool] = NotProvided,
        use_gae: Optional[bool] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        use_kl_loss: Optional[bool] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        kl_target: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        clip_param: Optional[float] = NotProvided,
        vf_clip_param: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        # @OldAPIStack
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        # Deprecated.
        vf_share_layers=DEPRECATED_VALUE,
        **kwargs,
    ) -> "PPOConfig":
        """Sets the training related configuration.

        Args:
            use_critic: Should use a critic as a baseline (otherwise don't use value
                baseline; required for using GAE).
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
            lambda_: The lambda parameter for General Advantage Estimation (GAE).
                Defines the exponential weight used between actually measured rewards
                vs value function estimates over multiple time steps. Specifically,
                `lambda_` balances short-term, low-variance estimates against long-term,
                high-variance returns. A `lambda_` of 0.0 makes the GAE rely only on
                immediate rewards (and vf predictions from there on, reducing variance,
                but increasing bias), while a `lambda_` of 1.0 only incorporates vf
                predictions at the truncation points of the given episodes or episode
                chunks (reducing bias but increasing variance).
            use_kl_loss: Whether to use the KL-term in the loss function.
            kl_coeff: Initial coefficient for KL divergence.
            kl_target: Target value for KL divergence.
            vf_loss_coeff: Coefficient of the value function loss. IMPORTANT: you must
                tune this if you set vf_share_layers=True inside your model's config.
            entropy_coeff: The entropy coefficient (float) or entropy coefficient
                schedule in the format of
                [[timestep, coeff-value], [timestep, coeff-value], ...]
                In case of a schedule, intermediary timesteps will be assigned to
                linearly interpolated coefficient values. A schedule config's first
                entry must start with timestep 0, i.e.: [[0, initial_value], [...]].
            clip_param: The PPO clip parameter.
            vf_clip_param: Clip param for the value function. Note that this is
                sensitive to the scale of the rewards. If your expected V is large,
                increase this.
            grad_clip: If specified, clip the global norm of gradients by this amount.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if use_critic is not NotProvided:
            self.use_critic = use_critic
            # TODO (Kourosh) This is experimental.
            #  Don't forget to remove .use_critic from algorithm config.
        if use_gae is not NotProvided:
            self.use_gae = use_gae
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if use_kl_loss is not NotProvided:
            self.use_kl_loss = use_kl_loss
        if kl_coeff is not NotProvided:
            self.kl_coeff = kl_coeff
        if kl_target is not NotProvided:
            self.kl_target = kl_target
        if vf_loss_coeff is not NotProvided:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if clip_param is not NotProvided:
            self.clip_param = clip_param
        if vf_clip_param is not NotProvided:
            self.vf_clip_param = vf_clip_param
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip

        # TODO (sven): Remove these once new API stack is only option for PPO.
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if entropy_coeff_schedule is not NotProvided:
            self.entropy_coeff_schedule = entropy_coeff_schedule

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Synchronous sampling, on-policy/PPO algos -> Check mismatches between
        # `rollout_fragment_length` and `train_batch_size_per_learner` to avoid user
        # confusion.
        # TODO (sven): Make rollout_fragment_length a property and create a private
        #  attribute to store (possibly) user provided value (or "auto") in. Deprecate
        #  `self.get_rollout_fragment_length()`.
        self.validate_train_batch_size_vs_rollout_fragment_length()

        # SGD minibatch size must be smaller than train_batch_size (b/c
        # we subsample a batch of `minibatch_size` from the train-batch for
        # each `num_epochs`).
        if (
            not self.enable_rl_module_and_learner
            and self.minibatch_size > self.train_batch_size
        ):
            self._value_error(
                f"`minibatch_size` ({self.minibatch_size}) must be <= "
                f"`train_batch_size` ({self.train_batch_size}). In PPO, the train batch"
                f" will be split into {self.minibatch_size} chunks, each of which "
                f"is iterated over (used for updating the policy) {self.num_epochs} "
                "times."
            )
        elif self.enable_rl_module_and_learner:
            mbs = self.minibatch_size
            tbs = self.train_batch_size_per_learner or self.train_batch_size
            if isinstance(mbs, int) and isinstance(tbs, int) and mbs > tbs:
                self._value_error(
                    f"`minibatch_size` ({mbs}) must be <= "
                    f"`train_batch_size_per_learner` ({tbs}). In PPO, the train batch"
                    f" will be split into {mbs} chunks, each of which is iterated over "
                    f"(used for updating the policy) {self.num_epochs} times."
                )

        # Episodes may only be truncated (and passed into PPO's
        # `postprocessing_fn`), iff generalized advantage estimation is used
        # (value function estimate at end of truncated episode to estimate
        # remaining value).
        if (
            not self.in_evaluation
            and self.batch_mode == "truncate_episodes"
            and not self.use_gae
        ):
            self._value_error(
                "Episode truncation is not supported without a value "
                "function (to estimate the return at the end of the truncated"
                " trajectory). Consider setting "
                "batch_mode=complete_episodes."
            )

        # New API stack checks.
        if self.enable_rl_module_and_learner:
            # `lr_schedule` checking.
            if self.lr_schedule is not None:
                self._value_error(
                    "`lr_schedule` is deprecated and must be None! Use the "
                    "`lr` setting to setup a schedule."
                )
            if self.entropy_coeff_schedule is not None:
                self._value_error(
                    "`entropy_coeff_schedule` is deprecated and must be None! Use the "
                    "`entropy_coeff` setting to setup a schedule."
                )
            Scheduler.validate(
                fixed_value_or_schedule=self.entropy_coeff,
                setting_name="entropy_coeff",
                description="entropy coefficient",
            )
        if isinstance(self.entropy_coeff, float) and self.entropy_coeff < 0.0:
            self._value_error("`entropy_coeff` must be >= 0.0")

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        return super()._model_config_auto_includes | {"vf_share_layers": False}


class PPO(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return PPOConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":

            from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy

            return PPOTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy

            return PPOTF1Policy
        else:
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF2Policy

            return PPOTF2Policy

    @override(Algorithm)
    def training_step(self) -> None:
        # Old API stack (Policy, RolloutWorker, Connector).
        if not self.config.enable_env_runner_and_connector_v2:
            return self._training_step_old_api_stack()

        # Collect batches from sample workers until we have a full batch.
        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            # Sample in parallel from the workers.
            if self.config.count_steps_by == "agent_steps":
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=(
                        self.config.enable_env_runner_and_connector_v2
                    ),
                    _return_metrics=True,
                )
            else:
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_env_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=(
                        self.config.enable_env_runner_and_connector_v2
                    ),
                    _return_metrics=True,
                )
            # Return early if all our workers failed.
            if not episodes:
                return

            # Reduce EnvRunner metrics over the n EnvRunners.
            self.metrics.merge_and_log_n_dicts(
                env_runner_results, key=ENV_RUNNER_RESULTS
            )

        # Perform a learner update step on the collected episodes.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update(
                episodes=episodes,
                timesteps={
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                        self.metrics.peek(
                            (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME)
                        )
                    ),
                },
                num_epochs=self.config.num_epochs,
                minibatch_size=self.config.minibatch_size,
                shuffle_batch_per_epoch=self.config.shuffle_batch_per_epoch,
            )
            self.metrics.merge_and_log_n_dicts(learner_results, key=LEARNER_RESULTS)

        # Update weights - after learning on the local worker - on all remote
        # workers.
        with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
            # The train results's loss keys are ModuleIDs to their loss values.
            # But we also return a total_loss key at the same level as the ModuleID
            # keys. So we need to subtract that to get the correct set of ModuleIDs to
            # update.
            # TODO (sven): We should also not be using `learner_results` as a messenger
            #  to infer which modules to update. `policies_to_train` might also NOT work
            #  as it might be a very large set (100s of Modules) vs a smaller Modules
            #  set that's present in the current train batch.
            modules_to_update = set(learner_results[0].keys()) - {ALL_MODULES}
            self.env_runner_group.sync_weights(
                # Sync weights from learner_group to all EnvRunners.
                from_worker_or_learner_group=self.learner_group,
                policies=modules_to_update,
                inference_only=True,
            )

    @OldAPIStack
    def _training_step_old_api_stack(self) -> ResultDict:
        # Collect batches from sample workers until we have a full batch.
        with self._timers[SAMPLE_TIMER]:
            if self.config.count_steps_by == "agent_steps":
                train_batch = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                )
            else:
                train_batch = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_env_steps=self.config.total_train_batch_size,
                    sample_timeout_s=self.config.sample_timeout_s,
                )
            # Return early if all our workers failed.
            if not train_batch:
                return {}
            train_batch = train_batch.as_multi_agent()
            self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
            self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()
            # Standardize advantages.
            train_batch = standardize_fields(train_batch, ["advantages"])

        if self.config.simple_optimizer:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        policies_to_update = list(train_results.keys())

        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            # TODO (sven): num_grad_updates per each policy should be
            #  accessible via `train_results` (and get rid of global_vars).
            "num_grad_updates_per_policy": {
                pid: self.env_runner.policy_map[pid].num_grad_updates
                for pid in policies_to_update
            },
        }

        # Update weights - after learning on the local worker - on all remote
        # workers.
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            if self.env_runner_group.num_remote_workers() > 0:
                from_worker_or_learner_group = None
                self.env_runner_group.sync_weights(
                    from_worker_or_learner_group=from_worker_or_learner_group,
                    policies=policies_to_update,
                    global_vars=global_vars,
                )

        # For each policy: Update KL scale and warn about possible issues
        for policy_id, policy_info in train_results.items():
            # Update KL loss with dynamic scaling
            # for each (possibly multiagent) policy we are training
            kl_divergence = policy_info[LEARNER_STATS_KEY].get("kl")
            self.get_policy(policy_id).update_kl(kl_divergence)

            # Warn about excessively high value function loss
            scaled_vf_loss = (
                self.config.vf_loss_coeff * policy_info[LEARNER_STATS_KEY]["vf_loss"]
            )
            policy_loss = policy_info[LEARNER_STATS_KEY]["policy_loss"]
            if (
                log_once("ppo_warned_lr_ratio")
                and self.config.get("model", {}).get("vf_share_layers")
                and scaled_vf_loss > 100
            ):
                logger.warning(
                    "The magnitude of your value function loss for policy: {} is "
                    "extremely large ({}) compared to the policy loss ({}). This "
                    "can prevent the policy from learning. Consider scaling down "
                    "the VF loss by reducing vf_loss_coeff, or disabling "
                    "vf_share_layers.".format(policy_id, scaled_vf_loss, policy_loss)
                )
            # Warn about bad clipping configs.
            train_batch.policy_batches[policy_id].set_get_interceptor(None)
            mean_reward = train_batch.policy_batches[policy_id]["rewards"].mean()
            if (
                log_once("ppo_warned_vf_clip")
                and mean_reward > self.config.vf_clip_param
            ):
                self.warned_vf_clip = True
                logger.warning(
                    f"The mean reward returned from the environment is {mean_reward}"
                    f" but the vf_clip_param is set to {self.config['vf_clip_param']}."
                    f" Consider increasing it for policy: {policy_id} to improve"
                    " value function convergence."
                )

        # Update global vars on local worker as well.
        # TODO (simon): At least in RolloutWorker obsolete I guess as called in
        #  `sync_weights()` called above if remote workers. Can we call this
        #  where `set_weights()` is called on the local_worker?
        self.env_runner.set_global_vars(global_vars)

        return train_results
