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

import numpy as np

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.execution.rollout_ops import (
    standardize_fields,
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    train_one_step,
    multi_gpu_train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_EPISODES,
    NUM_EPISODES_LIFETIME,
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
        config = config.training(gamma=0.9, lr=0.01, kl_coeff=0.3,
            train_batch_size=128)
        config = config.resources(num_gpus=0)
        config = config.env_runners(num_env_runners=1)

        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build(env="CartPole-v1")
        algo.train()

    .. testcode::

        from ray.rllib.algorithms.ppo import PPOConfig
        from ray import air
        from ray import tune
        config = PPOConfig()
        # Print out some default values.

        # Update the config object.
        config.training(
            lr=tune.grid_search([0.001 ]), clip_param=0.2
        )
        # Set the config object's env.
        config = config.environment(env="CartPole-v1")

        # Use to_dict() to get the old-style python config dict
        # when running with tune.
        tune.Tuner(
            "PPO",
            run_config=air.RunConfig(stop={"training_iteration": 1}),
            param_space=config.to_dict(),
        ).fit()

    .. testoutput::
        :hide:

        ...
    """

    def __init__(self, algo_class=None):
        """Initializes a PPOConfig instance."""
        super().__init__(algo_class=algo_class or PPO)

        # fmt: off
        # __sphinx_doc_begin__
        self.lr_schedule = None
        self.lr = 5e-5
        self.rollout_fragment_length = "auto"
        self.train_batch_size = 4000

        # PPO specific settings:
        self.use_critic = True
        self.use_gae = True
        self.lambda_ = 1.0
        self.use_kl_loss = True
        self.kl_coeff = 0.2
        self.kl_target = 0.01
        self.sgd_minibatch_size = 128
        # Simple logic for now: If None, use `train_batch_size`.
        self.mini_batch_size_per_learner = None
        self.num_sgd_iter = 30
        self.shuffle_sequences = True
        self.vf_loss_coeff = 1.0
        self.entropy_coeff = 0.0
        self.entropy_coeff_schedule = None
        self.clip_param = 0.3
        self.vf_clip_param = 10.0
        self.grad_clip = None

        # Override some of AlgorithmConfig's default values with PPO-specific values.
        self.num_env_runners = 2
        self.model["vf_share_layers"] = False
        # __sphinx_doc_end__
        # fmt: on

        # Deprecated keys.
        self.vf_share_layers = DEPRECATED_VALUE

        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> SingleAgentRLModuleSpec:
        from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

        if self.framework_str == "torch":
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule,
            )

            return SingleAgentRLModuleSpec(
                module_class=PPOTorchRLModule, catalog_class=PPOCatalog
            )
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule

            return SingleAgentRLModuleSpec(
                module_class=PPOTfRLModule, catalog_class=PPOCatalog
            )
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
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.ppo.tf.ppo_tf_learner import PPOTfLearner

            return PPOTfLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        use_critic: Optional[bool] = NotProvided,
        use_gae: Optional[bool] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        use_kl_loss: Optional[bool] = NotProvided,
        kl_coeff: Optional[float] = NotProvided,
        kl_target: Optional[float] = NotProvided,
        mini_batch_size_per_learner: Optional[int] = NotProvided,
        sgd_minibatch_size: Optional[int] = NotProvided,
        num_sgd_iter: Optional[int] = NotProvided,
        shuffle_sequences: Optional[bool] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        clip_param: Optional[float] = NotProvided,
        vf_clip_param: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        # Deprecated.
        vf_share_layers=DEPRECATED_VALUE,
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
            use_kl_loss: Whether to use the KL-term in the loss function.
            kl_coeff: Initial coefficient for KL divergence.
            kl_target: Target value for KL divergence.
            mini_batch_size_per_learner: Only use if new API stack is enabled.
                The mini batch size per Learner worker. This is the
                batch size that each Learner worker's training batch (whose size is
                `s`elf.train_batch_size_per_learner`) will be split into. For example,
                if the train batch size per Learner worker is 4000 and the mini batch
                size per Learner worker is 400, the train batch will be split into 10
                equal sized chunks (or "mini batches"). Each such mini batch will be
                used for one SGD update. Overall, the train batch on each Learner
                worker will be traversed `self.num_sgd_iter` times. In the above
                example, if `self.num_sgd_iter` is 5, we will altogether perform 50
                (10x5) SGD updates per Learner update step.
            sgd_minibatch_size: Total SGD batch size across all devices for SGD.
                This defines the minibatch size within each epoch. Deprecated on the
                new API stack (use `mini_batch_size_per_learner` instead).
            num_sgd_iter: Number of SGD iterations in each outer loop (i.e., number of
                epochs to execute per train batch).
            shuffle_sequences: Whether to shuffle sequences in the batch when training
                (recommended).
            vf_loss_coeff: Coefficient of the value function loss. IMPORTANT: you must
                tune this if you set vf_share_layers=True inside your model's config.
            entropy_coeff: Coefficient of the entropy regularizer.
            entropy_coeff_schedule: Decay schedule for the entropy regularizer.
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
        if mini_batch_size_per_learner is not NotProvided:
            self.mini_batch_size_per_learner = mini_batch_size_per_learner
        if sgd_minibatch_size is not NotProvided:
            self.sgd_minibatch_size = sgd_minibatch_size
        if num_sgd_iter is not NotProvided:
            self.num_sgd_iter = num_sgd_iter
        if shuffle_sequences is not NotProvided:
            self.shuffle_sequences = shuffle_sequences
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
        # we subsample a batch of `sgd_minibatch_size` from the train-batch for
        # each `num_sgd_iter`).
        if (
            not self.enable_rl_module_and_learner
            and self.sgd_minibatch_size > self.train_batch_size
        ):
            raise ValueError(
                f"`sgd_minibatch_size` ({self.sgd_minibatch_size}) must be <= "
                f"`train_batch_size` ({self.train_batch_size}). In PPO, the train batch"
                f" will be split into {self.sgd_minibatch_size} chunks, each of which "
                f"is iterated over (used for updating the policy) {self.num_sgd_iter} "
                "times."
            )
        elif self.enable_rl_module_and_learner:
            mbs = self.mini_batch_size_per_learner or self.sgd_minibatch_size
            tbs = self.train_batch_size_per_learner or self.train_batch_size
            if isinstance(mbs, int) and isinstance(tbs, int) and mbs > tbs:
                raise ValueError(
                    f"`mini_batch_size_per_learner` ({mbs}) must be <= "
                    f"`train_batch_size_per_learner` ({tbs}). In PPO, the train batch"
                    f" will be split into {mbs} chunks, each of which is iterated over "
                    f"(used for updating the policy) {self.num_sgd_iter} times."
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
            raise ValueError(
                "Episode truncation is not supported without a value "
                "function (to estimate the return at the end of the truncated"
                " trajectory). Consider setting "
                "batch_mode=complete_episodes."
            )

        # Entropy coeff schedule checking.
        if self.enable_rl_module_and_learner:
            if self.entropy_coeff_schedule is not None:
                raise ValueError(
                    "`entropy_coeff_schedule` is deprecated and must be None! Use the "
                    "`entropy_coeff` setting to setup a schedule."
                )
            Scheduler.validate(
                fixed_value_or_schedule=self.entropy_coeff,
                setting_name="entropy_coeff",
                description="entropy coefficient",
            )
        if isinstance(self.entropy_coeff, float) and self.entropy_coeff < 0.0:
            raise ValueError("`entropy_coeff` must be >= 0.0")

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
    def training_step(self):
        # New API stack (RLModule, Learner, EnvRunner, ConnectorV2).
        if self.config.enable_env_runner_and_connector_v2:
            return self._training_step_new_api_stack()
        # Old and hybrid API stacks (Policy, RolloutWorker, Connector, maybe RLModule,
        # maybe Learner).
        else:
            return self._training_step_old_and_hybrid_api_stacks()

    def _training_step_new_api_stack(self) -> ResultDict:
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
                return {}

            # Reduce EnvRunner metrics over the n EnvRunners.
            self.metrics.merge_and_log_n_dicts(
                env_runner_results, key=ENV_RUNNER_RESULTS
            )
            # Log lifetime counts for env- and agent steps.
            self.metrics.log_dict(
                {
                    NUM_AGENT_STEPS_SAMPLED_LIFETIME: self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_AGENT_STEPS_SAMPLED)
                    ),
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED)
                    ),
                    NUM_EPISODES_LIFETIME: self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_EPISODES)
                    ),
                },
                reduce="sum",
            )

        # Perform a learner update step on the collected episodes.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update_from_episodes(
                episodes=episodes,
                timesteps={
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: (
                        self.metrics.peek(NUM_ENV_STEPS_SAMPLED_LIFETIME)
                    ),
                },
                minibatch_size=(
                    self.config.mini_batch_size_per_learner
                    or self.config.sgd_minibatch_size
                ),
                num_iters=self.config.num_sgd_iter,
            )
            self.metrics.merge_and_log_n_dicts(learner_results, key=LEARNER_RESULTS)
            self.metrics.log_dict(
                {
                    NUM_ENV_STEPS_TRAINED_LIFETIME: self.metrics.peek(
                        (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED)
                    ),
                    # NUM_MODULE_STEPS_TRAINED_LIFETIME: self.metrics.peek(
                    #    (LEARNER_RESULTS, NUM_MODULE_STEPS_TRAINED)
                    # ),
                },
                reduce="sum",
            )

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
            # if self.env_runner_group.num_remote_workers() > 0:
            self.env_runner_group.sync_weights(
                # Sync weights from learner_group to all EnvRunners.
                from_worker_or_learner_group=self.learner_group,
                policies=modules_to_update,
                inference_only=True,
            )
            # else:
            #    weights = self.learner_group.get_weights(inference_only=True)
            #    self.env_runner.set_weights(weights)

        return self.metrics.reduce()

    def _training_step_old_and_hybrid_api_stacks(self) -> ResultDict:
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

        # Perform a train step on the collected batch.
        if self.config.enable_rl_module_and_learner:
            mini_batch_size_per_learner = (
                self.config.mini_batch_size_per_learner
                or self.config.sgd_minibatch_size
            )
            train_results = self.learner_group.update_from_batch(
                batch=train_batch,
                minibatch_size=mini_batch_size_per_learner,
                num_iters=self.config.num_sgd_iter,
            )

        elif self.config.simple_optimizer:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        if self.config.enable_rl_module_and_learner:
            # The train results's loss keys are pids to their loss values. But we also
            # return a total_loss key at the same level as the pid keys. So we need to
            # subtract that to get the total set of pids to update.
            # TODO (Kourosh): We should also not be using train_results as a message
            #  passing medium to infer which policies to update. We could use
            #  policies_to_train variable that is given by the user to infer this.
            policies_to_update = set(train_results.keys()) - {ALL_MODULES}
        else:
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
                if self.config.enable_rl_module_and_learner:
                    # sync weights from learner_group to all rollout workers
                    from_worker_or_learner_group = self.learner_group
                self.env_runner_group.sync_weights(
                    from_worker_or_learner_group=from_worker_or_learner_group,
                    policies=policies_to_update,
                    global_vars=global_vars,
                )
            elif self.config.enable_rl_module_and_learner:
                weights = self.learner_group.get_weights()
                self.env_runner.set_weights(weights)

        if self.config.enable_rl_module_and_learner:
            kl_dict = {}
            if self.config.use_kl_loss:
                for pid in policies_to_update:
                    kl = train_results[pid][LEARNER_RESULTS_KL_KEY]
                    kl_dict[pid] = kl
                    if np.isnan(kl):
                        logger.warning(
                            f"KL divergence for Module {pid} is non-finite, this will "
                            "likely destabilize your model and the training process. "
                            "Action(s) in a specific state have near-zero probability. "
                            "This can happen naturally in deterministic environments "
                            "where the optimal policy has zero mass for a specific "
                            "action. To fix this issue, consider setting `kl_coeff` to "
                            "0.0 or increasing `entropy_coeff` in your config."
                        )

            return train_results

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
