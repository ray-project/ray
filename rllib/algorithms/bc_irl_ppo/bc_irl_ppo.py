from gymnasium.spaces import Space
import copy
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import (
    AlgorithmConfig,
    DifferentiableAlgorithmConfig,
    _check_rl_module_spec,
    NotProvided,
)
from ray.rllib.algorithms.marwil import MARWIL
from ray.rllib.connectors.learner import (
    AddEpisodeLengthsToTrainBatch,
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core import ALL_MODULES, DEFAULT_MODULE_ID, DEFAULT_POLICY_ID
from ray.rllib.core.learner import Learner
from ray.rllib.core.learner.differentiable_learner_config import (
    DifferentiableLearnerConfig,
)
from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.execution.rollout_ops import synchronous_parallel_sample
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    OFFLINE_SAMPLING_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TIMERS,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EnvType, PolicyID


class BCIRLPPOConfig(DifferentiableAlgorithmConfig):
    def __init__(self, algo_class=None):
        """Initializes a BCIRLPPOConfig instance."""
        # fmt: off
        # __sphinx_doc_begin__

        # Initialize the super.
        super().__init__(algo_class=algo_class or BCIRLPPO)
        # ----------------------------

        # This is also an online algorithm.
        self._is_online = True

        # Mateiralize all data. Set this to `False` if the data
        # is large.
        self.materialize_data = True
        self.materialize_mapped_data = True

        # ----------------------------
        # PPO configurations
        # Multi-agent.
        self.policies ={DEFAULT_POLICY_ID: PolicySpec()}

        # training()
        self.grad_clip = None
        self.lr = 1e-4
        self.train_batch_size_per_learner = 256
        self.minibatch_size = 256
        # Update the reward model in each training iteration.
        self.reward_update_freq = 1
        self.ppo_lr = 1e-4
        self.ppo_train_batch_size_per_learner = 1280
        self.ppo_num_epochs = 2
        self.ppo_minibatch_size = 320
        self.ppo_shuffle_batch_per_epoch = True
        self.ppo_use_critic = True
        self.ppo_use_gae = True
        self.ppo_lambda_ = 0.95
        self.ppo_gamma = 0.99
        self.ppo_use_kl_loss = False
        self.ppo_kl_coeff = 0.01
        self.ppo_kl_target = 0.01
        self.ppo_vf_loss_coeff = 0.5
        self.ppo_entropy_coeff = 1e-3
        self.ppo_clip_param = 0.2
        self.ppo_vf_clip_param = 10.0


        from ray.rllib.algorithms.bc_irl_ppo.torch.bc_irl_ppo_torch_differentiable_learner import BCIRLPPOTorchDifferentiableLearner
        self.differentiable_learner_configs = [
            BCIRLPPODifferentiableLearnerConfig(
                learner_class=BCIRLPPOTorchDifferentiableLearner,
                is_multi_agent=self.is_multi_agent,
                policies_to_update=self.policies,
                lr=self.ppo_lr,
                num_epochs=self.ppo_num_epochs,
                minibatch_size=self.ppo_minibatch_size,
            )
        ]

        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpec:
        # If the framework is "torch" use the default `RLModule` for BCIRLPPO.
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc_irl_ppo.torch.default_bc_irl_ppo_torch_rl_module import (
                DefaultBCIRLRewardTorchRLModule,
            )
            from ray.rllib.algorithms.bc_irl_ppo.torch.bc_irl_ppo_torch_meta_learner import (
                REWARD_MODULE,
            )
            from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
            from ray.rllib.algorithms.ppo.torch.default_ppo_torch_rl_module import (
                DefaultPPOTorchRLModule,
            )

            # Note, BCIRLPPO needs a `MultiRLModule` to define the reward model as
            # a standalone module.
            # TODO (simon): Check, if a per-module reward module is possible.
            return MultiRLModuleSpec(
                rl_module_specs={
                    # The policy is defined by the default PPO `RLModule`.
                    DEFAULT_MODULE_ID: RLModuleSpec(
                        module_class=DefaultPPOTorchRLModule,
                        learner_only=False,
                        catalog_class=PPOCatalog,
                    ),
                    # The reward model is defined by the default BCIRLPPO `RLModule`.
                    REWARD_MODULE: RLModuleSpec(
                        module_class=DefaultBCIRLRewardTorchRLModule,
                        # The reward model is not rolled out in inference/exploration.
                        # Instead rewards are estimated during postprocessing on the main
                        # process.
                        learner_only=True,
                    ),
                }
            )
        # Otherwise, raise an error.
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc_irl_ppo.torch.bc_irl_ppo_torch_meta_learner import (
                BCIRLPPOTorchMetaLearner,
            )

            return BCIRLPPOTorchMetaLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `framework='torch'`."
            )

    @override(DifferentiableAlgorithmConfig)
    def get_differentiable_learner_classes(
        self,
    ) -> List[Union[Type["DifferentiableLearner"], str]]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.bc_irl_ppo.torch.bc_irl_ppo_torch_differentiable_learner import (
                BCIRLPPOTorchDifferentiableLearner,
            )

            return [BCIRLPPOTorchDifferentiableLearner]
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `framework='torch'`."
            )

    @override(AlgorithmConfig)
    def build_learner_connector(
        self, input_observation_space, input_action_space, device=None
    ):
        pipeline = super().build_learner_connector(
            input_observation_space, input_action_space, device
        )
        # TODO (simon): Set up for different learner configurations (i.e. local,
        # remote, multi).
        pipeline.remove("TensorToNumpy")

        return pipeline

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        reward_update_freq: Optional[int] = NotProvided,
        ppo_lr: Optional[float] = NotProvided,
        ppo_train_batch_size_per_learner: Optional[int] = NotProvided,
        ppo_minibatch_size: Optional[int] = NotProvided,
        ppo_num_epochs: Optional[int] = NotProvided,
        ppo_shuffle_batch_per_epoch: Optional[bool] = NotProvided,
        ppo_use_critic: Optional[bool] = NotProvided,
        ppo_use_gae: Optional[bool] = NotProvided,
        ppo_lambda_: Optional[float] = NotProvided,
        ppo_gamma: Optional[float] = NotProvided,
        ppo_use_kl_loss: Optional[bool] = NotProvided,
        ppo_kl_coeff: Optional[float] = NotProvided,
        ppo_kl_target: Optional[float] = NotProvided,
        ppo_vf_loss_coeff: Optional[float] = NotProvided,
        ppo_entropy_coeff: Optional[float] = NotProvided,
        ppo_clip_param: Optional[float] = NotProvided,
        ppo_vf_clip_param: Optional[float] = NotProvided,
        ppo_grad_clip: Optional[float] = NotProvided,
        **kwargs,
    ) -> "BCIRLPPOConfig":
        """Sets the training related configuration.

        Args:
            reward_update_freq: The update frequency for the reward model. The default
                updates the reward model in each iteration.
            ppo_lr: The learning rate for the differentiable PPO learner. This learning rate
                defines the step size of the differentiable update.
            ppo_train_batch_size_per_learner: The training batch size for each differentiable
                PPO learner.
            ppo_minibatch_size: The minibatch size to be used in minibatch SGD by the
                differentiable PPO learner.
            ppo_num_epochs: The number of epochs to be run per training batch size in each
                differentiable PPO learner.
            ppo_shuffle_batch_per_epoch: If the training batch should be shuffled between epochs.
            ppo_use_critic: Should use a critic as a baseline (otherwise don't use value
                baseline; required for using GAE).
            ppo_use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
            ppo_lambda_: The lambda parameter for General Advantage Estimation (GAE).
                Defines the exponential weight used between actually measured rewards
                vs value function estimates over multiple time steps. Specifically,
                `lambda_` balances short-term, low-variance estimates against long-term,
                high-variance returns. A `lambda_` of 0.0 makes the GAE rely only on
                immediate rewards (and vf predictions from there on, reducing variance,
                but increasing bias), while a `lambda_` of 1.0 only incorporates vf
                predictions at the truncation points of the given episodes or episode
                chunks (reducing bias but increasing variance).
            ppo_use_kl_loss: Whether to use the KL-term in the loss function.
            ppo_kl_coeff: Initial coefficient for KL divergence.
            ppo_kl_target: Target value for KL divergence.
            ppo_vf_loss_coeff: Coefficient of the value function loss. IMPORTANT: you must
                tune this if you set vf_share_layers=True inside your model's config.
            ppo_entropy_coeff: The entropy coefficient (float) or entropy coefficient
                schedule in the format of
                [[timestep, coeff-value], [timestep, coeff-value], ...]
                In case of a schedule, intermediary timesteps will be assigned to
                linearly interpolated coefficient values. A schedule config's first
                entry must start with timestep 0, i.e.: [[0, initial_value], [...]].
            ppo_clip_param: The PPO clip parameter.
            ppo_vf_clip_param: Clip param for the value function. Note that this is
                sensitive to the scale of the rewards. If your expected V is large,
                increase this.
            ppo_grad_clip: If specified, clip the global norm of gradients by this amount.

        Returns:
            This updated DifferentiableAlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if reward_update_freq is not NotProvided:
            self.reward_update_freq = reward_update_freq
        if ppo_lr is not NotProvided:
            self.ppo_lr = ppo_lr
            self.differentiable_learner_configs[0].lr = ppo_lr
        if ppo_train_batch_size_per_learner is not NotProvided:
            self.ppo_train_batch_size_per_learner = ppo_train_batch_size_per_learner
        if ppo_minibatch_size is not NotProvided:
            self.ppo_minibatch_size = ppo_minibatch_size
            self.differentiable_learner_configs[0].minibatch_size = ppo_minibatch_size
        if ppo_num_epochs is not NotProvided:
            self.ppo_num_epochs = ppo_num_epochs
            self.differentiable_learner_configs[0].num_epochs = ppo_num_epochs
        if ppo_shuffle_batch_per_epoch is not NotProvided:
            self.ppo_shuffle_batch_per_epoch = ppo_shuffle_batch_per_epoch
            self.differentiable_learner_configs[
                0
            ].shuffle_batch_per_epoch = ppo_shuffle_batch_per_epoch
        if ppo_use_critic is not NotProvided:
            self.ppo_use_critic = ppo_use_critic
            # TODO (simon) This is experimental.
            #  Don't forget to remove .use_critic from algorithm config.
        if ppo_use_gae is not NotProvided:
            self.ppo_use_gae = ppo_use_gae
        if ppo_gamma is not NotProvided:
            self.ppo_gamma = ppo_gamma
        if ppo_lambda_ is not NotProvided:
            self.ppo_lambda_ = ppo_lambda_
        if ppo_use_kl_loss is not NotProvided:
            self.ppo_use_kl_loss = ppo_use_kl_loss
        if ppo_kl_coeff is not NotProvided:
            self.ppo_kl_coeff = ppo_kl_coeff
        if ppo_kl_target is not NotProvided:
            self.ppo_kl_target = ppo_kl_target
        if ppo_vf_loss_coeff is not NotProvided:
            self.ppo_vf_loss_coeff = ppo_vf_loss_coeff
        if ppo_entropy_coeff is not NotProvided:
            self.ppo_entropy_coeff = ppo_entropy_coeff
        if ppo_clip_param is not NotProvided:
            self.ppo_clip_param = ppo_clip_param
        if ppo_vf_clip_param is not NotProvided:
            self.ppo_vf_clip_param = ppo_vf_clip_param
        if ppo_grad_clip is not NotProvided:
            self.grad_clip = ppo_grad_clip

        return self

    @override(AlgorithmConfig)
    def validate(self):
        super().validate()

        # Torch DDP does not work with higher-level optimizations.
        if self.num_learners > 1:
            self._value_error(
                "BC-IRL-PPO can use only a single learner, but "
                f"`num_learners={self.num_learners}`. Use either `num_learners=0` "
                "for a local learner or `num_learners=1` for a single remote learner."
            )

    @property
    @override(AlgorithmConfig)
    def _model_config_auto_includes(self) -> Dict[str, Any]:
        from ray.rllib.algorithms.bc_irl_ppo.default_bc_irl_ppo_rl_module import (
            DefaultBCIRLRewardModelType,
        )

        return super()._model_config_auto_includes | {
            "vf_share_layers": False,
            "reward_type": DefaultBCIRLRewardModelType.NEXT_STATE,
        }

    @property
    @override(AlgorithmConfig)
    def rl_module_spec(self):
        # Get the BCIRLPPO default `MultiRLModuleSpec`.
        default_rl_module_spec: MultiRLModuleSpec = self.get_default_rl_module_spec()
        # Validate the module spec.
        _check_rl_module_spec(default_rl_module_spec)

        # If the user has provided a custom `RLModuleSpec`.
        if self._rl_module_spec:
            # First validate the provided `RLModuleSpec`.
            _check_rl_module_spec(self._rl_module_spec)

            # If the provided module spec is a `RLModuleSpec`, merge it with
            # the default one, we expect it to be for the policy.
            if isinstance(self._rl_module_spec, RLModuleSpec):
                # Merge with the default RLModuleSpec for the policy.
                default_rl_module_spec.remove_modules(DEFAULT_MODULE_ID)
                default_rl_module_spec.add_modules(
                    {DEFAULT_MODULE_ID: self._rl_module_spec}
                )
            # If the provided module spec is a `MultiRLModuleSpec`, update
            # the default `MultiRLModuleSpec` with it.
            elif isinstance(self._rl_module_spec, MultiRLModuleSpec):
                # Deepcopy the provided module spec.
                multi_rl_module_spec = copy.deepcopy(self._rl_module_spec)
                # Update the default `MultiRLModuleSpec` with the provided one.
                # This could include a policy, reward model, or both.
                default_rl_module_spec.update(multi_rl_module_spec)
                return multi_rl_module_spec
        # Otherwise, use the default one.
        else:
            return default_rl_module_spec

    @override(AlgorithmConfig)
    def get_multi_rl_module_spec(
        self,
        *,
        env: Optional[EnvType] = None,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        inference_only: bool = False,
        # @HybridAPIStack
        policy_dict: Optional[Dict[str, PolicySpec]] = None,
        single_agent_rl_module_spec: Optional[RLModuleSpec] = None,
    ) -> MultiRLModuleSpec:
        # if single_agent_rl_module_spec is None:
        #     single_agent_rl_module_spec = super().get_default_rl_module_spec()
        # return super().get_multi_rl_module_spec(
        #     env=env,
        #     spaces=spaces,
        #     inference_only=inference_only,
        #     policy_dict=policy_dict,
        #     single_agent_rl_module_spec=single_agent_rl_module_spec
        # )
        # TODO (simon): `get_multi_rl_module_spec` is overriding an already
        # defined `MultiRLModule`. This should be avoided or the `policy_dict`
        # has to be enriched with the reward module.
        multi_rl_module_spec = self.rl_module_spec

        if policy_dict is None:
            policy_dict, _ = self.get_multi_agent_setup(env=env, spaces=spaces)
        # Fill in the missing values from the specs that we already have. By combining
        # PolicySpecs and the default RLModuleSpec.
        for module_id in multi_rl_module_spec.rl_module_specs:

            # Remove/skip `learner_only=True` RLModules if `inference_only` is True.
            module_spec = multi_rl_module_spec.rl_module_specs[module_id]
            if inference_only and module_spec.learner_only:
                multi_rl_module_spec.remove_modules(module_id)
                continue

            policy_spec = policy_dict.get(module_id)
            if policy_spec is None:
                policy_spec = policy_dict[DEFAULT_MODULE_ID]

            # if module_spec.catalog_class is None:
            #     if isinstance(default_rl_module_spec, RLModuleSpec):
            #         module_spec.catalog_class = default_rl_module_spec.catalog_class
            #     elif isinstance(default_rl_module_spec.rl_module_specs, RLModuleSpec):
            #         catalog_class = default_rl_module_spec.rl_module_specs.catalog_class
            #         module_spec.catalog_class = catalog_class
            #     elif module_id in default_rl_module_spec.rl_module_specs:
            #         module_spec.catalog_class = default_rl_module_spec.rl_module_specs[
            #             module_id
            #         ].catalog_class
            #     else:
            #         raise ValueError(
            #             f"Catalog class for module {module_id} cannot be inferred. "
            #             f"It is neither provided in the rl_module_spec that "
            #             "is passed in nor in the default module spec used in "
            #             "the algorithm."
            #         )
            # TODO (sven): Find a good way to pack module specific parameters from
            # the algorithms into the `model_config_dict`.
            if module_spec.observation_space is None:
                module_spec.observation_space = policy_spec.observation_space
            if module_spec.action_space is None:
                module_spec.action_space = policy_spec.action_space
            # In case the `RLModuleSpec` does not have a model config dict, we use the
            # the one defined by the auto keys and the `model_config_dict` arguments in
            # `self.rl_module()`.
            if module_spec.model_config is None:
                module_spec.model_config = self.model_config
            # Otherwise we combine the two dictionaries where settings from the
            # `RLModuleSpec` have higher priority.
            else:
                module_spec.model_config = (
                    self.model_config | module_spec._get_model_config()
                )

        return multi_rl_module_spec


class BCIRLPPODifferentiableLearnerConfig(DifferentiableLearnerConfig):
    def build_learner_connector(
        self, input_observation_space, input_action_space, device=None
    ):
        # First call the super's method.
        pipeline = super().build_learner_connector(
            input_observation_space, input_action_space, device
        )
        # Note, we need to insert this before we batch the items. The next
        # state is needed for the reward model.
        pipeline.insert_before(
            "BatchIndividualItems", AddNextObservationsFromEpisodesToTrainBatch()
        )
        # Note, we need the episode lengths for computation of advantages.
        pipeline.insert_before(
            "BatchIndividualItems",
            AddEpisodeLengthsToTrainBatch(),
        )
        return pipeline


class BCIRLPPO(MARWIL):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return BCIRLPPOConfig()

    @override(MARWIL)
    def training_step(self) -> None:

        # Collect batches from sample workers until we have a full batch.
        with self.metrics.log_time((TIMERS, ENV_RUNNER_SAMPLING_TIMER)):
            # Sample in parallel from the workers.
            if self.config.count_steps_by == "agent_steps":
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_agent_steps=self.config.ppo_train_batch_size_per_learner,
                    sample_timeout_s=self.config.sample_timeout_s,
                    _uses_new_env_runners=(
                        self.config.enable_env_runner_and_connector_v2
                    ),
                    _return_metrics=True,
                )
            else:
                episodes, env_runner_results = synchronous_parallel_sample(
                    worker_set=self.env_runner_group,
                    max_env_steps=self.config.ppo_train_batch_size_per_learner,
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

        # TODO (simon): Take care of sampler metrics: right
        #  now all rewards are `nan`, which possibly confuses
        #  the user that sth. is not right, although it is as
        #  we do not step the env.
        with self.metrics.log_time((TIMERS, OFFLINE_SAMPLING_TIMER)):
            # Sampling from offline data.
            iterator = self.offline_data.sample(
                num_samples=self.config.train_batch_size_per_learner,
                num_shards=self.config.num_learners,
                # Return an iterator, if a `Learner` should update
                # multiple times per RLlib iteration.
                return_iterator=True,
            )
            training_data = TrainingData(data_iterators=iterator)
            others_training_data = [TrainingData(episodes=episodes)]

        # Perform a learner update step on the collected episodes.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update(
                # episodes=episodes,
                training_data=training_data,
                others_training_data=others_training_data,
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
                num_iters=1,
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
