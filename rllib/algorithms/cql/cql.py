import logging
from typing import Optional, Type, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.cql.cql_tf_policy import CQLTFPolicy
from ray.rllib.algorithms.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.algorithms.sac.sac import (
    SAC,
    SACConfig,
)
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.rollout_ops import (
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    multi_gpu_train_one_step,
    train_one_step,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.metrics import (
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    OFFLINE_SAMPLING_TIMER,
    TARGET_NET_UPDATE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
    TIMERS,
)
from ray.rllib.utils.typing import ResultDict, RLModuleSpecType

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()
logger = logging.getLogger(__name__)


class CQLConfig(SACConfig):
    """Defines a configuration class from which a CQL can be built.

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.cql import CQLConfig
        config = CQLConfig().training(gamma=0.9, lr=0.01)
        config = config.resources(num_gpus=0)
        config = config.env_runners(num_env_runners=4)
        print(config.to_dict())
        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build(env="CartPole-v1")
        algo.train()
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or CQL)

        # fmt: off
        # __sphinx_doc_begin__
        # CQL-specific config settings:
        self.bc_iters = 20000
        self.temperature = 1.0
        self.num_actions = 10
        self.lagrangian = False
        self.lagrangian_thresh = 5.0
        self.min_q_weight = 5.0
        self.deterministic_backup = True
        self.lr = 3e-4
        # Note, the new stack defines learning rates for each component.
        # The base learning rate `lr` has to be set to `None`, if using
        # the new stack.
        self.actor_lr = 1e-4
        self.critic_lr = 1e-3
        self.alpha_lr = 1e-3

        self.replay_buffer_config = {
            "_enable_replay_buffer_api": True,
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": int(1e6),
            # If True prioritized replay buffer will be used.
            "prioritized_replay": False,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            # Whether to compute priorities already on the remote worker side.
            "worker_side_prioritization": False,
        }

        # Changes to Algorithm's/SACConfig's default:

        # .reporting()
        self.min_sample_timesteps_per_iteration = 0
        self.min_train_timesteps_per_iteration = 100
        # fmt: on
        # __sphinx_doc_end__

        self.timesteps_per_iteration = DEPRECATED_VALUE

    @override(SACConfig)
    def training(
        self,
        *,
        bc_iters: Optional[int] = NotProvided,
        temperature: Optional[float] = NotProvided,
        num_actions: Optional[int] = NotProvided,
        lagrangian: Optional[bool] = NotProvided,
        lagrangian_thresh: Optional[float] = NotProvided,
        min_q_weight: Optional[float] = NotProvided,
        deterministic_backup: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "CQLConfig":
        """Sets the training-related configuration.

        Args:
            bc_iters: Number of iterations with Behavior Cloning pretraining.
            temperature: CQL loss temperature.
            num_actions: Number of actions to sample for CQL loss
            lagrangian: Whether to use the Lagrangian for Alpha Prime (in CQL loss).
            lagrangian_thresh: Lagrangian threshold.
            min_q_weight: in Q weight multiplier.
            deterministic_backup: If the target in the Bellman update should have an
                entropy backup. Defaults to `True`.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if bc_iters is not NotProvided:
            self.bc_iters = bc_iters
        if temperature is not NotProvided:
            self.temperature = temperature
        if num_actions is not NotProvided:
            self.num_actions = num_actions
        if lagrangian is not NotProvided:
            self.lagrangian = lagrangian
        if lagrangian_thresh is not NotProvided:
            self.lagrangian_thresh = lagrangian_thresh
        if min_q_weight is not NotProvided:
            self.min_q_weight = min_q_weight
        if deterministic_backup is not NotProvided:
            self.deterministic_backup = deterministic_backup

        return self

    @override(AlgorithmConfig)
    def offline_data(self, **kwargs) -> "CQLConfig":

        super().offline_data(**kwargs)

        # Check, if the passed in class incorporates the `OfflinePreLearner`
        # interface.
        if "prelearner_class" in kwargs:
            from ray.rllib.offline.offline_data import OfflinePreLearner

            if not issubclass(kwargs.get("prelearner_class"), OfflinePreLearner):
                raise ValueError(
                    f"`prelearner_class` {kwargs.get('prelearner_class')} is not a "
                    "subclass of `OfflinePreLearner`. Any class passed to "
                    "`prelearner_class` needs to implement the interface given by "
                    "`OfflinePreLearner`."
                )

        return self

    @override(SACConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.cql.torch.cql_torch_learner import CQLTorchLearner

            return CQLTorchLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `'torch'` instead."
            )

    @override(AlgorithmConfig)
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        pipeline = super().build_learner_connector(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            device=device,
        )

        # Prepend the "add-NEXT_OBS-from-episodes-to-train-batch" connector piece (right
        # after the corresponding "add-OBS-..." default piece).
        pipeline.insert_after(
            AddObservationsFromEpisodesToBatch,
            AddNextObservationsFromEpisodesToTrainBatch(),
        )

        return pipeline

    @override(SACConfig)
    def validate(self) -> None:
        # First check, whether old `timesteps_per_iteration` is used.
        if self.timesteps_per_iteration != DEPRECATED_VALUE:
            deprecation_warning(
                old="timesteps_per_iteration",
                new="min_train_timesteps_per_iteration",
                error=True,
            )

        # Call super's validation method.
        super().validate()

        # CQL-torch performs the optimizer steps inside the loss function.
        # Using the multi-GPU optimizer will therefore not work (see multi-GPU
        # check above) and we must use the simple optimizer for now.
        if self.simple_optimizer is not True and self.framework_str == "torch":
            self.simple_optimizer = True

        if self.framework_str in ["tf", "tf2"] and tfp is None:
            logger.warning(
                "You need `tensorflow_probability` in order to run CQL! "
                "Install it via `pip install tensorflow_probability`. Your "
                f"tf.__version__={tf.__version__ if tf else None}."
                "Trying to import tfp results in the following error:"
            )
            try_import_tfp(error=True)

        # Assert that for a local learner the number of iterations is 1. Note,
        # this is needed because we have no iterators, but instead a single
        # batch returned directly from the `OfflineData.sample` method.
        if (
            self.num_learners == 0
            and not self.dataset_num_iters_per_learner
            and self.enable_rl_module_and_learner
        ):
            self._value_error(
                "When using a single local learner the number of iterations "
                "per learner, `dataset_num_iters_per_learner` has to be defined. "
                "Set this hyperparameter in the `AlgorithmConfig.offline_data`."
            )

    @override(SACConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.cql.torch.default_cql_torch_rl_module import (
                DefaultCQLTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultCQLTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. Use `torch`."
            )

    @property
    def _model_config_auto_includes(self):
        return super()._model_config_auto_includes | {
            "num_actions": self.num_actions,
        }


class CQL(SAC):
    """CQL (derived from SAC)."""

    @classmethod
    @override(SAC)
    def get_default_config(cls) -> AlgorithmConfig:
        return CQLConfig()

    @classmethod
    @override(SAC)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return CQLTorchPolicy
        else:
            return CQLTFPolicy

    @override(SAC)
    def training_step(self) -> None:
        # Old API stack (Policy, RolloutWorker, Connector).
        if not self.config.enable_env_runner_and_connector_v2:
            return self._training_step_old_api_stack()

        # Sampling from offline data.
        with self.metrics.log_time((TIMERS, OFFLINE_SAMPLING_TIMER)):
            # If we should use an iterator in the learner(s). Note, in case of
            # multiple learners we must always return a list of iterators.
            return_iterator = return_iterator = (
                self.config.num_learners > 0
                or self.config.dataset_num_iters_per_learner != 1
            )

            # Return an iterator in case we are using remote learners.
            batch_or_iterator = self.offline_data.sample(
                num_samples=self.config.train_batch_size_per_learner,
                num_shards=self.config.num_learners,
                # Return an iterator, if a `Learner` should update
                # multiple times per RLlib iteration.
                return_iterator=return_iterator,
            )

        # Updating the policy.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            learner_results = self.learner_group.update(
                data_iterators=batch_or_iterator,
                minibatch_size=self.config.train_batch_size_per_learner,
                num_iters=self.config.dataset_num_iters_per_learner,
            )

            # Log training results.
            self.metrics.aggregate(learner_results, key=LEARNER_RESULTS)

    @OldAPIStack
    def _training_step_old_api_stack(self) -> ResultDict:
        # Collect SampleBatches from sample workers.
        with self._timers[SAMPLE_TIMER]:
            train_batch = synchronous_parallel_sample(worker_set=self.env_runner_group)
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Postprocess batch before we learn on it.
        post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
        train_batch = post_fn(train_batch, self.env_runner_group, self.config)

        # Learn on training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer") is True:
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # Update target network every `target_network_update_freq` training steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_TRAINED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_TRAINED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update >= self.config.target_network_update_freq:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.env_runner.get_policies_to_train()
                self.env_runner.foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        # Update remote workers's weights after learning on local worker
        # (only those policies that were actually trained).
        if self.env_runner_group.num_remote_workers() > 0:
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                self.env_runner_group.sync_weights(policies=list(train_results.keys()))

        # Return all collected metrics for the iteration.
        return train_results
