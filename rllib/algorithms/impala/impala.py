import copy
import logging
import queue
from typing import Dict, List, Optional, Set, Tuple, Type, Union

import ray
from ray import ObjectRef
from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.connectors.learner import AddOneTsToEpisodesAndTruncate, NumpyToTensor
from ray.rllib.core import (
    COMPONENT_ENV_TO_MODULE_CONNECTOR,
    COMPONENT_MODULE_TO_ENV_CONNECTOR,
)
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.multi_gpu_learner_thread import MultiGPULearnerThread
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import concat_samples
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.metrics import (
    AGGREGATOR_ACTOR_RESULTS,
    ALL_MODULES,
    ENV_RUNNER_RESULTS,
    LEARNER_GROUP,
    LEARNER_RESULTS,
    LEARNER_UPDATE_TIMER,
    MEAN_NUM_EPISODE_LISTS_RECEIVED,
    MEAN_NUM_LEARNER_RESULTS_RECEIVED,
    MEAN_NUM_LEARNER_GROUP_UPDATE_CALLED,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_SYNCH_WORKER_WEIGHTS,
    NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
    TIMERS,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import ReplayMode
from ray.rllib.utils.replay_buffers.replay_buffer import _ALL_POLICIES
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import (
    LearningRateOrSchedule,
    PolicyID,
    ResultDict,
    SampleBatchType,
)


logger = logging.getLogger(__name__)


LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


class IMPALAConfig(AlgorithmConfig):
    """Defines a configuration class from which an Impala can be built.

    .. testcode::

        from ray.rllib.algorithms.impala import IMPALAConfig

        config = (
            IMPALAConfig()
            .environment("CartPole-v1")
            .env_runners(num_env_runners=1)
            .training(lr=0.0003, train_batch_size_per_learner=512)
            .learners(num_learners=1)
        )
        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()
        del algo

    .. testcode::

        from ray.rllib.algorithms.impala import IMPALAConfig
        from ray import tune

        config = (
            IMPALAConfig()
            .environment("CartPole-v1")
            .env_runners(num_env_runners=1)
            .training(lr=tune.grid_search([0.0001, 0.0002]), grad_clip=20.0)
            .learners(num_learners=1)
        )
        # Run with tune.
        tune.Tuner(
            "IMPALA",
            param_space=config,
            run_config=tune.RunConfig(stop={"training_iteration": 1}),
        ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a IMPALAConfig instance."""
        self.exploration_config = {  # @OldAPIstack
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

        super().__init__(algo_class=algo_class or IMPALA)

        # fmt: off
        # __sphinx_doc_begin__

        # IMPALA specific settings:
        self.vtrace = True
        self.vtrace_clip_rho_threshold = 1.0
        self.vtrace_clip_pg_rho_threshold = 1.0
        self.learner_queue_size = 3
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_aggregator_manager = 0.0
        self.broadcast_interval = 1
        self.num_gpu_loader_threads = 8

        self.grad_clip = 40.0
        # Note: Only when using enable_rl_module_and_learner=True can the clipping mode
        # be configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"

        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.01

        # Override some of AlgorithmConfig's default values with IMPALA-specific values.
        self.num_learners = 1
        self.num_aggregator_actors_per_learner = 0
        self.rollout_fragment_length = 50
        self.train_batch_size = 500  # @OldAPIstack
        self.num_env_runners = 2
        self.lr = 0.0005
        self.min_time_s_per_iteration = 10
        # __sphinx_doc_end__
        # fmt: on

        # IMPALA takes care of its own EnvRunner (weights, connector, metrics) synching.
        self._dont_auto_sync_env_runner_states = True

        # `.debugging()`
        self._env_runners_only = False
        self._skip_learners = False

        self.lr_schedule = None  # @OldAPIStack
        self.entropy_coeff_schedule = None  # @OldAPIStack
        self.num_multi_gpu_tower_stacks = 1  # @OldAPIstack
        self.minibatch_buffer_size = 1  # @OldAPIstack
        self.replay_proportion = 0.0  # @OldAPIstack
        self.replay_buffer_num_slots = 0  # @OldAPIstack
        self.learner_queue_timeout = 300  # @OldAPIstack
        self.opt_type = "adam"  # @OldAPIstack
        self.decay = 0.99  # @OldAPIstack
        self.momentum = 0.0  # @OldAPIstack
        self.epsilon = 0.1  # @OldAPIstack
        self._separate_vf_optimizer = False  # @OldAPIstack
        self._lr_vf = 0.0005  # @OldAPIstack
        self.num_gpus = 1  # @OldAPIstack
        self._tf_policy_handles_more_than_one_loss = True  # @OldAPIstack

        # Deprecated settings.
        self.num_aggregation_workers = DEPRECATED_VALUE
        self.max_requests_in_flight_per_aggregator_worker = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        vtrace: Optional[bool] = NotProvided,
        vtrace_clip_rho_threshold: Optional[float] = NotProvided,
        vtrace_clip_pg_rho_threshold: Optional[float] = NotProvided,
        num_gpu_loader_threads: Optional[int] = NotProvided,
        num_multi_gpu_tower_stacks: Optional[int] = NotProvided,
        minibatch_buffer_size: Optional[int] = NotProvided,
        replay_proportion: Optional[float] = NotProvided,
        replay_buffer_num_slots: Optional[int] = NotProvided,
        learner_queue_size: Optional[int] = NotProvided,
        learner_queue_timeout: Optional[float] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_aggregator_manager: Optional[float] = NotProvided,
        broadcast_interval: Optional[int] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        opt_type: Optional[str] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        decay: Optional[float] = NotProvided,
        momentum: Optional[float] = NotProvided,
        epsilon: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[LearningRateOrSchedule] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        _separate_vf_optimizer: Optional[bool] = NotProvided,
        _lr_vf: Optional[float] = NotProvided,
        # Deprecated args.
        num_aggregation_workers=DEPRECATED_VALUE,
        max_requests_in_flight_per_aggregator_worker=DEPRECATED_VALUE,
        **kwargs,
    ) -> "IMPALAConfig":
        """Sets the training related configuration.

        Args:
            vtrace: V-trace params (see vtrace_tf/torch.py).
            vtrace_clip_rho_threshold:
            vtrace_clip_pg_rho_threshold:
            num_gpu_loader_threads: The number of GPU-loader threads (per Learner
                worker), used to load incoming (CPU) batches to the GPU, if applicable.
                The incoming batches are produced by each Learner's LearnerConnector
                pipeline. After loading the batches on the GPU, the threads place them
                on yet another queue for the Learner thread (only one per Learner
                worker) to pick up and perform `forward_train/loss` computations.
            num_multi_gpu_tower_stacks: For each stack of multi-GPU towers, how many
                slots should we reserve for parallel data loading? Set this to >1 to
                load data into GPUs in parallel. This will increase GPU memory usage
                proportionally with the number of stacks.
                Example:
                2 GPUs and `num_multi_gpu_tower_stacks=3`:
                - One tower stack consists of 2 GPUs, each with a copy of the
                model/graph.
                - Each of the stacks will create 3 slots for batch data on each of its
                GPUs, increasing memory requirements on each GPU by 3x.
                - This enables us to preload data into these stacks while another stack
                is performing gradient calculations.
            minibatch_buffer_size: How many train batches should be retained for
                minibatching. This conf only has an effect if `num_epochs > 1`.
            replay_proportion: Set >0 to enable experience replay. Saved samples will
                be replayed with a p:1 proportion to new data samples.
            replay_buffer_num_slots: Number of sample batches to store for replay.
                The number of transitions saved total will be
                (replay_buffer_num_slots * rollout_fragment_length).
            learner_queue_size: Max queue size for train batches feeding into the
                learner.
            learner_queue_timeout: Wait for train batches to be available in minibatch
                buffer queue this many seconds. This may need to be increased e.g. when
                training with a slow environment.
            timeout_s_sampler_manager: The timeout for waiting for sampling results
                for workers -- typically if this is too low, the manager won't be able
                to retrieve ready sampling results.
            timeout_s_aggregator_manager: The timeout for waiting for replay worker
                results -- typically if this is too low, the manager won't be able to
                retrieve ready replay requests.
            broadcast_interval: Number of training step calls before weights are
                broadcasted to rollout workers that are sampled during any iteration.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            opt_type: Either "adam" or "rmsprop".
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            decay: Decay setting for the RMSProp optimizer, in case `opt_type=rmsprop`.
            momentum: Momentum setting for the RMSProp optimizer, in case
                `opt_type=rmsprop`.
            epsilon: Epsilon setting for the RMSProp optimizer, in case
                `opt_type=rmsprop`.
            vf_loss_coeff: Coefficient for the value function term in the loss function.
            entropy_coeff: Coefficient for the entropy regularizer term in the loss
                function.
            entropy_coeff_schedule: Decay schedule for the entropy regularizer.
            _separate_vf_optimizer: Set this to true to have two separate optimizers
                optimize the policy-and value networks. Only supported for some
                algorithms (APPO, IMPALA) on the old API stack.
            _lr_vf: If _separate_vf_optimizer is True, define separate learning rate
                for the value network.

        Returns:
            This updated AlgorithmConfig object.
        """
        if num_aggregation_workers != DEPRECATED_VALUE:
            deprecation_warning(
                old="config.training(num_aggregation_workers=..)",
                help="Aggregator workers are no longer supported on the old API "
                "stack! To use aggregation (and GPU pre-loading) on the new API "
                "stack, activate the new API stack, then set "
                "`config.learners(num_aggregator_actors_per_learner=..)`. Good "
                "choices are normally 1 or 2, but this depends on your overall "
                "setup, especially your `EnvRunner` throughput.",
                error=True,
            )
        if max_requests_in_flight_per_aggregator_worker != DEPRECATED_VALUE:
            deprecation_warning(
                old="config.training(max_requests_in_flight_per_aggregator_worker=..)",
                help="Aggregator workers are no longer supported on the old API "
                "stack! To use aggregation (and GPU pre-loading) on the new API "
                "stack, activate the new API stack and THEN set "
                "`config.learners(max_requests_in_flight_per_aggregator_actor=..)"
                "`.",
                error=True,
            )

        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if vtrace is not NotProvided:
            self.vtrace = vtrace
        if vtrace_clip_rho_threshold is not NotProvided:
            self.vtrace_clip_rho_threshold = vtrace_clip_rho_threshold
        if vtrace_clip_pg_rho_threshold is not NotProvided:
            self.vtrace_clip_pg_rho_threshold = vtrace_clip_pg_rho_threshold
        if num_gpu_loader_threads is not NotProvided:
            self.num_gpu_loader_threads = num_gpu_loader_threads
        if num_multi_gpu_tower_stacks is not NotProvided:
            self.num_multi_gpu_tower_stacks = num_multi_gpu_tower_stacks
        if minibatch_buffer_size is not NotProvided:
            self.minibatch_buffer_size = minibatch_buffer_size
        if replay_proportion is not NotProvided:
            self.replay_proportion = replay_proportion
        if replay_buffer_num_slots is not NotProvided:
            self.replay_buffer_num_slots = replay_buffer_num_slots
        if learner_queue_size is not NotProvided:
            self.learner_queue_size = learner_queue_size
        if learner_queue_timeout is not NotProvided:
            self.learner_queue_timeout = learner_queue_timeout
        if broadcast_interval is not NotProvided:
            self.broadcast_interval = broadcast_interval
        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_aggregator_manager is not NotProvided:
            self.timeout_s_aggregator_manager = timeout_s_aggregator_manager
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if opt_type is not NotProvided:
            self.opt_type = opt_type
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if decay is not NotProvided:
            self.decay = decay
        if momentum is not NotProvided:
            self.momentum = momentum
        if epsilon is not NotProvided:
            self.epsilon = epsilon
        if vf_loss_coeff is not NotProvided:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not NotProvided:
            self.entropy_coeff_schedule = entropy_coeff_schedule
        if _separate_vf_optimizer is not NotProvided:
            self._separate_vf_optimizer = _separate_vf_optimizer
        if _lr_vf is not NotProvided:
            self._lr_vf = _lr_vf

        return self

    def debugging(
        self,
        *,
        _env_runners_only: Optional[bool] = NotProvided,
        _skip_learners: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "IMPALAConfig":
        """Sets the debugging related configuration.

        Args:
            _env_runners_only: If True, only run (remote) EnvRunner requests, discard
                their episode/training data, but log their metrics results. Aggregator-
                and Learner actors won't be used.
            _skip_learners: If True, no `update` requests are sent to the LearnerGroup
                and Learner actors. Only EnvRunners and aggregator actors (if
                applicable) are used.
        """
        super().debugging(**kwargs)

        if _env_runners_only is not NotProvided:
            self._env_runners_only = _env_runners_only
        if _skip_learners is not NotProvided:
            self._skip_learners = _skip_learners

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call the super class' validation method first.
        super().validate()

        # IMPALA and APPO need vtrace (A3C Policies no longer exist).
        if not self.vtrace:
            self._value_error(
                "IMPALA and APPO do NOT support vtrace=False anymore! Set "
                "`config.training(vtrace=True)`."
            )

        # New API stack checks.
        if self.enable_env_runner_and_connector_v2:
            # Does NOT support aggregation workers yet or a mixin replay buffer.
            if self.replay_ratio != 0.0:
                self._value_error(
                    "The new API stack in combination with the new EnvRunner API "
                    "does NOT support a mixin replay buffer yet for "
                    f"{self} (set `config.replay_proportion` to 0.0)!"
                )
            # `lr_schedule` checking.
            if self.lr_schedule is not None:
                self._value_error(
                    "`lr_schedule` is deprecated and must be None! Use the "
                    "`lr` setting to setup a schedule."
                )
            # Entropy coeff schedule checking.
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
            if self.minibatch_size is not None and not (
                (self.minibatch_size % self.rollout_fragment_length == 0)
                and self.minibatch_size <= self.total_train_batch_size
            ):
                self._value_error(
                    f"`minibatch_size` ({self._minibatch_size}) must either be None "
                    "or a multiple of `rollout_fragment_length` "
                    f"({self.rollout_fragment_length}) while at the same time smaller "
                    "than or equal to `total_train_batch_size` "
                    f"({self.total_train_batch_size})!"
                )
        # Old API stack checks.
        else:
            if isinstance(self.entropy_coeff, float) and self.entropy_coeff < 0.0:
                self._value_error("`entropy_coeff` must be >= 0.0")

        # If two separate optimizers/loss terms used for tf, must also set
        # `_tf_policy_handles_more_than_one_loss` to True.
        if (
            self.framework_str in ["tf", "tf2"]
            and self._separate_vf_optimizer is True
            and self._tf_policy_handles_more_than_one_loss is False
        ):
            self._value_error(
                "`_tf_policy_handles_more_than_one_loss` must be set to True, for "
                "TFPolicy to support more than one loss term/optimizer! Try setting "
                "config.training(_tf_policy_handles_more_than_one_loss=True)."
            )

    @property
    def replay_ratio(self) -> float:
        """Returns replay ratio (between 0.0 and 1.0) based off self.replay_proportion.

        Formula: ratio = 1 / proportion
        """
        return (1 / self.replay_proportion) if self.replay_proportion > 0 else 0.0

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            from ray.rllib.algorithms.impala.torch.impala_torch_learner import (
                IMPALATorchLearner,
            )

            return IMPALATorchLearner
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
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        connector = super().build_learner_connector(
            input_observation_space,
            input_action_space,
            device,
        )
        if self.add_default_connectors_to_learner_pipeline:
            # Extend all episodes by one artificial timestep to allow the value function
            # net to compute the bootstrap values (and add a mask to the batch to know,
            # which slots to mask out).
            connector.prepend(AddOneTsToEpisodesAndTruncate())
            # Remove the NumpyToTensor connector if we have the GPULoaderThreads.
            if self.num_aggregator_actors_per_learner > 0:
                connector.remove(NumpyToTensor)
        return connector


ImpalaConfig = IMPALAConfig


class IMPALA(Algorithm):
    """Importance weighted actor/learner architecture (IMPALA) Algorithm

    == Overview of data flow in IMPALA ==
    1. Policy evaluation in parallel across `num_env_runners` actors produces
       batches of size `rollout_fragment_length * num_envs_per_env_runner`.
    2. If enabled, the replay buffer stores and produces batches of size
       `rollout_fragment_length * num_envs_per_env_runner`.
    3. If enabled, the minibatch ring buffer stores and replays batches of
       size `train_batch_size` up to `num_epochs` times per batch.
    4. The learner thread executes data parallel SGD across `num_gpus` GPUs
       on batches of size `train_batch_size`.
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return IMPALAConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config.framework_str == "torch":
            from ray.rllib.algorithms.impala.impala_torch_policy import (
                ImpalaTorchPolicy,
            )

            return ImpalaTorchPolicy

        elif config.framework_str == "tf":
            from ray.rllib.algorithms.impala.impala_tf_policy import (
                ImpalaTF1Policy,
            )

            return ImpalaTF1Policy
        else:
            from ray.rllib.algorithms.impala.impala_tf_policy import (
                ImpalaTF2Policy,
            )

            return ImpalaTF2Policy

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Queue of data to be sent to the Learner.
        self.data_to_place_on_learner = []
        self.local_mixin_buffer = None  # @OldAPIStack
        self._batch_being_built = []  # @OldAPIStack

        # Create extra aggregation workers and assign each rollout worker to one of
        # them.
        self._episode_packs_being_built = []
        self._ma_batches_being_built: Dict[int, list] = {
            i: [] for i in range(self.config.num_learners or 1)
        }

        # Create local mixin buffer if on old API stack and replay
        # proportion is set.
        if not self.config.enable_rl_module_and_learner:
            if self.config.replay_proportion > 0.0:
                self.local_mixin_buffer = MixInMultiAgentReplayBuffer(
                    capacity=(
                        self.config.replay_buffer_num_slots
                        if self.config.replay_buffer_num_slots > 0
                        else 1
                    ),
                    replay_ratio=self.config.replay_ratio,
                    replay_mode=ReplayMode.LOCKSTEP,
                )

        # This variable is used to keep track of the statistics from the most recent
        # update of the learner group
        self._results = {}

        if not self.config.enable_rl_module_and_learner:
            # Create and start the learner thread.
            self._learner_thread = make_learner_thread(self.env_runner, self.config)
            self._learner_thread.start()

    @override(Algorithm)
    def training_step(self):
        # Old API stack.
        if not self.config.enable_rl_module_and_learner:
            return self._training_step_old_api_stack()

        do_async_updates = self.config.num_learners > 0

        # Asynchronously request all EnvRunners to sample and return their current
        # (e.g. ConnectorV2) states and sampling metrics/stats.
        # Note that each item in `episode_refs` is a reference to a list of Episodes.
        with self.metrics.log_time((TIMERS, SAMPLE_TIMER)):
            (
                episode_refs,
                connector_states,
                env_runner_metrics,
                env_runner_indices_to_update,
            ) = self._sample_and_get_connector_states()
            # Reduce EnvRunner metrics over the n EnvRunners.
            self.metrics.aggregate(
                env_runner_metrics,
                key=ENV_RUNNER_RESULTS,
            )

            # Log the average number of sample results (list of episodes) received.
            self.metrics.log_value(
                (ENV_RUNNER_RESULTS, MEAN_NUM_EPISODE_LISTS_RECEIVED),
                len(episode_refs),
            )

        # Only run EnvRunners, nothing else.
        if self.config._env_runners_only:
            return

        # "Batch" collected episode refs into groups, such that exactly
        # `total_train_batch_size` timesteps are sent to
        # `LearnerGroup.update()`.
        if self.config.num_aggregator_actors_per_learner > 0:
            data_packages_for_aggregators = self._pre_queue_episode_refs(
                episode_refs, package_size=self.config.train_batch_size_per_learner
            )
            self.metrics.log_value(
                (AGGREGATOR_ACTOR_RESULTS, "mean_num_input_packages"),
                len(episode_refs),
            )

            ma_batches_refs_remote_results = (
                self._aggregator_actor_manager.fetch_ready_async_reqs(
                    timeout_seconds=0.0,
                    return_obj_refs=True,
                    tags="batches",
                )
            )
            ma_batches_refs = []
            for call_result in ma_batches_refs_remote_results:
                ma_batches_refs.append((call_result.actor_id, call_result.get()))
            self.metrics.log_value(
                (AGGREGATOR_ACTOR_RESULTS, "mean_num_output_batches"),
                len(ma_batches_refs),
            )

            while data_packages_for_aggregators:
                num_agg = self.config.num_aggregator_actors_per_learner * (
                    self.config.num_learners or 1
                )
                packs, data_packages_for_aggregators = (
                    data_packages_for_aggregators[:num_agg],
                    data_packages_for_aggregators[num_agg:],
                )
                sent = self._aggregator_actor_manager.foreach_actor_async(
                    func="get_batch",
                    kwargs=[dict(episode_refs=p) for p in packs],
                    tag="batches",
                )
                self.metrics.log_value(
                    (AGGREGATOR_ACTOR_RESULTS, "num_env_steps_dropped_lifetime"),
                    self.config.train_batch_size_per_learner * (len(packs) - sent),
                    reduce="sum",
                )

            # Get n lists of m ObjRef[MABatch] (m=num_learners) to perform n calls to
            # all learner workers with the already GPU-located batches.
            data_packages_for_learner_group = self._pre_queue_batch_refs(
                ma_batches_refs
            )
            self.metrics.log_value(
                (AGGREGATOR_ACTOR_RESULTS, "num_env_steps_aggregated_lifetime"),
                self.config.train_batch_size_per_learner
                * (self.config.num_learners or 1)
                * len(data_packages_for_learner_group),
                reduce="sum",
                with_throughput=True,
            )

        else:
            data_packages_for_learner_group = self._pre_queue_episode_refs(
                episode_refs, package_size=self.config.total_train_batch_size
            )

        # Skip Learner update calls.
        if self.config._skip_learners:
            return

        # Call the LearnerGroup's `update()` method.
        with self.metrics.log_time((TIMERS, LEARNER_UPDATE_TIMER)):
            self.metrics.log_value(
                key=MEAN_NUM_LEARNER_GROUP_UPDATE_CALLED,
                value=len(data_packages_for_learner_group),
            )
            rl_module_state = None
            num_learner_group_results_received = 0

            return_state = (
                self.metrics.peek(
                    NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS,
                    default=0,
                )
                >= self.config.broadcast_interval
            )

            for batch_ref_or_episode_list_ref in data_packages_for_learner_group:
                timesteps = {
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: self.metrics.peek(
                        (ENV_RUNNER_RESULTS, NUM_ENV_STEPS_SAMPLED_LIFETIME), default=0
                    ),
                    NUM_ENV_STEPS_TRAINED_LIFETIME: self.metrics.peek(
                        (LEARNER_RESULTS, ALL_MODULES, NUM_ENV_STEPS_TRAINED_LIFETIME),
                        default=0,
                    ),
                }
                # Update from batch refs coming from AggregatorActors.
                if self.config.num_aggregator_actors_per_learner > 0:
                    assert len(batch_ref_or_episode_list_ref) == (
                        self.config.num_learners or 1
                    )
                    training_data = TrainingData(
                        batch_refs=batch_ref_or_episode_list_ref
                    )
                # Update from episodes refs coming from EnvRunner actors.
                else:
                    training_data = TrainingData(
                        episodes_refs=batch_ref_or_episode_list_ref
                    )
                learner_results = self.learner_group.update(
                    training_data=training_data,
                    async_update=do_async_updates,
                    return_state=return_state,
                    timesteps=timesteps,
                    num_epochs=self.config.num_epochs,
                    minibatch_size=self.config.minibatch_size,
                    shuffle_batch_per_epoch=self.config.shuffle_batch_per_epoch,
                )
                # Only request weights from 1st Learner - at most - once per
                # `training_step` call.
                return_state = False

                num_learner_group_results_received += len(learner_results)
                # Extract the last (most recent) weights matrix, if available.
                for result_from_1_learner in learner_results:
                    rl_module_state = result_from_1_learner.pop(
                        "_rl_module_state_after_update", rl_module_state
                    )
                self.metrics.aggregate(
                    stats_dicts=learner_results,
                    key=LEARNER_RESULTS,
                )
            self.metrics.log_value(
                key=(LEARNER_GROUP, MEAN_NUM_LEARNER_RESULTS_RECEIVED),
                value=num_learner_group_results_received,
            )

        self.metrics.log_value(
            NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS, 1, reduce="sum"
        )

        # Update LearnerGroup's own stats.
        self.metrics.log_dict(self.learner_group.get_stats(), key=LEARNER_GROUP)

        # Figure out, whether we should sync/broadcast the (remote) EnvRunner states.
        # Note: `learner_results` is a List of n (num async calls) Lists of m
        # (num Learner workers) ResultDicts each.
        if rl_module_state is not None:
            self.metrics.set_value(
                NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS, 0
            )
            self.metrics.log_value(NUM_SYNCH_WORKER_WEIGHTS, 1, reduce="sum")
            with self.metrics.log_time((TIMERS, SYNCH_WORKER_WEIGHTS_TIMER)):
                self.env_runner_group.sync_env_runner_states(
                    config=self.config,
                    connector_states=connector_states,
                    rl_module_state=rl_module_state,
                    env_to_module=self.env_to_module_connector,
                    module_to_env=self.module_to_env_connector,
                )

    def _sample_and_get_connector_states(self):
        env_runner_indices_to_update = set()
        episode_refs = []
        connector_states = []
        env_runner_metrics = []
        num_healthy_remote_workers = self.env_runner_group.num_healthy_remote_workers()

        # Perform asynchronous sampling on all (healthy) remote rollout workers.
        if num_healthy_remote_workers > 0:
            async_results: List[
                Tuple[int, ObjectRef]
            ] = self.env_runner_group.fetch_ready_async_reqs(
                timeout_seconds=self.config.timeout_s_sampler_manager,
                return_obj_refs=False,
            )
            self.env_runner_group.foreach_env_runner_async(
                "sample_get_state_and_metrics"
            )
            # Get results from the n different async calls and store those EnvRunner
            # indices we should update.
            results = []
            for r in async_results:
                env_runner_indices_to_update.add(r[0])
                results.append(r[1])

            for (episodes, states, metrics) in results:
                episode_refs.append(episodes)
                connector_states.append(states)
                env_runner_metrics.append(metrics)
        # Sample from the local EnvRunner.
        else:
            episodes = self.env_runner.sample()
            env_runner_metrics = [self.env_runner.get_metrics()]
            episode_refs = [ray.put(episodes)]
            connector_states = [
                self.env_runner.get_state(
                    components=[
                        COMPONENT_ENV_TO_MODULE_CONNECTOR,
                        COMPONENT_MODULE_TO_ENV_CONNECTOR,
                    ]
                )
            ]

        return (
            episode_refs,
            connector_states,
            env_runner_metrics,
            env_runner_indices_to_update,
        )

    def _pre_queue_episode_refs(
        self, episode_refs: List[ObjectRef], package_size: int
    ) -> List[List[ObjectRef]]:
        # Each element in this list is itself a list of ObjRef[Episodes].
        # Each ObjRef was returned by one EnvRunner from a single sample() call.
        episodes: List[List[ObjectRef]] = []

        for ref in episode_refs:
            self._episode_packs_being_built.append(ref)
            if (
                len(self._episode_packs_being_built)
                * self.config.num_envs_per_env_runner
                * self.config.get_rollout_fragment_length()
                >= package_size
            ):
                episodes.append(self._episode_packs_being_built)
                self._episode_packs_being_built = []

        return episodes

    def _pre_queue_batch_refs(
        self, batch_refs: List[Tuple[int, ObjectRef]]
    ) -> List[List[ObjectRef]]:
        # `batch_refs` is a list of tuple(aggregator_actor_id, ObjRef[MABatch]).

        # Each ObjRef[MABatch] was returned by one AggregatorActor from a single
        # `get_batch()` call.
        # TODO (sven): Add this comment, once valid:
        #  .. and the underlying MABatch is already located on a particular GPU
        #  (matching one particular Learner).
        for agg_actor_id, ma_batch_ref in batch_refs:
            learner_actor_id = self._aggregator_actor_to_learner[agg_actor_id]
            self._ma_batches_being_built[learner_actor_id].append(ma_batch_ref)

        # Construct an n-group of batches (n=num_learners) as long as we still have
        # at least one batch per learner in our queue.
        batch_refs_for_learner_group: List[List[ObjectRef]] = []
        while all(
            learner_list for learner_list in self._ma_batches_being_built.values()
        ):
            batch_refs_for_learner_group.append(
                [
                    learner_list.pop(0)
                    for learner_list in self._ma_batches_being_built.values()
                ]
            )

        return batch_refs_for_learner_group

    @OldAPIStack
    def _training_step_old_api_stack(self):
        # First, check, whether our learner thread is still healthy.
        if not self._learner_thread.is_alive():
            raise RuntimeError("The learner thread died while training!")

        # Get sampled SampleBatches from our workers (by ray references if we use
        # tree-aggregation).
        unprocessed_sample_batches = self._get_samples_from_workers_old_api_stack(
            return_object_refs=False,
        )
        # Tag workers that actually produced ready sample batches this iteration.
        # Those workers will have to get updated at the end of the iteration.
        workers_that_need_updates = {
            worker_id for worker_id, _ in unprocessed_sample_batches
        }

        # Resolve collected batches here on local process (using the mixin buffer).
        batches = self._process_experiences_old_api_stack(unprocessed_sample_batches)

        # Increase sampling counters now that we have the actual SampleBatches on
        # the local process (and can measure their sizes).
        for batch in batches:
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.count
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        # Concatenate single batches into batches of size `total_train_batch_size`.
        self._concatenate_batches_and_pre_queue(batches)
        # Move train batches (of size `total_train_batch_size`) onto learner queue.
        self._place_processed_samples_on_learner_thread_queue()
        # Extract most recent train results from learner thread.
        train_results = self._process_trained_results()

        # Sync worker weights (only those policies that were actually updated).
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            pids = list(train_results.keys())
            self._update_workers_old_api_stack(
                workers_that_need_updates=workers_that_need_updates,
                policy_ids=pids,
            )

        # With a training step done, try to bring any aggregators back to life
        # if necessary.
        # AggregatorActor are stateless, so we do not need to restore any
        # state here.
        if self._aggregator_actor_manager:
            self._aggregator_actor_manager.probe_unhealthy_actors(
                timeout_seconds=self.config.env_runner_health_probe_timeout_s,
                mark_healthy=True,
            )

        return train_results

    @OldAPIStack
    def _get_samples_from_workers_old_api_stack(
        self,
        return_object_refs: Optional[bool] = False,
    ) -> List[Tuple[int, Union[ObjectRef, SampleBatchType]]]:
        """Get samples from rollout workers for training.

        Args:
            return_object_refs: If True, return ObjectRefs instead of the samples
                directly. This is useful when using aggregator workers so that data
                collected on rollout workers is directly de referenced on the aggregator
                workers instead of first in the driver and then on the aggregator
                workers.

        Returns:
            a list of tuples of (worker_index, sample batch or ObjectRef to a sample
                batch)

        """
        with self._timers[SAMPLE_TIMER]:
            # Sample from healthy remote workers by default. If there is no healthy
            # worker (either because they have all died, or because there was none to
            # begin) check if the local_worker exists. If the local worker has an
            # env_instance (either because there are no remote workers or
            # self.config.create_local_env_runner == True), then sample from the
            # local worker. Otherwise just return an empty list.
            if self.env_runner_group.num_healthy_remote_workers() > 0:
                # Perform asynchronous sampling on all (remote) rollout workers.
                self.env_runner_group.foreach_env_runner_async(
                    lambda worker: worker.sample()
                )
                sample_batches: List[
                    Tuple[int, ObjectRef]
                ] = self.env_runner_group.fetch_ready_async_reqs(
                    timeout_seconds=self.config.timeout_s_sampler_manager,
                    return_obj_refs=return_object_refs,
                )
            elif self.config.num_env_runners == 0 or (
                self.env_runner and self.env_runner.async_env is not None
            ):
                # Sampling from the local worker
                sample_batch = self.env_runner.sample()
                if return_object_refs:
                    sample_batch = ray.put(sample_batch)
                sample_batches = [(0, sample_batch)]
            else:
                # Not much we can do. Return empty list and wait.
                sample_batches = []

        return sample_batches

    @OldAPIStack
    def _process_experiences_old_api_stack(
        self,
        worker_to_sample_batches: List[Tuple[int, SampleBatch]],
    ) -> List[SampleBatchType]:
        """Process sample batches directly on the driver, for training.

        Args:
            worker_to_sample_batches: List of (worker_id, sample_batch) tuples.

        Returns:
            Batches that have been processed by the mixin buffer.

        """
        batches = [b for _, b in worker_to_sample_batches]
        processed_batches = []

        for batch in batches:
            assert not isinstance(
                batch, ObjectRef
            ), "`IMPALA._process_experiences_old_api_stack` can not handle ObjectRefs!"
            batch = batch.decompress_if_needed()
            # Only make a pass through the buffer, if replay proportion is > 0.0 (and
            # we actually have one).
            if self.local_mixin_buffer:
                self.local_mixin_buffer.add(batch)
                batch = self.local_mixin_buffer.replay(_ALL_POLICIES)
            else:
                # TODO(jjyao) somehow deep copy the batch
                # fix a memory leak issue. Need to investigate more
                # to know why.
                batch = batch.copy()
            if batch:
                processed_batches.append(batch)

        return processed_batches

    @OldAPIStack
    def _concatenate_batches_and_pre_queue(self, batches: List[SampleBatch]) -> None:
        """Concatenate batches that are being returned from rollout workers

        Args:
            batches: List of batches of experiences from EnvRunners.
        """

        def aggregate_into_larger_batch():
            if (
                sum(b.count for b in self._batch_being_built)
                >= self.config.total_train_batch_size
            ):
                batch_to_add = concat_samples(self._batch_being_built)
                self.data_to_place_on_learner.append(batch_to_add)
                self._batch_being_built = []

        for batch in batches:
            # TODO (sven): Strange bug after a RolloutWorker crash and proper
            #  restart. The bug is related to (old, non-V2) connectors being used and
            #  seems to happen inside the AgentCollector's `add_action_reward_next_obs`
            #  method, at the end of which the number of vf_preds (and all other
            #  extra action outs) in the batch is one smaller than the number of obs/
            #  actions/rewards, which then leads to a malformed train batch.
            #  IMPALA/APPO crash inside the loss function (during v-trace operations)
            #  b/c of the resulting shape mismatch. The following if-block prevents
            #  this from happening and it can be removed once we are on the new API
            #  stack for good (and use the new connectors and also no longer
            #  AgentCollectors, RolloutWorkers, Policies, TrajectoryView API, etc..):
            if (
                self.config.batch_mode == "truncate_episodes"
                and self.config.restart_failed_env_runners
            ):
                if any(
                    SampleBatch.VF_PREDS in pb
                    and (
                        pb[SampleBatch.VF_PREDS].shape[0]
                        != pb[SampleBatch.REWARDS].shape[0]
                    )
                    for pb in batch.policy_batches.values()
                ):
                    continue

            self._batch_being_built.append(batch)
            aggregate_into_larger_batch()

    @OldAPIStack
    def _place_processed_samples_on_learner_thread_queue(self) -> None:
        """Place processed samples on the learner queue for training."""
        for i, batch in enumerate(self.data_to_place_on_learner):
            try:
                self._learner_thread.inqueue.put(
                    batch,
                    # Setting block = True for the very last item in our list prevents
                    # the learner thread, this main thread, and the GPU loader threads
                    # from thrashing when there are more samples than the learner can
                    # reasonably process.
                    # see https://github.com/ray-project/ray/pull/26581#issuecomment-1187877674  # noqa
                    block=i == len(self.data_to_place_on_learner) - 1,
                )
                self._counters["num_samples_added_to_queue"] += (
                    batch.agent_steps()
                    if self.config.count_steps_by == "agent_steps"
                    else batch.count
                )
            except queue.Full:
                self._counters["num_times_learner_queue_full"] += 1

        self.data_to_place_on_learner.clear()

    @OldAPIStack
    def _process_trained_results(self) -> ResultDict:
        """Process training results that are outputed by the learner thread.

        Returns:
            Aggregated results from the learner thread after an update is completed.

        """
        # Get learner outputs/stats from output queue.
        num_env_steps_trained = 0
        num_agent_steps_trained = 0
        learner_infos = []
        # Loop through output queue and update our counts.
        for _ in range(self._learner_thread.outqueue.qsize()):
            (
                env_steps,
                agent_steps,
                learner_results,
            ) = self._learner_thread.outqueue.get(timeout=0.001)
            num_env_steps_trained += env_steps
            num_agent_steps_trained += agent_steps
            if learner_results:
                learner_infos.append(learner_results)
        # Nothing new happened since last time, use the same learner stats.
        if not learner_infos:
            final_learner_info = copy.deepcopy(self._learner_thread.learner_info)
        # Accumulate learner stats using the `LearnerInfoBuilder` utility.
        else:
            builder = LearnerInfoBuilder()
            for info in learner_infos:
                builder.add_learn_on_batch_results_multi_agent(info)
            final_learner_info = builder.finalize()

        # Update the steps trained counters.
        self._counters[NUM_ENV_STEPS_TRAINED] += num_env_steps_trained
        self._counters[NUM_AGENT_STEPS_TRAINED] += num_agent_steps_trained

        return final_learner_info

    @OldAPIStack
    def _update_workers_old_api_stack(
        self,
        workers_that_need_updates: Set[int],
        policy_ids: Optional[List[PolicyID]] = None,
    ) -> None:
        """Updates all RolloutWorkers that require updating.

        Updates only if NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS has been
        reached and the worker has sent samples in this iteration. Also only updates
        those policies, whose IDs are given via `policies` (if None, update all
        policies).

        Args:
            workers_that_need_updates: Set of worker IDs that need to be updated.
            policy_ids: Optional list of Policy IDs to update. If None, will update all
                policies on the to-be-updated workers.
        """
        # Update global vars of the local worker.
        if self.config.policy_states_are_swappable:
            self.env_runner.lock()
        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_TRAINED],
            "num_grad_updates_per_policy": {
                pid: self.env_runner.policy_map[pid].num_grad_updates
                for pid in policy_ids or []
            },
        }
        self.env_runner.set_global_vars(global_vars, policy_ids=policy_ids)
        if self.config.policy_states_are_swappable:
            self.env_runner.unlock()

        # Only need to update workers if there are remote workers.
        self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] += 1
        if (
            self.env_runner_group.num_remote_workers() > 0
            and self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS]
            >= self.config.broadcast_interval
            and workers_that_need_updates
        ):
            if self.config.policy_states_are_swappable:
                self.env_runner.lock()
            weights = self.env_runner.get_weights(policy_ids)
            if self.config.policy_states_are_swappable:
                self.env_runner.unlock()
            weights_ref = ray.put(weights)

            self._learner_thread.policy_ids_updated.clear()
            self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] = 0
            self._counters[NUM_SYNCH_WORKER_WEIGHTS] += 1
            self.env_runner_group.foreach_env_runner(
                func=lambda w: w.set_weights(ray.get(weights_ref), global_vars),
                local_env_runner=False,
                remote_worker_ids=list(workers_that_need_updates),
                timeout_seconds=0,  # Don't wait for the workers to finish.
            )

    @override(Algorithm)
    def _compile_iteration_results_old_api_stack(self, *args, **kwargs):
        result = super()._compile_iteration_results_old_api_stack(*args, **kwargs)
        if not self.config.enable_rl_module_and_learner:
            result = self._learner_thread.add_learner_metrics(
                result, overwrite_learner_info=False
            )
        return result


Impala = IMPALA


@OldAPIStack
def make_learner_thread(local_worker, config):
    if not config["simple_optimizer"]:
        logger.info(
            "Enabling multi-GPU mode, {} GPUs, {} parallel tower-stacks".format(
                config["num_gpus"], config["num_multi_gpu_tower_stacks"]
            )
        )
        num_stacks = config["num_multi_gpu_tower_stacks"]
        buffer_size = config["minibatch_buffer_size"]
        if num_stacks < buffer_size:
            logger.warning(
                "In multi-GPU mode you should have at least as many "
                "multi-GPU tower stacks (to load data into on one device) as "
                "you have stack-index slots in the buffer! You have "
                f"configured {num_stacks} stacks and a buffer of size "
                f"{buffer_size}. Setting "
                f"`minibatch_buffer_size={num_stacks}`."
            )
            config["minibatch_buffer_size"] = num_stacks

        learner_thread = MultiGPULearnerThread(
            local_worker,
            num_gpus=config["num_gpus"],
            lr=config["lr"],
            train_batch_size=config["train_batch_size"],
            num_multi_gpu_tower_stacks=config["num_multi_gpu_tower_stacks"],
            num_sgd_iter=config["num_epochs"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"],
            num_data_load_threads=config["num_gpu_loader_threads"],
        )
    else:
        learner_thread = LearnerThread(
            local_worker,
            minibatch_buffer_size=config["minibatch_buffer_size"],
            num_sgd_iter=config["num_epochs"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"],
        )
    return learner_thread
