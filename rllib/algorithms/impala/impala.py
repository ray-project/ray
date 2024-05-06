import copy
from functools import partial
import logging
import platform
import queue
import random
from typing import Callable, List, Optional, Set, Tuple, Type, Union

import numpy as np
import tree  # pip install dm_tree

import ray
from ray import ObjectRef
from ray.rllib import SampleBatch
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.env.env_runner_group import _handle_remote_call_result_errors
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.multi_gpu_learner_thread import MultiGPULearnerThread
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import concat_samples
from ray.rllib.utils.actor_manager import (
    FaultAwareApply,
    FaultTolerantActorManager,
    RemoteCallResults,
)
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_MODULE_STEPS_TRAINED,
    NUM_SYNCH_WORKER_WEIGHTS,
    NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS,
    SYNCH_WORKER_WEIGHTS_TIMER,
    SAMPLE_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import ReplayMode
from ray.rllib.utils.replay_buffers.replay_buffer import _ALL_POLICIES
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import (
    PartialAlgorithmConfigDict,
    PolicyID,
    ResultDict,
    SampleBatchType,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory


logger = logging.getLogger(__name__)


LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


class ImpalaConfig(AlgorithmConfig):
    """Defines a configuration class from which an Impala can be built.

    .. testcode::

        from ray.rllib.algorithms.impala import ImpalaConfig
        config = ImpalaConfig()
        config = config.training(lr=0.0003, train_batch_size=512)
        config = config.resources(num_gpus=0)
        config = config.env_runners(num_env_runners=1)
        # Build a Algorithm object from the config and run 1 training iteration.
        algo = config.build(env="CartPole-v1")
        algo.train()
        del algo

    .. testcode::

        from ray.rllib.algorithms.impala import ImpalaConfig
        from ray import air
        from ray import tune
        config = ImpalaConfig()

        # Update the config object.
        config = config.training(
            lr=tune.grid_search([0.0001, 0.0002]), grad_clip=20.0
        )
        config = config.resources(num_gpus=0)
        config = config.env_runners(num_env_runners=1)
        # Set the config object's env.
        config = config.environment(env="CartPole-v1")
        # Run with tune.
        tune.Tuner(
            "IMPALA",
            param_space=config,
            run_config=air.RunConfig(stop={"training_iteration": 1}),
        ).fit()

    .. testoutput::
        :hide:

        ...
    """

    def __init__(self, algo_class=None):
        """Initializes a ImpalaConfig instance."""
        super().__init__(algo_class=algo_class or Impala)

        # fmt: off
        # __sphinx_doc_begin__

        # IMPALA specific settings:
        self.vtrace = True
        self.vtrace_clip_rho_threshold = 1.0
        self.vtrace_clip_pg_rho_threshold = 1.0
        self.num_multi_gpu_tower_stacks = 1
        self.minibatch_buffer_size = 1
        self.num_sgd_iter = 1
        self.replay_proportion = 0.0
        self.replay_buffer_num_slots = 0
        self.learner_queue_size = 16
        self.learner_queue_timeout = 300
        self.max_requests_in_flight_per_aggregator_worker = 2
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_aggregator_manager = 0.0
        self.broadcast_interval = 1
        self.num_aggregation_workers = 0

        self.grad_clip = 40.0
        # Note: Only when using enable_rl_module_and_learner=True can the clipping mode
        # be configured by the user. On the old API stack, RLlib will always clip by
        # global_norm, no matter the value of `grad_clip_by`.
        self.grad_clip_by = "global_norm"

        self.opt_type = "adam"
        self.lr_schedule = None
        self.decay = 0.99
        self.momentum = 0.0
        self.epsilon = 0.1
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.01
        self.entropy_coeff_schedule = None
        self._separate_vf_optimizer = False
        self._lr_vf = 0.0005
        self.after_train_step = None

        # Override some of AlgorithmConfig's default values with IMPALA-specific values.
        self.rollout_fragment_length = 50
        self.train_batch_size = 500
        self._minibatch_size = "auto"
        self.num_env_runners = 2
        self.num_gpus = 1
        self.lr = 0.0005
        self.min_time_s_per_iteration = 10
        self._tf_policy_handles_more_than_one_loss = True
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

        # Deprecated value.
        self.num_data_loader_buffers = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        vtrace: Optional[bool] = NotProvided,
        vtrace_clip_rho_threshold: Optional[float] = NotProvided,
        vtrace_clip_pg_rho_threshold: Optional[float] = NotProvided,
        gamma: Optional[float] = NotProvided,
        num_multi_gpu_tower_stacks: Optional[int] = NotProvided,
        minibatch_buffer_size: Optional[int] = NotProvided,
        minibatch_size: Optional[Union[int, str]] = NotProvided,
        num_sgd_iter: Optional[int] = NotProvided,
        replay_proportion: Optional[float] = NotProvided,
        replay_buffer_num_slots: Optional[int] = NotProvided,
        learner_queue_size: Optional[int] = NotProvided,
        learner_queue_timeout: Optional[float] = NotProvided,
        max_requests_in_flight_per_aggregator_worker: Optional[int] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_aggregator_manager: Optional[float] = NotProvided,
        broadcast_interval: Optional[int] = NotProvided,
        num_aggregation_workers: Optional[int] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        opt_type: Optional[str] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        decay: Optional[float] = NotProvided,
        momentum: Optional[float] = NotProvided,
        epsilon: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        _separate_vf_optimizer: Optional[bool] = NotProvided,
        _lr_vf: Optional[float] = NotProvided,
        after_train_step: Optional[Callable[[dict], None]] = NotProvided,
        **kwargs,
    ) -> "ImpalaConfig":
        """Sets the training related configuration.

        Args:
            vtrace: V-trace params (see vtrace_tf/torch.py).
            vtrace_clip_rho_threshold:
            vtrace_clip_pg_rho_threshold:
            gamma: Float specifying the discount factor of the Markov Decision process.
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
                minibatching. This conf only has an effect if `num_sgd_iter > 1`.
            minibatch_size: The size of minibatches that are trained over during
                each SGD iteration. If "auto", will use the same value as
                `train_batch_size`.
                Note that this setting only has an effect if
                `enable_rl_module_and_learner=True` and it must be a multiple of
                `rollout_fragment_length` or `sequence_length` and smaller than or equal
                to `train_batch_size`.
            num_sgd_iter: Number of passes to make over each train batch.
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
            max_requests_in_flight_per_aggregator_worker: Level of queuing for replay
                aggregator operations (if using aggregator workers).
            timeout_s_sampler_manager: The timeout for waiting for sampling results
                for workers -- typically if this is too low, the manager won't be able
                to retrieve ready sampling results.
            timeout_s_aggregator_manager: The timeout for waiting for replay worker
                results -- typically if this is too low, the manager won't be able to
                retrieve ready replay requests.
            broadcast_interval: Number of training step calls before weights are
                broadcasted to rollout workers that are sampled during any iteration.
            num_aggregation_workers: Use n (`num_aggregation_workers`) extra Actors for
                multi-level aggregation of the data produced by the m RolloutWorkers
                (`num_workers`). Note that n should be much smaller than m.
                This can make sense if ingesting >2GB/s of samples, or if
                the data requires decompression.
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
            after_train_step: Callback for APPO to use to update KL, target network
                periodically. The input to the callback is the learner fetches dict.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if vtrace is not NotProvided:
            self.vtrace = vtrace
        if vtrace_clip_rho_threshold is not NotProvided:
            self.vtrace_clip_rho_threshold = vtrace_clip_rho_threshold
        if vtrace_clip_pg_rho_threshold is not NotProvided:
            self.vtrace_clip_pg_rho_threshold = vtrace_clip_pg_rho_threshold
        if num_multi_gpu_tower_stacks is not NotProvided:
            self.num_multi_gpu_tower_stacks = num_multi_gpu_tower_stacks
        if minibatch_buffer_size is not NotProvided:
            self.minibatch_buffer_size = minibatch_buffer_size
        if num_sgd_iter is not NotProvided:
            self.num_sgd_iter = num_sgd_iter
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
        if num_aggregation_workers is not NotProvided:
            self.num_aggregation_workers = num_aggregation_workers
        if max_requests_in_flight_per_aggregator_worker is not NotProvided:
            self.max_requests_in_flight_per_aggregator_worker = (
                max_requests_in_flight_per_aggregator_worker
            )
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
        if after_train_step is not NotProvided:
            self.after_train_step = after_train_step
        if gamma is not NotProvided:
            self.gamma = gamma
        if minibatch_size is not NotProvided:
            self._minibatch_size = minibatch_size

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call the super class' validation method first.
        super().validate()

        # IMPALA and APPO need vtrace (A3C Policies no longer exist).
        if not self.vtrace:
            raise ValueError(
                "IMPALA and APPO do NOT support vtrace=False anymore! Set "
                "`config.training(vtrace=True)`."
            )

        # New stack w/ EnvRunners does NOT support aggregation workers yet or a mixin
        # replay buffer.
        if self.enable_env_runner_and_connector_v2:
            if self.num_aggregation_workers > 0:
                raise ValueError(
                    "Aggregation workers not supported on new API stack w/ new "
                    "EnvRunner API! Set `config.num_aggregation_workers = 0` or "
                    "disable the new API stack via "
                    "`config.api_stack(enable_rl_module_and_learner=False)`."
                )
            if self.replay_ratio != 0.0:
                raise ValueError(
                    "The new API stack in combination with the new EnvRunner API "
                    "does NOT support a mixin replay buffer yet for "
                    f"{self} (set `config.replay_proportion` to 0.0)!"
                )

        if self.num_data_loader_buffers != DEPRECATED_VALUE:
            deprecation_warning(
                "num_data_loader_buffers", "num_multi_gpu_tower_stacks", error=True
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
        elif isinstance(self.entropy_coeff, float) and self.entropy_coeff < 0.0:
            raise ValueError("`entropy_coeff` must be >= 0.0")

        # Check whether worker to aggregation-worker ratio makes sense.
        if self.num_aggregation_workers > self.num_env_runners:
            raise ValueError(
                "`num_aggregation_workers` must be smaller than or equal "
                "`num_env_runners`! Aggregation makes no sense otherwise."
            )
        elif self.num_aggregation_workers > self.num_env_runners / 2:
            logger.warning(
                "`num_aggregation_workers` should be significantly smaller "
                "than `num_workers`! Try setting it to 0.5*`num_workers` or "
                "less."
            )

        # If two separate optimizers/loss terms used for tf, must also set
        # `_tf_policy_handles_more_than_one_loss` to True.
        if (
            self.framework_str in ["tf", "tf2"]
            and self._separate_vf_optimizer is True
            and self._tf_policy_handles_more_than_one_loss is False
        ):
            raise ValueError(
                "`_tf_policy_handles_more_than_one_loss` must be set to True, for "
                "TFPolicy to support more than one loss term/optimizer! Try setting "
                "config.training(_tf_policy_handles_more_than_one_loss=True)."
            )
        # Learner API specific checks.
        if (
            self.enable_rl_module_and_learner
            and self._minibatch_size != "auto"
            and not (
                (self.minibatch_size % self.rollout_fragment_length == 0)
                and self.minibatch_size <= self.total_train_batch_size
            )
        ):
            raise ValueError(
                f"`minibatch_size` ({self._minibatch_size}) must either be 'auto' "
                "or a multiple of `rollout_fragment_length` "
                f"({self.rollout_fragment_length}) while at the same time smaller "
                "than or equal to `total_train_batch_size` "
                f"({self.total_train_batch_size})!"
            )

    @property
    def replay_ratio(self) -> float:
        """Returns replay ratio (between 0.0 and 1.0) based off self.replay_proportion.

        Formula: ratio = 1 / proportion
        """
        return (1 / self.replay_proportion) if self.replay_proportion > 0 else 0.0

    @property
    def minibatch_size(self):
        # If 'auto', use the train_batch_size (meaning each SGD iter is a single pass
        # through the entire train batch). Otherwise, use user provided setting.
        return (
            (
                self.train_batch_size_per_learner
                if self.enable_env_runner_and_connector_v2
                else self.train_batch_size
            )
            if self._minibatch_size == "auto"
            else self._minibatch_size
        )

    @override(AlgorithmConfig)
    def get_default_learner_class(self):
        if self.framework_str == "torch":
            from ray.rllib.algorithms.impala.torch.impala_torch_learner import (
                ImpalaTorchLearner,
            )

            return ImpalaTorchLearner
        elif self.framework_str == "tf2":
            from ray.rllib.algorithms.impala.tf.impala_tf_learner import ImpalaTfLearner

            return ImpalaTfLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> SingleAgentRLModuleSpec:
        from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

        if self.framework_str == "tf2":
            from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule

            return SingleAgentRLModuleSpec(
                module_class=PPOTfRLModule, catalog_class=PPOCatalog
            )
        elif self.framework_str == "torch":
            from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
                PPOTorchRLModule,
            )

            return SingleAgentRLModuleSpec(
                module_class=PPOTorchRLModule, catalog_class=PPOCatalog
            )
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use either 'torch' or 'tf2'."
            )


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
            num_sgd_iter=config["num_sgd_iter"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"],
        )
    else:
        learner_thread = LearnerThread(
            local_worker,
            minibatch_buffer_size=config["minibatch_buffer_size"],
            num_sgd_iter=config["num_sgd_iter"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"],
        )
    return learner_thread


class Impala(Algorithm):
    """Importance weighted actor/learner architecture (IMPALA) Algorithm

    == Overview of data flow in IMPALA ==
    1. Policy evaluation in parallel across `num_workers` actors produces
       batches of size `rollout_fragment_length * num_envs_per_env_runner`.
    2. If enabled, the replay buffer stores and produces batches of size
       `rollout_fragment_length * num_envs_per_env_runner`.
    3. If enabled, the minibatch ring buffer stores and replays batches of
       size `train_batch_size` up to `num_sgd_iter` times per batch.
    4. The learner thread executes data parallel SGD across `num_gpus` GPUs
       on batches of size `train_batch_size`.
    """

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return ImpalaConfig()

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

        # Queue of batches to be sent to the Learner.
        self.batches_to_place_on_learner = []

        # Create extra aggregation workers and assign each rollout worker to
        # one of them.
        self.batch_being_built = []
        if self.config.num_aggregation_workers > 0:
            # This spawns `num_aggregation_workers` actors that aggregate
            # experiences coming from RolloutWorkers in parallel. We force
            # colocation on the same node (localhost) to maximize data bandwidth
            # between them and the learner.
            localhost = platform.node()
            assert localhost != "", (
                "ERROR: Cannot determine local node name! "
                "`platform.node()` returned empty string."
            )
            all_co_located = create_colocated_actors(
                actor_specs=[
                    # (class, args, kwargs={}, count=1)
                    (
                        AggregatorWorker,
                        [
                            self.config,
                        ],
                        {},
                        self.config.num_aggregation_workers,
                    )
                ],
                node=localhost,
            )
            aggregator_workers = [
                actor for actor_groups in all_co_located for actor in actor_groups
            ]
            self._aggregator_actor_manager = FaultTolerantActorManager(
                aggregator_workers,
                max_remote_requests_in_flight_per_actor=(
                    self.config.max_requests_in_flight_per_aggregator_worker
                ),
            )
            self._timeout_s_aggregator_manager = (
                self.config.timeout_s_aggregator_manager
            )
        else:
            # Create our local mixin buffer if the num of aggregation workers is 0.
            self.local_mixin_buffer = MixInMultiAgentReplayBuffer(
                capacity=(
                    self.config.replay_buffer_num_slots
                    if self.config.replay_buffer_num_slots > 0
                    else 1
                ),
                replay_ratio=self.config.replay_ratio,
                replay_mode=ReplayMode.LOCKSTEP,
            )
            self._aggregator_actor_manager = None

        # This variable is used to keep track of the statistics from the most recent
        # update of the learner group
        self._results = {}

        if not self.config.enable_rl_module_and_learner:
            # Create and start the learner thread.
            self._learner_thread = make_learner_thread(
                self.workers.local_worker(), self.config
            )
            self._learner_thread.start()

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # First, check, whether our learner thread is still healthy.
        if (
            not self.config.enable_rl_module_and_learner
            and not self._learner_thread.is_alive()
        ):
            raise RuntimeError("The learner thread died while training!")

        use_tree_aggregation = (
            self._aggregator_actor_manager
            and self._aggregator_actor_manager.num_healthy_actors() > 0
        )

        # Get sampled SampleBatches from our workers (by ray references if we use
        # tree-aggregation).
        unprocessed_sample_batches = self.get_samples_from_workers(
            return_object_refs=use_tree_aggregation,
        )
        # Tag workers that actually produced ready sample batches this iteration.
        # Those workers will have to get updated at the end of the iteration.
        workers_that_need_updates = {
            worker_id for worker_id, _ in unprocessed_sample_batches
        }

        # Send the collected batches (still object refs) to our aggregation workers.
        if use_tree_aggregation:
            batches = self.process_experiences_tree_aggregation(
                unprocessed_sample_batches
            )
        # Resolve collected batches here on local process (using the mixin buffer).
        else:
            batches = self.process_experiences_directly(unprocessed_sample_batches)

        # Increase sampling counters now that we have the actual SampleBatches on
        # the local process (and can measure their sizes).
        for batch in batches:
            self._counters[NUM_ENV_STEPS_SAMPLED] += batch.count
            self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
        # Concatenate single batches into batches of size `train_batch_size`.
        self.concatenate_batches_and_pre_queue(batches)
        # Using the Learner API. Call `update()` on our LearnerGroup object with
        # all collected batches.
        if self.config.enable_rl_module_and_learner:
            train_results = self.learn_on_processed_samples()
            module_ids_to_update = set(train_results.keys()) - {ALL_MODULES}
            additional_results = self.learner_group.additional_update(
                module_ids_to_update=module_ids_to_update,
                timestep=self._counters[
                    NUM_ENV_STEPS_TRAINED
                    if self.config.count_steps_by == "env_steps"
                    else NUM_AGENT_STEPS_TRAINED
                ],
                # TODO (sven): Feels hacked, but solves the problem of algos inheriting
                #  from IMPALA (like APPO). In the old stack, we didn't have this
                #  problem b/c IMPALA didn't need to call any additional update methods
                #  as the entropy- and lr-schedules were handled by
                #  `Policy.on_global_var_update()`.
                **self._get_additional_update_kwargs(train_results),
            )
            for key, res in additional_results.items():
                if key in train_results:
                    train_results[key].update(res)
        else:
            # Move train batches (of size `train_batch_size`) onto learner queue.
            self.place_processed_samples_on_learner_thread_queue()
            # Extract most recent train results from learner thread.
            train_results = self.process_trained_results()

        # Sync worker weights (only those policies that were actually updated).
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            if self.config.enable_rl_module_and_learner:
                if train_results:
                    pids = list(set(train_results.keys()) - {ALL_MODULES})
                    self.update_workers_from_learner_group(
                        workers_that_need_updates=workers_that_need_updates,
                        policy_ids=pids,
                    )
            else:
                pids = list(train_results.keys())
                self.update_workers_if_necessary(
                    workers_that_need_updates=workers_that_need_updates,
                    policy_ids=pids,
                )

        # With a training step done, try to bring any aggregators back to life
        # if necessary.
        # Aggregation workers are stateless, so we do not need to restore any
        # state here.
        if self._aggregator_actor_manager:
            self._aggregator_actor_manager.probe_unhealthy_actors(
                timeout_seconds=self.config.env_runner_health_probe_timeout_s,
            )

        if self.config.enable_rl_module_and_learner:
            if train_results:
                # Store the most recent result and return it if no new result is
                # available. This keeps backwards compatibility with the old
                # training stack / results reporting stack. This is necessary
                # any time we develop an asynchronous algorithm.
                self._results = train_results
            return self._results
        else:
            return train_results

    @classmethod
    @override(Algorithm)
    def default_resource_request(
        cls,
        config: Union[AlgorithmConfig, PartialAlgorithmConfigDict],
    ):
        if isinstance(config, AlgorithmConfig):
            cf: ImpalaConfig = config
        else:
            cf: ImpalaConfig = cls.get_default_config().update_from_dict(config)

        eval_config = cf.get_evaluation_config_object()

        bundles = (
            [
                {
                    # Driver + Aggregation Workers:
                    # Force to be on same node to maximize data bandwidth
                    # between aggregation workers and the learner (driver).
                    # Aggregation workers tree-aggregate experiences collected
                    # from RolloutWorkers (n rollout workers map to m
                    # aggregation workers, where m < n) and always use 1 CPU
                    # each.
                    "CPU": cf.num_cpus_for_local_worker + cf.num_aggregation_workers,
                    "GPU": 0 if cf._fake_gpus else cf.num_gpus,
                }
            ]
            + [
                {
                    # RolloutWorkers.
                    "CPU": cf.num_cpus_per_worker,
                    "GPU": cf.num_gpus_per_worker,
                    **cf.custom_resources_per_worker,
                }
                for _ in range(cf.num_env_runners)
            ]
            + (
                [
                    {
                        # Evaluation (remote) workers.
                        # Note: The local eval worker is located on the driver
                        # CPU or not even created iff >0 eval workers.
                        "CPU": eval_config.num_cpus_per_worker,
                        "GPU": eval_config.num_gpus_per_worker,
                        **eval_config.custom_resources_per_worker,
                    }
                    for _ in range(cf.evaluation_num_env_runners)
                ]
                if cf.evaluation_interval
                else []
            )
        )
        # TODO (avnishn): Remove this once we have a way to extend placement group
        #  factories.
        if cf.enable_rl_module_and_learner:
            # Resources for the Algorithm.
            learner_bundles = cls._get_learner_bundles(cf)

            bundles += learner_bundles

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=bundles,
            strategy=cf.placement_strategy,
        )

    def concatenate_batches_and_pre_queue(self, batches: List[SampleBatch]) -> None:
        """Concatenate batches that are being returned from rollout workers

        Args:
            batches: List of batches of experiences from EnvRunners.
        """

        def aggregate_into_larger_batch():
            if (
                sum(b.count for b in self.batch_being_built)
                >= self.config.total_train_batch_size
            ):
                batch_to_add = concat_samples(self.batch_being_built)
                self.batches_to_place_on_learner.append(batch_to_add)
                self.batch_being_built = []

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
                and self.config.enable_connectors
                and self.config.recreate_failed_env_runners
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

            self.batch_being_built.append(batch)
            aggregate_into_larger_batch()

    def get_samples_from_workers(
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
            # self.config.create_env_on_local_worker == True), then sample from the
            # local worker. Otherwise just return an empty list.
            if self.workers.num_healthy_remote_workers() > 0:
                # Perform asynchronous sampling on all (remote) rollout workers.
                self.workers.foreach_worker_async(lambda worker: worker.sample())
                sample_batches: List[
                    Tuple[int, ObjectRef]
                ] = self.workers.fetch_ready_async_reqs(
                    timeout_seconds=self.config.timeout_s_sampler_manager,
                    return_obj_refs=return_object_refs,
                )
            elif (
                self.workers.local_worker()
                and self.workers.local_worker().async_env is not None
            ):
                # Sampling from the local worker
                sample_batch = self.workers.local_worker().sample()
                if return_object_refs:
                    sample_batch = ray.put(sample_batch)
                sample_batches = [(0, sample_batch)]
            else:
                # Not much we can do. Return empty list and wait.
                sample_batches = []

        return sample_batches

    def learn_on_processed_samples(self) -> ResultDict:
        """Update the learner group with the latest batch of processed samples.

        Returns:
            Aggregated results from the learner group after an update is completed.

        """
        # There are batches on the queue -> Send them all to the learner group.
        if self.batches_to_place_on_learner:
            batches = self.batches_to_place_on_learner[:]
            self.batches_to_place_on_learner.clear()
            # If there are no learner workers and learning is directly on the driver
            # Then we can't do async updates, so we need to block.
            async_update = self.config.num_learner_workers > 0
            results = []
            for batch in batches:
                result = self.learner_group.update_from_batch(
                    batch=batch,
                    async_update=async_update,
                    num_iters=self.config.num_sgd_iter,
                    minibatch_size=self.config.minibatch_size,
                )
                if not async_update:
                    results = [result]

                for r in results:
                    self._counters[NUM_ENV_STEPS_TRAINED] += r[ALL_MODULES].pop(
                        NUM_ENV_STEPS_TRAINED
                    )
                    self._counters[NUM_AGENT_STEPS_TRAINED] += r[ALL_MODULES].pop(
                        NUM_MODULE_STEPS_TRAINED
                    )

            self._counters.update(self.learner_group.get_stats())
            # If there are results, reduce-mean over each individual value and return.
            if results:
                return tree.map_structure(lambda *x: np.mean(x), *results)

        # Nothing on the queue -> Don't send requests to learner group
        # or no results ready (from previous `self.learner_group.update_from_batch()`
        # calls) for reducing.
        return {}

    def place_processed_samples_on_learner_thread_queue(self) -> None:
        """Place processed samples on the learner queue for training.

        NOTE: This method is called if self.config.enable_rl_module_and_learner is
        False.
        """
        for i, batch in enumerate(self.batches_to_place_on_learner):
            try:
                self._learner_thread.inqueue.put(
                    batch,
                    # Setting block = True for the very last item in our list prevents
                    # the learner thread, this main thread, and the GPU loader threads
                    # from thrashing when there are more samples than the learner can
                    # reasonably process.
                    # see https://github.com/ray-project/ray/pull/26581#issuecomment-1187877674  # noqa
                    block=i == len(self.batches_to_place_on_learner) - 1,
                )
                self._counters["num_samples_added_to_queue"] += (
                    batch.agent_steps()
                    if self.config.count_steps_by == "agent_steps"
                    else batch.count
                )
            except queue.Full:
                self._counters["num_times_learner_queue_full"] += 1

        self.batches_to_place_on_learner.clear()

    def process_trained_results(self) -> ResultDict:
        """Process training results that are outputed by the learner thread.

        NOTE: This method is called if self.config.enable_rl_module_and_learner is
        False.

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

    def process_experiences_directly(
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
            ), "process_experiences_directly can not handle ObjectRefs. "
            batch = batch.decompress_if_needed()
            self.local_mixin_buffer.add(batch)
            batch = self.local_mixin_buffer.replay(_ALL_POLICIES)
            if batch:
                processed_batches.append(batch)

        return processed_batches

    def process_experiences_tree_aggregation(
        self,
        worker_to_sample_batches_refs: List[Tuple[int, ObjectRef]],
    ) -> List[SampleBatchType]:
        """Process sample batches using tree aggregation workers.

        Args:
            worker_to_sample_batches_refs: List of (worker_id, sample_batch_ref)

        NOTE: This will provide speedup when sample batches have been compressed,
        and the decompression can happen on the aggregation workers in parallel to
        the training.

        Returns:
            Batches that have been processed by the mixin buffers on the aggregation
            workers.

        """

        def _process_episodes(actor, batch):
            return actor.process_episodes(ray.get(batch))

        for _, batch in worker_to_sample_batches_refs:
            assert isinstance(batch, ObjectRef), (
                "For efficiency, process_experiences_tree_aggregation should "
                f"be given ObjectRefs instead of {type(batch)}."
            )
            # Randomly pick an aggregation worker to process this batch.
            aggregator_id = random.choice(
                self._aggregator_actor_manager.healthy_actor_ids()
            )
            calls_placed = self._aggregator_actor_manager.foreach_actor_async(
                partial(_process_episodes, batch=batch),
                remote_actor_ids=[aggregator_id],
            )
            if calls_placed <= 0:
                self._counters["num_times_no_aggregation_worker_available"] += 1

        waiting_processed_sample_batches: RemoteCallResults = (
            self._aggregator_actor_manager.fetch_ready_async_reqs(
                timeout_seconds=self._timeout_s_aggregator_manager,
            )
        )
        _handle_remote_call_result_errors(
            waiting_processed_sample_batches,
            self.config.ignore_env_runner_failures,
        )

        return [b.get() for b in waiting_processed_sample_batches.ignore_errors()]

    def update_workers_from_learner_group(
        self,
        workers_that_need_updates: Set[int],
        policy_ids: Optional[List[PolicyID]] = None,
    ):
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
        # Only need to update workers if there are remote workers.
        self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] += 1
        if (
            self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS]
            >= self.config.broadcast_interval
            and workers_that_need_updates
        ):
            self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] = 0
            self._counters[NUM_SYNCH_WORKER_WEIGHTS] += 1
            weights = self.learner_group.get_weights(policy_ids)
            if self.config.num_env_runners == 0:
                worker = self.workers.local_worker()
                worker.set_weights(weights)
            else:
                weights_ref = ray.put(weights)
                self.workers.foreach_worker(
                    func=lambda w: w.set_weights(ray.get(weights_ref)),
                    local_worker=False,
                    remote_worker_ids=list(workers_that_need_updates),
                    timeout_seconds=0,  # Don't wait for the workers to finish.
                )
                # If we have a local worker that we sample from in addition to
                # our remote workers, we need to update its weights as well.
                if self.config.create_env_on_local_worker:
                    self.workers.local_worker().set_weights(weights)

    def update_workers_if_necessary(
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
        local_worker = self.workers.local_worker()
        # Update global vars of the local worker.
        if self.config.policy_states_are_swappable:
            local_worker.lock()
        global_vars = {
            "timestep": self._counters[NUM_AGENT_STEPS_TRAINED],
            "num_grad_updates_per_policy": {
                pid: local_worker.policy_map[pid].num_grad_updates
                for pid in policy_ids or []
            },
        }
        local_worker.set_global_vars(global_vars, policy_ids=policy_ids)
        if self.config.policy_states_are_swappable:
            local_worker.unlock()

        # Only need to update workers if there are remote workers.
        self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] += 1
        if (
            self.workers.num_remote_workers() > 0
            and self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS]
            >= self.config.broadcast_interval
            and workers_that_need_updates
        ):
            if self.config.policy_states_are_swappable:
                local_worker.lock()
            weights = local_worker.get_weights(policy_ids)
            if self.config.policy_states_are_swappable:
                local_worker.unlock()
            weights_ref = ray.put(weights)

            self._learner_thread.policy_ids_updated.clear()
            self._counters[NUM_TRAINING_STEP_CALLS_SINCE_LAST_SYNCH_WORKER_WEIGHTS] = 0
            self._counters[NUM_SYNCH_WORKER_WEIGHTS] += 1
            self.workers.foreach_worker(
                func=lambda w: w.set_weights(ray.get(weights_ref), global_vars),
                local_worker=False,
                remote_worker_ids=list(workers_that_need_updates),
                timeout_seconds=0,  # Don't wait for the workers to finish.
            )

    def _get_additional_update_kwargs(self, train_results: dict) -> dict:
        """Returns the kwargs to `LearnerGroup.additional_update()`.

        Should be overridden by subclasses to specify wanted/needed kwargs for
        their own implementation of `Learner.additional_update_for_module()`.
        """
        return {}

    @override(Algorithm)
    def _compile_iteration_results_old_and_hybrid_api_stacks(self, *args, **kwargs):
        result = super()._compile_iteration_results_old_and_hybrid_api_stacks(
            *args, **kwargs
        )
        if not self.config.enable_rl_module_and_learner:
            result = self._learner_thread.add_learner_metrics(
                result, overwrite_learner_info=False
            )
        return result


@ray.remote(num_cpus=0, max_restarts=-1)
class AggregatorWorker(FaultAwareApply):
    """A worker for doing tree aggregation of collected episodes"""

    def __init__(self, config: AlgorithmConfig):
        self.config = config
        self._mixin_buffer = MixInMultiAgentReplayBuffer(
            capacity=(
                self.config.replay_buffer_num_slots
                if self.config.replay_buffer_num_slots > 0
                else 1
            ),
            replay_ratio=self.config.replay_ratio,
            replay_mode=ReplayMode.LOCKSTEP,
        )

    def process_episodes(self, batch: SampleBatchType) -> SampleBatchType:
        batch = batch.decompress_if_needed()
        self._mixin_buffer.add(batch)
        processed_batches = self._mixin_buffer.replay(_ALL_POLICIES)
        return processed_batches

    def get_host(self) -> str:
        return platform.node()
