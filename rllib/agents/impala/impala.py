import copy
import logging
import platform
import random
from collections import defaultdict

import queue
from typing import Optional, Type, List, Dict, Union, DefaultDict, Set, Callable, Any

import ray
from ray.actor import ActorHandle
from ray.rllib import SampleBatch
from ray.rllib.agents.impala.vtrace_tf_policy import VTraceTFPolicy
from ray.rllib.agents.trainer import Trainer, TrainerConfig
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.multi_gpu_learner_thread import MultiGPULearnerThread
from ray.rllib.execution.parallel_requests import (
    asynchronous_parallel_requests,
    wait_asynchronous_requests,
)
from ray.rllib.execution.tree_agg import gather_experiences_tree_aggregation
from ray.rllib.execution.common import (
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_global_vars,
    _get_shared_metrics,
)
from ray.rllib.execution.replay_ops import MixInReplay
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
)

# from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
    SampleBatchType,
    T,
)
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


class ImpalaConfig(TrainerConfig):
    """Defines a configuration class from which an ImpalaTrainer can be built.

    Example:
        >>> from ray.rllib.agents.impala import ImpalaConfig
        >>> config = ImpalaConfig().training(lr=0.0003, train_batch_size=512)\
        ...     .resources(num_gpus=4)\
        ...     .rollouts(num_rollout_workers=64)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> from ray.rllib.agents.impala import ImpalaConfig
        >>> from ray import tune
        >>> config = ImpalaConfig()
        >>> # Print out some default values.
        >>> print(config.vtrace)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.0001, 0.0003]), grad_clip=20.0)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "IMPALA",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, trainer_class=None):
        """Initializes a ImpalaConfig instance."""
        super().__init__(trainer_class=trainer_class or ImpalaTrainer)

        # fmt: off
        # __sphinx_doc_begin__

        # IMPALA specific settings:
        self.vtrace = True
        self.vtrace_clip_rho_threshold = 1.0
        self.vtrace_clip_pg_rho_threshold = 1.0
        self.vtrace_drop_last_ts = True
        self.num_multi_gpu_tower_stacks = 1
        self.minibatch_buffer_size = 1
        self.num_sgd_iter = 1
        self.replay_proportion = 0.0
        self.replay_ratio = ((1 / self.replay_proportion)
                             if self.replay_proportion > 0 else 0.0)
        self.replay_buffer_num_slots = 0
        self.learner_queue_size = 16
        self.learner_queue_timeout = 300
        self.max_sample_requests_in_flight_per_worker = 2
        self.aggregator_wait_timeout = 0.03
        self.sample_wait_timeout = 0.03
        self.broadcast_interval = 1
        self.num_aggregation_workers = 0
        self.grad_clip = 40.0
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

        # Override some of TrainerConfig's default values with ARS-specific values.
        self.rollout_fragment_length = 50
        self.train_batch_size = 500
        self.num_workers = 2
        self.num_gpus = 1
        self.lr = 0.0005
        self.min_time_s_per_reporting = 10
        # __sphinx_doc_end__
        # fmt: on

        # Deprecated value.
        self.num_data_loader_buffers = DEPRECATED_VALUE

    @override(TrainerConfig)
    def training(
        self,
        *,
        vtrace: Optional[bool] = None,
        vtrace_clip_rho_threshold: Optional[float] = None,
        vtrace_clip_pg_rho_threshold: Optional[float] = None,
        vtrace_drop_last_ts: Optional[bool] = None,
        num_multi_gpu_tower_stacks: Optional[int] = None,
        minibatch_buffer_size: Optional[int] = None,
        num_sgd_iter: Optional[int] = None,
        replay_proportion: Optional[float] = None,
        replay_buffer_num_slots: Optional[int] = None,
        learner_queue_size: Optional[int] = None,
        learner_queue_timeout: Optional[float] = None,
        max_sample_requests_in_flight_per_worker: Optional[int] = None,
        aggregator_wait_timeout: Optional[float] = None,
        sample_wait_timeout: Optional[float] = None,
        broadcast_interval: Optional[int] = None,
        num_aggregation_workers: Optional[int] = None,
        grad_clip: Optional[float] = None,
        opt_type: Optional[str] = None,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        decay: Optional[float] = None,
        momentum: Optional[float] = None,
        epsilon: Optional[float] = None,
        vf_loss_coeff: Optional[float] = None,
        entropy_coeff: Optional[float] = None,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None,
        _separate_vf_optimizer: Optional[bool] = None,
        _lr_vf: Optional[float] = None,
        after_train_step: Optional[Callable[[dict], None]] = None,
        **kwargs,
    ) -> "ImpalaConfig":
        """Sets the training related configuration.

        Args:
            vtrace: V-trace params (see vtrace_tf/torch.py).
            vtrace_clip_rho_threshold:
            vtrace_clip_pg_rho_threshold:
            vtrace_drop_last_ts: If True, drop the last timestep for the vtrace
                calculations, such that all data goes into the calculations as [B x T-1]
                (+ the bootstrap value). This is the default and legacy RLlib behavior,
                however, could potentially have a destabilizing effect on learning,
                especially in sparse reward or reward-at-goal environments.
                False for not dropping the last timestep.
                System params.
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
            num_sgd_iter: Number of passes to make over each train batch.
            replay_proportion: Set >0 to enable experience replay. Saved samples will
                be replayed with a p:1 proportion to new data samples. Used in the
                execution plan API.
            replay_buffer_num_slots: Number of sample batches to store for replay.
                The number of transitions saved total will be
                (replay_buffer_num_slots * rollout_fragment_length).
            learner_queue_size: Max queue size for train batches feeding into the
                learner.
            learner_queue_timeout: Wait for train batches to be available in minibatch
                buffer queue this many seconds. This may need to be increased e.g. when
                training with a slow environment.
            max_sample_requests_in_flight_per_worker: Level of queuing for sampling
                and replay aggregator operations (if using aggregator workers).
            aggregator_wait_timeout: Amount of time to block and wait on pending calls
                to replay aggregator workers.
            sample_wait_timeout: Amount of time to block and wait on pending calls to
                sampling workers.
            broadcast_interval: Max number of workers to broadcast one set of
                weights to.
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
                optimize the policy-and value networks.
            _lr_vf: If _separate_vf_optimizer is True, define separate learning rate
                for the value network.
            after_train_step: Callback for APPO to use to update KL, target network
                periodically. The input to the callback is the learner fetches dict.

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if vtrace is not None:
            self.vtrace = vtrace
        if vtrace_clip_rho_threshold is not None:
            self.vtrace_clip_rho_threshold = vtrace_clip_rho_threshold
        if vtrace_clip_pg_rho_threshold is not None:
            self.vtrace_clip_pg_rho_threshold = vtrace_clip_pg_rho_threshold
        if vtrace_drop_last_ts is not None:
            self.vtrace_drop_last_ts = vtrace_drop_last_ts
        if num_multi_gpu_tower_stacks is not None:
            self.num_multi_gpu_tower_stacks = num_multi_gpu_tower_stacks
        if minibatch_buffer_size is not None:
            self.minibatch_buffer_size = minibatch_buffer_size
        if num_sgd_iter is not None:
            self.num_sgd_iter = num_sgd_iter
        if replay_proportion is not None:
            self.replay_proportion = replay_proportion
        if replay_buffer_num_slots is not None:
            self.replay_buffer_num_slots = replay_buffer_num_slots
        if learner_queue_size is not None:
            self.learner_queue_size = learner_queue_size
        if learner_queue_timeout is not None:
            self.learner_queue_timeout = learner_queue_timeout
        if max_sample_requests_in_flight_per_worker is not None:
            self.max_sample_requests_in_flight_per_worker = (
                max_sample_requests_in_flight_per_worker
            )
        if aggregator_wait_timeout is not None:
            self.aggregator_wait_timeout = aggregator_wait_timeout
        if sample_wait_timeout is not None:
            self.sample_wait_timeout = sample_wait_timeout
        if broadcast_interval is not None:
            self.broadcast_interval = broadcast_interval
        if num_aggregation_workers is not None:
            self.num_aggregation_workers = num_aggregation_workers
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if opt_type is not None:
            self.opt_type = opt_type
        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if decay is not None:
            self.decay = decay
        if momentum is not None:
            self.momentum = momentum
        if epsilon is not None:
            self.epsilon = epsilon
        if vf_loss_coeff is not None:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not None:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not None:
            self.entropy_coeff_schedule = entropy_coeff_schedule
        if _separate_vf_optimizer is not None:
            self._separate_vf_optimizer = _separate_vf_optimizer
        if _lr_vf is not None:
            self._lr_vf = _lr_vf
        if after_train_step is not None:
            self.after_train_step = after_train_step

        return self


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


def gather_experiences_directly(workers, config):
    rollouts = ParallelRollouts(
        workers,
        mode="async",
        num_async=config["max_sample_requests_in_flight_per_worker"],
    )

    # Augment with replay and concat to desired train batch size.
    train_batches = (
        rollouts.for_each(lambda batch: batch.decompress_if_needed())
        .for_each(
            MixInReplay(
                num_slots=config["replay_buffer_num_slots"],
                replay_proportion=config["replay_proportion"],
            )
        )
        .flatten()
        .combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"]["count_steps_by"],
            )
        )
    )

    return train_batches


# Update worker weights as they finish generating experiences.
class BroadcastUpdateLearnerWeights:
    def __init__(self, learner_thread, workers, broadcast_interval):
        self.learner_thread = learner_thread
        self.steps_since_broadcast = 0
        self.broadcast_interval = broadcast_interval
        self.workers = workers
        self.weights = workers.local_worker().get_weights()

    def __call__(self, item):
        actor, batch = item
        self.steps_since_broadcast += 1
        if (
            self.steps_since_broadcast >= self.broadcast_interval
            and self.learner_thread.weights_updated
        ):
            self.weights = ray.put(self.workers.local_worker().get_weights())
            self.steps_since_broadcast = 0
            self.learner_thread.weights_updated = False
            # Update metrics.
            metrics = _get_shared_metrics()
            metrics.counters["num_weight_broadcasts"] += 1
        actor.set_weights.remote(self.weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())


class ImpalaTrainer(Trainer):
    """Importance weighted actor/learner architecture (IMPALA) Trainer

    == Overview of data flow in IMPALA ==
    1. Policy evaluation in parallel across `num_workers` actors produces
       batches of size `rollout_fragment_length * num_envs_per_worker`.
    2. If enabled, the replay buffer stores and produces batches of size
       `rollout_fragment_length * num_envs_per_worker`.
    3. If enabled, the minibatch ring buffer stores and replays batches of
       size `train_batch_size` up to `num_sgd_iter` times per batch.
    4. The learner thread executes data parallel SGD across `num_gpus` GPUs
       on batches of size `train_batch_size`.
    """

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return ImpalaConfig().to_dict()

    @override(Trainer)
    def get_default_policy_class(
        self, config: PartialTrainerConfigDict
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            if config["vtrace"]:
                from ray.rllib.agents.impala.vtrace_torch_policy import (
                    VTraceTorchPolicy,
                )

                return VTraceTorchPolicy
            else:
                from ray.rllib.agents.a3c.a3c_torch_policy import A3CTorchPolicy

                return A3CTorchPolicy
        else:
            if config["vtrace"]:
                return VTraceTFPolicy
            else:
                from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy

                return A3CTFPolicy

    @override(Trainer)
    def validate_config(self, config):
        # Call the super class' validation method first.
        super().validate_config(config)

        # Check the IMPALA specific config.

        if config["num_data_loader_buffers"] != DEPRECATED_VALUE:
            deprecation_warning(
                "num_data_loader_buffers", "num_multi_gpu_tower_stacks", error=False
            )
            config["num_multi_gpu_tower_stacks"] = config["num_data_loader_buffers"]

        if config["entropy_coeff"] < 0.0:
            raise ValueError("`entropy_coeff` must be >= 0.0!")

        # Check whether worker to aggregation-worker ratio makes sense.
        if config["num_aggregation_workers"] > config["num_workers"]:
            raise ValueError(
                "`num_aggregation_workers` must be smaller than or equal "
                "`num_workers`! Aggregation makes no sense otherwise."
            )
        elif config["num_aggregation_workers"] > config["num_workers"] / 2:
            logger.warning(
                "`num_aggregation_workers` should be significantly smaller "
                "than `num_workers`! Try setting it to 0.5*`num_workers` or "
                "less."
            )

        # If two separate optimizers/loss terms used for tf, must also set
        # `_tf_policy_handles_more_than_one_loss` to True.
        if config["_separate_vf_optimizer"] is True:
            # Only supported to tf so far.
            # TODO(sven): Need to change APPO|IMPALATorchPolicies (and the
            #  models to return separate sets of weights in order to create
            #  the different torch optimizers).
            if config["framework"] not in ["tf", "tf2", "tfe"]:
                raise ValueError(
                    "`_separate_vf_optimizer` only supported to tf so far!"
                )
            if config["_tf_policy_handles_more_than_one_loss"] is False:
                logger.warning(
                    "`_tf_policy_handles_more_than_one_loss` must be set to "
                    "True, for TFPolicy to support more than one loss "
                    "term/optimizer! Auto-setting it to True."
                )
                config["_tf_policy_handles_more_than_one_loss"] = True

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)
        self.remote_sampling_requests_in_flight: DefaultDict[
            ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)

        if self.config["_disable_execution_plan_api"]:
            # Setup after_train_step callback.
            self._after_train_step = lambda *a, **k: None
            if self.config["after_train_step"]:
                self._after_train_step = self.config["after_train_step"](
                    self.workers, self.config
                )

            # Create extra aggregation workers and assign each rollout worker to
            # one of them.
            self.batches_to_place_on_learner = []
            self.batch_being_built = []
            if self.config["num_aggregation_workers"] > 0:
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
                            self.config["num_aggregation_workers"],
                        )
                    ],
                    node=localhost,
                )
                self.aggregator_workers = [
                    actor for actor_groups in all_co_located for actor in actor_groups
                ]
                self.remote_aggregator_requests_in_flight: DefaultDict[
                    ActorHandle, Set[ray.ObjectRef]
                ] = defaultdict(set)

            else:
                # Create our local mixin buffer if the num of aggregation workers is 0.
                self.local_mixin_buffer = MixInMultiAgentReplayBuffer(
                    capacity=(
                        self.config["replay_buffer_num_slots"]
                        if self.config["replay_buffer_num_slots"] > 0
                        else 1
                    ),
                    replay_ratio=self.config["replay_ratio"],
                )

            # Create and start the learner thread.
            self._learner_thread = make_learner_thread(
                self.workers.local_worker(), self.config
            )
            self._learner_thread.start()
            self.workers_that_need_updates = set()

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        unprocessed_sample_batches = self.get_samples_from_workers()

        self.workers_that_need_updates |= unprocessed_sample_batches.keys()

        if self.config["num_aggregation_workers"] > 0:
            batch = self.process_experiences_tree_aggregation(
                unprocessed_sample_batches
            )
        else:
            batch = self.process_experiences_directly(unprocessed_sample_batches)

        self.concatenate_batches_and_pre_queue(batch)
        self.place_processed_samples_on_learner_queue()
        learner_results = self.process_trained_results()

        self.update_workers_if_necessary()

        # Callback for APPO to use to update KL, target network periodically.
        # The input to the callback is the learner fetches dict.
        self._after_train_step(learner_results)

        return learner_results

    @staticmethod
    @override(Trainer)
    def execution_plan(workers, config, **kwargs):
        assert (
            len(kwargs) == 0
        ), "IMPALA execution_plan does NOT take any additional parameters"

        if config["num_aggregation_workers"] > 0:
            train_batches = gather_experiences_tree_aggregation(workers, config)
        else:
            train_batches = gather_experiences_directly(workers, config)

        # Start the learner thread.
        learner_thread = make_learner_thread(workers.local_worker(), config)
        learner_thread.start()

        # This sub-flow sends experiences to the learner.
        enqueue_op = train_batches.for_each(Enqueue(learner_thread.inqueue))
        # Only need to update workers if there are remote workers.
        if workers.remote_workers():
            enqueue_op = enqueue_op.zip_with_source_actor().for_each(
                BroadcastUpdateLearnerWeights(
                    learner_thread,
                    workers,
                    broadcast_interval=config["broadcast_interval"],
                )
            )

        def record_steps_trained(item):
            count, fetches = item
            metrics = _get_shared_metrics()
            # Manually update the steps trained counter since the learner
            # thread is executing outside the pipeline.
            metrics.counters[STEPS_TRAINED_THIS_ITER_COUNTER] = count
            metrics.counters[STEPS_TRAINED_COUNTER] += count
            return item

        # This sub-flow updates the steps trained counter based on learner
        # output.
        dequeue_op = Dequeue(
            learner_thread.outqueue, check=learner_thread.is_alive
        ).for_each(record_steps_trained)

        merged_op = Concurrently(
            [enqueue_op, dequeue_op], mode="async", output_indexes=[1]
        )

        # Callback for APPO to use to update KL, target network periodically.
        # The input to the callback is the learner fetches dict.
        if config["after_train_step"]:
            merged_op = merged_op.for_each(lambda t: t[1]).for_each(
                config["after_train_step"](workers, config)
            )

        return StandardMetricsReporting(merged_op, workers, config).for_each(
            learner_thread.add_learner_metrics
        )

    @classmethod
    @override(Trainer)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)

        eval_config = cf["evaluation_config"]

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[
                {
                    # Driver + Aggregation Workers:
                    # Force to be on same node to maximize data bandwidth
                    # between aggregation workers and the learner (driver).
                    # Aggregation workers tree-aggregate experiences collected
                    # from RolloutWorkers (n rollout workers map to m
                    # aggregation workers, where m < n) and always use 1 CPU
                    # each.
                    "CPU": cf["num_cpus_for_driver"] + cf["num_aggregation_workers"],
                    "GPU": 0 if cf["_fake_gpus"] else cf["num_gpus"],
                }
            ]
            + [
                {
                    # RolloutWorkers.
                    "CPU": cf["num_cpus_per_worker"],
                    "GPU": cf["num_gpus_per_worker"],
                }
                for _ in range(cf["num_workers"])
            ]
            + (
                [
                    {
                        # Evaluation (remote) workers.
                        # Note: The local eval worker is located on the driver
                        # CPU or not even created iff >0 eval workers.
                        "CPU": eval_config.get(
                            "num_cpus_per_worker", cf["num_cpus_per_worker"]
                        ),
                        "GPU": eval_config.get(
                            "num_gpus_per_worker", cf["num_gpus_per_worker"]
                        ),
                    }
                    for _ in range(cf["evaluation_num_workers"])
                ]
                if cf["evaluation_interval"]
                else []
            ),
            strategy=config.get("placement_strategy", "PACK"),
        )

    def concatenate_batches_and_pre_queue(self, batches: List[SampleBatch]):
        """Concatenate batches that are being returned from rollout workers

        Args:
            batches: batches of experiences from rollout workers

        """

        def aggregate_into_larger_batch():
            if (
                sum(b.count for b in self.batch_being_built)
                >= self.config["train_batch_size"]
            ):
                batch_to_add = SampleBatch.concat_samples(self.batch_being_built)
                self.batches_to_place_on_learner.append(batch_to_add)
                self.batch_being_built = []

        for batch in batches:
            self.batch_being_built.append(batch)
            aggregate_into_larger_batch()

    def get_samples_from_workers(self) -> Dict[ActorHandle, List[SampleBatch]]:
        # Perform asynchronous sampling on all (remote) rollout workers.
        if self.workers.remote_workers():
            sample_batches: Dict[
                ActorHandle, List[ObjectRef]
            ] = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
                actors=self.workers.remote_workers(),
                ray_wait_timeout_s=self.config["sample_wait_timeout"],
                max_remote_requests_in_flight_per_actor=self.config[
                    "max_sample_requests_in_flight_per_worker"
                ],
                return_result_obj_ref_ids=True,
            )
        else:
            # only sampling on the local worker
            sample_batches = {
                self.workers.local_worker(): [self.workers.local_worker().sample()]
            }
        return sample_batches

    def place_processed_samples_on_learner_queue(self) -> None:
        self._counters["num_samples_added_to_queue"] = 0

        while self.batches_to_place_on_learner:
            batch = self.batches_to_place_on_learner[0]
            try:
                self._learner_thread.inqueue.put(batch, block=False)
                self.batches_to_place_on_learner.pop(0)
                self._counters[NUM_ENV_STEPS_SAMPLED] += batch.count
                self._counters[NUM_AGENT_STEPS_SAMPLED] += batch.agent_steps()
                self._counters["num_samples_added_to_queue"] = batch.count
            except queue.Full:
                self._counters["num_times_learner_queue_full"] += 1

    def process_trained_results(self) -> ResultDict:
        # Get learner outputs/stats from output queue.
        learner_infos = []
        num_agent_steps_trained = 0

        for _ in range(self._learner_thread.outqueue.qsize()):
            if self._learner_thread.is_alive():
                (
                    num_trained_samples,
                    learner_results,
                ) = self._learner_thread.outqueue.get(timeout=0.001)
                num_agent_steps_trained += num_trained_samples
                if learner_results:
                    learner_infos.append(learner_results)
            else:
                raise RuntimeError("The learner thread died in while training")
        learner_info = copy.deepcopy(self._learner_thread.learner_info)

        # Update the steps trained counters.
        self._counters[STEPS_TRAINED_THIS_ITER_COUNTER] = num_agent_steps_trained
        self._counters[NUM_AGENT_STEPS_TRAINED] += num_agent_steps_trained

        return learner_info

    def process_experiences_directly(
        self, actor_to_sample_batches_refs: Dict[ActorHandle, List[ObjectRef]]
    ) -> Union[SampleBatchType, None]:
        processed_batches = []
        batches = [
            sample_batch_ref
            for refs_batch in actor_to_sample_batches_refs.values()
            for sample_batch_ref in refs_batch
        ]
        if not batches:
            return processed_batches
        if batches and isinstance(batches[0], ray.ObjectRef):
            batches = ray.get(batches)
        for batch in batches:
            batch = batch.decompress_if_needed()
            self.local_mixin_buffer.add_batch(batch)
            batch = self.local_mixin_buffer.replay()
            if batch:
                processed_batches.append(batch)
        return processed_batches

    def process_experiences_tree_aggregation(
        self, actor_to_sample_batches_refs: Dict[ActorHandle, List[ObjectRef]]
    ) -> Union[SampleBatchType, None]:
        batches = [
            sample_batch_ref
            for refs_batch in actor_to_sample_batches_refs.values()
            for sample_batch_ref in refs_batch
        ]
        ready_processed_batches = []
        for batch in batches:
            aggregator = random.choice(self.aggregator_workers)
            processed_sample_batches: Dict[
                ActorHandle, List[ObjectRef]
            ] = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_aggregator_requests_in_flight,
                actors=[aggregator],
                remote_fn=lambda actor, b: actor.process_episodes(b),
                remote_kwargs=[{"b": batch}],
                ray_wait_timeout_s=self.config["aggregator_wait_timeout"],
                max_remote_requests_in_flight_per_actor=float("inf"),
            )
            for ready_sub_batches in processed_sample_batches.values():
                ready_processed_batches.extend(ready_sub_batches)

        waiting_processed_sample_batches: Dict[
            ActorHandle, List[ObjectRef]
        ] = wait_asynchronous_requests(
            remote_requests_in_flight=self.remote_aggregator_requests_in_flight,
            ray_wait_timeout_s=self.config["aggregator_wait_timeout"],
        )
        for ready_sub_batches in waiting_processed_sample_batches.values():
            ready_processed_batches.extend(ready_sub_batches)

        return ready_processed_batches

    def update_workers_if_necessary(self) -> None:
        # Only need to update workers if there are remote workers.
        global_vars = {"timestep": self._counters[NUM_AGENT_STEPS_TRAINED]}
        self._counters["steps_since_broadcast"] += 1
        if (
            self.workers.remote_workers()
            and self._counters["steps_since_broadcast"]
            >= self.config["broadcast_interval"]
            and self.workers_that_need_updates
        ):
            weights = ray.put(self.workers.local_worker().get_weights())
            self._counters["steps_since_broadcast"] = 0
            self._learner_thread.weights_updated = False
            self._counters["num_weight_broadcasts"] += 1

            for worker in self.workers_that_need_updates:
                worker.set_weights.remote(weights, global_vars)
            self.workers_that_need_updates = set()

        # Update global vars of the local worker.
        self.workers.local_worker().set_global_vars(global_vars)

    @override(Trainer)
    def _compile_step_results(self, *, step_ctx, step_attempt_results=None):
        result = super()._compile_step_results(
            step_ctx=step_ctx, step_attempt_results=step_attempt_results
        )
        result = self._learner_thread.add_learner_metrics(
            result, overwrite_learner_info=False
        )
        return result


@ray.remote(num_cpus=0)
class AggregatorWorker:
    """A worker for doing tree aggregation of collected episodes"""

    def __init__(self, config: TrainerConfigDict):
        self.config = config
        self._mixin_buffer = MixInMultiAgentReplayBuffer(
            capacity=(
                self.config["replay_buffer_num_slots"]
                if self.config["replay_buffer_num_slots"] > 0
                else 1
            ),
            replay_ratio=self.config["replay_ratio"],
        )

    def process_episodes(self, batch: SampleBatchType) -> SampleBatchType:
        batch = batch.decompress_if_needed()
        self._mixin_buffer.add_batch(batch)
        processed_batches = self._mixin_buffer.replay()
        return processed_batches

    def apply(
        self,
        func: Callable[["AggregatorWorker", Optional[Any], Optional[Any]], T],
        *_args,
        **kwargs,
    ) -> T:
        """Calls the given function with this AggregatorWorker instance."""
        return func(self, *_args, **kwargs)

    def get_host(self) -> str:
        return platform.node()


# Deprecated: Use ray.rllib.agents.pg.PGConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(ImpalaConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.impala.default_config::DEFAULT_CONFIG",
        new="ray.rllib.agents.impala.impala.IMPALAConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
