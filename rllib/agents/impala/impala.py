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
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.multi_gpu_learner_thread import MultiGPULearnerThread
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
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
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, deprecation_warning
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
)

# from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.replay_buffers.multi_agent_mixin_replay_buffer import (
    MultiAgentMixInReplayBuffer,
)
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
    SampleBatchType,
    T,
)
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.types import ObjectRef

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace_tf/torch.py).
    "vtrace": True,
    "vtrace_clip_rho_threshold": 1.0,
    "vtrace_clip_pg_rho_threshold": 1.0,
    # If True, drop the last timestep for the vtrace calculations, such that
    # all data goes into the calculations as [B x T-1] (+ the bootstrap value).
    # This is the default and legacy RLlib behavior, however, could potentially
    # have a destabilizing effect on learning, especially in sparse reward
    # or reward-at-goal environments.
    # False for not dropping the last timestep.
    "vtrace_drop_last_ts": True,
    # System params.
    #
    # == Overview of data flow in IMPALA ==
    # 1. Policy evaluation in parallel across `num_workers` actors produces
    #    batches of size `rollout_fragment_length * num_envs_per_worker`.
    # 2. If enabled, the replay buffer stores and produces batches of size
    #    `rollout_fragment_length * num_envs_per_worker`.
    # 3. If enabled, the minibatch ring buffer stores and replays batches of
    #    size `train_batch_size` up to `num_sgd_iter` times per batch.
    # 4. The learner thread executes data parallel SGD across `num_gpus` GPUs
    #    on batches of size `train_batch_size`.
    #
    "rollout_fragment_length": 50,
    "train_batch_size": 500,
    "min_time_s_per_reporting": 10,
    "num_workers": 2,
    # Number of GPUs the learner should use.
    "num_gpus": 1,
    # For each stack of multi-GPU towers, how many slots should we reserve for
    # parallel data loading? Set this to >1 to load data into GPUs in
    # parallel. This will increase GPU memory usage proportionally with the
    # number of stacks.
    # Example:
    # 2 GPUs and `num_multi_gpu_tower_stacks=3`:
    # - One tower stack consists of 2 GPUs, each with a copy of the
    #   model/graph.
    # - Each of the stacks will create 3 slots for batch data on each of its
    #   GPUs, increasing memory requirements on each GPU by 3x.
    # - This enables us to preload data into these stacks while another stack
    #   is performing gradient calculations.
    "num_multi_gpu_tower_stacks": 1,
    # How many train batches should be retained for minibatching. This conf
    # only has an effect if `num_sgd_iter > 1`.
    "minibatch_buffer_size": 1,
    # Number of passes to make over each train batch.
    "num_sgd_iter": 1,
    # The ratio of old (replayed) samples over new ones.
    #
    "replay_ratio": 0.0,
    # Number of sample batches to store for replay. The number of transitions
    # saved total will be (replay_buffer_num_slots * rollout_fragment_length).
    "replay_buffer_num_slots": 0,
    # Max queue size for train batches feeding into the learner.
    "learner_queue_size": 16,
    # Wait for train batches to be available in minibatch buffer queue
    # this many seconds. This may need to be increased e.g. when training
    # with a slow environment.
    "learner_queue_timeout": 300,
    # Maximum number of still un-returned `sample()` requests per RolloutWorker.
    # B/c we are calling `sample()` in an async manner via ray.wait()
    "max_sample_requests_in_flight_per_worker": 2,
    # Max number of training iterations to wait before broadcast weights are updated
    # from the local_worker.
    "broadcast_interval": 1,
    # Use n (`num_aggregation_workers`) extra Actors for multi-level
    # aggregation of the data produced by the m RolloutWorkers
    # (`num_workers`). Note that n should be much smaller than m.
    # This can make sense if ingesting >2GB/s of samples, or if
    # the data requires decompression.
    "num_aggregation_workers": 0,

    # Learning params.
    "grad_clip": 40.0,
    # Either "adam" or "rmsprop".
    "opt_type": "adam",
    "lr": 0.0005,
    "lr_schedule": None,
    # `opt_type=rmsprop` settings.
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    # Balancing the three losses.
    "vf_loss_coeff": 0.5,
    "entropy_coeff": 0.01,
    "entropy_coeff_schedule": None,
    # Set this to true to have two separate optimizers optimize the policy-
    # and value networks.
    "_separate_vf_optimizer": False,
    # If _separate_vf_optimizer is True, define separate learning rate
    # for the value network.
    "_lr_vf": 0.0005,

    # Callback for APPO to use to update KL, target network periodically.
    # The input to the callback is the learner fetches dict.
    "after_train_step": None,

    # Use `self.training_iteration` method, instead of
    # `self.execution_plan`.
    "_disable_execution_plan_api": True,

    # DEPRECATED:
    "num_data_loader_buffers": DEPRECATED_VALUE,
    "replay_proportion": DEPRECATED_VALUE,
})
# __sphinx_doc_end__
# fmt: on


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
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

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

        if config["replay_proportion"] != DEPRECATED_VALUE:
            deprecation_warning("replay_proportion", "replay_ratio", error=False)
            # r = P / (P+1)
            # Examples:
            #  P = 2 -> 2:1 -> r=2/3 (66% replay)
            #  P = 1 -> 1:1 -> r=1/2 (50% replay)
            config["replay_ratio"] = config["replay_proportion"] / (
                config["replay_proportion"] + 1
            )

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
            # Create extra aggregation workers and assign each rollout worker to
            # one of them.
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
                # self.local_mixin_buffer = MixInMultiAgentReplayBuffer(
                #     capacity=self.config["replay_buffer_num_slots"],
                #     replay_ratio=self.config["replay_ratio"],
                # )
                self.local_mixin_buffer = MultiAgentMixInReplayBuffer(
                    capacity=(
                        self.config["replay_buffer_num_slots"]
                        if self.config["replay_buffer_num_slots"] > 0
                        else 1
                    ),
                    replay_ratio=self.config["replay_ratio"],
                    storage_unit="fragments",
                    learning_starts=0,
                )

            # Create and start the learner thread.
            self._learner_thread = make_learner_thread(
                self.workers.local_worker(), self.config
            )
            self._learner_thread.start()
            self._batch_concatenator = ConcatBatches(
                min_batch_size=self.config["train_batch_size"],
                count_steps_by=self.config["multiagent"]["count_steps_by"],
                using_iterators=False,
            )
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

        train_batch = self._batch_concatenator(batch)

        if train_batch:
            self.place_processed_samples_on_learner_queue(train_batch[0])
        learner_results = self.process_trained_results()

        self.update_workers_if_necessary()

        # Callback for APPO to use to update KL, target network periodically.
        # The input to the callback is the learner fetches dict.
        if self.config["after_train_step"]:
            self.config["after_train_step"](trainer=self, config=self.config)

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

        # output_indices=[1] -> Use output only from `dequeue_op`, not from
        # `enqueue_op`.
        merged_op = Concurrently(
            [enqueue_op, dequeue_op], mode="async", output_indexes=[1]
        )

        # Callback for APPO to use to update KL, target network periodically.
        # The input to the callback is the learner fetches dict.
        if config["after_train_step"]:
            # t[1] -> Use the [1] element of the output op of the above op
            # (dequeue_op as indicated by output_indices=[1] above), which is
            # the metrics dict placed into the output queue of the learner thread,
            # to be fed to the `after_train_step()` function.
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

    def get_samples_from_workers(self) -> Dict[ActorHandle, List[SampleBatch]]:
        # Perform asynchronous sampling on all (remote) rollout workers.
        if self.workers.remote_workers():
            sample_batches: Dict[
                ActorHandle, List[ObjectRef]
            ] = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
                actors=self.workers.remote_workers(),
                ray_wait_timeout_s=0.03,
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

    def place_processed_samples_on_learner_queue(
        self, processed_samples: SampleBatchType
    ) -> None:
        try:
            if processed_samples:
                self._learner_thread.inqueue.put(processed_samples, block=False)
                self._counters[NUM_ENV_STEPS_SAMPLED] += processed_samples.count
                self._counters[
                    NUM_AGENT_STEPS_SAMPLED
                ] += processed_samples.agent_steps()
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
        # if len(learner_infos) > 0:
        #     learner_info_builder = LearnerInfoBuilder(num_devices=1)
        #     for _learner_info in learner_infos:
        #         for policy_id in _learner_info:
        #             learner_info_builder.add_learn_on_batch_results(
        #                 _learner_info[policy_id],
        #             )
        #     learner_info = learner_info_builder.finalize()
        # else:
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
            return None
        if batches and isinstance(batches[0], ray.ObjectRef):
            batches = ray.get(batches)
        for batch in batches:
            batch = batch.decompress_if_needed()
            self.local_mixin_buffer.add(batch)
            batch = self.local_mixin_buffer.sample(self.config["train_batch_size"])
            if batch:
                processed_batches.append(batch)
        if processed_batches:
            return SampleBatch.concat_samples(processed_batches)
        return None

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
                ray_wait_timeout_s=0.0,
                max_remote_requests_in_flight_per_actor=float("inf"),
            )
            for ready_sub_batches in processed_sample_batches.values():
                ready_processed_batches.extend(ready_sub_batches)

        waiting_processed_sample_batches: Dict[
            ActorHandle, List[ObjectRef]
        ] = asynchronous_parallel_requests(
            remote_requests_in_flight=self.remote_aggregator_requests_in_flight,
            actors=self.aggregator_workers,
            remote_fn=None,
            ray_wait_timeout_s=0.005,
            max_remote_requests_in_flight_per_actor=float("inf"),
        )
        for ready_sub_batches in waiting_processed_sample_batches.values():
            ready_processed_batches.extend(ready_sub_batches)

        if ready_processed_batches:
            return SampleBatch.concat_samples(ready_processed_batches)
        return None

    def update_workers_if_necessary(self) -> None:
        # Only need to update workers if there are remote workers.
        global_vars = {"timestep": self._counters[NUM_ENV_STEPS_SAMPLED]}
        self._counters["steps_since_broadcast"] += 1
        if (
            self.workers.remote_workers()
            and self._counters["steps_since_broadcast"]
            >= self.config["broadcast_interval"]
            and self._learner_thread.weights_updated
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
        self.local_mixin_buffer = MultiAgentMixInReplayBuffer(
            capacity=(
                self.config["replay_buffer_num_slots"]
                if self.config["replay_buffer_num_slots"] > 0
                else 1
            ),
            replay_ratio=self.config["replay_ratio"],
            storage_unit="fragments",
            learning_starts=0,
        )

    def process_episodes(self, batch: SampleBatchType) -> SampleBatchType:
        processed_batches = []

        batch = batch.decompress_if_needed()
        self.local_mixin_buffer.add(batch)
        batch = self.local_mixin_buffer.sample(self.config["train_batch_size"])
        if batch:
            processed_batches.append(batch)
        if processed_batches:
            return SampleBatch.concat_samples(processed_batches)
        return None

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
