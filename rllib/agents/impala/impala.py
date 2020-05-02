import copy
import logging
import os
import random
from typing import List

import ray
from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.impala.vtrace_tf_policy import VTraceTFPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.execution.common import STEPS_TRAINED_COUNTER, \
    STEPS_SAMPLED_COUNTER, SampleBatchType
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.optimizers import AsyncSamplesOptimizer
from ray.rllib.optimizers.aso_tree_aggregator import TreeAggregator
from ray.rllib.optimizers.aso_learner import LearnerThread
from ray.rllib.optimizers.aso_multi_gpu_learner import TFMultiGPULearner
from ray.rllib.utils.actors import create_colocated
from ray.rllib.utils.annotations import override
from ray.tune.trainable import Trainable
from ray.tune.resources import Resources
from ray.util.iter import LocalIterator, ParallelIterator, \
    ParallelIteratorWorker, from_actors

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace_tf.py).
    "vtrace": True,
    "vtrace_clip_rho_threshold": 1.0,
    "vtrace_clip_pg_rho_threshold": 1.0,
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
    "min_iter_time_s": 10,
    "num_workers": 2,
    # number of GPUs the learner should use.
    "num_gpus": 1,
    # set >1 to load data into GPUs in parallel. Increases GPU memory usage
    # proportionally with the number of buffers.
    "num_data_loader_buffers": 1,
    # how many train batches should be retained for minibatching. This conf
    # only has an effect if `num_sgd_iter > 1`.
    "minibatch_buffer_size": 1,
    # number of passes to make over each train batch
    "num_sgd_iter": 1,
    # set >0 to enable experience replay. Saved samples will be replayed with
    # a p:1 proportion to new data samples.
    "replay_proportion": 0.0,
    # number of sample batches to store for replay. The number of transitions
    # saved total will be (replay_buffer_num_slots * rollout_fragment_length).
    "replay_buffer_num_slots": 0,
    # max queue size for train batches feeding into the learner
    "learner_queue_size": 16,
    # wait for train batches to be available in minibatch buffer queue
    # this many seconds. This may need to be increased e.g. when training
    # with a slow environment
    "learner_queue_timeout": 300,
    # level of queuing for sampling.
    "max_sample_requests_in_flight_per_worker": 2,
    # max number of workers to broadcast one set of weights to
    "broadcast_interval": 1,
    # use intermediate actors for multi-level aggregation. This can make sense
    # if ingesting >2GB/s of samples, or if the data requires decompression.
    "num_aggregation_workers": 0,

    # Learning params.
    "grad_clip": 40.0,
    # either "adam" or "rmsprop"
    "opt_type": "adam",
    "lr": 0.0005,
    "lr_schedule": None,
    # rmsprop considered
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    # balancing the three losses
    "vf_loss_coeff": 0.5,
    "entropy_coeff": 0.01,
    "entropy_coeff_schedule": None,
})
# __sphinx_doc_end__
# yapf: enable


def choose_policy(config):
    if config["vtrace"]:
        return VTraceTFPolicy
    else:
        return A3CTFPolicy


def validate_config(config):
    # PyTorch check.
    if config["use_pytorch"]:
        raise ValueError(
            "IMPALA does not support PyTorch yet! Use tf instead.")
    if config["entropy_coeff"] < 0:
        raise DeprecationWarning("entropy_coeff must be >= 0")


def defer_make_workers(trainer, env_creator, policy, config):
    # Defer worker creation to after the optimizer has been created.
    return trainer._make_workers(env_creator, policy, config, 0)


def make_aggregators_and_optimizer(workers, config):
    if config["num_aggregation_workers"] > 0:
        # Create co-located aggregator actors first for placement pref
        aggregators = TreeAggregator.precreate_aggregators(
            config["num_aggregation_workers"])
    else:
        aggregators = None
    workers.add_workers(config["num_workers"])

    optimizer = AsyncSamplesOptimizer(
        workers,
        lr=config["lr"],
        num_gpus=config["num_gpus"],
        rollout_fragment_length=config["rollout_fragment_length"],
        train_batch_size=config["train_batch_size"],
        replay_buffer_num_slots=config["replay_buffer_num_slots"],
        replay_proportion=config["replay_proportion"],
        num_data_loader_buffers=config["num_data_loader_buffers"],
        max_sample_requests_in_flight_per_worker=config[
            "max_sample_requests_in_flight_per_worker"],
        broadcast_interval=config["broadcast_interval"],
        num_sgd_iter=config["num_sgd_iter"],
        minibatch_buffer_size=config["minibatch_buffer_size"],
        num_aggregation_workers=config["num_aggregation_workers"],
        learner_queue_size=config["learner_queue_size"],
        learner_queue_timeout=config["learner_queue_timeout"],
        **config["optimizer"])

    if aggregators:
        # Assign the pre-created aggregators to the optimizer
        optimizer.aggregator.init(aggregators)
    return optimizer


class OverrideDefaultResourceRequest:
    @classmethod
    @override(Trainable)
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)
        return Resources(
            cpu=cf["num_cpus_for_driver"],
            gpu=cf["num_gpus"],
            memory=cf["memory"],
            object_store_memory=cf["object_store_memory"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"] +
            cf["num_aggregation_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"],
            extra_memory=cf["memory_per_worker"] * cf["num_workers"],
            extra_object_store_memory=cf["object_store_memory_per_worker"] *
            cf["num_workers"])


def make_learner_thread(local_worker, config):
    if config["num_gpus"] > 1 or config["num_data_loader_buffers"] > 1:
        logger.info(
            "Enabling multi-GPU mode, {} GPUs, {} parallel loaders".format(
                config["num_gpus"], config["num_data_loader_buffers"]))
        if config["num_data_loader_buffers"] < config["minibatch_buffer_size"]:
            raise ValueError(
                "In multi-gpu mode you must have at least as many "
                "parallel data loader buffers as minibatch buffers: "
                "{} vs {}".format(config["num_data_loader_buffers"],
                                  config["minibatch_buffer_size"]))
        learner_thread = TFMultiGPULearner(
            local_worker,
            num_gpus=config["num_gpus"],
            lr=config["lr"],
            train_batch_size=config["train_batch_size"],
            num_data_loader_buffers=config["num_data_loader_buffers"],
            minibatch_buffer_size=config["minibatch_buffer_size"],
            num_sgd_iter=config["num_sgd_iter"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"])
    else:
        learner_thread = LearnerThread(
            local_worker,
            minibatch_buffer_size=config["minibatch_buffer_size"],
            num_sgd_iter=config["num_sgd_iter"],
            learner_queue_size=config["learner_queue_size"],
            learner_queue_timeout=config["learner_queue_timeout"])
    return learner_thread


# Mix in replay to a stream of experiences.
class MixInReplay:
    def __init__(self, num_slots, replay_proportion):
        if replay_proportion > 0 and num_slots == 0:
            raise ValueError(
                "You must set num_slots > 0 if replay_proportion > 0.")
        self.num_slots = num_slots
        self.replay_proportion = replay_proportion
        self.replay_batches = []
        self.replay_index = 0

    def __call__(self, sample_batch):
        output_batches = [sample_batch]
        if self.num_slots <= 0:
            return output_batches  # Replay is disabled.

        # Put in replay buffer if enabled.
        if len(self.replay_batches) < self.num_slots:
            self.replay_batches.append(sample_batch)
        else:
            self.replay_batches[self.replay_index] = sample_batch
            self.replay_index += 1
            self.replay_index %= self.num_slots

        # Replay with some probability.
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            replay_batch = random.choice(self.replay_batches)
            output_batches.append(replay_batch)

        return output_batches


# Update worker weights as they finish generating experiences.
class UpdateWorkerWeights:
    def __init__(self, learner_thread, workers, broadcast_interval):
        self.learner_thread = learner_thread
        self.steps_since_broadcast = 0
        self.broadcast_interval = broadcast_interval
        self.workers = workers
        self.weights = workers.local_worker().get_weights()

    def __call__(self, item):
        actor, batch = item
        self.steps_since_broadcast += 1
        if (self.steps_since_broadcast >= self.broadcast_interval
                and self.learner_thread.weights_updated):
            self.weights = ray.put(self.workers.local_worker().get_weights())
            self.steps_since_broadcast = 0
            self.learner_thread.weights_updated = False
            # Update metrics.
            metrics = LocalIterator.get_metrics()
            metrics.counters["num_weight_broadcasts"] += 1
        actor.set_weights.remote(self.weights)


def record_steps_trained(count):
    metrics = LocalIterator.get_metrics()
    metrics.counters[STEPS_TRAINED_COUNTER] += count


def gather_experiences_directly(workers, config):
    rollouts = ParallelRollouts(
        workers,
        mode="async",
        async_queue_depth=config["max_sample_requests_in_flight_per_worker"])
    train_batches = rollouts \
        .for_each(lambda batch: batch.decompress_if_needed()) \
        .for_each(MixInReplay(
            num_slots=config["replay_buffer_num_slots"],
            replay_proportion=config["replay_proportion"])) \
        .flatten() \
        .combine(
            ConcatBatches(min_batch_size=config["train_batch_size"]))
    return train_batches


@ray.remote(num_cpus=0)
class Aggregator(ParallelIteratorWorker):
    def __init__(self, config: dict,
                 rollout_group: "ParallelIterator[SampleBatchType]"):
        self.weights = None

        def generator():
            it = rollout_group.gather_async(async_queue_depth=config[
                "max_sample_requests_in_flight_per_worker"])

            def update_worker(item):
                worker, batch = item
                if self.weights:
                    worker.set_weights.remote(self.weights)
                return batch

            it = it.zip_with_source_actor() \
                .for_each(update_worker) \
                .for_each(lambda batch: batch.decompress_if_needed()) \
                .for_each(MixInReplay(
                    num_slots=config["replay_buffer_num_slots"],
                    replay_proportion=config["replay_proportion"])) \
                .flatten() \
                .combine(
                    ConcatBatches(
                        min_batch_size=config["train_batch_size"]))
            for train_batch in it:
                yield train_batch

        super().__init__(generator, repeat=False)

    def get_host(self):
        return os.uname()[1]

    def set_weights(self, weights):
        self.weights = weights


def gather_experiences_tree_aggregation(workers, config):
    rollouts = ParallelRollouts(workers, mode="raw")

    # Divide up the workers between aggregators.
    worker_assignments = [[] for _ in range(config["num_aggregation_workers"])]
    i = 0
    for w in range(len(workers.remote_workers())):
        worker_assignments[i].append(w)
        i += 1
        i %= len(worker_assignments)
    print("Worker assignments", worker_assignments)

    rollout_groups: List["ParallelIterator[SampleBatchType]"] = [
        rollouts.select_shards(assigned) for assigned in worker_assignments
    ]

    # This spawns |num_aggregation_workers| intermediate actors that aggregate
    # experiences in parallel. We force colocation on the same node to maximize
    # data bandwidth between them and the driver.
    train_batches = from_actors([
        create_colocated(Aggregator, [config, g], 1)[0] for g in rollout_groups
    ])

    # TODO(ekl) properly account for replay.
    def record_steps_sampled(batch):
        metrics = LocalIterator.get_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += batch.count
        return batch

    return train_batches.gather_async().for_each(record_steps_sampled)


# Experimental distributed execution impl; enable with "use_exec_api": True.
def execution_plan(workers, config):
    if config["num_aggregation_workers"] > 0:
        train_batches = gather_experiences_tree_aggregation(workers, config)
    else:
        train_batches = gather_experiences_directly(workers, config)

    # Start the learner thread.
    learner_thread = make_learner_thread(workers.local_worker(), config)
    learner_thread.start()

    # Here `rollouts` is a stream of batches of size `train_batch_size`.
    enqueue_op = train_batches \
        .for_each(Enqueue(learner_thread.inqueue)) \
        .zip_with_source_actor() \
        .for_each(UpdateWorkerWeights(
            learner_thread, workers,
            broadcast_interval=config["broadcast_interval"]))

    dequeue_op = Dequeue(
            learner_thread.outqueue, check=learner_thread.is_alive) \
        .for_each(record_steps_trained)

    merged_op = Concurrently(
        [enqueue_op, dequeue_op], mode="async", output_indexes=[1])

    def add_learner_metrics(result):
        def timer_to_ms(timer):
            return round(1000 * timer.mean, 3)

        result["info"].update({
            "learner_queue": learner_thread.learner_queue_size.stats(),
            "learner": copy.deepcopy(learner_thread.stats),
            "timing_breakdown": {
                "learner_grad_time_ms": timer_to_ms(learner_thread.grad_timer),
                "learner_load_time_ms": timer_to_ms(learner_thread.load_timer),
                "learner_load_wait_time_ms": timer_to_ms(
                    learner_thread.load_wait_timer),
                "learner_dequeue_time_ms": timer_to_ms(
                    learner_thread.queue_timer),
            }
        })
        return result

    return StandardMetricsReporting(merged_op, workers, config) \
        .for_each(add_learner_metrics)


ImpalaTrainer = build_trainer(
    name="IMPALA",
    default_config=DEFAULT_CONFIG,
    default_policy=VTraceTFPolicy,
    validate_config=validate_config,
    get_policy_class=choose_policy,
    make_workers=defer_make_workers,
    make_policy_optimizer=make_aggregators_and_optimizer,
    execution_plan=execution_plan,
    mixins=[OverrideDefaultResourceRequest])
