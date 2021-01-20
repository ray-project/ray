import logging

import ray
from ray.rllib.agents.impala.vtrace_tf_policy import VTraceTFPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.execution.learner_thread import LearnerThread
from ray.rllib.execution.multi_gpu_learner import TFMultiGPULearner
from ray.rllib.execution.tree_agg import gather_experiences_tree_aggregation
from ray.rllib.execution.common import STEPS_TRAINED_COUNTER, \
    _get_global_vars, _get_shared_metrics
from ray.rllib.execution.replay_ops import MixInReplay
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.utils.annotations import override
from ray.tune.trainable import Trainable
from ray.tune.resources import Resources

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace_tf/torch.py).
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

    # Callback for APPO to use to update KL, target network periodically.
    # The input to the callback is the learner fetches dict.
    "after_train_step": None,
})
# __sphinx_doc_end__
# yapf: enable


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


def get_policy_class(config):
    if config["framework"] == "torch":
        if config["vtrace"]:
            from ray.rllib.agents.impala.vtrace_torch_policy import \
                VTraceTorchPolicy
            return VTraceTorchPolicy
        else:
            from ray.rllib.agents.a3c.a3c_torch_policy import \
                A3CTorchPolicy
            return A3CTorchPolicy
    else:
        if config["vtrace"]:
            return VTraceTFPolicy
        else:
            from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
            return A3CTFPolicy


def validate_config(config):
    if config["entropy_coeff"] < 0.0:
        raise DeprecationWarning("`entropy_coeff` must be >= 0.0!")

    if config["vtrace"] and not config["in_evaluation"]:
        if config["batch_mode"] != "truncate_episodes":
            raise ValueError(
                "Must use `batch_mode`=truncate_episodes if `vtrace` is True.")


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
        if (self.steps_since_broadcast >= self.broadcast_interval
                and self.learner_thread.weights_updated):
            self.weights = ray.put(self.workers.local_worker().get_weights())
            self.steps_since_broadcast = 0
            self.learner_thread.weights_updated = False
            # Update metrics.
            metrics = _get_shared_metrics()
            metrics.counters["num_weight_broadcasts"] += 1
        actor.set_weights.remote(self.weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())


def record_steps_trained(item):
    count, fetches = item
    metrics = _get_shared_metrics()
    # Manually update the steps trained counter since the learner thread
    # is executing outside the pipeline.
    metrics.counters[STEPS_TRAINED_COUNTER] += count
    return item


def gather_experiences_directly(workers, config):
    rollouts = ParallelRollouts(
        workers,
        mode="async",
        num_async=config["max_sample_requests_in_flight_per_worker"])

    # Augment with replay and concat to desired train batch size.
    train_batches = rollouts \
        .for_each(lambda batch: batch.decompress_if_needed()) \
        .for_each(MixInReplay(
            num_slots=config["replay_buffer_num_slots"],
            replay_proportion=config["replay_proportion"])) \
        .flatten() \
        .combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"]["count_steps_by"],
            ))

    return train_batches


def execution_plan(workers, config):
    if config["num_aggregation_workers"] > 0:
        train_batches = gather_experiences_tree_aggregation(workers, config)
    else:
        train_batches = gather_experiences_directly(workers, config)

    # Start the learner thread.
    learner_thread = make_learner_thread(workers.local_worker(), config)
    learner_thread.start()

    # This sub-flow sends experiences to the learner.
    enqueue_op = train_batches \
        .for_each(Enqueue(learner_thread.inqueue))
    # Only need to update workers if there are remote workers.
    if workers.remote_workers():
        enqueue_op = enqueue_op.zip_with_source_actor() \
            .for_each(BroadcastUpdateLearnerWeights(
                learner_thread, workers,
                broadcast_interval=config["broadcast_interval"]))

    # This sub-flow updates the steps trained counter based on learner output.
    dequeue_op = Dequeue(
            learner_thread.outqueue, check=learner_thread.is_alive) \
        .for_each(record_steps_trained)

    merged_op = Concurrently(
        [enqueue_op, dequeue_op], mode="async", output_indexes=[1])

    # Callback for APPO to use to update KL, target network periodically.
    # The input to the callback is the learner fetches dict.
    if config["after_train_step"]:
        merged_op = merged_op.for_each(lambda t: t[1]).for_each(
            config["after_train_step"](workers, config))

    return StandardMetricsReporting(merged_op, workers, config) \
        .for_each(learner_thread.add_learner_metrics)


ImpalaTrainer = build_trainer(
    name="IMPALA",
    default_config=DEFAULT_CONFIG,
    default_policy=VTraceTFPolicy,
    validate_config=validate_config,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    mixins=[OverrideDefaultResourceRequest])
