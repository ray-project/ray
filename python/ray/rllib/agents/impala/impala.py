from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
from ray.rllib.agents.impala.vtrace_policy_graph import VTracePolicyGraph
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.optimizers import AsyncSamplesOptimizer
from ray.rllib.optimizers.aso_tree_aggregator import TreeAggregator
from ray.rllib.utils.annotations import override
from ray.tune.trainable import Trainable
from ray.tune.trial import Resources

OPTIMIZER_SHARED_CONFIGS = [
    "lr",
    "num_envs_per_worker",
    "num_gpus",
    "sample_batch_size",
    "train_batch_size",
    "replay_buffer_num_slots",
    "replay_proportion",
    "num_data_loader_buffers",
    "max_sample_requests_in_flight_per_worker",
    "broadcast_interval",
    "num_sgd_iter",
    "minibatch_buffer_size",
    "num_aggregation_workers",
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace.py).
    "vtrace": True,
    "vtrace_clip_rho_threshold": 1.0,
    "vtrace_clip_pg_rho_threshold": 1.0,

    # System params.
    #
    # == Overview of data flow in IMPALA ==
    # 1. Policy evaluation in parallel across `num_workers` actors produces
    #    batches of size `sample_batch_size * num_envs_per_worker`.
    # 2. If enabled, the replay buffer stores and produces batches of size
    #    `sample_batch_size * num_envs_per_worker`.
    # 3. If enabled, the minibatch ring buffer stores and replays batches of
    #    size `train_batch_size` up to `num_sgd_iter` times per batch.
    # 4. The learner thread executes data parallel SGD across `num_gpus` GPUs
    #    on batches of size `train_batch_size`.
    #
    "sample_batch_size": 50,
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
    # saved total will be (replay_buffer_num_slots * sample_batch_size).
    "replay_buffer_num_slots": 0,
    # max queue size for train batches feeding into the learner
    "learner_queue_size": 16,
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

    # use fake (infinite speed) sampler for testing
    "_fake_sampler": False,
})
# __sphinx_doc_end__
# yapf: enable


class ImpalaTrainer(Trainer):
    """IMPALA implementation using DeepMind's V-trace."""

    _name = "IMPALA"
    _default_config = DEFAULT_CONFIG
    _policy_graph = VTracePolicyGraph

    @override(Trainer)
    def _init(self, config, env_creator):
        for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in config["optimizer"]:
                config["optimizer"][k] = config[k]
        policy_cls = self._get_policy_graph()
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)

        if self.config["num_aggregation_workers"] > 0:
            # Create co-located aggregator actors first for placement pref
            aggregators = TreeAggregator.precreate_aggregators(
                self.config["num_aggregation_workers"])

        self.remote_evaluators = self.make_remote_evaluators(
            env_creator, policy_cls, config["num_workers"])
        self.optimizer = AsyncSamplesOptimizer(self.local_evaluator,
                                               self.remote_evaluators,
                                               **config["optimizer"])
        if config["entropy_coeff"] < 0:
            raise DeprecationWarning("entropy_coeff must be >= 0")

        if self.config["num_aggregation_workers"] > 0:
            # Assign the pre-created aggregators to the optimizer
            self.optimizer.aggregator.init(aggregators)

    @classmethod
    @override(Trainable)
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)
        return Resources(
            cpu=cf["num_cpus_for_driver"],
            gpu=cf["num_gpus"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"] +
            cf["num_aggregation_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    @override(Trainer)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        start = time.time()
        self.optimizer.step()
        while (time.time() - start < self.config["min_iter_time_s"]
               or self.optimizer.num_steps_sampled == prev_steps):
            self.optimizer.step()
        result = self.collect_metrics()
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result

    def _get_policy_graph(self):
        if self.config["vtrace"]:
            policy_cls = self._policy_graph
        else:
            policy_cls = A3CPolicyGraph
        return policy_cls
