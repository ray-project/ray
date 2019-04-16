from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ppo.appo_policy_graph import AsyncPPOPolicyGraph
from ray.rllib.agents.trainer import with_base_config, Trainer, with_common_config
from ray.rllib.agents import impala
from ray.rllib.utils.annotations import override
from ray.rllib.optimizers import AsyncSamplesOptimizer


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
]


# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(impala.DEFAULT_CONFIG, {
    # Whether to use V-trace weighted advantages. If false, PPO GAE advantages
    # will be used instead.
    "vtrace": False,

    # == These two options only apply if vtrace: False ==
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,

    # == PPO surrogate loss options ==
    "clip_param": 0.4,

    # == IMPALA optimizer params (see documentation in impala.py) ==
    "sample_batch_size": 50,
    "train_batch_size": 500,
    "min_iter_time_s": 10,
    "num_workers": 2,
    "num_gpus": 1,
    "num_data_loader_buffers": 1,
    "minibatch_buffer_size": 1,
    "num_sgd_iter": 1,
    "replay_proportion": 0.0,
    "replay_buffer_num_slots": 100,
    "learner_queue_size": 16,
    "max_sample_requests_in_flight_per_worker": 2,
    "broadcast_interval": 1,
    "grad_clip": 40.0,
    "opt_type": "adam",
    "lr": 0.0005,
    "lr_schedule": None,
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    "vf_loss_coeff": 0.5,
    "entropy_coeff": -0.01,
})
# __sphinx_doc_end__
# yapf: enable


class APPOTrainer(impala.ImpalaTrainer):
    """PPO surrogate loss with IMPALA-architecture."""

    _name = "APPO"
    _default_config = DEFAULT_CONFIG
    _policy_graph = AsyncPPOPolicyGraph

    @override(impala.ImpalaTrainer)
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

        if self.config["num_aggregation_workers"] > 0:
            # Assign the pre-created aggregators to the optimizer
            self.optimizer.aggregator.init(aggregators)

    @override(impala.ImpalaTrainer)
    def _get_policy_graph(self):
        return AsyncPPOPolicyGraph
