from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ppo.appo_policy_graph import AsyncPPOPolicyGraph
from ray.rllib.agents.agent import with_base_config
from ray.rllib.agents import impala
from ray.rllib.utils.annotations import override
from ray.rllib.agents.agent import Agent, with_common_config
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
    "old_policy_lag",
    "use_kl_loss",
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

    # Old Policy Parameters
    "old_policy_lag": 100,

    # KL Loss Parameters
    "use_kl_loss": False,
    "kl_coeff": 0.05,
    "kl_target": 0.35,

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

class APPOAgent(impala.ImpalaAgent):
    """PPO surrogate loss with IMPALA-architecture."""

    _agent_name = "APPO"
    _default_config = DEFAULT_CONFIG
    _policy_graph = AsyncPPOPolicyGraph

    @override(impala.ImpalaAgent)
    def _init(self):
        for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]
        policy_cls = self._get_policy_graph()
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)

        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"])
        
        # Need to fix make_local_evaluator for later
        #self.old_policy_evaluator = self.make_local_evaluator(self.env_creator, policy_cls)

        self.old_policy_evaluator = None

        self.config["optimizer"]["old_policy_evaluator"] = self.old_policy_evaluator
        self.optimizer = AsyncSamplesOptimizer(self.local_evaluator,
                                               self.remote_evaluators,
                                               self.config["optimizer"])

    @override(impala.ImpalaAgent)
    def _get_policy_graph(self):
        return AsyncPPOPolicyGraph
