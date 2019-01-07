from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.rllib.agents import Agent, with_common_config
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.optimizers import SyncSamplesOptimizer, LocalMultiGPUOptimizer
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.2,
    # Size of batches collected from each worker
    "sample_batch_size": 200,
    # Number of timesteps collected for each SGD round
    "train_batch_size": 4000,
    # Total SGD batch size across all devices for SGD
    "sgd_minibatch_size": 128,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # Stepsize of SGD
    "lr": 5e-5,
    # Learning rate schedule
    "lr_schedule": None,
    # Share layers for value function
    "vf_share_layers": False,
    # Coefficient of the value function loss
    "vf_loss_coeff": 1.0,
    # Coefficient of the entropy regularizer
    "entropy_coeff": 0.0,
    # PPO clip parameter
    "clip_param": 0.3,
    # Clip param for the value function. Note that this is sensitive to the
    # scale of the rewards. If your expected V is large, increase this.
    "vf_clip_param": 10.0,
    # Target value for KL divergence
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "MeanStdFilter",
    # Uses the sync samples optimizer instead of the multi-gpu one. This does
    # not support minibatches.
    "simple_optimizer": False,
    # Use a centralized value function
    "use_centralized_vf": False,
    # how many agents worth of observations to potentially pad the
    # global observation with if agent number varies
    "max_vf_agents": 100,
    # (Deprecated) Use the sampling behavior as of 0.6, which launches extra
    # sampling tasks for performance but can waste a large portion of samples.
    "straggler_mitigation": False,
})
# __sphinx_doc_end__
# yapf: enable


class PPOAgent(Agent):
    """Multi-GPU optimized implementation of PPO in TensorFlow."""

    _agent_name = "PPO"
    _default_config = DEFAULT_CONFIG
    _policy_graph = PPOPolicyGraph

    @override(Agent)
    def _init(self):
        self._validate_config()
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, self._policy_graph, self.config["num_workers"])
        if self.config["simple_optimizer"]:
            self.optimizer = SyncSamplesOptimizer(
                self.local_evaluator, self.remote_evaluators, {
                    "num_sgd_iter": self.config["num_sgd_iter"],
                    "train_batch_size": self.config["train_batch_size"],
                })
        else:
            self.optimizer = LocalMultiGPUOptimizer(
                self.local_evaluator, self.remote_evaluators, {
                    "sgd_batch_size": self.config["sgd_minibatch_size"],
                    "num_sgd_iter": self.config["num_sgd_iter"],
                    "num_gpus": self.config["num_gpus"],
                    "sample_batch_size": self.config["sample_batch_size"],
                    "num_envs_per_worker": self.config["num_envs_per_worker"],
                    "train_batch_size": self.config["train_batch_size"],
                    "standardize_fields": ["advantages"],
                    "straggler_mitigation": (
                        self.config["straggler_mitigation"]),
                })

    @override(Agent)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        fetches = self.optimizer.step()
        if "kl" in fetches:
            # single-agent
            self.local_evaluator.for_policy(
                lambda pi: pi.update_kl(fetches["kl"]))
        else:
            # multi-agent
            self.local_evaluator.foreach_trainable_policy(
                lambda pi, pi_id: pi.update_kl(fetches[pi_id]["kl"]))
        res = self.optimizer.collect_metrics(
            self.config["collect_metrics_timeout"])
        res.update(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
            info=dict(fetches, **res.get("info", {})))
        return res

    def _validate_config(self):
        if self.config["sgd_minibatch_size"] > self.config["train_batch_size"]:
            raise ValueError(
                "Minibatch size {} must be <= train batch size {}.".format(
                    self.config["sgd_minibatch_size"],
                    self.config["train_batch_size"]))
        if (self.config["batch_mode"] == "truncate_episodes"
                and not self.config["use_gae"]):
            raise ValueError(
                "Episode truncation is not supported without a value function")
        if (self.config["multiagent"]["policy_graphs"]
                and not self.config["simple_optimizer"]):
            logger.info(
                "In multi-agent mode, policies will be optimized sequentially "
                "by the multi-GPU optimizer. Consider setting "
                "simple_optimizer=True if this doesn't work for you.")
        if self.config["observation_filter"] != "NoFilter":
            # TODO(ekl): consider setting the default to be NoFilter
            logger.warning(
                "By default, observations will be normalized with {}".format(
                    self.config["observation_filter"]))
