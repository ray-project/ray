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
    # If specified, clip the global norm of gradients by this amount
    "grad_clip": None,
    # Target value for KL divergence
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Uses the sync samples optimizer instead of the multi-gpu one. This does
    # not support minibatches.
    "simple_optimizer": False,
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
        if "observation_filter" not in self.raw_user_config:
            # TODO(ekl) remove this message after a few releases
            logger.info(
                "Important! Since 0.7.0, observation normalization is no "
                "longer enabled by default. To enable running-mean "
                "normalization, set 'observation_filter': 'MeanStdFilter'. "
                "You can ignore this message if your environment doesn't "
                "require observation normalization.")
        prev_steps = self.optimizer.num_steps_sampled
        fetches = self.optimizer.step()
        if "kl" in fetches:
            # single-agent
            self.local_evaluator.for_policy(
                lambda pi: pi.update_kl(fetches["kl"]))
        else:

            def update(pi, pi_id):
                if pi_id in fetches:
                    pi.update_kl(fetches[pi_id]["kl"])
                else:
                    logger.debug(
                        "No data for {}, not updating kl".format(pi_id))

            # multi-agent
            self.local_evaluator.foreach_trainable_policy(update)
        res = self.collect_metrics()
        res.update(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
            info=dict(fetches, **res.get("info", {})))

        # Warn about bad clipping configs
        if self.config["vf_clip_param"] <= 0:
            rew_scale = float("inf")
        elif res["policy_reward_mean"]:
            rew_scale = 0  # punt on handling multiagent case
        else:
            rew_scale = round(
                abs(res["episode_reward_mean"]) / self.config["vf_clip_param"],
                0)
        if rew_scale > 100:
            logger.warning(
                "The magnitude of your environment rewards are more than "
                "{}x the scale of `vf_clip_param`. ".format(rew_scale) +
                "This means that it will take more than "
                "{} iterations for your value ".format(rew_scale) +
                "function to converge. If this is not intended, consider "
                "increasing `vf_clip_param`.")
        return res

    def _validate_config(self):
        if self.config["entropy_coeff"] < 0:
            raise DeprecationWarning("entropy_coeff must be >= 0")
        if self.config["sgd_minibatch_size"] > self.config["train_batch_size"]:
            raise ValueError(
                "Minibatch size {} must be <= train batch size {}.".format(
                    self.config["sgd_minibatch_size"],
                    self.config["train_batch_size"]))
        if (self.config["batch_mode"] == "truncate_episodes"
                and not self.config["use_gae"]):
            raise ValueError(
                "Episode truncation is not supported without a value "
                "function. Consider setting batch_mode=complete_episodes.")
        if (self.config["multiagent"]["policy_graphs"]
                and not self.config["simple_optimizer"]):
            logger.info(
                "In multi-agent mode, policies will be optimized sequentially "
                "by the multi-GPU optimizer. Consider setting "
                "simple_optimizer=True if this doesn't work for you.")
        if not self.config["vf_share_layers"]:
            logger.warning(
                "FYI: By default, the value function will not share layers "
                "with the policy model ('vf_share_layers': False).")
