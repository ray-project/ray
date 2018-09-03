from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle

import ray
from ray.rllib.agents import Agent, with_common_config
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.utils import FilterManager, merge_dicts
from ray.rllib.optimizers import SyncSamplesOptimizer, LocalMultiGPUOptimizer
from ray.tune.trial import Resources

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
    # Total SGD batch size across all devices for SGD (multi-gpu only)
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
    # Target value for KL divergence
    "kl_target": 0.01,
    # Number of GPUs to use for SGD
    "num_gpus": 0,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "MeanStdFilter",
    # Use the sync samples optimizer instead of the multi-gpu one
    "simple_optimizer": False,
    # Override model config
    "model": {
        # Whether to use LSTM model
        "use_lstm": False,
        # Max seq length for LSTM training.
        "max_seq_len": 20,
    },
})


class PPOAgent(Agent):
    """Multi-GPU optimized implementation of PPO in TensorFlow."""

    _agent_name = "PPO"
    _default_config = DEFAULT_CONFIG
    _policy_graph = PPOPolicyGraph

    @classmethod
    def default_resource_request(cls, config):
        cf = merge_dicts(cls._default_config, config)
        return Resources(
            cpu=1,
            gpu=cf["num_gpus"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def _init(self):
        waste_ratio = (
            self.config["sample_batch_size"] * self.config["num_workers"] /
            self.config["train_batch_size"])
        if waste_ratio > 1:
            msg = ("sample_batch_size * num_workers >> train_batch_size. "
                   "This means that many steps will be discarded. Consider "
                   "reducing sample_batch_size, or increase train_batch_size.")
            if waste_ratio > 1.5:
                raise ValueError(msg)
            else:
                print("Warning: " + msg)
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, self._policy_graph, self.config["num_workers"], {
                "num_cpus": self.config["num_cpus_per_worker"],
                "num_gpus": self.config["num_gpus_per_worker"]
            })
        if self.config["simple_optimizer"]:
            self.optimizer = SyncSamplesOptimizer(
                self.local_evaluator, self.remote_evaluators, {
                    "num_sgd_iter": self.config["num_sgd_iter"],
                    "train_batch_size": self.config["train_batch_size"]
                })
        else:
            self.optimizer = LocalMultiGPUOptimizer(
                self.local_evaluator, self.remote_evaluators, {
                    "sgd_batch_size": self.config["sgd_minibatch_size"],
                    "num_sgd_iter": self.config["num_sgd_iter"],
                    "num_gpus": self.config["num_gpus"],
                    "train_batch_size": self.config["train_batch_size"],
                    "standardize_fields": ["advantages"],
                })

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
        FilterManager.synchronize(self.local_evaluator.filters,
                                  self.remote_evaluators)
        res = self.optimizer.collect_metrics()
        res.update(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
            info=dict(fetches, **res.get("info", {})))
        return res

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = [self.local_evaluator.save(), agent_state]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.local_evaluator.restore(extra_data[0])
        ray.get([
            a.restore.remote(o)
            for (a, o) in zip(self.remote_evaluators, extra_data[1])
        ])
