import logging

import numpy as np
import ray
from ray.rllib.utils.sgd import standardized
from ray.rllib.agents import with_common_config
from ray.rllib.agents.mbmpo.mbmpo_torch_policy import MBMPOTorchPolicy
from ray.rllib.agents.trainer_template import build_trainer
from typing import List
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER, LEARNER_INFO, _get_shared_metrics
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.util.iter import from_actors
from ray.rllib.utils.types import SampleBatchType
from ray.rllib.agents.mbmpo.model_ensemble import DynamicsEnsembleCustomModel
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID 

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
    "kl_coeff": 0.0005,
    # Size of batches collected from each worker
    "rollout_fragment_length": 200,
    # Stepsize of SGD
    "lr": 1e-3,
    # Share layers for value function
    "vf_share_layers": False,
    # Coefficient of the value function loss
    "vf_loss_coeff": 0.5,
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
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Number of Inner adaptation steps for the MAML algorithm
    "inner_adaptation_steps": 1,
    # Number of MAML steps per meta-update iteration (PPO steps)
    "maml_optimizer_steps": 5,
    # Inner Adaptation Step size
    "inner_lr": 1e-3,
    # Horizon of Environment (200 in MB-MPO paper)
    "horizon": 200,
    #============================================================#
    #                Model-based RL Parameters                   #
    #============================================================#
    "dynamics_model":{
        "custom_model": DynamicsEnsembleCustomModel,
        # Number of Transition-Dynamics Models for Ensemble
        "model_ensemble_size": 5,
        # Hidden Layers for Model Ensemble
        "model_hiddens": [512, 512],
        # Model Learning Rate
        "model_lr": 1e-3,
        # Max number of training epochs per MBMPO iter
        "model_train_epochs": 1,
        # Model Batch Size
        "model_batch_size": 500,
        # Training/Validation Split
        "valid_split_ratio": 0.2,
        # Normalize Data (obs, action, and deltas)
        "normalize_data": True,

    },
    # How many iterations through MAML per MBMPO iteration
    "num_maml_steps": 5,
})
# __sphinx_doc_end__
# yapf: enable


class InnerAdaptationSteps:
    def __init__(self, workers, inner_adaptation_steps, metric_gen):
        self.workers = workers
        self.n = inner_adaptation_steps
        self.buffer = []
        self.split = []
        self.metrics = {}
        self.metric_gen = metric_gen

    def __call__(self, samples: List[SampleBatchType]):
        samples, split_lst = self.post_process_samples(samples)
        self.buffer.extend(samples)
        self.split.append(split_lst)
        self.post_process_metrics()
        if len(self.split) > self.n:
            out = SampleBatch.concat_samples(self.buffer)
            out["split"] = np.array(self.split)
            self.buffer = []
            self.split = []

            # Metrics Reporting
            #metrics = _get_shared_metrics()
            #metrics.counters[STEPS_SAMPLED_COUNTER] += out.count

            # Reporting Adaptation Rew Diff
            ep_rew_pre = self.metrics["episode_reward_mean"]
            ep_rew_post = self.metrics["episode_reward_mean_adapt_" +
                                       str(self.n)]
            self.metrics["adaptation_delta"] = ep_rew_post - ep_rew_pre
            return [(out, self.metrics)]
        else:
            self.inner_adaptation_step(samples)
            return []

    def post_process_samples(self, samples):
        split_lst = []
        for sample in samples:
            sample["advantages"] = standardized(sample["advantages"])
            split_lst.append(sample.count)
        return samples, split_lst

    def inner_adaptation_step(self, samples):
        for i, e in enumerate(self.workers.remote_workers()):
            e.learn_on_batch.remote(samples[i])

    def post_process_metrics(self, prefix=None):
        # Obtain Current Dataset Metrics and filter out
        name = "_adapt_" + str(len(self.split) - 1) if len(
            self.split) > 1 else ""
        res = self.metric_gen.__call__(None)

        self.metrics["episode_reward_max" +
                     str(name)] = res["episode_reward_max"]
        self.metrics["episode_reward_mean" +
                     str(name)] = res["episode_reward_mean"]
        self.metrics["episode_reward_min" +
                     str(name)] = res["episode_reward_min"]


class MetaUpdate:
    def __init__(self, workers, num_steps, maml_steps, metric_gen):
        self.workers = workers
        self.num_steps = num_steps
        self.step_counter = 0
        self.maml_optimizer_steps = maml_steps
        self.metric_gen = metric_gen

    def __call__(self, data_tuple):
        # Metaupdate Step
        samples = data_tuple[0]
        adapt_metrics_dict = data_tuple[1]
        for i in range(self.maml_optimizer_steps):
            fetches = self.workers.local_worker().learn_on_batch(samples)
        fetches = get_learner_stats(fetches)

        # Update KLS
        def update(pi, pi_id):
            assert "inner_kl" not in fetches, (
                "inner_kl should be nested under policy id key", fetches)
            if pi_id in fetches:
                assert "inner_kl" in fetches[pi_id], (fetches, pi_id)
                pi.update_kls(fetches[pi_id]["inner_kl"])
            else:
                logger.warning("No data for {}, not updating kl".format(pi_id))

        self.workers.local_worker().foreach_trainable_policy(update)

        # Modify Reporting Metrics
        metrics = _get_shared_metrics()
        metrics.info[LEARNER_INFO] = fetches
        metrics.counters[STEPS_TRAINED_COUNTER] += samples.count
        print("MAML Step ", self.step_counter)
        if self.step_counter == self.num_steps:
            self.workers.local_worker().policy_map[DEFAULT_POLICY_ID].dynamics_model.fit()

            # Sync workers with meta policy
            self.workers.sync_weights()

            # Sync TD Models with workers
            sync_ensemble(self.workers)

            # Send normalization statistics to workers
            normalization_dict = ray.put(self.workers.local_worker().policy_map[DEFAULT_POLICY_ID].dynamics_model.normalizations)
            for e in self.workers.remote_workers():
                e.set_dict.remote(normalization_dict)
            
            res = self.metric_gen.__call__(None)
            res.update(adapt_metrics_dict)
            self.step_counter = 0
            print(res)
            return [res]
        else:
            self.step_counter += 1
            return []


def sync_ensemble(workers, model_attr="dynamics_model"):
    if workers.remote_workers():
        weights = ray.put(workers.local_worker().get_extra_weights(model_attr))
        for e in workers.remote_workers():
            e.set_extra_weights.remote(weights, model_attr)

def execution_plan(workers, config):
    # Train TD Models
    # TODO: @mluo Make this less messy :(
    metrics = workers.local_worker().policy_map[DEFAULT_POLICY_ID].dynamics_model.fit()
    #metrics = _get_shared_metrics()
    #metrics.counters[STEPS_SAMPLED_COUNTER] += metrics[STEPS_SAMPLED_COUNTER]

    # Sync workers policy with workers
    workers.sync_weights()

    # Sync TD Models and other important data (normalization stats) with workers
    sync_ensemble(workers)
    normalization_dict = ray.put(workers.local_worker().policy_map[DEFAULT_POLICY_ID].dynamics_model.normalizations)
    for e in workers.remote_workers():
        e.set_dict.remote(normalization_dict)

    # Metric Collector
    metric_collect = CollectMetrics(
        workers,
        min_history=config["metrics_smoothing_episodes"],
        timeout_seconds=config["collect_metrics_timeout"])

    # Iterator for Inner Adaptation Data gathering (from pre->post adaptation)
    rollouts = from_actors(workers.remote_workers())
    rollouts = rollouts.batch_across_shards()
    rollouts = rollouts.combine(
        InnerAdaptationSteps(workers, config["inner_adaptation_steps"],
                             metric_collect))

    # Metaupdate Step
    train_op = rollouts.combine(
        MetaUpdate(workers, config["num_maml_steps"], config["maml_optimizer_steps"], metric_collect))
    return train_op


def get_policy_class(config):
    # @mluo: TODO
    config["framework"] = "torch"
    if config["framework"] == "tf":
        raise ValueError("MB-MPO not supported in Tensorflow yet!")
    return MBMPOTorchPolicy


def validate_config(config):
    if config["inner_adaptation_steps"] <= 0:
        raise ValueError("Inner Adaptation Steps must be >=1.")
    if config["maml_optimizer_steps"] <= 0:
        raise ValueError("PPO steps for meta-update needs to be >=0")
    if config["entropy_coeff"] < 0:
        raise ValueError("entropy_coeff must be >=0")
    if config["batch_mode"] != "complete_episodes":
        raise ValueError("truncate_episodes not supported")
    if config["num_workers"] <= 0:
        raise ValueError("Must have at least 1 worker/task.")


MBMPOTrainer = build_trainer(
    name="MBMPO",
    default_config=DEFAULT_CONFIG,
    default_policy=MBMPOTorchPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    validate_config=validate_config)
