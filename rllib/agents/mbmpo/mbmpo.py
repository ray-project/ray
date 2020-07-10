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
from ray.rllib.utils.torch_ops import convert_to_non_torch_type, \
    convert_to_torch_tensor
from ray.rllib.evaluation.metrics import collect_episodes

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
    "clip_param": 0.5,
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
    "maml_optimizer_steps": 8,
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
        "model_train_epochs": 1000,
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
        self.metrics_keys= ["episode_reward_mean", "episode_reward_min", "episode_reward_max"]

    def __call__(self, samples: List[SampleBatchType]):
        print("Collecting Samples, Inner Adaptation {}".format(len(self.split)))
        self.post_process_metrics(prefix="DynaTrajInner_" + str(len(self.split)))
        samples, split_lst = self.post_process_samples(samples)
        self.buffer.extend(samples)
        self.split.append(split_lst)
        if len(self.split) > self.n:
            out = SampleBatch.concat_samples(self.buffer)
            out["split"] = np.array(self.split)
            self.buffer = []
            self.split = []
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

    def post_process_metrics(self, prefix=""):
        # Obtain Current Dataset Metrics and filter out
        res = self.metric_gen.__call__(None)
        for key in self.metrics_keys:
            self.metrics[prefix + "_" + key] = res[key]


class MetaUpdate:
    def __init__(self, workers, num_steps, maml_steps, metric_gen):
        self.workers = workers
        self.num_steps = num_steps
        self.step_counter = 0
        self.maml_optimizer_steps = maml_steps
        self.metric_gen = metric_gen
        self.metrics={}

    def __call__(self, data_tuple):
        # Metaupdate Step
        print("Meta-Update Step")
        samples = data_tuple[0]
        adapt_metrics_dict = data_tuple[1]
        self.postprocess_metrics(adapt_metrics_dict, prefix="MAMLIter{}".format(self.step_counter))
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

        if self.step_counter == self.num_steps:
            td_metric = self.workers.local_worker().foreach_policy(fit_dynamics)[0]

            # Sync workers with meta policy
            self.workers.sync_weights()

            # Sync TD Models with workers
            sync_ensemble(self.workers)
            sync_stats(self.workers)

            metrics.counters[STEPS_SAMPLED_COUNTER] = td_metric[STEPS_SAMPLED_COUNTER]
            import pdb; pdb.set_trace()
            res = self.metric_gen.__call__(None)
            res.update(self.metrics)
            self.step_counter = 0
            print("MB-MPO Iteration Completed")
            return [res]
        else:
            print("MAML Iteration {} Completed".format(self.step_counter))
            self.step_counter += 1

            # Sync workers with meta policy
            self.workers.sync_weights()
            return []

    def postprocess_metrics(self, metrics, prefix=""):
        for key in metrics.keys():
            self.metrics[prefix + "_" + key] = metrics[key]


def sync_ensemble(workers, model_attr="dynamics_model"):
    if workers.remote_workers():
        weights = ray.put(get_ensemble_weights(workers.local_worker()))
        set_func = ray.put(set_ensemble_weights)
        for e in workers.remote_workers():
            e.foreach_policy.remote(set_func, weights=weights)

def sync_stats(workers):
    if workers.remote_workers():
        normalization_dict = ray.put(workers.local_worker().policy_map[DEFAULT_POLICY_ID].dynamics_model.normalizations)
        set_func = ray.put(set_normalizations)
        for e in workers.remote_workers():
            e.foreach_policy.remote(set_func, normalizations=normalization_dict)

def fit_dynamics(policy, pid):
    return policy.dynamics_model.fit()

def get_ensemble_weights(worker):
    policy_map = worker.policy_map
    policies = policy_map.keys()
    def policy_ensemble_weights(policy):
        model = policy.dynamics_model
        return {
            k: v.cpu().detach().numpy()
            for k, v in model.state_dict().items()
        }
    return {
            pid: policy_ensemble_weights(policy)
            for pid, policy in policy_map.items() if pid in policies
    }

def set_ensemble_weights(policy, pid, weights):
    weights = weights[pid]
    weights = convert_to_torch_tensor(weights, device=policy.device)
    model = policy.dynamics_model
    model.load_state_dict(weights)

def set_normalizations(policy, pid, normalizations):
    policy.dynamics_model.set_norms(normalizations)

def execution_plan(workers, config):
    # Train TD Models
    metrics = workers.local_worker().foreach_policy(fit_dynamics)[0]

    # Sync workers policy with workers
    workers.sync_weights()

    # Sync TD Models and other important data (normalization stats) with workers
    sync_ensemble(workers)
    sync_stats(workers)

    # Dropping metrics from the first iteration 
    episodes, to_be_collected = collect_episodes(
            workers.local_worker(),
            workers.remote_workers(),
            [],
            timeout_seconds=9999)

    # Metrics Collector
    metric_collect = CollectMetrics(
        workers,
        min_history=0,
        timeout_seconds=config["collect_metrics_timeout"])

    # Iterator for Inner Adaptation Data gathering (from pre->post adaptation)
    rollouts = from_actors(workers.remote_workers())
    rollouts = rollouts.batch_across_shards()
    rollouts = rollouts.combine(
        InnerAdaptationSteps(workers, config["inner_adaptation_steps"],
                             metric_collect))

    # Metaupdate Step with outer combine loop for multiple MAML iterations
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
