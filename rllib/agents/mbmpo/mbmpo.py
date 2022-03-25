import logging
import numpy as np
from typing import List, Type

import ray
from ray.rllib.agents import with_common_config
from ray.rllib.agents.mbmpo.mbmpo_torch_policy import MBMPOTorchPolicy
from ray.rllib.agents.mbmpo.model_ensemble import DynamicsEnsembleCustomModel
from ray.rllib.agents.mbmpo.utils import calculate_gae_advantages, MBMPOExploration
from ray.rllib.agents.trainer import Trainer
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.wrappers.model_vector_env import model_vector_env
from ray.rllib.evaluation.metrics import (
    collect_episodes,
    collect_metrics,
    get_learner_stats,
)
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import EnvType, TrainerConfigDict
from ray.util.iter import from_actors, LocalIterator

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__

# Adds the following updates to the (base) `Trainer` config in
# rllib/agents/trainer.py (`COMMON_CONFIG` dict).
DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter.
    "lambda": 1.0,
    # Initial coefficient for KL divergence.
    "kl_coeff": 0.0005,
    # Size of batches collected from each worker.
    "rollout_fragment_length": 200,
    # Do create an actual env on the local worker (worker-idx=0).
    "create_env_on_driver": True,
    # Step size of SGD.
    "lr": 1e-3,
    # Coefficient of the value function loss.
    "vf_loss_coeff": 0.5,
    # Coefficient of the entropy regularizer.
    "entropy_coeff": 0.0,
    # PPO clip parameter.
    "clip_param": 0.5,
    # Clip param for the value function. Note that this is sensitive to the
    # scale of the rewards. If your expected V is large, increase this.
    "vf_clip_param": 10.0,
    # If specified, clip the global norm of gradients by this amount.
    "grad_clip": None,
    # Target value for KL divergence.
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes".
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation.
    "observation_filter": "NoFilter",
    # Number of Inner adaptation steps for the MAML algorithm.
    "inner_adaptation_steps": 1,
    # Number of MAML steps per meta-update iteration (PPO steps).
    "maml_optimizer_steps": 8,
    # Inner adaptation step size.
    "inner_lr": 1e-3,
    # Horizon of the environment (200 in MB-MPO paper).
    "horizon": 200,
    # Dynamics ensemble hyperparameters.
    "dynamics_model": {
        "custom_model": DynamicsEnsembleCustomModel,
        # Number of Transition-Dynamics (TD) models in the ensemble.
        "ensemble_size": 5,
        # Hidden layers for each model in the TD-model ensemble.
        "fcnet_hiddens": [512, 512, 512],
        # Model learning rate.
        "lr": 1e-3,
        # Max number of training epochs per MBMPO iter.
        "train_epochs": 500,
        # Model batch size.
        "batch_size": 500,
        # Training/validation split.
        "valid_split_ratio": 0.2,
        # Normalize data (obs, action, and deltas).
        "normalize_data": True,
    },
    # Exploration for MB-MPO is based on StochasticSampling, but uses 8000
    # random timesteps up-front for worker=0.
    "exploration_config": {
        "type": MBMPOExploration,
        "random_timesteps": 8000,
    },
    # Workers sample from dynamics models, not from actual envs.
    "custom_vector_env": model_vector_env,
    # How many iterations through MAML per MBMPO iteration.
    "num_maml_steps": 10,

    # Deprecated keys:
    # Share layers for value function. If you set this to True, it's important
    # to tune vf_loss_coeff.
    # Use config.model.vf_share_layers instead.
    "vf_share_layers": DEPRECATED_VALUE,
})
# __sphinx_doc_end__
# fmt: on

# Select Metric Keys for MAML Stats Tracing
METRICS_KEYS = ["episode_reward_mean", "episode_reward_min", "episode_reward_max"]


class MetaUpdate:
    def __init__(self, workers, num_steps, maml_steps, metric_gen):
        """Computes the MetaUpdate step in MAML.

        Adapted for MBMPO for multiple MAML Iterations.

        Args:
            workers (WorkerSet): Set of Workers
            num_steps (int): Number of meta-update steps per MAML Iteration
            maml_steps (int): MAML Iterations per MBMPO Iteration
            metric_gen (Iterator): Generates metrics dictionary

        Returns:
            metrics (dict): MBMPO metrics for logging.
        """
        self.workers = workers
        self.num_steps = num_steps
        self.step_counter = 0
        self.maml_optimizer_steps = maml_steps
        self.metric_gen = metric_gen
        self.metrics = {}

    def __call__(self, data_tuple):
        """Args:
        data_tuple (tuple): 1st element is samples collected from MAML
        Inner adaptation steps and 2nd element is accumulated metrics
        """
        # Metaupdate Step.
        print("Meta-Update Step")
        samples = data_tuple[0]
        adapt_metrics_dict = data_tuple[1]
        self.postprocess_metrics(
            adapt_metrics_dict, prefix="MAMLIter{}".format(self.step_counter)
        )

        # MAML Meta-update.
        fetches = None
        for i in range(self.maml_optimizer_steps):
            fetches = self.workers.local_worker().learn_on_batch(samples)
        learner_stats = get_learner_stats(fetches)

        # Update KLs.
        def update(pi, pi_id):
            assert "inner_kl" not in learner_stats, (
                "inner_kl should be nested under policy id key",
                learner_stats,
            )
            if pi_id in learner_stats:
                assert "inner_kl" in learner_stats[pi_id], (learner_stats, pi_id)
                pi.update_kls(learner_stats[pi_id]["inner_kl"])
            else:
                logger.warning("No data for {}, not updating kl".format(pi_id))

        self.workers.local_worker().foreach_policy_to_train(update)

        # Modify Reporting Metrics.
        metrics = _get_shared_metrics()
        metrics.info[LEARNER_INFO] = fetches
        metrics.counters[STEPS_TRAINED_THIS_ITER_COUNTER] = samples.count
        metrics.counters[STEPS_TRAINED_COUNTER] += samples.count

        if self.step_counter == self.num_steps - 1:
            td_metric = self.workers.local_worker().foreach_policy(fit_dynamics)[0]

            # Sync workers with meta policy.
            self.workers.sync_weights()

            # Sync TD Models with workers.
            sync_ensemble(self.workers)
            sync_stats(self.workers)

            metrics.counters[STEPS_SAMPLED_COUNTER] = td_metric[STEPS_SAMPLED_COUNTER]

            # Modify to CollectMetrics.
            res = self.metric_gen.__call__(None)
            res.update(self.metrics)
            self.step_counter = 0
            print("MB-MPO Iteration Completed")
            return [res]
        else:
            print("MAML Iteration {} Completed".format(self.step_counter))
            self.step_counter += 1

            # Sync workers with meta policy
            print("Syncing Weights with Workers")
            self.workers.sync_weights()
            return []

    def postprocess_metrics(self, metrics, prefix=""):
        """Appends prefix to current metrics

        Args:
            metrics (dict): Dictionary of current metrics
            prefix (str): Prefix string to be appended
        """
        for key in metrics.keys():
            self.metrics[prefix + "_" + key] = metrics[key]


def post_process_metrics(prefix, workers, metrics):
    """Update current dataset metrics and filter out specific keys.

    Args:
        prefix (str): Prefix string to be appended
        workers (WorkerSet): Set of workers
        metrics (dict): Current metrics dictionary
    """
    res = collect_metrics(remote_workers=workers.remote_workers())
    for key in METRICS_KEYS:
        metrics[prefix + "_" + key] = res[key]
    return metrics


def inner_adaptation(workers: WorkerSet, samples: List[SampleBatch]):
    """Performs one gradient descend step on each remote worker.

    Args:
        workers (WorkerSet): The WorkerSet of the Trainer.
        samples (List[SampleBatch]): The list of SampleBatches to perform
            a training step on (one for each remote worker).
    """

    for i, e in enumerate(workers.remote_workers()):
        e.learn_on_batch.remote(samples[i])


def fit_dynamics(policy, pid):
    return policy.dynamics_model.fit()


def sync_ensemble(workers: WorkerSet) -> None:
    """Syncs dynamics ensemble weights from driver (main) to workers.

    Args:
        workers (WorkerSet): Set of workers, including driver (main).
    """

    def get_ensemble_weights(worker):
        policy_map = worker.policy_map
        policies = policy_map.keys()

        def policy_ensemble_weights(policy):
            model = policy.dynamics_model
            return {k: v.cpu().detach().numpy() for k, v in model.state_dict().items()}

        return {
            pid: policy_ensemble_weights(policy)
            for pid, policy in policy_map.items()
            if pid in policies
        }

    def set_ensemble_weights(policy, pid, weights):
        weights = weights[pid]
        weights = convert_to_torch_tensor(weights, device=policy.device)
        model = policy.dynamics_model
        model.load_state_dict(weights)

    if workers.remote_workers():
        weights = ray.put(get_ensemble_weights(workers.local_worker()))
        set_func = ray.put(set_ensemble_weights)
        for e in workers.remote_workers():
            e.foreach_policy.remote(set_func, weights=weights)


def sync_stats(workers: WorkerSet) -> None:
    def get_normalizations(worker):
        policy = worker.policy_map[DEFAULT_POLICY_ID]
        return policy.dynamics_model.normalizations

    def set_normalizations(policy, pid, normalizations):
        policy.dynamics_model.set_norms(normalizations)

    if workers.remote_workers():
        normalization_dict = ray.put(get_normalizations(workers.local_worker()))
        set_func = ray.put(set_normalizations)
        for e in workers.remote_workers():
            e.foreach_policy.remote(set_func, normalizations=normalization_dict)


def post_process_samples(samples, config: TrainerConfigDict):
    # Instead of using NN for value function, we use regression
    split_lst = []
    for sample in samples:
        indexes = np.asarray(sample["dones"]).nonzero()[0]
        indexes = indexes + 1

        reward_list = np.split(sample["rewards"], indexes)[:-1]
        observation_list = np.split(sample["obs"], indexes)[:-1]

        paths = []
        for i in range(0, len(reward_list)):
            paths.append(
                {"rewards": reward_list[i], "observations": observation_list[i]}
            )

        paths = calculate_gae_advantages(paths, config["gamma"], config["lambda"])

        advantages = np.concatenate([path["advantages"] for path in paths])
        sample["advantages"] = standardized(advantages)
        split_lst.append(sample.count)
    return samples, split_lst


class MBMPOTrainer(Trainer):
    """Model-Based Meta Policy Optimization (MB-MPO) Trainer.

    This file defines the distributed Trainer class for model-based meta
    policy optimization.
    See `mbmpo_[tf|torch]_policy.py` for the definition of the policy loss.

    Detailed documentation:
    https://docs.ray.io/en/master/rllib-algorithms.html#mbmpo
    """

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for MB-MPO!")
        if config["framework"] != "torch":
            logger.warning(
                "MB-MPO only supported in PyTorch so far! Switching to "
                "`framework=torch`."
            )
            config["framework"] = "torch"
        if config["inner_adaptation_steps"] <= 0:
            raise ValueError("Inner adaptation steps must be >=1!")
        if config["maml_optimizer_steps"] <= 0:
            raise ValueError("PPO steps for meta-update needs to be >=0!")
        if config["entropy_coeff"] < 0:
            raise ValueError("`entropy_coeff` must be >=0.0!")
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("`batch_mode=truncate_episodes` not supported!")
        if config["num_workers"] <= 0:
            raise ValueError("Must have at least 1 worker/task.")
        if config["create_env_on_driver"] is False:
            raise ValueError(
                "Must have an actual Env created on the driver "
                "(local) worker! Set `create_env_on_driver` to True."
            )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        return MBMPOTorchPolicy

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "MBMPO execution_plan does NOT take any additional parameters"

        # Train TD Models on the driver.
        workers.local_worker().foreach_policy(fit_dynamics)

        # Sync driver's policy with workers.
        workers.sync_weights()

        # Sync TD Models and normalization stats with workers
        sync_ensemble(workers)
        sync_stats(workers)

        # Dropping metrics from the first iteration
        _, _ = collect_episodes(
            workers.local_worker(), workers.remote_workers(), [], timeout_seconds=9999
        )

        # Metrics Collector.
        metric_collect = CollectMetrics(
            workers,
            min_history=0,
            timeout_seconds=config["metrics_episode_collection_timeout_s"],
        )

        num_inner_steps = config["inner_adaptation_steps"]

        def inner_adaptation_steps(itr):
            buf = []
            split = []
            metrics = {}
            for samples in itr:
                print("Collecting Samples, Inner Adaptation {}".format(len(split)))
                # Processing Samples (Standardize Advantages)
                samples, split_lst = post_process_samples(samples, config)

                buf.extend(samples)
                split.append(split_lst)

                adapt_iter = len(split) - 1
                prefix = "DynaTrajInner_" + str(adapt_iter)
                metrics = post_process_metrics(prefix, workers, metrics)

                if len(split) > num_inner_steps:
                    out = SampleBatch.concat_samples(buf)
                    out["split"] = np.array(split)
                    buf = []
                    split = []

                    yield out, metrics
                    metrics = {}
                else:
                    inner_adaptation(workers, samples)

        # Iterator for Inner Adaptation Data gathering (from pre->post
        # adaptation).
        rollouts = from_actors(workers.remote_workers())
        rollouts = rollouts.batch_across_shards()
        rollouts = rollouts.transform(inner_adaptation_steps)

        # Meta update step with outer combine loop for multiple MAML
        # iterations.
        train_op = rollouts.combine(
            MetaUpdate(
                workers,
                config["num_maml_steps"],
                config["maml_optimizer_steps"],
                metric_collect,
            )
        )
        return train_op

    @staticmethod
    @override(Trainer)
    def validate_env(env: EnvType, env_context: EnvContext) -> None:
        """Validates the local_worker's env object (after creation).

        Args:
            env: The env object to check (for worker=0 only).
            env_context: The env context used for the instantiation of
                the local worker's env (worker=0).

        Raises:
            ValueError: In case something is wrong with the config.
        """
        if not hasattr(env, "reward") or not callable(env.reward):
            raise ValueError(
                f"Env {env} doest not have a `reward()` method, needed for "
                "MB-MPO! This `reward()` method should return "
            )
