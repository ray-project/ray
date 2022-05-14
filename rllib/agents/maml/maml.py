import logging
import numpy as np
from typing import Type

from ray.rllib.utils.sgd import standardized
from ray.rllib.agents import with_common_config
from ray.rllib.agents.maml.maml_tf_policy import MAMLTFPolicy
from ray.rllib.agents.maml.maml_torch_policy import MAMLTorchPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import from_actors, LocalIterator

logger = logging.getLogger(__name__)

# fmt: off
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
    # Do create an actual env on the local worker (worker-idx=0).
    "create_env_on_driver": True,
    # Stepsize of SGD
    "lr": 1e-3,
    "model": {
        # Share layers for value function.
        "vf_share_layers": False,
    },
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
    "inner_lr": 0.1,
    # Use Meta Env Template
    "use_meta_env": True,

    # Deprecated keys:
    # Share layers for value function. If you set this to True, it's important
    # to tune vf_loss_coeff.
    # Use config.model.vf_share_layers instead.
    "vf_share_layers": DEPRECATED_VALUE,

    # Use `execution_plan` instead of `training_iteration`.
    "_disable_execution_plan_api": False,
})
# __sphinx_doc_end__
# fmt: on


# @mluo: TODO
def set_worker_tasks(workers, use_meta_env):
    if use_meta_env:
        n_tasks = len(workers.remote_workers())
        tasks = workers.local_worker().foreach_env(lambda x: x)[0].sample_tasks(n_tasks)
        for i, worker in enumerate(workers.remote_workers()):
            worker.foreach_env.remote(lambda env: env.set_task(tasks[i]))


class MetaUpdate:
    def __init__(self, workers, maml_steps, metric_gen, use_meta_env):
        self.workers = workers
        self.maml_optimizer_steps = maml_steps
        self.metric_gen = metric_gen
        self.use_meta_env = use_meta_env

    def __call__(self, data_tuple):
        # Metaupdate Step
        samples = data_tuple[0]
        adapt_metrics_dict = data_tuple[1]

        # Metric Updating
        metrics = _get_shared_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += samples.count
        fetches = None
        for i in range(self.maml_optimizer_steps):
            fetches = self.workers.local_worker().learn_on_batch(samples)
        learner_stats = get_learner_stats(fetches)

        # Sync workers with meta policy
        self.workers.sync_weights()

        # Set worker tasks
        set_worker_tasks(self.workers, self.use_meta_env)

        # Update KLS
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

        # Modify Reporting Metrics
        metrics = _get_shared_metrics()
        metrics.info[LEARNER_INFO] = fetches
        metrics.counters[STEPS_TRAINED_THIS_ITER_COUNTER] = samples.count
        metrics.counters[STEPS_TRAINED_COUNTER] += samples.count

        res = self.metric_gen.__call__(None)
        res.update(adapt_metrics_dict)

        return res


def post_process_metrics(adapt_iter, workers, metrics):
    # Obtain Current Dataset Metrics and filter out
    name = "_adapt_" + str(adapt_iter) if adapt_iter > 0 else ""

    # Only workers are collecting data
    res = collect_metrics(remote_workers=workers.remote_workers())

    metrics["episode_reward_max" + str(name)] = res["episode_reward_max"]
    metrics["episode_reward_mean" + str(name)] = res["episode_reward_mean"]
    metrics["episode_reward_min" + str(name)] = res["episode_reward_min"]

    return metrics


def inner_adaptation(workers, samples):
    # Each worker performs one gradient descent
    for i, e in enumerate(workers.remote_workers()):
        e.learn_on_batch.remote(samples[i])


class MAMLTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for MAML!")
        if config["inner_adaptation_steps"] <= 0:
            raise ValueError("Inner Adaptation Steps must be >=1!")
        if config["maml_optimizer_steps"] <= 0:
            raise ValueError("PPO steps for meta-update needs to be >=0!")
        if config["entropy_coeff"] < 0:
            raise ValueError("`entropy_coeff` must be >=0.0!")
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("`batch_mode`=truncate_episodes not supported!")
        if config["num_workers"] <= 0:
            raise ValueError("Must have at least 1 worker/task!")
        if config["create_env_on_driver"] is False:
            raise ValueError(
                "Must have an actual Env created on the driver "
                "(local) worker! Set `create_env_on_driver` to True."
            )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return MAMLTorchPolicy
        else:
            return MAMLTFPolicy

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "MAML execution_plan does NOT take any additional parameters"

        # Sync workers with meta policy
        workers.sync_weights()

        # Samples and sets worker tasks
        use_meta_env = config["use_meta_env"]
        set_worker_tasks(workers, use_meta_env)

        # Metric Collector
        metric_collect = CollectMetrics(
            workers,
            min_history=config["metrics_num_episodes_for_smoothing"],
            timeout_seconds=config["metrics_episode_collection_timeout_s"],
        )

        # Iterator for Inner Adaptation Data gathering (from pre->post
        # adaptation)
        inner_steps = config["inner_adaptation_steps"]

        def inner_adaptation_steps(itr):
            buf = []
            split = []
            metrics = {}
            for samples in itr:

                # Processing Samples (Standardize Advantages)
                split_lst = []
                for sample in samples:
                    sample["advantages"] = standardized(sample["advantages"])
                    split_lst.append(sample.count)

                buf.extend(samples)
                split.append(split_lst)

                adapt_iter = len(split) - 1
                metrics = post_process_metrics(adapt_iter, workers, metrics)
                if len(split) > inner_steps:
                    out = SampleBatch.concat_samples(buf)
                    out["split"] = np.array(split)
                    buf = []
                    split = []

                    # Reporting Adaptation Rew Diff
                    ep_rew_pre = metrics["episode_reward_mean"]
                    ep_rew_post = metrics[
                        "episode_reward_mean_adapt_" + str(inner_steps)
                    ]
                    metrics["adaptation_delta"] = ep_rew_post - ep_rew_pre
                    yield out, metrics
                    metrics = {}
                else:
                    inner_adaptation(workers, samples)

        rollouts = from_actors(workers.remote_workers())
        rollouts = rollouts.batch_across_shards()
        rollouts = rollouts.transform(inner_adaptation_steps)

        # Metaupdate Step
        train_op = rollouts.for_each(
            MetaUpdate(
                workers, config["maml_optimizer_steps"], metric_collect, use_meta_env
            )
        )
        return train_op
