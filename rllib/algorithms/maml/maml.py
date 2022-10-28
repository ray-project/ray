import logging
import numpy as np
from typing import Optional, Type

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import concat_samples
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.typing import AlgorithmConfigDict
from ray.util.iter import from_actors, LocalIterator

logger = logging.getLogger(__name__)


class MAMLConfig(AlgorithmConfig):
    """Defines a configuration class from which a MAML Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.maml import MAMLConfig
        >>> config = MAMLConfig().training(use_gae=False).resources(num_gpus=1)
        >>> print(config.to_dict())
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")
        >>> algo.train()

    Example:
        >>> from ray.rllib.algorithms.maml import MAMLConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = MAMLConfig()
        >>> # Print out some default values.
        >>> print(config.lr)
        >>> # Update the config object.
        >>> config.training(grad_clip=tune.grid_search([10.0, 40.0]))
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(
        ...     "MAML",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a PGConfig instance."""
        super().__init__(algo_class=algo_class or MAML)

        # fmt: off
        # __sphinx_doc_begin__
        # MAML-specific config settings.
        self.use_gae = True
        self.lambda_ = 1.0
        self.kl_coeff = 0.0005
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.0
        self.clip_param = 0.3
        self.vf_clip_param = 10.0
        self.grad_clip = None
        self.kl_target = 0.01
        self.inner_adaptation_steps = 1
        self.maml_optimizer_steps = 5
        self.inner_lr = 0.1
        self.use_meta_env = True

        # Override some of AlgorithmConfig's default values with MAML-specific values.
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 200
        self.create_env_on_local_worker = True
        self.lr = 1e-3

        # Share layers for value function.
        self.model.update({
            "vf_share_layers": False,
        })

        self.batch_mode = "complete_episodes"
        self._disable_execution_plan_api = False
        # __sphinx_doc_end__
        # fmt: on

        # Deprecated keys:
        self.vf_share_layers = DEPRECATED_VALUE

    def training(
        self,
        *,
        use_gae: Optional[bool] = None,
        lambda_: Optional[float] = None,
        kl_coeff: Optional[float] = None,
        vf_loss_coeff: Optional[float] = None,
        entropy_coeff: Optional[float] = None,
        clip_param: Optional[float] = None,
        vf_clip_param: Optional[float] = None,
        grad_clip: Optional[float] = None,
        kl_target: Optional[float] = None,
        inner_adaptation_steps: Optional[int] = None,
        maml_optimizer_steps: Optional[int] = None,
        inner_lr: Optional[float] = None,
        use_meta_env: Optional[bool] = None,
        **kwargs,
    ) -> "MAMLConfig":
        """Sets the training related configuration.

        Args:
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
            lambda_: The GAE (lambda) parameter.
            kl_coeff: Initial coefficient for KL divergence.
            vf_loss_coeff: Coefficient of the value function loss.
            entropy_coeff: Coefficient of the entropy regularizer.
            clip_param: PPO clip parameter.
            vf_clip_param: Clip param for the value function. Note that this is
                sensitive to the scale of the rewards. If your expected V is large,
                increase this.
            grad_clip: If specified, clip the global norm of gradients by this amount.
            kl_target: Target value for KL divergence.
            inner_adaptation_steps: Number of Inner adaptation steps for the MAML
                algorithm.
            maml_optimizer_steps: Number of MAML steps per meta-update iteration
                (PPO steps).
            inner_lr: Inner Adaptation Step size.
            use_meta_env: Use Meta Env Template.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if use_gae is not None:
            self.use_gae = use_gae
        if lambda_ is not None:
            self.lambda_ = lambda_
        if kl_coeff is not None:
            self.kl_coeff = kl_coeff
        if vf_loss_coeff is not None:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not None:
            self.entropy_coeff = entropy_coeff
        if clip_param is not None:
            self.clip_param = clip_param
        if vf_clip_param is not None:
            self.vf_clip_param = vf_clip_param
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if kl_target is not None:
            self.kl_target = kl_target
        if inner_adaptation_steps is not None:
            self.inner_adaptation_steps = inner_adaptation_steps
        if maml_optimizer_steps is not None:
            self.maml_optimizer_steps = maml_optimizer_steps
        if inner_lr is not None:
            self.inner_lr = inner_lr
        if use_meta_env is not None:
            self.use_meta_env = use_meta_env

        return self


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


class MAML(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return MAMLConfig().to_dict()

    @override(Algorithm)
    def validate_config(self, config: AlgorithmConfigDict) -> None:
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

    @override(Algorithm)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.maml.maml_torch_policy import MAMLTorchPolicy

            return MAMLTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.maml.maml_tf_policy import MAMLTF1Policy

            return MAMLTF1Policy
        else:
            from ray.rllib.algorithms.maml.maml_tf_policy import MAMLTF2Policy

            return MAMLTF2Policy

    @staticmethod
    @override(Algorithm)
    def execution_plan(
        workers: WorkerSet, config: AlgorithmConfigDict, **kwargs
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
                    out = concat_samples(buf)
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


# Deprecated: Use ray.rllib.algorithms.qmix.qmix.QMixConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(MAMLConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.maml.maml.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.maml.maml.MAMLConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
