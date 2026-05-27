import logging
import platform
from typing import List, Tuple

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.metrics.ray_metrics import (
    DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
    TimerAndPrometheusLogger,
)
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import DeveloperAPI
from ray.util.metrics import Counter, Histogram

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

# Timeouts for `ray.get` on episode refs inside `AggregatorActor.get_batch`.
# Without a timeout, a wedged ref can block the aggregator forever — e.g. when
# the producing EnvRunner is preempted and restarted on a spot node, the
# owner-death notification can be lost (the GCS drops the producer task's
# status), so `OwnerDiedError` is never raised and the plasma Pull retries
# indefinitely. The bulk timeout bounds the common-case fetch; the per-ref
# timeout bounds the fallback so a single wedged ref cannot stall the others.
GET_BATCH_BULK_TIMEOUT_S = 600.0
GET_BATCH_PER_REF_TIMEOUT_S = 60.0

# Exceptions that mean an episode ref is permanently unfetchable and should be
# dropped rather than retried.
_UNFETCHABLE_REF_EXCEPTIONS = (
    ray.exceptions.GetTimeoutError,
    ray.exceptions.OwnerDiedError,
    ray.exceptions.ObjectLostError,
    ray.exceptions.ObjectFetchTimedOutError,
    ray.exceptions.RayActorError,
    ray.exceptions.WorkerCrashedError,
    ray.exceptions.NodeDiedError,
)


def _collect_episodes(
    episode_refs: List[ray.ObjectRef],
    bulk_timeout_s: float = GET_BATCH_BULK_TIMEOUT_S,
    per_ref_timeout_s: float = GET_BATCH_PER_REF_TIMEOUT_S,
) -> Tuple[List[EpisodeType], int]:
    """Resolve a list of episode refs with bounded waits.

    Tries one bulk ``ray.get`` first; on failure, falls back to fetching each
    ref individually and drops the unfetchable ones. See the module-level
    comment on ``GET_BATCH_BULK_TIMEOUT_S`` for why the timeouts are needed.

    Returns ``(episodes, num_dropped)``.
    """
    try:
        return tree.flatten(ray.get(episode_refs, timeout=bulk_timeout_s)), 0
    except _UNFETCHABLE_REF_EXCEPTIONS as bulk_err:
        logger.warning(
            "Bulk ray.get on %d episode refs failed (%s); falling back to "
            "per-ref fetch.",
            len(episode_refs),
            type(bulk_err).__name__,
        )

    episodes: List[EpisodeType] = []
    dropped = 0
    for ref in episode_refs:
        try:
            episodes.extend(ray.get(ref, timeout=per_ref_timeout_s))
        except _UNFETCHABLE_REF_EXCEPTIONS:
            dropped += 1
    if dropped:
        logger.warning(
            "Dropped %d/%d unfetchable episode refs after per-ref fallback.",
            dropped,
            len(episode_refs),
        )
    return episodes, dropped


@DeveloperAPI(stability="alpha")
class AggregatorActor(FaultAwareApply):
    """Runs episode lists through ConnectorV2 pipeline and creates train batches.

    The actor should be co-located with a Learner worker. Ideally, there should be one
    or two aggregator actors per Learner worker (having even more per Learner probably
    won't help. Then the main process driving the RL algo can perform the following
    execution logic:
    - query n EnvRunners to sample the environment and return n lists of episodes as
    Ray.ObjectRefs.
    - remote call the set of aggregator actors (in round-robin fashion) with these
    list[episodes] refs in async fashion.
    - gather the results asynchronously, as each actor returns refs pointing to
    ready-to-go train batches.
    - as soon as we have at least one train batch per Learner, call the LearnerGroup
    with the (already sharded) refs.
    - an aggregator actor - when receiving p refs to List[EpisodeType] - does:
    -- ray.get() the actual p lists and concatenate the p lists into one
    List[EpisodeType].
    -- pass the lists of episodes through its LearnerConnector pipeline
    -- buffer the output batches of this pipeline until enough batches have been
    collected for creating one train batch (matching the config's
    `train_batch_size_per_learner`).
    -- concatenate q batches into a train batch and return that train batch.
    - the algo main process then passes the ray.ObjectRef to the ready-to-go train batch
    to the LearnerGroup for calling each Learner with one train batch.
    """

    def __init__(self, config: AlgorithmConfig, rl_module_spec):
        self.config = config

        # Set device and node.
        self._node = platform.node()
        self._device = torch.device("cpu")
        self.metrics: MetricsLogger = MetricsLogger(
            stats_cls_lookup=config.stats_cls_lookup,
            root=True,
        )

        # Create the RLModule.
        # TODO (sven): For now, this RLModule (its weights) never gets updated.
        #  The reason the module is needed is for the connector to know, which
        #  sub-modules are stateful (and what their initial state tensors are), and
        #  which IDs the submodules have (to figure out, whether its multi-agent or
        #  not).
        self._module = rl_module_spec.build()
        self._module = self._module.as_multi_rl_module()

        # Create the Learner connector pipeline.
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
            device=self._device,
        )

        # Ray metrics
        self._metrics_get_batch_time = Histogram(
            name="rllib_utils_aggregator_actor_get_batch_time",
            description="Time spent in AggregatorActor.get_batch()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_get_batch_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_episode_owner_died = Counter(
            name="rllib_utils_aggregator_actor_episode_owner_died_counter",
            description="N times ray.get() on an episode ref failed ",
            tag_keys=("rllib",),
        )
        self._metrics_episode_owner_died.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_get_batch_input_episode_refs = Counter(
            name="rllib_utils_aggregator_actor_get_batch_input_episode_refs_counter",
            description="Number of episode refs received as input to get_batch()",
            tag_keys=("rllib",),
        )
        self._metrics_get_batch_input_episode_refs.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_get_batch_output_batches = Counter(
            name="rllib_utils_aggregator_actor_get_batch_output_batches_counter",
            description="Number of policy batches output by get_batch()",
            tag_keys=("rllib",),
        )
        self._metrics_get_batch_output_batches.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

    def get_batch(self, episode_refs: List[ray.ObjectRef]):
        with TimerAndPrometheusLogger(self._metrics_get_batch_time):
            if len(episode_refs) > 0:
                self._metrics_get_batch_input_episode_refs.inc(value=len(episode_refs))

            # Resolve refs with bounded waits
            episodes, dropped = _collect_episodes(episode_refs)
            if dropped:
                self._metrics_episode_owner_died.inc(value=dropped)

            env_steps = sum(len(e) for e in episodes)

            # If we have enough episodes collected to create a single train batch, pass
            # them at once through the connector to receive a single train batch.
            batch = self._learner_connector(
                episodes=episodes,
                rl_module=self._module,
                metrics=self.metrics,
            )
            # Convert to a dict into a `MultiAgentBatch`.
            # TODO (sven): Try to get rid of dependency on MultiAgentBatch (once our mini-
            #  batch iterators support splitting over a dict).
            ma_batch = MultiAgentBatch(
                policy_batches={
                    pid: SampleBatch(pol_batch) for pid, pol_batch in batch.items()
                },
                env_steps=env_steps,
            )
            self._metrics_get_batch_output_batches.inc(value=1)
        return ma_batch

    def get_metrics(self):
        return self.metrics.reduce()


def _get_env_runner_bundles(config):
    return [
        {
            "CPU": config.num_cpus_per_env_runner,
            "GPU": config.num_gpus_per_env_runner,
            **config.custom_resources_per_env_runner,
        }
        for _ in range(config.num_env_runners)
    ]


def _get_offline_eval_runner_bundles(config):
    return [
        {
            "CPU": config.num_cpus_per_offline_eval_runner,
            "GPU": config.num_gpus_per_offline_eval_runner,
            **config.custom_resources_per_offline_eval_runner,
        }
        for _ in range(config.num_offline_eval_runners)
    ]


def _get_learner_bundles(config):
    if config.num_learners == 0:
        if config.num_aggregator_actors_per_learner > 0:
            return [{"CPU": 1} for _ in range(config.num_aggregator_actors_per_learner)]
        else:
            return []

    if config.num_cpus_per_learner != "auto":
        num_cpus_per_learner = config.num_cpus_per_learner
    elif config.num_gpus_per_learner == 0:
        num_cpus_per_learner = 1
    else:
        num_cpus_per_learner = 0

    # aggregator actors are co-located with learners and use 1 CPU each
    bundles = [
        {
            "CPU": num_cpus_per_learner + config.num_aggregator_actors_per_learner,
            "GPU": config.num_gpus_per_learner,
            **(config.custom_resources_per_learner or {}),
        }
        for _ in range(config.num_learners)
    ]

    return bundles


def _get_main_process_bundle(config):
    if config.num_learners == 0:
        if config.num_cpus_per_learner != "auto":
            num_cpus_per_learner = config.num_cpus_per_learner
        elif config.num_gpus_per_learner == 0:
            num_cpus_per_learner = 1
        else:
            num_cpus_per_learner = 0

        bundle = {
            "CPU": max(num_cpus_per_learner, config.num_cpus_for_main_process),
            "GPU": config.num_gpus_per_learner,
            **config.custom_resources_for_main_process,
        }
    else:
        bundle = {
            "CPU": config.num_cpus_for_main_process,
            "GPU": 0,
            **config.custom_resources_for_main_process,
        }

    return bundle
