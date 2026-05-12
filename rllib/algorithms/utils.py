import platform
from typing import List

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

            episodes: List[EpisodeType] = []
            # It's possible that individual refs are invalid due to the EnvRunner
            # that produced the ref has crashed or had its entire node go down.
            # In this case, try each ref individually and collect only valid results.
            try:
                episodes = tree.flatten(ray.get(episode_refs))
            except ray.exceptions.OwnerDiedError:
                for ref in episode_refs:
                    try:
                        episodes.extend(ray.get(ref))
                    except ray.exceptions.OwnerDiedError:
                        self._metrics_episode_owner_died.inc(value=1)

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
    n_agg = config.num_aggregator_actors_per_learner
    cpus_per_agg = config.num_cpus_per_aggregator_actor
    custom_per_agg = config.custom_resources_per_aggregator_actor or {}

    # Total custom-resource demand contributed by `n_agg` aggregators that
    # share this learner's bundle.
    agg_custom_resources_total = {k: v * n_agg for k, v in custom_per_agg.items()}

    if config.num_learners == 0:
        if n_agg > 0:
            # Aggregators get their own dedicated bundles; learner is local.
            return [{"CPU": cpus_per_agg, **custom_per_agg} for _ in range(n_agg)]
        else:
            return []

    if config.num_cpus_per_learner != "auto":
        num_cpus_per_learner = config.num_cpus_per_learner
    elif config.num_gpus_per_learner == 0:
        num_cpus_per_learner = 1
    else:
        num_cpus_per_learner = 0

    # Each learner bundle includes the learner's own resources + enough
    # capacity for `n_agg` co-located aggregator actors (their resources
    # are claimed against the same bundle via
    # `PlacementGroupSchedulingStrategy(placement_group_bundle_index=...)`
    # in `Algorithm.setup()`).
    bundles = []
    for _ in range(config.num_learners):
        bundle = {
            "CPU": num_cpus_per_learner + n_agg * cpus_per_agg,
            "GPU": config.num_gpus_per_learner,
        }
        # Add aggregator custom resources × `n_agg` so the bundle reserves
        # enough capacity for all aggregators that share it.
        for k, v in agg_custom_resources_total.items():
            bundle[k] = bundle.get(k, 0) + v
        bundles.append(bundle)

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


def compute_aggregator_bundle_indices(
    *,
    num_learners: int,
    num_aggregator_actors_per_learner: int,
    learner_bundle_offset: int,
    learner_node_ids: list,
    bundles_to_node: dict,
):
    """Map each logical Learner index to the PG bundle indices its
    aggregators should claim.

    This is the bridge between Ray Train's post-placement, post-sort
    Learner ordering and ``Algorithm.setup``'s aggregator scheduling.
    Ray Train calls ``WorkerGroup.sort_workers_by_node_id_and_gpu_id``
    after placing workers in PG bundles, so the Learner at logical
    index ``i`` is *not* necessarily the one in bundle
    ``learner_bundle_offset + i``. This helper queries each Learner's
    actual node and picks a co-located bundle for its aggregators.

    Args:
        num_learners: ``config.num_learners`` (0 means a local Learner).
        num_aggregator_actors_per_learner: per-Learner aggregator count.
        learner_bundle_offset: index of the first learner-related bundle
            in the trial PG (output of ``get_learner_bundle_offset``).
        learner_node_ids: actual node IDs of each logical Learner. For
            ``num_learners == 0`` this is a one-element list with the
            trial driver's node.
        bundles_to_node: ``bundle_idx -> node_id`` from
            ``ray.util.placement_group_table(pg)["bundles_to_node_id"]``.

    Returns:
        Dict ``{learner_idx: [bundle_idx, bundle_idx, ...]}`` of length
        ``num_aggregator_actors_per_learner`` per entry. Entries are
        omitted (or shorter than the per-Learner count) if no bundle
        could be found on the Learner's node -- the caller falls back to
        unhinted scheduling for those aggregators.
    """
    if num_aggregator_actors_per_learner == 0:
        return {}

    out: dict = {}
    if num_learners == 0:
        # `_get_learner_bundles` places `num_aggregator_actors_per_learner`
        # dedicated aggregator bundles starting at `learner_bundle_offset`.
        # The single logical Learner is local; pick its node from index 0.
        local_node = learner_node_ids[0]
        candidate_bundles = list(
            range(
                learner_bundle_offset,
                learner_bundle_offset + num_aggregator_actors_per_learner,
            )
        )
        out[0] = [b for b in candidate_bundles if bundles_to_node.get(b) == local_node]
        return out

    # `num_learners > 0`: aggregator CPU is baked into each Learner's
    # bundle (one bundle per Learner). Build a per-node pool of those
    # bundles and pop one per logical Learner by node match.
    from collections import defaultdict

    node_to_unused: dict = defaultdict(list)
    for b in range(learner_bundle_offset, learner_bundle_offset + num_learners):
        node_id = bundles_to_node.get(b)
        if node_id is not None:
            node_to_unused[node_id].append(b)

    for learner_idx, learner_node in enumerate(learner_node_ids):
        pool = node_to_unused.get(learner_node, [])
        if pool:
            # All `num_aggregator_actors_per_learner` aggregators for
            # this Learner share the Learner's bundle.
            out[learner_idx] = [pool.pop(0)] * num_aggregator_actors_per_learner
    return out


def get_learner_bundle_offset(config, eval_config, *, has_eval, has_offline_eval):
    """Index in the trial's PG where learner bundles start.

    Single source of truth for both ``Algorithm.default_resource_request``
    (which builds the bundle list) and ``Algorithm.setup`` (which pins
    aggregator actors to specific learner bundles via
    ``PlacementGroupSchedulingStrategy(placement_group_bundle_index=...)``).

    The PG bundle order produced by ``default_resource_request`` is:
        [main_process, env_runners, eval_env_runners,
         offline_eval_runners, learner_bundles]
    so the offset = 1 (main) + len(env_runners) + len(eval_env_runners) +
    len(offline_eval_runners). Callers pass ``has_eval`` /
    ``has_offline_eval`` because deciding whether those sections exist
    requires algo-specific predicates that don't live in ``utils``.
    """
    n_eval = (eval_config.evaluation_num_env_runners or 0) if has_eval else 0
    n_offline = (eval_config.num_offline_eval_runners or 0) if has_offline_eval else 0
    return 1 + (config.num_env_runners or 0) + n_eval + n_offline
