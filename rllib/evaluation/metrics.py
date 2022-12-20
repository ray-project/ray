import collections
import logging
import numpy as np
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import GradInfoDict, LearnerStatsDict, ResultDict

if TYPE_CHECKING:
    from ray.rllib.evaluation.worker_set import WorkerSet

logger = logging.getLogger(__name__)

RolloutMetrics = DeveloperAPI(
    collections.namedtuple(
        "RolloutMetrics",
        [
            "episode_length",
            "episode_reward",
            "agent_rewards",
            "custom_metrics",
            "perf_stats",
            "hist_data",
            "media",
            "episode_faulty",
        ],
    )
)
RolloutMetrics.__new__.__defaults__ = (0, 0, {}, {}, {}, {}, {}, False)


def _extract_stats(stats: Dict, key: str) -> Dict[str, Any]:
    if key in stats:
        return stats[key]

    multiagent_stats = {}
    for k, v in stats.items():
        if isinstance(v, dict):
            if key in v:
                multiagent_stats[k] = v[key]

    return multiagent_stats


@DeveloperAPI
def get_learner_stats(grad_info: GradInfoDict) -> LearnerStatsDict:
    """Return optimization stats reported from the policy.

    Example:
        >>> grad_info = worker.learn_on_batch(samples)
        {"td_error": [...], "learner_stats": {"vf_loss": ..., ...}}
        >>> print(get_stats(grad_info))
        {"vf_loss": ..., "policy_loss": ...}
    """
    if LEARNER_STATS_KEY in grad_info:
        return grad_info[LEARNER_STATS_KEY]

    multiagent_stats = {}
    for k, v in grad_info.items():
        if type(v) is dict:
            if LEARNER_STATS_KEY in v:
                multiagent_stats[k] = v[LEARNER_STATS_KEY]

    return multiagent_stats


@DeveloperAPI
def collect_metrics(
    workers: "WorkerSet",
    remote_worker_ids: Optional[List[int]] = None,
    timeout_seconds: int = 180,
    keep_custom_metrics: bool = False,
) -> ResultDict:
    """Gathers episode metrics from rollout worker set.

    Args:
        workers: WorkerSet.
        remote_worker_ids: Optional list of IDs of remote workers to collect
            metrics from.
        timeout_seconds: Timeout in seconds for collecting metrics from remote workers.

    Returns:
        A result dict of metrics.
    """
    episodes = collect_episodes(
        workers, remote_worker_ids, timeout_seconds=timeout_seconds
    )
    metrics = summarize_episodes(
        episodes, episodes, keep_custom_metrics=keep_custom_metrics
    )
    return metrics


@DeveloperAPI
def collect_episodes(
    workers: "WorkerSet",
    remote_worker_ids: Optional[List[int]] = None,
    timeout_seconds: int = 180,
) -> List[RolloutMetrics]:
    """Gathers new episodes metrics tuples from the given RolloutWorkers.

    Args:
        workers: WorkerSet.
        remote_worker_ids: Optional list of IDs of remote workers to collect
            metrics from.
        timeout_seconds: Timeout in seconds for collecting metrics from remote workers.

    Returns:
        List of RolloutMetrics.
    """
    # This will drop get_metrics() calls that are too slow.
    # We can potentially make this an asynchronous call if this turns
    # out to be a problem.
    metric_lists = workers.foreach_worker(
        lambda w: w.get_metrics(),
        local_worker=True,
        remote_worker_ids=remote_worker_ids,
        timeout_seconds=timeout_seconds,
    )
    if len(metric_lists) == 0:
        logger.warning("WARNING: collected no metrics.")

    episodes = []
    for metrics in metric_lists:
        episodes.extend(metrics)

    return episodes


@DeveloperAPI
def summarize_episodes(
    episodes: List[RolloutMetrics],
    new_episodes: List[RolloutMetrics] = None,
    keep_custom_metrics: bool = False,
) -> ResultDict:
    """Summarizes a set of episode metrics tuples.

    Args:
        episodes: List of most recent n episodes. This may include historical ones
            (not newly collected in this iteration) in order to achieve the size of
            the smoothing window.
        new_episodes: All the episodes that were completed in this iteration.

    Returns:
        A result dict of metrics.
    """

    if new_episodes is None:
        new_episodes = episodes

    episode_rewards = []
    episode_lengths = []
    policy_rewards = collections.defaultdict(list)
    custom_metrics = collections.defaultdict(list)
    perf_stats = collections.defaultdict(list)
    hist_stats = collections.defaultdict(list)
    episode_media = collections.defaultdict(list)
    num_faulty_episodes = 0

    for episode in episodes:
        # Faulty episodes may still carry perf_stats data.
        for k, v in episode.perf_stats.items():
            perf_stats[k].append(v)

        # Continue if this is a faulty episode.
        # There should be other meaningful stats to be collected.
        if episode.episode_faulty:
            num_faulty_episodes += 1
            continue

        episode_lengths.append(episode.episode_length)
        episode_rewards.append(episode.episode_reward)
        for k, v in episode.custom_metrics.items():
            custom_metrics[k].append(v)
        for (_, policy_id), reward in episode.agent_rewards.items():
            if policy_id != DEFAULT_POLICY_ID:
                policy_rewards[policy_id].append(reward)
        for k, v in episode.hist_data.items():
            hist_stats[k] += v
        for k, v in episode.media.items():
            episode_media[k].append(v)

    if episode_rewards:
        min_reward = min(episode_rewards)
        max_reward = max(episode_rewards)
        avg_reward = np.mean(episode_rewards)
    else:
        min_reward = float("nan")
        max_reward = float("nan")
        avg_reward = float("nan")
    if episode_lengths:
        avg_length = np.mean(episode_lengths)
    else:
        avg_length = float("nan")

    # Show as histogram distributions.
    hist_stats["episode_reward"] = episode_rewards
    hist_stats["episode_lengths"] = episode_lengths

    policy_reward_min = {}
    policy_reward_mean = {}
    policy_reward_max = {}
    for policy_id, rewards in policy_rewards.copy().items():
        policy_reward_min[policy_id] = np.min(rewards)
        policy_reward_mean[policy_id] = np.mean(rewards)
        policy_reward_max[policy_id] = np.max(rewards)

        # Show as histogram distributions.
        hist_stats["policy_{}_reward".format(policy_id)] = rewards

    for k, v_list in custom_metrics.copy().items():
        filt = [v for v in v_list if not np.any(np.isnan(v))]
        if keep_custom_metrics:
            custom_metrics[k] = filt
        else:
            custom_metrics[k + "_mean"] = np.mean(filt)
            if filt:
                custom_metrics[k + "_min"] = np.min(filt)
                custom_metrics[k + "_max"] = np.max(filt)
            else:
                custom_metrics[k + "_min"] = float("nan")
                custom_metrics[k + "_max"] = float("nan")
            del custom_metrics[k]

    for k, v_list in perf_stats.copy().items():
        perf_stats[k] = np.mean(v_list)

    return dict(
        episode_reward_max=max_reward,
        episode_reward_min=min_reward,
        episode_reward_mean=avg_reward,
        episode_len_mean=avg_length,
        episode_media=dict(episode_media),
        episodes_this_iter=len(new_episodes),
        policy_reward_min=policy_reward_min,
        policy_reward_max=policy_reward_max,
        policy_reward_mean=policy_reward_mean,
        custom_metrics=dict(custom_metrics),
        hist_stats=dict(hist_stats),
        sampler_perf=dict(perf_stats),
        num_faulty_episodes=num_faulty_episodes,
    )
