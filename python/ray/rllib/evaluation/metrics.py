from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import collections

import ray
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.evaluation.sampler import RolloutMetrics
from ray.rllib.offline.off_policy_estimator import OffPolicyEstimate
from ray.rllib.utils.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
def collect_metrics(local_evaluator, remote_evaluators=[],
                    timeout_seconds=180):
    """Gathers episode metrics from PolicyEvaluator instances."""

    episodes, num_dropped = collect_episodes(
        local_evaluator, remote_evaluators, timeout_seconds=timeout_seconds)
    metrics = summarize_episodes(episodes, episodes, num_dropped)
    return metrics


@DeveloperAPI
def collect_episodes(local_evaluator,
                     remote_evaluators=[],
                     timeout_seconds=180):
    """Gathers new episodes metrics tuples from the given evaluators."""

    pending = [
        a.apply.remote(lambda ev: ev.get_metrics()) for a in remote_evaluators
    ]
    collected, _ = ray.wait(
        pending, num_returns=len(pending), timeout=timeout_seconds * 1.0)
    num_metric_batches_dropped = len(pending) - len(collected)

    metric_lists = ray.get(collected)
    metric_lists.append(local_evaluator.get_metrics())
    episodes = []
    for metrics in metric_lists:
        episodes.extend(metrics)
    return episodes, num_metric_batches_dropped


@DeveloperAPI
def summarize_episodes(episodes, new_episodes, num_dropped):
    """Summarizes a set of episode metrics tuples.

    Arguments:
        episodes: smoothed set of episodes including historical ones
        new_episodes: just the new episodes in this iteration
        num_dropped: number of workers haven't returned their metrics
    """

    if num_dropped > 0:
        logger.warning("WARNING: {} workers have NOT returned metrics".format(
            num_dropped))

    episodes, estimates = _partition(episodes)
    new_episodes, _ = _partition(new_episodes)

    episode_rewards = []
    episode_lengths = []
    policy_rewards = collections.defaultdict(list)
    custom_metrics = collections.defaultdict(list)
    for episode in episodes:
        episode_lengths.append(episode.episode_length)
        episode_rewards.append(episode.episode_reward)
        for k, v in episode.custom_metrics.items():
            custom_metrics[k].append(v)
        for (_, policy_id), reward in episode.agent_rewards.items():
            if policy_id != DEFAULT_POLICY_ID:
                policy_rewards[policy_id].append(reward)
    if episode_rewards:
        min_reward = min(episode_rewards)
        max_reward = max(episode_rewards)
    else:
        min_reward = float('nan')
        max_reward = float('nan')
    avg_reward = np.mean(episode_rewards)
    avg_length = np.mean(episode_lengths)

    for policy_id, rewards in policy_rewards.copy().items():
        policy_rewards[policy_id] = np.mean(rewards)

    for k, v_list in custom_metrics.copy().items():
        custom_metrics[k + "_mean"] = np.mean(v_list)
        filt = [v for v in v_list if not np.isnan(v)]
        if filt:
            custom_metrics[k + "_min"] = np.min(filt)
            custom_metrics[k + "_max"] = np.max(filt)
        else:
            custom_metrics[k + "_min"] = float("nan")
            custom_metrics[k + "_max"] = float("nan")
        del custom_metrics[k]

    estimators = collections.defaultdict(lambda: collections.defaultdict(list))
    for e in estimates:
        acc = estimators[e.estimator_name]
        for k, v in e.metrics.items():
            acc[k].append(v)
    for name, metrics in estimators.items():
        for k, v_list in metrics.items():
            metrics[k] = np.mean(v_list)
        estimators[name] = dict(metrics)

    return dict(
        episode_reward_max=max_reward,
        episode_reward_min=min_reward,
        episode_reward_mean=avg_reward,
        episode_len_mean=avg_length,
        episodes_this_iter=len(new_episodes),
        policy_reward_mean=dict(policy_rewards),
        custom_metrics=dict(custom_metrics),
        off_policy_estimator=dict(estimators),
        num_metric_batches_dropped=num_dropped)


def _partition(episodes):
    """Divides metrics data into true rollouts vs off-policy estimates."""

    rollouts, estimates = [], []
    for e in episodes:
        if isinstance(e, RolloutMetrics):
            rollouts.append(e)
        elif isinstance(e, OffPolicyEstimate):
            estimates.append(e)
        else:
            raise ValueError("Unknown metric type: {}".format(e))
    return rollouts, estimates
