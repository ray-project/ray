from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import collections

import ray
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID


def collect_metrics(local_evaluator, remote_evaluators=[], timeout=180):
    """Gathers episode metrics from PolicyEvaluator instances."""

    episodes, num_worker_still_sample = collect_episodes(
        local_evaluator, remote_evaluators, timeout=timeout)
    metrics = summarize_episodes(episodes, episodes)
    metrics["num_worker_still_sample"] = num_worker_still_sample
    return metrics


def collect_episodes(local_evaluator, remote_evaluators=[], timeout=180):
    """Gathers new episodes metrics tuples from the given evaluators."""

    pending = [
        a.apply.remote(lambda ev: ev.sampler.get_metrics())
        for a in remote_evaluators
    ]
    collected, _ = ray.wait(
        pending, num_returns=len(pending), timeout=timeout * 1000)
    num_worker_still_sample = len(pending) - len(collected)
    if num_worker_still_sample > 0:
        print("{}/{} workers returned metrics within {}s".format(
            len(collected), len(pending), timeout))
    metric_lists = ray.get(collected)
    metric_lists.append(local_evaluator.sampler.get_metrics())
    episodes = []
    for metrics in metric_lists:
        episodes.extend(metrics)
    return episodes, num_worker_still_sample


def summarize_episodes(episodes, new_episodes):
    """Summarizes a set of episode metrics tuples.

    Arguments:
        episodes: smoothed set of episodes including historical ones
        new_episodes: just the new episodes in this iteration
    """

    episode_rewards = []
    episode_lengths = []
    policy_rewards = collections.defaultdict(list)
    for episode in episodes:
        episode_lengths.append(episode.episode_length)
        episode_rewards.append(episode.episode_reward)
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

    return dict(
        episode_reward_max=max_reward,
        episode_reward_min=min_reward,
        episode_reward_mean=avg_reward,
        episode_len_mean=avg_length,
        episodes=len(new_episodes),
        policy_reward_mean=dict(policy_rewards))
