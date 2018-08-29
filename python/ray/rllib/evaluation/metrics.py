from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import collections

import ray
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID


def collect_metrics(local_evaluator, remote_evaluators=[]):
    """Gathers episode metrics from PolicyEvaluator instances."""

    episodes = collect_episodes(local_evaluator, remote_evaluators)
    return summarize_episodes(episodes)


def collect_episodes(local_evaluator, remote_evaluators=[]):
    """Gathers new episodes metrics tuples from the given evaluators."""

    metric_lists = ray.get([
        a.apply.remote(lambda ev: ev.sampler.get_metrics())
        for a in remote_evaluators
    ])
    metric_lists.append(local_evaluator.sampler.get_metrics())
    episodes = []
    for metrics in metric_lists:
        episodes.extend(metrics)
    return episodes


def summarize_episodes(episodes):
    """Summarizes a set of episode metrics tuples."""

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
        episodes_total=len(episode_lengths),
        policy_reward_mean=dict(policy_rewards))
