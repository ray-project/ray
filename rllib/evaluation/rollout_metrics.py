import collections

# Define this in its own file, see #5125
RolloutMetrics = collections.namedtuple("RolloutMetrics", [
    "episode_length", "episode_reward", "agent_rewards", "custom_metrics",
    "perf_stats"
])
