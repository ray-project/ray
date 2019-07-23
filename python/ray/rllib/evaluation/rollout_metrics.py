from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

# Define this in its own file, see #5125
RolloutMetrics = collections.namedtuple("RolloutMetrics", [
    "episode_length", "episode_reward", "agent_rewards", "custom_metrics",
    "perf_stats"
])
