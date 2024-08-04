# flake8: noqa

# __rllib-sa-episode-01-begin__
import gymnasium as gym
import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.test_utils import check

# Construct a new episode (without any data in it yet).
episode = SingleAgentEpisode()
assert len(episode) == 0

# Create a gym.Env to fill the episode with data.
env = gym.make("CartPole-v1")
reset_obs, reset_infos = env.reset()
episode.add_env_reset(reset_obs, reset_infos)
# Even with the initial obs/infos, the episode is still considered len=0.
assert len(episode) == 0

# Fill the episode with some data (5 timesteps) using random action.
for _ in range(5):
    action = env.action_space.sample()
    obs, reward, term, trunc, infos = env.step(action)
    episode.add_env_step(
        observation=obs,
        action=action,
        reward=reward,
        terminated=term,
        truncated=trunc,
        infos=infos,
    )
assert len(episode) == 5
# __rllib-sa-episode-01-end__


# __rllib-sa-episode-02-begin__
# We can now access information from the episode via its getter APIs.

# Get the very first observation ("reset observation"). Note that a single observation
# is returned here (not a list of size 1 or a batch of size 1).
check(episode.get_observations(0), reset_obs)
# ... which is the same as using the indexing operator on the `observations` property:
check(episode.observations[0], reset_obs)

# Get the last 3 rewards. Note that a list of len 3 is returned.
check(episode.get_rewards(slice(-3, None)), [1.0, 1.0, 1.0])
# ... which is the same as using the slice operator on the `rewards` property:
check(episode.rewards[-3:], [1.0, 1.0, 1.0])
# To get a single reward, use a single index:
episode.get_rewards(0)  # return the first reward (after taking the first action)
# .. or, equivalently:
episode.rewards[0]

# Get the most recent action (single item, not batched).
# This works regardless of the action space.
check(episode.get_actions(-1), action)
# ... which is the same as using the indexing operator on the `actions` property:
check(episode.actions[-1], action)

# __rllib-sa-episode-02-end__


# Looking back from ts=1, get the previous 4 rewards AND fill with 0.0
# in case we go over the beginning (ts=0). So we would expect
# [0.0, 0.0, 0.0, r0] to be returned here, where r0 is the very first received
# reward in the episode:
episode.get_rewards(slice(-4, 0), neg_indices_left_of_zero=True, fill=0.0)

# Note the use of fill=0.0 here (fill everything that's out of range with this
# value) AND the argument `neg_indices_left_of_zero=True`, which interprets
# negative indices as being left of ts=0 (e.g. -1 being the timestep before
# ts=0).

# Assuming we had a complex action space (nested gym.spaces.Dict) with one or
# more elements being Discrete or MultiDiscrete spaces:
# 1) The `fill=...` argument would still work, filling all spaces (Boxes,
# Discrete) with that provided value.
# 2) Setting the flag `one_hot_discrete=True` would convert those discrete
# sub-components automatically into one-hot (or multi-one-hot) tensors.
# This simplifies the task of having to provide the previous 4 (nested and
# partially discrete/multi-discrete) actions for each timestep within a training
# batch, thereby filling timesteps before the episode started with 0.0s and
# one-hot'ing the discrete/multi-discrete components in these actions:
episode = SingleAgentEpisode(action_space=gym.spaces.Dict({
    "a": gym.spaces.Discrete(3),
    "b": gym.spaces.MultiDiscrete([2, 3]),
    "c": gym.spaces.Box(-1.0, 1.0, (2,)),
}))

# ... fill episode with data ...
episode.add_env_reset(observation=0)
# ... from a few steps.
episode.add_env_step(
    observation=1,
    action={"a": 0, "b": np.array([1, 2]), "c": np.array([.5, -.5], np.float32)},
    reward=1.0,
)

# In your connector
prev_4_a = []
# Note here that len(episode) does NOT include the lookback buffer.
for ts in range(len(episode)):
    prev_4_a.append(
        episode.get_actions(
            indices=slice(ts - 4, ts),
            # Make sure negative indices are interpreted as
            # "into lookback buffer"
            neg_indices_left_of_zero=True,
            # Zero-out everything even further before the lookback buffer.
            fill=0.0,
            # Take care of discrete components (get ready as NN input).
            one_hot_discrete=True,
        )
    )

# Finally, convert from list of batch items to a struct (same as action space)
# of batched (numpy) arrays, in which all leafs have B==len(prev_4_a).
from ray.rllib.utils.spaces.space_utils import batch

prev_4_actions_col = batch(prev_4_a)
