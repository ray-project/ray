# flake8: noqa
import copy

# __rllib-sa-episode-01-begin__
from ray.rllib.env.single_agent_episode import SingleAgentEpisode

# Construct a new episode (without any data in it yet).
episode = SingleAgentEpisode()
assert len(episode) == 0

episode.add_env_reset(observation="obs_0", infos="info_0")
# Even with the initial obs/infos, the episode is still considered len=0.
assert len(episode) == 0

# Fill the episode with some fake data (5 timesteps).
for i in range(5):
    episode.add_env_step(
        observation=f"obs_{i+1}",
        action=f"act_{i}",
        reward=f"rew_{i}",
        terminated=False,
        truncated=False,
        infos=f"info_{i+1}",
    )
assert len(episode) == 5
# __rllib-sa-episode-01-end__


# __rllib-sa-episode-02-begin__
# We can now access information from the episode via its getter APIs.

from ray.rllib.utils.test_utils import check

# Get the very first observation ("reset observation"). Note that a single observation
# is returned here (not a list of size 1 or a batch of size 1).
check(episode.get_observations(0), "obs_0")
# ... which is the same as using the indexing operator on the Episode's
# `observations` property:
check(episode.observations[0], "obs_0")

# You can also get several observations at once by providing a list of indices:
check(episode.get_observations([1, 2]), ["obs_1", "obs_2"])
# .. or a slice of observations by providing a python slice object:
check(episode.get_observations(slice(1, 3)), ["obs_1", "obs_2"])

# Note that when passing only a single index, a single item is returned.
# Whereas when passing a list of indices or a slice, a list of items is returned.

# Similarly for getting rewards:
# Get the last reward.
check(episode.get_rewards(-1), "rew_4")
# ... which is the same as using the slice operator on the `rewards` property:
check(episode.rewards[-1], "rew_4")

# Similarly for getting actions:
# Get the first action in the episode (single item, not batched).
# This works regardless of the action space.
check(episode.get_actions(0), "act_0")
# ... which is the same as using the indexing operator on the `actions` property:
check(episode.actions[0], "act_0")

# Finally, you can slice the entire episode using the []-operator with a slice notation:
sliced_episode = episode[3:4]
check(list(sliced_episode.observations), ["obs_3", "obs_4"])
check(list(sliced_episode.actions), ["act_3"])
check(list(sliced_episode.rewards), ["rew_3"])

# __rllib-sa-episode-02-end__

import copy  # noqa

episode_2 = copy.deepcopy(episode)

# __rllib-sa-episode-03-begin__

# Episodes start in the non-numpy'ized state (in which data is stored
# under the hood in lists).
assert episode.is_numpy is False

# Call `to_numpy()` to convert all stored data from lists of individual (possibly
# complex) items to numpy arrays. Note that RLlib normally performs this method call,
# so users don't need to call `to_numpy()` themselves.
episode.to_numpy()
assert episode.is_numpy is True

# __rllib-sa-episode-03-end__

episode = episode_2

# __rllib-sa-episode-04-begin__

# An ongoing episode (of length 5):
assert len(episode) == 5
assert episode.is_done is False

# During an `EnvRunner.sample()` rollout, when enough data has been collected into
# one or more Episodes, the `EnvRunner` calls the `cut()` method, interrupting
# the ongoing Episode and returning a new continuation chunk (with which the
# `EnvRunner` can continue collecting data during the next call to `sample()`):
continuation_episode = episode.cut()

# The length is still 5, but the length of the continuation chunk is 0.
assert len(episode) == 5
assert len(continuation_episode) == 0

# Thanks to the lookback buffer, we can still access the most recent observation
# in the continuation chunk:
check(continuation_episode.get_observations(-1), "obs_5")

# __rllib-sa-episode-04-end__


# __rllib-sa-episode-05-begin__

# Construct a new episode (with some data in its lookback buffer).
episode = SingleAgentEpisode(
    observations=["o0", "o1", "o2", "o3"],
    actions=["a0", "a1", "a2"],
    rewards=[0.0, 1.0, 2.0],
    len_lookback_buffer=3,
)
# Since our lookback buffer is 3, all data already specified in the constructor should
# now be in the lookback buffer (and not be part of the `episode` chunk), meaning
# the length of `episode` should still be 0.
assert len(episode) == 0

# .. and trying to get the first reward will hence lead to an IndexError.
try:
    episode.get_rewards(0)
except IndexError:
    pass

# Get the last 3 rewards (using the lookback buffer).
check(episode.get_rewards(slice(-3, None)), [0.0, 1.0, 2.0])

# Assuming the episode actually started with `obs_0` (reset obs),
# then `obs_1` + `act_0` + reward=0.0, but your model always requires a 1D reward tensor
# of shape (5,) with the 5 most recent rewards in it.
# You could try to code for this by manually filling the missing 2 timesteps with zeros:
last_5_rewards = [0.0, 0.0] + episode.get_rewards(slice(-3, None))
# However, this will become extremely tedious, especially when moving to (possibly more
# complex) observations and actions.

# Instead, `SingleAgentEpisode` getters offer some useful options to solve this problem:
last_5_rewards = episode.get_rewards(slice(-5, None), fill=0.0)
# Note that the `fill` argument allows you to even go further back into the past, provided
# you are ok with filling timesteps that are not covered by the lookback buffer with
# a fixed value.

# __rllib-sa-episode-05-end__


# __rllib-sa-episode-06-begin__

# Construct a new episode (len=3 and lookback buffer=3).
episode = SingleAgentEpisode(
    observations=[
        "o-3",
        "o-2",
        "o-1",  # <- lookback  # noqa
        "o0",
        "o1",
        "o2",
        "o3",  # <- actual episode data  # noqa
    ],
    actions=[
        "a-3",
        "a-2",
        "a-1",  # <- lookback  # noqa
        "a0",
        "a1",
        "a2",  # <- actual episode data  # noqa
    ],
    rewards=[
        -3.0,
        -2.0,
        -1.0,  # <- lookback  # noqa
        0.0,
        1.0,
        2.0,  # <- actual episode data  # noqa
    ],
    len_lookback_buffer=3,
)
assert len(episode) == 3

# In case you want to loop through global timesteps 0 to 2 (timesteps -3, -2, and -1
# being the lookback buffer) and at each such global timestep look 2 timesteps back,
# you can do so easily using the `neg_index_as_lookback` arg like so:
for global_ts in [0, 1, 2]:
    rewards = episode.get_rewards(
        slice(global_ts - 2, global_ts + 1),
        # Switch behavior of negative indices from "from-the-end" to
        # "into the lookback buffer":
        neg_index_as_lookback=True,
    )
    print(rewards)

# The expected output should be:
# [-2.0, -1.0, 0.0]  # global ts=0 (plus looking back 2 ts)
# [-1.0, 0.0, 1.0]   # global ts=1 (plus looking back 2 ts)
# [0.0, 1.0, 2.0]    # global ts=2 (plus looking back 2 ts)

# __rllib-sa-episode-06-end__


# Looking back from ts=1, get the previous 4 rewards AND fill with 0.0
# in case we go over the beginning (ts=0). So we would expect
# [0.0, 0.0, 0.0, r0] to be returned here, where r0 is the very first received
# reward in the episode:
episode.get_rewards(slice(-4, 0), neg_index_as_lookback=True, fill=0.0)

# Note the use of fill=0.0 here (fill everything that's out of range with this
# value) AND the argument `neg_index_as_lookback=True`, which interprets
# negative indices as being left of ts=0 (e.g. -1 being the timestep before
# ts=0).

import gymnasium as gym
import numpy as np

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
episode = SingleAgentEpisode(
    action_space=gym.spaces.Dict(
        {
            "a": gym.spaces.Discrete(3),
            "b": gym.spaces.MultiDiscrete([2, 3]),
            "c": gym.spaces.Box(-1.0, 1.0, (2,)),
        }
    )
)

# ... fill episode with data ...
episode.add_env_reset(observation=0)
# ... from a few steps.
episode.add_env_step(
    observation=1,
    action={"a": 0, "b": np.array([1, 2]), "c": np.array([0.5, -0.5], np.float32)},
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
            neg_index_as_lookback=True,
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
