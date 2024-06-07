from typing import List, Tuple

import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
def add_one_ts_to_episodes_and_truncate(episodes: List[SingleAgentEpisode]):
    """Adds an artificial timestep to an episode at the end.

    In detail: The last observations, infos, actions, and all `extra_model_outputs`
    will be duplicated and appended to each episode's data. An extra 0.0 reward
    will be appended to the episode's rewards. The episode's timestep will be
    increased by 1. Also, adds the truncated=True flag to each episode if the
    episode is not already done (terminated or truncated).

    Useful for value function bootstrapping, where it is required to compute a
    forward pass for the very last timestep within the episode,
    i.e. using the following input dict: {
      obs=[final obs],
      state=[final state output],
      prev. reward=[final reward],
      etc..
    }

    Args:
        episodes: The list of SingleAgentEpisode objects to extend by one timestep
            and add a truncation flag if necessary.

    Returns:
        A list of the original episodes' truncated values (so the episodes can be
        properly restored later into their original states).
    """
    orig_truncateds = []
    for episode in episodes:
        # Make sure the episode is already in numpy format.
        assert episode.is_finalized, episode
        orig_truncateds.append(episode.is_truncated)

        # Add timestep.
        episode.t += 1
        # Use the episode API that allows appending (possibly complex) structs
        # to the data.
        episode.observations.append(episode.observations[-1])
        episode.infos.append(episode.infos[-1])
        episode.actions.append(episode.actions[-1])
        episode.rewards.append(0.0)
        for v in episode.extra_model_outputs.values():
            v.append(v[-1])
        # Artificially make this episode truncated for the upcoming GAE
        # computations.
        if not episode.is_done:
            episode.is_truncated = True
        # Validate to make sure, everything is in order.
        episode.validate()

    return orig_truncateds


@DeveloperAPI
def remove_last_ts_from_data(
    episode_lens: List[int],
    *data: Tuple[np._typing.NDArray],
) -> Tuple[np._typing.NDArray]:
    """Removes the last timesteps from each given data item.

    Each item in data is a concatenated sequence of episodes data.
    For example if `episode_lens` is [2, 4], then data is a shape=(6,)
    ndarray. The returned corresponding value will have shape (4,), meaning
    both episodes have been shortened by exactly one timestep to 1 and 3.

    ..testcode::

        from ray.rllib.algorithms.ppo.ppo_learner import PPOLearner
        import numpy as np

        unpadded = PPOLearner._remove_last_ts_from_data(
            [5, 3],
            np.array([0, 1, 2, 3, 4,  0, 1, 2]),
        )
        assert (unpadded[0] == [0, 1, 2, 3, 0, 1]).all()

        unpadded = PPOLearner._remove_last_ts_from_data(
            [4, 2, 3],
            np.array([0, 1, 2, 3,  0, 1,  0, 1, 2]),
            np.array([4, 5, 6, 7,  2, 3,  3, 4, 5]),
        )
        assert (unpadded[0] == [0, 1, 2,  0,  0, 1]).all()
        assert (unpadded[1] == [4, 5, 6,  2,  3, 4]).all()

    Args:
        episode_lens: A list of current episode lengths. The returned
            data will have the same lengths minus 1 timestep.
        data: A tuple of data items (np.ndarrays) representing concatenated episodes
            to be shortened by one timestep per episode.
            Note that only arrays with `shape=(n,)` are supported! The
            returned data will have `shape=(n-len(episode_lens),)` (each
            episode gets shortened by one timestep).

    Returns:
        A tuple of new data items shortened by one timestep.
    """
    # Figure out the new slices to apply to each data item based on
    # the given episode_lens.
    slices = []
    sum = 0
    for len_ in episode_lens:
        slices.append(slice(sum, sum + len_ - 1))
        sum += len_
    # Compiling return data by slicing off one timestep at the end of
    # each episode.
    ret = []
    for d in data:
        ret.append(np.concatenate([d[s] for s in slices]))
    return tuple(ret) if len(ret) > 1 else ret[0]


@DeveloperAPI
def remove_last_ts_from_episodes_and_restore_truncateds(
    episodes: List[SingleAgentEpisode],
    orig_truncateds: List[bool],
) -> None:
    """Reverts the effects of `_add_ts_to_episodes_and_truncate`.

    Args:
        episodes: The list of SingleAgentEpisode objects to extend by one timestep
            and add a truncation flag if necessary.
        orig_truncateds: A list of the original episodes' truncated values to be
            applied to the `episodes`.
    """

    # Fix all episodes.
    for episode, orig_truncated in zip(episodes, orig_truncateds):
        # Reduce timesteps by 1.
        episode.t -= 1
        # Remove all extra timestep data from the episode's buffers.
        episode.observations.pop()
        episode.infos.pop()
        episode.actions.pop()
        episode.rewards.pop()
        for v in episode.extra_model_outputs.values():
            v.pop()
        # Fix the truncateds flag again.
        episode.is_truncated = orig_truncated
