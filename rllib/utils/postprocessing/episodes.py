import numpy as np


def add_one_ts_to_episodes_and_truncate(episodes):
    """Adds an artificial timestep to an episode at the end.

    Useful for value function bootstrapping, where it is required to compute
    a forward pass for the very last timestep within the episode,
    i.e. using the following input dict: {
      obs=[final obs],
      state=[final state output],
      prev. reward=[final reward],
      etc..
    }
    """
    orig_truncateds = []
    for episode in episodes:
        # Make sure the episode is already in numpy format.
        assert episode.is_finalized
        orig_truncateds.append(episode.is_truncated)

        # Add timestep.
        episode.t += 1
        # Use the episode API that allows appending (possibly complex) structs
        # to the data.
        episode.observations.append(episode.observations[-1])
        # = tree.map_structure(
        # lambda s: np.concatenate([s, [s[-1]]]),
        # episode.observations,
        # )
        episode.infos.append(episode.infos[-1])
        episode.actions.append(episode.actions[-1])  # = tree.map_structure(
        # lambda s: np.concatenate([s, [s[-1]]]),
        # episode.actions,
        # )
        episode.rewards.append(0.0)  # = np.append(episode.rewards, 0.0)
        for v in list(episode.extra_model_outputs.values()):
            v.append(v[-1])
        # episode.extra_model_outputs = tree.map_structure(
        #    lambda s: np.concatenate([s, [s[-1]]], axis=0),
        #    episode.extra_model_outputs,
        # )
        # Artificially make this episode truncated for the upcoming
        # GAE computations.
        if not episode.is_done:
            episode.is_truncated = True
        # Validate to make sure, everything is in order.
        episode.validate()

    return orig_truncateds


def remove_last_ts_from_data(episode_lens, *data):
    slices = []
    sum = 0
    for len_ in episode_lens:
        slices.append(slice(sum, sum + len_ - 1))
        sum += len_
    ret = []
    for d in data:
        ret.append(np.concatenate([d[s] for s in slices]))
    return tuple(ret) if len(ret) > 1 else ret[0]


def remove_last_ts_from_episodes_and_restore_truncateds(episodes, orig_truncateds):
    # Fix episodes (remove the extra timestep).
    for episode, orig_truncated in zip(episodes, orig_truncateds):
        # Reduce timesteps by 1.
        episode.t -= 1
        episode.observations.pop()
        episode.infos.pop()
        episode.actions.pop()
        episode.rewards.pop()
        for v in episode.extra_model_outputs.values():
            v.pop()
        # Fix the truncateds flag again.
        episode.is_truncated = orig_truncated
