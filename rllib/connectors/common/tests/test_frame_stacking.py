import numpy as np
import pytest

from ray.rllib.connectors.common.frame_stacking import FrameStacking
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.test_utils import check

H, W = 4, 4


def _make_episode(T, num_frames, *, terminated=False):
    """Numpy-ized episode: lookback is zeros, obs_t is filled with (t+1)."""
    lookback = num_frames - 1
    observations = [np.zeros((H, W, 1))] * lookback + [
        np.full((H, W, 1), float(t + 1)) for t in range(T + 1)
    ]
    actions = [0] * lookback + list(range(T))
    rewards = [0.0] * lookback + [1.0] * T
    episode = SingleAgentEpisode(
        observations=observations,
        actions=actions,
        rewards=rewards,
        terminated=terminated,
        len_lookback_buffer=lookback,
    )
    episode.to_numpy()
    return episode


def _run(episode, num_frames, with_gae=False):
    """Run the frame stacking connector and return the observations and the bootstrap observation."""
    connector = FrameStacking(
        input_observation_space=None,
        input_action_space=None,
        num_frames=num_frames,
        as_learner_connector=True,
        used_together_with_gae=with_gae,
    )
    batch, shared_data = {}, {} if with_gae else None
    connector(rl_module=None, batch=batch, episodes=[episode], shared_data=shared_data)
    obs = batch[Columns.OBS][(episode.id_,)][0]
    bootstrap = None
    if with_gae and not episode.is_terminated:
        bootstrap = shared_data["stacked_bootstrap_obs"].get(episode.id_)
    return obs, bootstrap


class TestFrameStackingLearner:
    def test_bootstrap_non_terminated(self):
        """Test that the bootstrap observation is correctly computed for a non-terminated episode.

        For this test, we construct an episode that has 3 obserations that are all zeros.
        This is followed by 6 observations that are [<all ones>, <all twos>, ..., <all sevens>].
        We then stack 4 frames and check that the bootstrap observation is correctly computed.
        The bootstrap observation should be the last 4 observations,
        which are [<all fours>, <all fives>, <all sixes>, <all sevens>].
        """
        _, bootstrap = _run(
            _make_episode(T=6, num_frames=4), num_frames=4, with_gae=True
        )
        check(bootstrap.shape, (H, W, 4))
        # Window ending at obs[6]=7: channels [4, 5, 6, 7]
        for c, val in enumerate([4, 5, 6, 7]):
            check(bootstrap[:, :, c], val)

    def test_no_bootstrap_terminated(self):
        """Test that the bootstrap observation is None for a terminated episode."""
        _, bootstrap = _run(
            _make_episode(T=6, num_frames=4, terminated=True),
            num_frames=4,
            with_gae=True,
        )
        check(bootstrap, None)

    @pytest.mark.parametrize("T,num_frames", [(1, 4), (3, 2), (8, 1)])
    def test_output_shape(self, T, num_frames):
        """Test that the output shape is correct for a given number of frames."""
        obs, _ = _run(_make_episode(T, num_frames), num_frames)
        check(obs.shape, (T, H, W, num_frames))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
