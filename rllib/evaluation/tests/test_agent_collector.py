import gym
import numpy as np
import unittest
import ray

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.test_utils import check
from ray.rllib.evaluation.collectors.simple_list_collector import _AgentCollector


# TODO: @kourosh remove it once we have removed the dependency _agent_collector to
# policy
class FakeRNNPolicy:
    def __init__(self, max_seq_len=1) -> None:
        self.config = {
            "_disable_action_flattening": True,
            "model": {
                "max_seq_len": max_seq_len,
            },
        }

    def is_recurrent(self):
        return True


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _simulate_env_steps(self, ac, n_steps=1):
        obses = []
        obses.append(np.random.rand(4))
        ac.add_init_obs(
            episode_id=0,
            agent_index=1,
            env_id=0,
            t=-1,
            init_obs=obses[-1],
        )

        for t in range(n_steps):
            obses.append(np.random.rand(4))
            ac.add_action_reward_next_obs(
                {SampleBatch.NEXT_OBS: obses[-1], SampleBatch.T: t}
            )

        return obses

    def test_slice_with_repeat_value_1(self):

        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift=f"-{ctx_len}:-1"),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        check(expected_obses, sample_batch[SampleBatch.OBS])

        for t in range(10):
            # no padding
            if t > ctx_len - 1:
                check(sample_batch["prev_obses"][t], expected_obses[t - ctx_len : t])
            else:
                # with padding
                for offset in range(ctx_len):
                    if offset < ctx_len - t:
                        # check the padding
                        check(sample_batch["prev_obses"][t, offset], expected_obses[0])
                    else:
                        # check the rest of the data
                        check(
                            sample_batch["prev_obses"][t, offset:],
                            expected_obses[t - ctx_len + offset : t],
                        )
                        break

    def test_slice_with_repeat_value_larger_1(self):

        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement(
                "obs", shift=f"-{ctx_len}:-1", batch_repeat_value=ctx_len
            ),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        check(expected_obses, sample_batch[SampleBatch.OBS])
        self.assertEqual(sample_batch["prev_obses"].shape, (2, ctx_len, 4))

        # the first prev_obses should be just the first obses repeated ctx_len times
        check(sample_batch["prev_obses"][0], np.ones((ctx_len, 1)) * expected_obses[0])
        # the second prev_obses should be ctx_len slice of obses started at index 0
        check(sample_batch["prev_obses"][1], expected_obses[:ctx_len])

    def test_shift_by_one_with_repeat_value_larger_1(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift=-1, batch_repeat_value=ctx_len),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        self.assertEqual(sample_batch["prev_obses"].shape, (2, 4))

        # should be the same as padding
        check(sample_batch["prev_obses"][0], expected_obses[0])

        # should be the same as index ctx_len - 1
        check(sample_batch["prev_obses"][1], expected_obses[ctx_len - 1])

    def test_shift_by_one_with_repeat_1(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift=-1),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        # check the padding
        check(sample_batch["prev_obses"][0], expected_obses[0])

        # check the data
        check(sample_batch["prev_obses"][1:], expected_obses[:-1])

    def test_shift_positive_one_with_repeat_1(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            SampleBatch.NEXT_OBS: ViewRequirement("obs", shift=1),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)
        sample_batch = ac.build(view_reqs)
        check(sample_batch[SampleBatch.NEXT_OBS], np.stack(obses)[1:])

    def test_shift_positive_one_with_repeat_larger_1(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            SampleBatch.NEXT_OBS: ViewRequirement(
                "obs", shift=1, batch_repeat_value=ctx_len
            ),
        }
        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        expected_obses = np.stack(obses)

        self.assertEqual(sample_batch[SampleBatch.NEXT_OBS].shape, (2, 4))

        # next_obs at index = 0 should be equal to obs at index = 1
        check(sample_batch[SampleBatch.NEXT_OBS][0], expected_obses[1])

        # next_obs at index = 1 should be equal to next_obs at index = ctx_len - 1
        #  which is obs at index = ctx_len
        check(sample_batch[SampleBatch.NEXT_OBS][1], expected_obses[ctx_len + 1])

    def test_slice_with_array(self):

        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift=[-3, -1]),
        }

        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        self.assertEqual(sample_batch["prev_obses"].shape, (10, 2, 4))

        # check if the last time step is correct
        check(sample_batch["prev_obses"][-1], expected_obses[-4:-1:2])

        # check if the padding in the beginning is correct
        check(sample_batch["prev_obses"][0], np.ones((2, 1)) * expected_obses[0])

    def test_view_requirement_with_shfit_step(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift="-5:-1:2"),  # [-5, -3, -1]
        }

        ac = _AgentCollector(view_reqs=view_reqs, policy=FakeRNNPolicy(max_seq_len=1))

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build(view_reqs)
        # exclude the last one since these are the next_obses
        expected_obses = np.stack(obses[:-1])

        self.assertEqual(sample_batch["prev_obses"].shape, (10, 3, 4))

        # check if the last time step is correct
        check(sample_batch["prev_obses"][-1], expected_obses[-6:-1:2])

        # check if the padding in the beginning is correct
        check(sample_batch["prev_obses"][0], np.ones((3, 1)) * expected_obses[0])


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
