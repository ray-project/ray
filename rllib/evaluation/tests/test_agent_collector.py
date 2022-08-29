import gym
import numpy as np
import unittest
import ray
import math

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.test_utils import check
from ray.rllib.evaluation.collectors.agent_collector import AgentCollector


class TestAgentCollector(unittest.TestCase):
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

    def test_inference_vs_training_batch(self):
        """Test whether build_for_inference and build_for_training return the same
        batch when they have to."""
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            # include the current obs in the context
            "prev_obses": ViewRequirement("obs", shift=f"-{ctx_len - 1}:0"),
        }

        n_steps = 100
        obses = np.random.rand(n_steps, 4)
        # list to store the last ctx_len obses
        for training_mode in [False, True]:
            ac = AgentCollector(
                view_reqs=view_reqs,
                is_policy_recurrent=True,
                max_seq_len=20,  # default max_seq_len in lstm
                is_training=training_mode,
            )
            obses_ctx = []
            for t, obs in enumerate(obses):
                if t == 0:
                    # e.g. state = env.reset()
                    ac.add_init_obs(
                        episode_id=0,
                        agent_index=1,
                        env_id=0,
                        t=-1,
                        init_obs=obs,
                    )
                    obses_ctx.extend([obs for _ in range(ctx_len)])
                else:
                    # e.g. next_state = env.step()
                    ac.add_action_reward_next_obs(
                        {SampleBatch.NEXT_OBS: obs, SampleBatch.T: t - 1}
                    )
                    # pop from front and add to the end
                    obses_ctx.pop(0)
                    obses_ctx.append(obs)
                eval_batch = ac.build_for_inference()
                # batch size should always be one
                self.assertEqual(eval_batch.count, 1)
                # shape of prev_obses should be (1, ctx_len, 4)
                self.assertEqual(eval_batch["prev_obses"].shape, (1, ctx_len, 4))
                # obs should always be the last time step obs added
                check(eval_batch["obs"], obs[None])
                # prev_obs should always be the last ctx_len time steps obs added
                # (excluding the current time step)
                check(eval_batch["prev_obses"], np.stack(obses_ctx, 0)[None])

            # in inference mode the buffer length at the end should be just ctx_len
            if not training_mode:
                check(len(ac.buffers[SampleBatch.OBS][0]), ctx_len)
            else:
                # otherwise it should be n_steps + ctx_len - 1
                check(len(ac.buffers[SampleBatch.OBS][0]), n_steps + ctx_len - 1)

        self.assertTrue(ac.training, "Training mode should be True.")
        train_batch = ac.build_for_training(view_reqs)
        self.assertEqual(
            len(train_batch["seq_lens"]), math.ceil(n_steps / ac.max_seq_len)
        )
        self.assertEqual(train_batch["prev_obses"].shape, (n_steps - 1, ctx_len, 4))
        self.assertEqual(train_batch[SampleBatch.OBS].shape, (n_steps - 1, 4))

    def test_inference_respects_causality(self):
        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "future_obs": ViewRequirement("obs", shift=1),
            "past_obs": ViewRequirement("obs", shift=-1),
        }
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        self._simulate_env_steps(ac, n_steps=10)

        # build_for_train should return all keys
        train_batch = ac.build_for_training(view_reqs)
        self.assertTrue(all(key in train_batch.keys() for key in view_reqs.keys()))

        # should error out since future_obs has used_for_compute_actions=True but
        # depends on future
        with self.assertRaises(ValueError):
            ac.build_for_inference()

        view_reqs["future_obs"] = ViewRequirement(
            "obs", shift=1, used_for_compute_actions=False
        )
        # since future_obs is shoulld not be used in inference, it should not be in the
        # batch
        eval_batch = ac.build_for_inference()
        self.assertTrue(
            all(
                k in eval_batch.keys()
                for k, vr in view_reqs.items()
                if vr.used_for_compute_actions
            )
        )

    def test_slice_with_repeat_value_1(self):

        obs_space = gym.spaces.Box(-np.ones(4), np.ones(4))
        ctx_len = 5
        view_reqs = {
            SampleBatch.T: ViewRequirement(SampleBatch.T),
            SampleBatch.OBS: ViewRequirement("obs", space=obs_space),
            "prev_obses": ViewRequirement("obs", shift=f"-{ctx_len}:-1"),
        }
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)
        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)
        sample_batch = ac.build_for_training(view_reqs)
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
        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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

        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)
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

        ac = AgentCollector(view_reqs=view_reqs, is_policy_recurrent=True)

        obses = self._simulate_env_steps(ac, n_steps=10)

        sample_batch = ac.build_for_training(view_reqs)

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
