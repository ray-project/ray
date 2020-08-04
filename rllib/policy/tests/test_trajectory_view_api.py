import numpy as np
import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.evaluation.rollout_sample_collector import \
    RolloutSampleCollector
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.test_utils import check, framework_iterator


class TestTrajectoryViewAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_plain(self):
        config = ppo.DEFAULT_CONFIG.copy()
        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.get_view_requirements()
            view_req_policy = policy.get_view_requirements()
            assert len(view_req_model) == 1
            assert len(view_req_policy) == 6
            for key in [
                SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                SampleBatch.DONES, SampleBatch.NEXT_OBS, SampleBatch.VF_PREDS
            ]:
                assert key in view_req_policy
                # None of the view cols has a special underlying data_col,
                # except next-obs.
                if key != SampleBatch.NEXT_OBS:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1
            trainer.stop()

    def test_lstm_prev_actions_and_rewards(self):
        config = ppo.DEFAULT_CONFIG.copy()
        config["model"] = config["model"].copy()
        # Activate LSTM + prev-action + rewards.
        config["model"]["use_lstm"] = True
        config["model"]["lstm_use_prev_action_reward"] = True

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req_model = policy.model.get_view_requirements()
            view_req_policy = policy.get_view_requirements()
            assert len(view_req_model) == 3  # obs, prev_a, prev_r
            assert len(view_req_policy) == 8
            for key in [
                SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                SampleBatch.DONES, SampleBatch.NEXT_OBS, SampleBatch.VF_PREDS,
                SampleBatch.PREV_ACTIONS, SampleBatch.PREV_REWARDS
            ]:
                assert key in view_req_policy

                if key == SampleBatch.PREV_ACTIONS:
                    assert view_req_policy[key].data_col == SampleBatch.ACTIONS
                    assert view_req_policy[key].shift == -1
                elif key == SampleBatch.PREV_REWARDS:
                    assert view_req_policy[key].data_col == SampleBatch.REWARDS
                    assert view_req_policy[key].shift == -1
                elif key not in [SampleBatch.NEXT_OBS,
                                 SampleBatch.PREV_ACTIONS,
                                 SampleBatch.PREV_REWARDS]:
                    assert view_req_policy[key].data_col is None
                else:
                    assert view_req_policy[key].data_col == SampleBatch.OBS
                    assert view_req_policy[key].shift == 1
            trainer.stop()

    def test_atari_frame_stacking(self):
        """Test whether trajectory view API supports Atari frame-stacking.
        """
        agent_id = 0
        episode_id = 1234
        env_id = 0
        view_reqs_model = {
            "obs": ViewRequirement(shift=[-3, -2, -1, 0])
        }
        #policy_id = "pol0"
        collector = RolloutSampleCollector(
            num_agents=10, num_timesteps=5, time_major=False,
            shift_before=3, shift_after=0)
        # Create a trajectory for some agent.
        obs = np.array([1.0, 2.0, 3.0])
        collector.add_init_obs(episode_id, agent_id, env_id, 0, obs)
        # Get input_dict for forward pass.
        input_dict = collector.get_inference_input_dict(view_reqs_model)
        assert SampleBatch.OBS in input_dict
        assert input_dict[SampleBatch.OBS].shape == (4, 3)
        check(input_dict[SampleBatch.OBS],
            [[0, 0, 0], [0, 0, 0], [0, 0, 0], [1, 2, 3]])
        # Reset inference registry.
        collector.reset_inference_call()

        # Collect some experience and test.
        collector.add_action_reward_next_obs(
            episode_id, agent_id, env_id, False, {
                SampleBatch.NEXT_OBS: np.array([4.0, 5.0, 6.0]),
                SampleBatch.ACTIONS: 2, SampleBatch.REWARDS: 0.1,
                SampleBatch.DONES: False,
            })
        input_dict = collector.get_inference_input_dict(view_reqs_model)
        assert SampleBatch.ACTIONS not in input_dict
        assert input_dict[SampleBatch.OBS].shape == (4, 3)
        check(input_dict[SampleBatch.OBS],
            [[0, 0, 0], [0, 0, 0], [1, 2, 3], [4, 5, 6]])
        collector.reset_inference_call()

        collector.add_action_reward_next_obs(
            episode_id, agent_id, env_id, False, {
                SampleBatch.NEXT_OBS: np.array([7.0, 8.0, 9.0]),
                SampleBatch.ACTIONS: 2, SampleBatch.REWARDS: 0.1,
                SampleBatch.DONES: False,
            })
        input_dict = collector.get_inference_input_dict(view_reqs_model)
        assert SampleBatch.ACTIONS not in input_dict
        assert input_dict[SampleBatch.OBS].shape == (4, 3)
        check(input_dict[SampleBatch.OBS],
            [[0, 0, 0], [1, 2, 3], [4, 5, 6], [7, 8, 9]])
        collector.reset_inference_call()

        collector.add_action_reward_next_obs(
            episode_id, agent_id, env_id, False, {
                SampleBatch.NEXT_OBS: np.array([10.0, 11.0, 12.0]),
                SampleBatch.ACTIONS: 2, SampleBatch.REWARDS: 0.1,
                SampleBatch.DONES: False,
            })
        # Check input_dict again.
        input_dict = collector.get_inference_input_dict(view_reqs_model)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
