import unittest

import ray
import ray.rllib.agents.ppo as ppo
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import framework_iterator


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
            view_req = policy.get_view_requirements()
            assert len(view_req) == 6
            for key in [
                SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                SampleBatch.DONES, SampleBatch.NEXT_OBS, SampleBatch.VF_PREDS
            ]:
                assert key in view_req
                # All of the view cols are important for postprocessing.
                assert view_req[key].postprocessing
                # None of the view cols has a special underlying data_col,
                # except next-obs.
                if key != SampleBatch.NEXT_OBS:
                    assert view_req[key].data_col is None
                    assert view_req[key].training
                else:
                    assert view_req[key].data_col == SampleBatch.OBS
                    assert view_req[key].shift == 1
                if key != SampleBatch.OBS:
                    assert view_req[key].sampling is False

    def test_lstm_prev_actions_and_rewards(self):
        config = ppo.DEFAULT_CONFIG.copy()
        # Activate LSTM + prev-action + rewards.
        config["model"]["use_lstm"] = True
        config["model"]["lstm_use_prev_action_reward"] = True

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = ppo.PPOTrainer(config, env="CartPole-v0")
            policy = trainer.get_policy()
            view_req = policy.get_view_requirements()
            assert len(view_req) == 8
            for key in [
                SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.REWARDS,
                SampleBatch.DONES, SampleBatch.NEXT_OBS, SampleBatch.VF_PREDS,
                SampleBatch.PREV_ACTIONS, SampleBatch.PREV_REWARDS
            ]:
                assert key in view_req
                # All of the view cols are important for postprocessing.
                assert view_req[key].postprocessing

                if key == SampleBatch.PREV_ACTIONS:
                    assert view_req[key].data_col == SampleBatch.ACTIONS
                    assert view_req[key].shift == -1
                elif key == SampleBatch.PREV_REWARDS:
                    assert view_req[key].data_col == SampleBatch.REWARDS
                    assert view_req[key].shift == -1
                elif key not in [SampleBatch.NEXT_OBS,
                                 SampleBatch.PREV_ACTIONS,
                                 SampleBatch.PREV_REWARDS]:
                    assert view_req[key].data_col is None
                    assert view_req[key].training
                else:
                    assert view_req[key].data_col == SampleBatch.OBS
                    assert view_req[key].shift == 1
                if key not in [SampleBatch.OBS, SampleBatch.PREV_ACTIONS,
                               SampleBatch.PREV_REWARDS]:
                    assert view_req[key].sampling is False


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
