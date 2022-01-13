import numpy as np
import pprint
import pyspiel
import unittest

import ray
import ray.rllib.agents.alpha_star as alpha_star
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check_compute_single_action, \
    check_train_results, framework_iterator
from ray.tune import register_env

# Connect-4 OpenSpiel env.
register_env("connect_four",
             lambda _: OpenSpielEnv(pyspiel.load_game("connect_four")))


class TestAlphaStar(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=10, local_mode=True)  #TODO

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_alpha_star_compilation(self):
        """Test whether a AlphaStarTrainer can be built with all frameworks."""

        # Depending on the episode ID, assign agent 0 to p0 or p1 and
        # vice-versa.
        #def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        #    head_pol = episode.episode_id % 4
        #    # 50% of the time, agent0's policy depends directly on episode ID.
        #    # The opponend gets drawn randomly from the rest.
        #    # 50% of the time, agent1's policy depends on episode ID, etc..
        #    if (episode.episode_id % 2 and agent_id == 0) or \
        #            (not episode.episode_id % 2 and agent_id == 1):
        #        return f"p{head_pol}"
        #    else:
        #        return f"p{np.random.choice(list({0, 1, 2, 3} - {head_pol}))}"

        config = alpha_star.DEFAULT_CONFIG.copy()

        config["gamma"] = 1.0
        #config["batch_mode"] = "complete_episodes"
        #config["rollout_fragment_length"] = 500
        #config["train_batch_size"] = 1000
        config["num_workers"] = 4
        #config["lr"] = 0.0002
        config["num_envs_per_worker"] = 5
        #config["observation_filter"] = "MeanStdFilter"
        config["vtrace_drop_last_ts"] = False
        config["model"] = {
            "fcnet_hiddens": [256, 256, 256],
            #"fcnet_activation": "linear",
            #"vf_share_layers": True
        }
        config["vf_loss_coeff"] = 0.01
        config["entropy_coeff"] = 0.004

        config["win_rate_threshold_for_new_snapshot"] = 0.5#TEST: 0.8 or 0.9 are good values

        #config["num_sgd_iter"] = 5#TEST
        config["grad_clip"] = 10.0

        config["replay_buffer_capacity"] = 10
        config["replay_buffer_replay_ratio"] = 0.0

        # Multi-agent cartpole with 2 agents (IDs: 0, 1)
        # mapping to 4 different policies ("p0" to "p3").
        config["env"] = "connect_four"
        # Two-player game.
        #config["env_config"] = {"num_agents": 4}
        # Two GPUs -> 2 policies per GPU.
        config["num_gpus"] = 1
        config["_fake_gpus"] = True
        # Let the algo know about our 4 policies.
        #config["multiagent"] = {
        #    "policies": {"p0", "p1", "p2", "p3"},
        #    # Agent IDs are 0, 1 (ints) -> Map to "p0" to "p3" randomly.
        #    "policy_mapping_fn": policy_mapping_fn,
        #}

        num_iterations = 1000

        for _ in framework_iterator(config, frameworks=("tf2", "torch")):
            _config = config.copy()
            trainer = alpha_star.AlphaStarTrainer(config=_config)
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                #pprint.pprint(results)
            check_compute_single_action(trainer)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
