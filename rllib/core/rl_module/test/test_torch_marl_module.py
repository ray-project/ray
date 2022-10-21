from distutils.command.config import config
import unittest
import gym
from ray.rllib.core.examples.simple_ppo_rl_module import (
    SimplePPOModule,
    PPOModuleConfig,
    FCConfig,
    get_shared_encoder_config,
)
from ray.rllib.core.rl_module.torch_rl_module import TorchMARLModule
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override

import torch
from ray.rllib.utils.test_utils import check
import tree
import numpy as np


def to_numpy(tensor):
    return tensor.detach().cpu().numpy()


def to_tensor(array, device=None):
    if device:
        return torch.from_numpy(array).float().to(device)
    return torch.from_numpy(array).float()

def key_int_to_str(d):
    if not isinstance(d, dict):
        return d
    return {str(k): v for k, v in d.items()}

def get_policy_data_from_agent_data(agent_data, policy_map_fn):
    policy_data = {}
    for agent_id, agent_data in agent_data.items():
        policy_id = policy_map_fn(agent_id)
        policy_data.setdefault(policy_id, {})
        policy_data[policy_id].setdefault("agent_id", [])
        for k, v in agent_data.items():
            policy_data[policy_id].setdefault(k, [])
            policy_data[policy_id][k].append(v)
            policy_data[policy_id]["agent_id"].append(agent_id)

        ref_len = None
        for k, v in policy_data[policy_id].items():
            if ref_len is None:
                ref_len = len(v)
            else:
                if len(v) != ref_len:
                    raise ValueError(f"policy data length mismatch at key {k}")
    
    for policy_id in policy_data:
        policy_data[policy_id] = {k: np.stack(v) if k != "agent_id" else v for k, v in policy_data[policy_id].items()}
        
    return policy_data


class TestMARLModule(unittest.TestCase):

    def test_as_multi_agent(self):
        
        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        # make sure that the action/obs space is as expected  (Box or Discrete)
        assert isinstance(env.action_space, (gym.spaces.Box, gym.spaces.Discrete))
        assert isinstance(env.observation_space, (gym.spaces.Box, gym.spaces.Discrete))

        config = get_shared_encoder_config(env)
        module = SimplePPOModule(config)
        marl_module = module.as_multi_agent()
        
        self.assertNotIsInstance(marl_module, SimplePPOModule)
        self.assertIsInstance(marl_module, TorchMARLModule)
        
        self.assertEqual(set([DEFAULT_POLICY_ID]), set(marl_module.keys()))

        # check as_multi_agent() for the second time
        marl_module2 = marl_module.as_multi_agent()
        self.assertEqual(id(marl_module), id(marl_module2))

    def test_get_set_state(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        config = get_shared_encoder_config(env)
        module = SimplePPOModule(config).as_multi_agent()

        state = module.get_state()
        self.assertIsInstance(state, dict)
        self.assertEqual(set(state.keys()), set(module.keys()))
        self.assertEqual(set(state[DEFAULT_POLICY_ID].keys()), set(module[DEFAULT_POLICY_ID].get_state().keys()))

        module2 = SimplePPOModule(config).as_multi_agent()
        state2 = module2.get_state()
        self.assertRaises(AssertionError, lambda: check(state, state2))

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_add_remove_modules(self):
        # TODO (Avnish): Modify this test to make sure that the distributed 
        # functionality won't break the add / remove.

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        config = get_shared_encoder_config(env)
        module = SimplePPOModule(config).as_multi_agent()

        module.add_module("test", SimplePPOModule(config))
        self.assertEqual(set(module.keys()), set([DEFAULT_POLICY_ID, "test"]))
        module.remove_module("test")
        self.assertEqual(set(module.keys()), set([DEFAULT_POLICY_ID]))

        # test if add works with a conflicting name
        self.assertRaises(ValueError, lambda: module.add_module(DEFAULT_POLICY_ID, SimplePPOModule(config)))

        module.add_module(DEFAULT_POLICY_ID, SimplePPOModule(config), override=True)

    def test_rollouts(self):
        for fwd_fn in ["forward_exploration", "forward_inference"]:
            env_class = make_multi_agent("CartPole-v0")
            env = env_class({"num_agents": 2})
            config = get_shared_encoder_config(env)
            module = SimplePPOModule(config).as_multi_agent()

            policy_map = lambda agent_id: DEFAULT_POLICY_ID

            obs = env.reset()

            tstep = 0
            while tstep < 10:
                # go from agent_id to module_id, compute the actions in batches 
                # and come back to the original per agent format. 
                agent_obs = tree.map_structure(lambda x: {"obs": x}, obs)
                fwd_in = get_policy_data_from_agent_data(agent_obs, policy_map)
                fwd_in = tree.map_structure(
                    lambda x: to_tensor(x) if isinstance(x, np.ndarray) else x, 
                    fwd_in
                )
                if fwd_fn == "forward_exploration":
                    fwd_out = module.forward_exploration(fwd_in)
                    
                    action = to_numpy(
                        fwd_out["action_dist"].sample().squeeze(0)
                    )

                # elif fwd_fn == "forward_inference":
                #     # check if I sample twice, I get the same action
                #     fwd_out = module.forward_inference(
                #         {"obs": to_tensor(obs)[None]}
                #     )
                #     action = to_numpy(
                #         fwd_out["action_dist"].sample().squeeze(0)
                #     )
                #     action2 = to_numpy(
                #         fwd_out["action_dist"].sample().squeeze(0)
                #     )
                #     check(action, action2)
            # obs = tree.map_structure(lambda x: to_tensor(x), obs)
            # output = module.inference(obs)
            # self.assertIsInstance(output, dict)
            # self.assertEqual(set(output.keys()), set(module.keys()))
            # self.assertIsInstance(output[DEFAULT_POLICY_ID], dict)
            # self.assertEqual(set(output[DEFAULT_POLICY_ID].keys()), set(["logits", "value"]))

            # # check if the output is deterministic
            # output2 = module.inference(obs)
            # check(output, output2)





if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
