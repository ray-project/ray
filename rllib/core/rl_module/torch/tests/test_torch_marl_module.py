import unittest
import gym
import tree
import torch
import numpy as np

from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
    get_separate_encoder_config,
    get_shared_encoder_config,
    get_ppo_loss,
)
from ray.rllib.core.rl_module.torch import TorchMultiAgentRLModule
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import check
from ray.rllib.policy.sample_batch import SampleBatch


def to_numpy(tensor):
    return tensor.detach().cpu().numpy()


def to_tensor(array, device=None):
    if device:
        return torch.from_numpy(array).float().to(device)
    return torch.from_numpy(array).float()


def get_policy_data_from_agent_data(agent_data, policy_map_fn):
    """Utility function to get policy data from agent data and policy map function.

    It also keeps track of agent_id for each row so that we can retreive the agent
    level information after the forward pass.

    Returns:
        dict of module_id to module data
    """
    policy_data = {}
    for agent_id, data in agent_data.items():
        policy_id = policy_map_fn(agent_id)
        policy_data.setdefault(policy_id, {})
        policy_data[policy_id].setdefault(SampleBatch.AGENT_INDEX, [])

        if data[SampleBatch.OBS].ndim == 1:
            policy_data[policy_id][SampleBatch.AGENT_INDEX].append(agent_id)
        else:
            policy_data[policy_id][SampleBatch.AGENT_INDEX] += [agent_id] * len(
                data[SampleBatch.OBS]
            )

        for k, v in data.items():
            policy_data[policy_id].setdefault(k, [])
            if v.ndim == 1:
                v = v[None]
            policy_data[policy_id][k].append(v)

    for policy_id in policy_data:
        policy_data[policy_id] = {
            k: np.concatenate(v) if k != SampleBatch.AGENT_INDEX else v
            for k, v in policy_data[policy_id].items()
        }

    return policy_data


def get_action_from_ma_fwd_pass(agent_obs, fwd_out, fwd_in, policy_map_fn):
    """Utility function to get action from policy level data to agent data format.

    Using agen_ids in agent_obs and the correspondance of fwd_out, and fwd_in, it maps
    the policy level output to agent level output.

    Returns:
        dict of agent_id to action
    """
    policy_to_agent_to_action = {}
    for policy_id, policy_out in fwd_out.items():
        policy_to_agent_to_action[policy_id] = {}
        for agent_id, agent_out in zip(
            fwd_in[policy_id][SampleBatch.AGENT_INDEX],
            policy_out[SampleBatch.ACTION_DIST].sample(),
        ):
            policy_to_agent_to_action[policy_id][agent_id] = to_numpy(agent_out)

    action = {
        aid: policy_to_agent_to_action[policy_map_fn(aid)][aid] for aid in agent_obs
    }

    return action


class TestMARLModule(unittest.TestCase):
    def test_from_config(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        config1 = get_shared_encoder_config(env)
        config2 = get_separate_encoder_config(env)

        multi_agent_dict = {
            "module1": (PPOTorchRLModule, config1),
            "module2": (PPOTorchRLModule, config2),
        }
        marl_module = TorchMultiAgentRLModule.from_multi_agent_config(multi_agent_dict)

        self.assertEqual(set(marl_module.keys()), {"module1", "module2"})
        self.assertIsInstance(marl_module["module1"], PPOTorchRLModule)
        self.assertIsInstance(marl_module["module2"], PPOTorchRLModule)

    def test_as_multi_agent(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        # make sure that the action/obs space is as expected  (Box or Discrete)
        assert isinstance(env.action_space, (gym.spaces.Box, gym.spaces.Discrete))
        assert isinstance(env.observation_space, (gym.spaces.Box, gym.spaces.Discrete))

        config = get_shared_encoder_config(env)
        module = PPOTorchRLModule(config)
        marl_module = module.as_multi_agent()

        self.assertNotIsInstance(marl_module, PPOTorchRLModule)
        self.assertIsInstance(marl_module, TorchMultiAgentRLModule)

        self.assertEqual({DEFAULT_POLICY_ID}, set(marl_module.keys()))

        # check as_multi_agent() for the second time
        marl_module2 = marl_module.as_multi_agent()
        self.assertEqual(id(marl_module), id(marl_module2))

    def test_get_set_state(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        config = get_shared_encoder_config(env)
        module = PPOTorchRLModule(config).as_multi_agent()

        state = module.get_state()
        self.assertIsInstance(state, dict)
        self.assertEqual(set(state.keys()), set(module.keys()))
        self.assertEqual(
            set(state[DEFAULT_POLICY_ID].keys()),
            set(module[DEFAULT_POLICY_ID].get_state().keys()),
        )

        module2 = PPOTorchRLModule(config).as_multi_agent()
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
        module = PPOTorchRLModule(config).as_multi_agent()

        module.add_module("test", PPOTorchRLModule(config))
        self.assertEqual(set(module.keys()), {DEFAULT_POLICY_ID, "test"})
        module.remove_module("test")
        self.assertEqual(set(module.keys()), {DEFAULT_POLICY_ID})

        # test if add works with a conflicting name
        self.assertRaises(
            ValueError,
            lambda: module.add_module(DEFAULT_POLICY_ID, PPOTorchRLModule(config)),
        )

        module.add_module(DEFAULT_POLICY_ID, PPOTorchRLModule(config), override=True)

    def test_rollouts(self):
        for env_name in ["CartPole-v0", "Pendulum-v1"]:
            for fwd_fn in ["forward_exploration", "forward_inference"]:
                env_class = make_multi_agent(env_name)
                env = env_class({"num_agents": 2})
                config = get_shared_encoder_config(env)
                module = PPOTorchRLModule(config).as_multi_agent()

                def policy_map(agent_id):
                    return DEFAULT_POLICY_ID

                obs = env.reset()

                tstep = 0
                while tstep < 10:
                    # go from agent_id to module_id, compute the actions in batches
                    # and come back to the original per agent format.
                    agent_obs = tree.map_structure(lambda x: {SampleBatch.OBS: x}, obs)
                    fwd_in = get_policy_data_from_agent_data(agent_obs, policy_map)
                    fwd_in = tree.map_structure(
                        lambda x: to_tensor(x) if isinstance(x, np.ndarray) else x,
                        fwd_in,
                    )

                    if fwd_fn == "forward_exploration":
                        fwd_out = module.forward_exploration(fwd_in)

                        action = get_action_from_ma_fwd_pass(
                            agent_obs, fwd_out, fwd_in, policy_map
                        )

                    elif fwd_fn == "forward_inference":
                        # check if I sample twice, I get the same action
                        fwd_out = module.forward_inference(fwd_in)

                        action = get_action_from_ma_fwd_pass(
                            agent_obs, fwd_out, fwd_in, policy_map
                        )
                        action2 = get_action_from_ma_fwd_pass(
                            agent_obs, fwd_out, fwd_in, policy_map
                        )

                        check(action, action2)

                    obs, reward, done, info = env.step(action)
                    print(
                        f"obs: {obs}, action: {action}, reward: {reward}, "
                        "done: {done}, info: {info}"
                    )
                    tstep += 1

    def test_forward_train(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        config = get_shared_encoder_config(env)
        module = PPOTorchRLModule(config).as_multi_agent()

        def policy_map(agent_id):
            return DEFAULT_POLICY_ID

        obs = env.reset()

        batch = []
        tstep = 0
        while tstep < 10:
            # go from agent_id to module_id, compute the actions in batches
            # and come back to the original per agent format.
            agent_obs = tree.map_structure(lambda x: {SampleBatch.OBS: x}, obs)
            fwd_in = get_policy_data_from_agent_data(agent_obs, policy_map)
            fwd_in = tree.map_structure(
                lambda x: to_tensor(x) if isinstance(x, np.ndarray) else x, fwd_in
            )
            fwd_out = module.forward_exploration(fwd_in)
            action = get_action_from_ma_fwd_pass(agent_obs, fwd_out, fwd_in, policy_map)
            next_obs, reward, done, info = env.step(action)
            tstep += 1

            # construct the data from this iteration
            iteration_data = {}
            for aid in agent_obs.keys():
                iteration_data[aid] = {
                    SampleBatch.OBS: obs[aid],
                    SampleBatch.ACTIONS: action[aid][None]
                    if action[aid].ndim == 0
                    else action[aid],
                    SampleBatch.REWARDS: np.array(reward[aid])[None],
                    SampleBatch.DONES: np.array(done[aid])[None],
                    SampleBatch.NEXT_OBS: next_obs[aid],
                }
            batch.append(iteration_data)

            obs = next_obs

        # convert a list of similarly structured nested dicts that end with np.array
        # leaves to a nested dict of the same structure that ends with stacked np.arrays
        batch = tree.map_structure(lambda *x: np.stack(x, axis=0), *batch)
        fwd_in = get_policy_data_from_agent_data(batch, policy_map)
        fwd_in = tree.map_structure(
            lambda x: to_tensor(x).squeeze(-1) if isinstance(x, np.ndarray) else x,
            fwd_in,
        )
        fwd_out = module.forward_train(fwd_in)

        # compute loss per each module and then sum them up to get the total loss
        loss = {}
        for module_id in module.keys():
            loss[module_id] = get_ppo_loss(fwd_in[module_id], fwd_out[module_id])
        loss_total = sum(loss.values())
        loss_total.backward()

        # check that all neural net parameters have gradients
        for param in module.parameters():
            self.assertIsNotNone(param.grad)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
