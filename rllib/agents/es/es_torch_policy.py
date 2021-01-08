# Code in this file is adapted from:
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np
import tree

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space, \
    unbatch
from ray.rllib.utils.torch_ops import convert_to_torch_tensor

torch, _ = try_import_torch()


def before_init(policy, observation_space, action_space, config):
    policy.action_noise_std = config["action_noise_std"]
    policy.action_space_struct = get_base_struct_from_space(action_space)
    policy.preprocessor = ModelCatalog.get_preprocessor_for_space(
        observation_space)
    policy.observation_filter = get_filter(config["observation_filter"],
                                           policy.preprocessor.shape)
    policy.single_threaded = config.get("single_threaded", False)

    def _set_flat_weights(policy, theta):
        pos = 0
        theta_dict = policy.model.state_dict()
        new_theta_dict = {}

        for k in sorted(theta_dict.keys()):
            shape = policy.param_shapes[k]
            num_params = int(np.prod(shape))
            new_theta_dict[k] = torch.from_numpy(
                np.reshape(theta[pos:pos + num_params], shape))
            pos += num_params
        policy.model.load_state_dict(new_theta_dict)

    def _get_flat_weights(policy):
        # Get the parameter tensors.
        theta_dict = policy.model.state_dict()
        # Flatten it into a single np.ndarray.
        theta_list = []
        for k in sorted(theta_dict.keys()):
            theta_list.append(torch.reshape(theta_dict[k], (-1, )))
        cat = torch.cat(theta_list, dim=0)
        return cat.cpu().numpy()

    type(policy).set_flat_weights = _set_flat_weights
    type(policy).get_flat_weights = _get_flat_weights

    def _compute_actions(policy,
                         obs_batch,
                         add_noise=False,
                         update=True,
                         **kwargs):
        # Batch is given as list -> Try converting to numpy first.
        if isinstance(obs_batch, list) and len(obs_batch) == 1:
            obs_batch = obs_batch[0]
        observation = policy.preprocessor.transform(obs_batch)
        observation = policy.observation_filter(
            observation[None], update=update)

        observation = convert_to_torch_tensor(observation, policy.device)
        dist_inputs, _ = policy.model({
            SampleBatch.CUR_OBS: observation
        }, [], None)
        dist = policy.dist_class(dist_inputs, policy.model)
        action = dist.sample()

        def _add_noise(single_action, single_action_space):
            single_action = single_action.detach().cpu().numpy()
            if add_noise and isinstance(single_action_space, gym.spaces.Box):
                single_action += np.random.randn(*single_action.shape) * \
                                 policy.action_noise_std
            return single_action

        action = tree.map_structure(_add_noise, action,
                                    policy.action_space_struct)
        action = unbatch(action)
        return action, [], {}

    def _compute_single_action(policy,
                               observation,
                               add_noise=False,
                               update=True,
                               **kwargs):
        action, state_outs, extra_fetches = policy.compute_actions(
            [observation], add_noise=add_noise, update=update, **kwargs)
        return action[0], state_outs, extra_fetches

    type(policy).compute_actions = _compute_actions
    type(policy).compute_single_action = _compute_single_action


def after_init(policy, observation_space, action_space, config):
    state_dict = policy.model.state_dict()
    policy.param_shapes = {
        k: tuple(state_dict[k].size())
        for k in sorted(state_dict.keys())
    }
    policy.num_params = sum(np.prod(s) for s in policy.param_shapes.values())


def make_model_and_action_dist(policy, observation_space, action_space,
                               config):
    # Policy network.
    dist_class, dist_dim = ModelCatalog.get_action_dist(
        action_space,
        config["model"],  # model_options
        dist_type="deterministic",
        framework="torch")
    model = ModelCatalog.get_model_v2(
        policy.preprocessor.observation_space,
        action_space,
        num_outputs=dist_dim,
        model_config=config["model"],
        framework="torch")
    # Make all model params not require any gradients.
    for p in model.parameters():
        p.requires_grad = False
    return model, dist_class


ESTorchPolicy = build_policy_class(
    name="ESTorchPolicy",
    framework="torch",
    loss_fn=None,
    get_default_config=lambda: ray.rllib.agents.es.es.DEFAULT_CONFIG,
    before_init=before_init,
    after_init=after_init,
    make_model_and_action_dist=make_model_and_action_dist)
