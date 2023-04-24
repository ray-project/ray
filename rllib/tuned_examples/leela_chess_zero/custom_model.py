from ray.rllib.algorithms.alpha_zero.models.custom_torch_models import (
    ActorCriticModel,
    Flatten,
)
import torch.nn as nn


class DenseModel(ActorCriticModel):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        ActorCriticModel.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        in_shape = 84
        self.shared_layers = nn.Sequential(
            Flatten(),
            nn.Linear(in_features=in_shape, out_features=2048),
            nn.ReLU(),
            nn.Linear(in_features=2048, out_features=2048),
            nn.ReLU(),
        )
        self.actor_layers = nn.Sequential(
            nn.Linear(in_features=2048, out_features=action_space.n)
        )
        self.critic_layers = nn.Sequential(
            nn.Linear(in_features=2048, out_features=1), nn.Tanh()
        )
        self._value_out = None

    def forward(self, input_dict, state, seq_lens):
        try:
            x = input_dict["obs"]
        except KeyError:
            x = input_dict["observation"]
        x = self.shared_layers(x)
        # actor outputs
        logits = self.actor_layers(x)

        # compute value
        self._value_out = self.critic_layers(x)
        return logits, None


class PolicyMappingFn:
    """Example for a callable class specifyable in yaml files as `policy_mapping_fn`.

    See for example:
    ray/rllib/tuned_examples/alpha_star/multi-agent-cartpole-alpha-star.yaml
    """

    def __call__(self, agent_id, episode, worker, **kwargs):
        return "p_" + (
            str("0" if int(agent_id.split("_")[-1]) % 2 == 0 else "1")
            if (episode.episode_id) % 2 == 0
            else str("1" if int(agent_id.split("_")[-1]) % 2 == 0 else "0")
        )
