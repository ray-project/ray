import torch
import torch.nn as nn

from rllib2.models.torch.v_function import VFunctionOutput, VNet


class MultiAgentCentralizedVF(VNet):
    def __init__(self):
        ...

        self.net = nn.Linear(obs_dim * 2 + act_dim, 1)

    def forward(self, batch: SampleBatch, **kwargs) -> VFunctionOutput:
        agent_state = self.encoder(batch["obs"]).state
        opponent_state = self.encoder(batch["opponent_obs"]).state
        opponent_act = batch["opponent_act"]

        value_input = torch.cat([agent_state, opponent_state, opponent_act], -1)
        value = self.net(value_input)
        return VFunctionOutput(values=[value])


if __name__ == "__main__":

    config = PPOConfig().multi_agent(
        policy_mapping_fn=lambda agent_id: "pol1" if agent_id % 2 == 0 else "pol2",
        policies={
            "pol1": (PPORLModule, pol1_config),
            "pol1": (PPORLModule, pol2_config),
        },
        policies_to_train=["pol1", "pol2"],
        shared_module={
            "vf": {
                "class": MultiAgentCentralizedVF,
                "config": vf_config,
                "shared_between": {"pol1": "vf", "pol2": "vf"},
            }
        },
    )

    config.build()
    config.train()
