from ray.rllib.core.rl_module.torch_rl_module import TorchRLModule

import torch
import torch.nn as nn


class SimplePPOModule(TorchRLModule):
    def __init__(self, config) -> None:
        super().__init__(config)

        self.encoder = nn.Sequential(
            nn.Linear(4, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
        )

        self.pi_head = nn.Linear(64, 2)
        self.v_head = nn.Linear(64, 1)

        self.pi = nn.Sequential(self.encoder, self.pi_head)

        self.v = nn.Sequential(self.encoder, self.v_head)

    def forward_inference(self, batch):
        """During inference, we only return action = mu"""
        ret = self.forward_exploration(batch)
        ret["action"] = ret["action_dist"].mean
        return ret

    def forward_exploration(self, batch):
        mu, scale = self.pi(batch["obs"]).chunk(2, dim=-1)
        action_dist = torch.distributions.Normal(mu, scale.exp())
        return {"action_dist": action_dist}

    def forward_train(self, batch, **kwargs):

        encoded_state = self.encoder(batch["obs"])
        mu, scale = self.pi_head(encoded_state).chunk(2, dim=-1)
        action_dist = torch.distributions.Normal(mu, scale.exp())
        logp = action_dist.log_prob(batch["actions"])
        entropy = action_dist.entropy()
        v = self.v_head(encoded_state)
        return {
            "action_dist": action_dist,
            "logp": logp,
            "entropy": entropy,
            "vf": v,
        }


if __name__ == "__main__":

    model = SimplePPOModule({})
    print(model)

    bsize = 1
    batch = {
        "obs": torch.randn(bsize, 4),
        "actions": torch.randn(bsize, 2),
    }
    breakpoint()

    print(model(batch))

    print(model.forward_inference(batch))

    print(model.forward_exploration(batch))

    print(model.forward_train(batch))
