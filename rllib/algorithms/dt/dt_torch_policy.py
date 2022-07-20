import gym
import numpy as np

from typing import (
    Dict,
    List,
    Tuple,
    Type,
    Union,
)

from gym.spaces import Discrete, Box

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.algorithms.dt.dt_torch_model import DTTorchModel
from ray.rllib.algorithms.ddpg.noop_model import TorchNoopModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.mingpt import configure_gpt_optimizer
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper, TorchCategorical, TorchDeterministic
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
)

torch, nn = try_import_torch()


class DTTorchPolicy(TorchPolicyV2):
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: TrainerConfigDict,
    ):
        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

    @override(TorchPolicyV2)
    def make_model_and_action_dist(
        self,
    ) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
        # copying ddpg build model to here to be explicit
        model_config = self.config["model"]
        model_config.update(
            dict(
                embed_dim=self.config["embed_dim"],
                max_seq_len=self.config["max_seq_len"],
                max_ep_len=self.config["horizon"],
                num_layers=self.config["num_layers"],
                num_heads=self.config["num_heads"],
                embed_pdrop=self.config["embed_pdrop"],
                resid_pdrop=self.config["resid_pdrop"],
                attn_pdrop=self.config["attn_pdrop"],
            )
        )

        num_outputs = int(np.product(self.observation_space.shape))

        # TODO: why do we even have to go through this get_model_v2 function?
        model = ModelCatalog.get_model_v2(
            obs_space=self.observation_space,
            action_space=self.action_space,
            num_outputs=num_outputs,
            model_config=model_config,
            framework=self.config["framework"],
            model_interface=DTTorchModel,
            default_model=TorchNoopModel,
            name="model",
        )

        if isinstance(self.action_space, Discrete):
            action_dist = TorchCategorical
        elif isinstance(self.action_space, Box):
            action_dist = TorchDeterministic
        else:
            raise NotImplementedError

        return model, action_dist

    @override(TorchPolicyV2)
    def optimizer(
        self,
    ) -> Union[List["torch.optim.Optimizer"], "torch.optim.Optimizer"]:
        optimizer = configure_gpt_optimizer(
            model=self.model,
            learning_rate=self.config["lr"],
            weight_decay=self.config["weight_decay"],
            betas=self.config["betas"]
        )

        return optimizer

    @override(TorchPolicyV2)
    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        obs_batch: TensorType,
        state_batches: TensorType,
        **kwargs,
    ) -> Tuple[TensorType, type, List[TensorType]]:

        model_out, _ = model(obs_batch)
        dist_class = self.dist_class

        return model_out, dist_class, []

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:

        model_out, _ = self.model(train_batch)
        preds = self.model.get_prediction(model_out, train_batch)
        targets = self.model.get_targets(train_batch)

        losses = {
            k: nn.MSELoss(preds[k], targets[k]) for k in preds
        }

        for k, v in losses.items():
            self.log(f"{k}_loss", v)

        loss = sum(v for v in losses.values())
        return loss

    def log(self, key, value):
        # internal log function
        self.model.tower_stats[key] = value

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        stats_dict = {
            k: torch.stack(self.get_tower_stats(k)).mean().item()
            for k in self.model.tower_stats
        }
        return stats_dict


if __name__ == "__main__":

    obs_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    act_space = gym.spaces.Box(np.array((-1, -1)), np.array((1, 1)))
    config = AlgorithmConfig().framework(framework="torch").to_dict()
    print(config["framework"])
    DTTorchPolicy(obs_space, act_space, config=config)
