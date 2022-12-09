from dataclasses import dataclass, field
import gym
import tree # pip install dm-tree
from typing import List, Mapping, Any

from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.specs.specs_dict import ModelSpec, check_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDeterministic,
    TorchDiagGaussian,
)
from ray.rllib.core.rl_module.encoder import (
    FCNet,
    FCConfig, 
    LSTMConfig
)


torch, nn = try_import_torch()


def get_ppo_loss(fwd_in, fwd_out):
    # TODO: we should replace these components later with real ppo components when
    # RLOptimizer and RLModule are integrated together.
    # this is not exactly a ppo loss, just something to show that the
    # forward train works
    adv = fwd_in[SampleBatch.REWARDS] - fwd_out[SampleBatch.VF_PREDS]
    actor_loss = -(fwd_out[SampleBatch.ACTION_LOGP] * adv).mean()
    critic_loss = (adv**2).mean()
    loss = actor_loss + critic_loss

    return loss


# TODO: Most of the neural network, and model specs in this file will eventually be
# retreived from the model catalog. That includes FCNet, Encoder, etc.
def get_shared_encoder_config(env):
    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        encoder_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        pi_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        vf_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
    )


def get_separate_encoder_config(env):
    return PPOModuleConfig(
        observation_space=env.observation_space,
        action_space=env.action_space,
        pi_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
        vf_config=FCConfig(
            hidden_layers=[32],
            activation="ReLU",
        ),
    )



@dataclass
class PPOModuleConfig(RLModuleConfig):
    """Configuration for the PPO module.

    Attributes:
        pi_config: The configuration for the policy network.
        vf_config: The configuration for the value network.
        encoder_config: The configuration for the encoder network.
        free_log_std: For DiagGaussian action distributions, make the second half of
            the model outputs floating bias variables instead of state-dependent. This
            only has an effect is using the default fully connected net.
    """
    pi_config: FCConfig = None
    vf_config: FCConfig = None
    encoder_config: FCConfig = None
    free_log_std: bool = False

class PPOTorchRLModule(TorchRLModule):
    def __init__(self, config: PPOModuleConfig) -> None:

        super().__init__(config)

    def setup(self) -> None:

        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."

        self.encoder = None
        if self.config.encoder_config:
            self.encoder = self.config.encoder_config.build()

        self.pi = FCNet(
            input_dim=self.config.pi_config.input_dim,
            output_dim=self.config.pi_config.output_dim,
            hidden_layers=self.config.pi_config.hidden_layers,
            activation=self.config.pi_config.activation,
        )

        self.vf = FCNet(
            input_dim=self.config.vf_config.input_dim,
            output_dim=self.config.vf_config.output_dim,
            hidden_layers=self.config.vf_config.hidden_layers,
            activation=self.config.vf_config.activation,
        )

        self._is_discrete = isinstance(self.config.action_space, gym.spaces.Discrete)

    def get_initial_state(self) -> NestedDict:
        if isinstance(self.config.encoder_config, LSTMConfig):
            # TODO (Kourosh): How does this work in RLlib today?
            return self.encoder.get_inital_state()
        return {}
    
    @override(RLModule)
    def input_specs_inference(self) -> ModelSpec:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpec:
        return ModelSpec({SampleBatch.ACTION_DIST: TorchDeterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        encoder_out = {"embedding": batch[SampleBatch.OBS]}
        if self.encoder:
            encoder_out = self.encoder(batch)
        action_logits = self.pi(encoder_out["embedding"])

        if self._is_discrete:
            action = torch.argmax(action_logits, dim=-1)
        else:
            action, _ = action_logits.chunk(2, dim=-1)

        action_dist = TorchDeterministic(action)
        output = {SampleBatch.ACTION_DIST: action_dist}
        output["state_out"] = encoder_out.get("state_out", [])
        return output

    @override(RLModule)
    def input_specs_exploration(self):
        return ModelSpec(
            {
                SampleBatch.OBS: (
                    self.encoder.input_specs()
                    if self.encoder
                    else self.pi.input_specs()
                )
            }
        )

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpec:
        specs = {SampleBatch.ACTION_DIST: self.__get_action_dist_type()}
        if self._is_discrete:
            specs[SampleBatch.ACTION_DIST_INPUTS] = {
                "logits": TorchTensorSpec("b, h", h=self.config.action_space.n)
            }
        else:
            specs[SampleBatch.ACTION_DIST_INPUTS] = {
                "loc": TorchTensorSpec("b, h", h=self.config.action_space.shape[0]),
                "scale": TorchTensorSpec("b, h", h=self.config.action_space.shape[0]),
            }

        return ModelSpec(specs)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        encoder_out = {"embedding": batch[SampleBatch.OBS]}
        if self.encoder:
            encoder_out = self.encoder(batch)
        action_logits = self.pi(encoder_out["embedding"])

        output = {}
        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = action_logits.chunk(2, dim=-1)
            scale = log_std.exp()
            action_dist = TorchDiagGaussian(loc, scale)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}
        output[SampleBatch.ACTION_DIST] = action_dist

        # compute the value function
        output[SampleBatch.VF_PREDS] = self.vf(encoder_out["embedding"]).squeeze(-1)
        output["state_out"] = encoder_out.get("state_out", [])
        return output

    @override(RLModule)
    def input_specs_train(self) -> ModelSpec:
        if self._is_discrete:
            action_spec = TorchTensorSpec("b")
        else:
            action_dim = self.config.action_space.shape[0]
            action_spec = TorchTensorSpec("b, h", h=action_dim)

        obs_specs = (
            self.encoder.input_specs() if self.encoder else self.pi.input_specs()
        )
        spec = ModelSpec(
            {
                SampleBatch.OBS: obs_specs,
                SampleBatch.NEXT_OBS: obs_specs,
                SampleBatch.ACTIONS: action_spec,
            }
        )

        return spec

    @override(RLModule)
    def output_specs_train(self) -> ModelSpec:
        spec = ModelSpec(
            {
                SampleBatch.ACTION_DIST: self.__get_action_dist_type(),
                SampleBatch.ACTION_LOGP: TorchTensorSpec("b", dtype=torch.float32),
                SampleBatch.VF_PREDS: TorchTensorSpec("b", dtype=torch.float32),
                "entropy": TorchTensorSpec("b", dtype=torch.float32),
                # "vf_preds_next_obs": TorchTensorSpec("b", dtype=torch.float32),
            }
        )
        return spec

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        encoder_out = {"embedding": batch[SampleBatch.OBS]}
        if self.encoder:
            encoder_out = self.encoder(batch)

        action_logits = self.pi(encoder_out["embedding"])
        vf = self.vf(encoder_out["embedding"])

        if self._is_discrete:
            action_dist = TorchCategorical(logits=action_logits)
        else:
            mu, scale = action_logits.chunk(2, dim=-1)
            action_dist = TorchDiagGaussian(mu, scale.exp())

        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()

        # # TODO (Kourosh): This design pattern is not ideal.
        # encoder_out_next = {"embedding": batch[SampleBatch.NEXT_OBS]}
        # if self.encoder:
        #     # get vf of the next obs
        #     encoder_in = SampleBatch({
        #         SampleBatch.OBS: batch[SampleBatch.NEXT_OBS],
        #     })
        #     if "state_in" in batch:
        #         encoder_in["state_in"] = batch["state_out"]
            
        #     if SampleBatch.SEQ_LENS in batch:
        #         encoder_in[SampleBatch.SEQ_LENS] = batch[SampleBatch.SEQ_LENS]
        #     encoder_out_next = self.encoder(encoder_in)
        
        # vf_next_obs = self.vf(encoder_out_next["embedding"])

        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_LOGP: logp,
            SampleBatch.VF_PREDS: vf.squeeze(-1),
            "entropy": entropy,
            # "vf_preds_next_obs": vf_next_obs.squeeze(-1),
        }

        output["state_out"] = encoder_out.get("state_out", [])
        return output

    def __get_action_dist_type(self):
        return TorchCategorical if self._is_discrete else TorchDiagGaussian

    @classmethod
    def from_model_config_dict(cls, observation_space, action_space, model_config, return_config=False):
        # TODO: use the new catalog to perform this logic and construct the final config

        activation = model_config["fcnet_activation"]
        if activation == "tanh":
            activation = "Tanh"
        elif activation == "relu":
            activation = "ReLU"
        elif activation == "linear":
            activation = "linear"
        else:
            raise ValueError(f"Unsupported activation: {activation}")

        fcnet_hiddens = model_config["fcnet_hiddens"]
        vf_share_layers = model_config["vf_share_layers"]
        free_log_std = model_config["free_log_std"]
        use_lstm = model_config["use_lstm"]

        if vf_share_layers or use_lstm:
            if use_lstm:
                encoder_config = LSTMConfig(
                    hidden_dim=model_config["lstm_cell_size"],
                    batch_first=not model_config["_time_major"],
                    output_dim=model_config["lstm_cell_size"],
                    num_layers=1,
                )
            else:
                encoder_config = FCConfig(
                    hidden_layers=fcnet_hiddens,
                    activation=activation,
                    output_dim=model_config["fcnet_hiddens"][-1],
                )
            pi_config = FCConfig()
            vf_config = FCConfig()
        else:
            pi_config = FCConfig(
                hidden_layers=fcnet_hiddens,
                activation=activation,
            )
            vf_config = FCConfig(
                hidden_layers=fcnet_hiddens,
                activation=activation,
            )
            encoder_config = None

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(
            action_space, (gym.spaces.Discrete, gym.spaces.Box)
        ), ("This simple PPOModule only supports Discrete and Box action space.",)


        # build pi network
        if encoder_config is None:
            pi_config.input_dim = observation_space.shape[0]
        else:
            encoder_config.input_dim = observation_space.shape[0]
            pi_config.input_dim = encoder_config.output_dim

        if isinstance(action_space, gym.spaces.Discrete):
            pi_config.output_dim = action_space.n
        else:
            pi_config.output_dim = action_space.shape[0] * 2
        

        # build vf network
        if encoder_config is None:
            vf_config.input_dim = observation_space.shape[0]
        else:
            vf_config.input_dim = encoder_config.output_dim

        vf_config.output_dim = 1

        config_ = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            max_seq_len=model_config["max_seq_len"],
            encoder_config=encoder_config,
            pi_config=pi_config,
            vf_config=vf_config,
            free_log_std=free_log_std,
        )

        if return_config:
            return config_
            
        module = PPOTorchRLModule(config_)
        return module
