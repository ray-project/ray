from typing import TYPE_CHECKING, Any, Dict

import tree  # pip install dm_tree

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import one_hot
from ray.rllib.utils.typing import ModuleID

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.learner.torch.torch_learner import TorchLearner

torch, nn = try_import_torch()


class IntrinsicCuriosityModel(TorchRLModule, SelfSupervisedLossAPI):
    """An intrinsic curiosity model (ICM) as TorchRLModule for better exploration.

    For more details, see:
    [1] Curiosity-driven Exploration by Self-supervised Prediction
    Pathak, Agrawal, Efros, and Darrell - UC Berkeley - ICML 2017.
    https://arxiv.org/pdf/1705.05363.pdf

    Learns a simplified model of the environment based on three networks:
    1) Embedding observations into latent space ("feature" network).
    2) Predicting the action, given two consecutive embedded observations
    ("inverse" network).
    3) Predicting the next embedded obs, given an obs and action
    ("forward" network).

    The less the agent is able to predict the actually observed next feature
    vector, given obs and action (through the forwards network), the larger the
    "intrinsic reward", which will be added to the extrinsic reward.
    Therefore, if a state transition was unexpected, the agent becomes
    "curious" and will further explore this transition leading to better
    exploration in sparse rewards environments.

    .. testcode::

            import numpy as np
            import gymnasium as gym
            import torch

            from ray.rllib.core import Columns
            from ray.rllib.examples.rl_modules.classes.intrinsic_curiosity_model_rlm import (  # noqa
                IntrinsicCuriosityModel
            )

            B = 10  # batch size
            O = 4  # obs (1D) dim
            A = 2  # num actions
            f = 25  # feature dim

            # Construct the RLModule.
            icm_net = IntrinsicCuriosityModel(
                observation_space=gym.spaces.Box(-1.0, 1.0, (O,), np.float32),
                action_space=gym.spaces.Discrete(A),
            )

            # Create some dummy input.
            obs = torch.from_numpy(
                np.random.random_sample(size=(B, O)).astype(np.float32)
            )
            next_obs = torch.from_numpy(
                np.random.random_sample(size=(B, O)).astype(np.float32)
            )
            actions = torch.from_numpy(
                np.random.random_integers(0, A - 1, size=(B,))
            )
            input_dict = {
                Columns.OBS: obs,
                Columns.NEXT_OBS: next_obs,
                Columns.ACTIONS: actions,
            }

            # Call `forward_train()` to get phi (feature vector from obs), next-phi
            # (feature vector from next obs), and the intrinsic rewards (individual, per
            # batch-item forward loss values).
            print(icm_net.forward_train(input_dict))

            # Print out the number of parameters.
            num_all_params = sum(int(np.prod(p.size())) for p in icm_net.parameters())
            print(f"num params = {num_all_params}")
    """

    @override(TorchRLModule)
    def setup(self):
        # Get the ICM achitecture settings from the `model_config` attribute:
        cfg = self.model_config

        feature_dim = cfg.get("feature_dim", 288)

        # Build the feature model (encoder of observations to feature space).
        layers = []
        dense_layers = cfg.get("feature_net_hiddens", (256, 256))
        # `in_size` is the observation space (assume a simple Box(1D)).
        in_size = self.observation_space.shape[0]
        for out_size in dense_layers:
            layers.append(nn.Linear(in_size, out_size))
            if cfg.get("feature_net_activation") not in [None, "linear"]:
                layers.append(
                    get_activation_fn(cfg["feature_net_activation"], "torch")()
                )
            in_size = out_size
        # Last feature layer of n nodes (feature dimension).
        layers.append(nn.Linear(in_size, feature_dim))
        self._feature_net = nn.Sequential(*layers)

        # Build the inverse model (predicting the action between two observations).
        layers = []
        dense_layers = cfg.get("inverse_net_hiddens", (256,))
        # `in_size` is 2x the feature dim.
        in_size = feature_dim * 2
        for out_size in dense_layers:
            layers.append(nn.Linear(in_size, out_size))
            if cfg.get("inverse_net_activation") not in [None, "linear"]:
                layers.append(
                    get_activation_fn(cfg["inverse_net_activation"], "torch")()
                )
            in_size = out_size
        # Last feature layer of n nodes (action space).
        layers.append(nn.Linear(in_size, self.action_space.n))
        self._inverse_net = nn.Sequential(*layers)

        # Build the forward model (predicting the next observation from current one and
        # action).
        layers = []
        dense_layers = cfg.get("forward_net_hiddens", (256,))
        # `in_size` is the feature dim + action space (one-hot).
        in_size = feature_dim + self.action_space.n
        for out_size in dense_layers:
            layers.append(nn.Linear(in_size, out_size))
            if cfg.get("forward_net_activation") not in [None, "linear"]:
                layers.append(
                    get_activation_fn(cfg["forward_net_activation"], "torch")()
                )
            in_size = out_size
        # Last feature layer of n nodes (feature dimension).
        layers.append(nn.Linear(in_size, feature_dim))
        self._forward_net = nn.Sequential(*layers)

    @override(TorchRLModule)
    def _forward_train(self, batch, **kwargs):
        # Push both observations through feature net to get feature vectors (phis).
        # We cat/batch them here for efficiency reasons (save one forward pass).
        obs = tree.map_structure(
            lambda obs, next_obs: torch.cat([obs, next_obs], dim=0),
            batch[Columns.OBS],
            batch[Columns.NEXT_OBS],
        )
        phis = self._feature_net(obs)
        # Split again to yield 2 individual phi tensors.
        phi, next_phi = torch.chunk(phis, 2)

        # Predict next feature vector (next_phi) with forward model (using obs and
        # actions).
        predicted_next_phi = self._forward_net(
            torch.cat(
                [
                    phi,
                    one_hot(batch[Columns.ACTIONS].long(), self.action_space).float(),
                ],
                dim=-1,
            )
        )

        # Forward loss term: Predicted phi - given phi and action - vs actually observed
        # phi (square-root of L2 norm). Note that this is the intrinsic reward that
        # will be used and the mean of this is the forward net loss.
        forward_l2_norm_sqrt = 0.5 * torch.sum(
            torch.pow(predicted_next_phi - next_phi, 2.0), dim=-1
        )

        output = {
            Columns.INTRINSIC_REWARDS: forward_l2_norm_sqrt,
            # Computed feature vectors (used to compute the losses later).
            "phi": phi,
            "next_phi": next_phi,
        }

        return output

    @override(SelfSupervisedLossAPI)
    def compute_self_supervised_loss(
        self,
        *,
        learner: "TorchLearner",
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, Any],
    ) -> Dict[str, Any]:
        module = learner.module[module_id].unwrapped()

        # Forward net loss.
        forward_loss = torch.mean(fwd_out[Columns.INTRINSIC_REWARDS])

        # Inverse loss term (predicted action that led from phi to phi' vs
        # actual action taken).
        dist_inputs = module._inverse_net(
            torch.cat([fwd_out["phi"], fwd_out["next_phi"]], dim=-1)
        )
        action_dist = module.get_train_action_dist_cls().from_logits(dist_inputs)

        # Neg log(p); p=probability of observed action given the inverse-NN
        # predicted action distribution.
        inverse_loss = -action_dist.logp(batch[Columns.ACTIONS])
        inverse_loss = torch.mean(inverse_loss)

        # Calculate the ICM loss.
        total_loss = (
            config.learner_config_dict["forward_loss_weight"] * forward_loss
            + (1.0 - config.learner_config_dict["forward_loss_weight"]) * inverse_loss
        )

        learner.metrics.log_dict(
            {
                "mean_intrinsic_rewards": forward_loss,
                "forward_loss": forward_loss,
                "inverse_loss": inverse_loss,
            },
            key=module_id,
            window=1,
        )

        return total_loss

    # Inference and exploration not supported (this is a world-model that should only
    # be used for training).
    @override(TorchRLModule)
    def _forward(self, batch, **kwargs):
        raise NotImplementedError(
            "`IntrinsicCuriosityModel` should only be used for training! "
            "Only calls to `forward_train()` supported."
        )
