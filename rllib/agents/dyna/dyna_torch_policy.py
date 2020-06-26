import gym
import logging

import ray
from ray.rllib.agents.dyna.dyna_torch_model import DYNATorchModel
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils import try_import_torch

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def make_model_and_dist(policy, obs_space, action_space, config):
    # Get the output distribution class for predicting rewards and next-obs.
    policy.distr_cls_next_obs, num_outputs = ModelCatalog.get_action_dist(
        obs_space, config, dist_type="deterministic", framework="torch")
    if config["predict_reward"]:
        # TODO: (sven) implement reward prediction.
        _ = ModelCatalog.get_action_dist(
            gym.spaces.Box(float("-inf"), float("inf"), ()),
            config,
            dist_type="")

    # Build one dynamics model if we are a Worker.
    # If we are the main MAML learner, build n (num_workers) dynamics Models
    # for being able to create checkpoints for the current state of training.
    policy.dynamics_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["dynamics_model"],
        framework="torch",
        name="dynamics_model",
        model_interface=DYNATorchModel,
    )

    action_dist, num_outputs = ModelCatalog.get_action_dist(
        action_space, config, dist_type="deterministic", framework="torch")
    # Create the pi-model and register it with the Policy.
    policy.pi = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework="torch",
        name="policy_model",
    )

    return policy.pi, action_dist


def dyna_torch_loss(policy, model, dist_class, train_batch):
    # Split batch into train and validation sets according to
    # `train_set_ratio`.
    predicted_next_state_deltas = \
        policy.dynamics_model.get_next_observation(
            train_batch[SampleBatch.CUR_OBS], train_batch[SampleBatch.ACTIONS])
    labels = train_batch[SampleBatch.NEXT_OBS] - train_batch[SampleBatch.
                                                             CUR_OBS]
    loss = torch.pow(
        torch.sum(
            torch.pow(labels - predicted_next_state_deltas, 2.0), dim=-1), 0.5)
    batch_size = int(loss.shape[0])
    train_set_size = int(batch_size * policy.config["train_set_ratio"])
    train_loss, validation_loss = \
        torch.split(loss, (train_set_size, batch_size - train_set_size), dim=0)
    policy.dynamics_train_loss = torch.mean(train_loss)
    policy.dynamics_validation_loss = torch.mean(validation_loss)
    return policy.dynamics_train_loss


def stats_fn(policy, train_batch):
    return {
        "dynamics_train_loss": policy.dynamics_train_loss,
        "dynamics_validation_loss": policy.dynamics_validation_loss,
    }


def torch_optimizer(policy, config):
    return torch.optim.Adam(
        policy.dynamics_model.parameters(), lr=config["lr"])


DYNATorchPolicy = build_torch_policy(
    name="DYNATorchPolicy",
    loss_fn=dyna_torch_loss,
    get_default_config=lambda: ray.rllib.agents.dyna.dyna.DEFAULT_CONFIG,
    stats_fn=stats_fn,
    optimizer_fn=torch_optimizer,
    make_model_and_action_dist=make_model_and_dist,
)
